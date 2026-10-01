// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::persistence::OpLogPersistence;
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray};
use provider::factory::sql_derived::{SqlDerivedConfig, SqlDerivedLocality};
use provider::factory::temporal_reduce::{
    AggregationConfig, AggregationType, TemporalReduceConfig, TemporalReduceSeriesConfig,
};
use provider::factory::timeseries_join::{TimeseriesInput, TimeseriesJoinConfig};
use provider::factory::timeseries_pivot::TimeseriesPivotConfig;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tinyfs::arrow::ParquetExt;
use tokio::io::AsyncWriteExt;

use super::test_dir;

fn source_batch(timestamp: i64, value: f64) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "timestamp",
            Arc::new(TimestampMicrosecondArray::from(vec![timestamp])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Float64Array::from(vec![value])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn source_history(values: &[f64]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "timestamp",
            Arc::new(TimestampMicrosecondArray::from(
                (1..=values.len() as i64)
                    .map(|hour| hour * 3_600_000_000)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Float64Array::from(values.to_vec())) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn source_samples(samples: &[(i64, f64)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "timestamp",
            Arc::new(TimestampMicrosecondArray::from(
                samples
                    .iter()
                    .map(|(timestamp, _)| *timestamp)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Float64Array::from(
                samples.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn yaml<T: serde::Serialize>(config: &T) -> Vec<u8> {
    serde_yaml::to_string(config).unwrap().into_bytes()
}

fn join_config() -> TimeseriesJoinConfig {
    TimeseriesJoinConfig {
        time_column: "timestamp".to_owned(),
        inputs: vec![
            TimeseriesInput {
                pattern: provider::Url::parse("series:///sources/a.series").unwrap(),
                range: None,
                scope: Some("A".to_owned()),
                transforms: None,
            },
            TimeseriesInput {
                pattern: provider::Url::parse("series:///sources/b.series").unwrap(),
                range: None,
                scope: Some("B".to_owned()),
                transforms: None,
            },
        ],
    }
}

async fn append_source(root: &tinyfs::WD, path: &str, timestamp: i64, value: f64) {
    let batch = source_batch(timestamp, value);
    let mut bytes = Vec::new();
    {
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(Cursor::new(&mut bytes), batch.schema(), None)
                .unwrap();
        writer.write(&batch).unwrap();
        _ = writer.close().unwrap();
    }
    let mut writer = root
        .async_writer_path_with_type(path, tinyfs::EntryType::TablePhysicalSeries)
        .await
        .unwrap();
    writer.write_all(&bytes).await.unwrap();
    writer.set_temporal_metadata(timestamp, timestamp, "timestamp".to_owned());
    writer.shutdown().await.unwrap();
}

fn cache_snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, dir: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        if !dir.exists() {
            return;
        }
        let mut entries = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect::<Vec<_>>();
        entries.sort_by_key(std::fs::DirEntry::path);
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                let relative = path.strip_prefix(root).unwrap().to_path_buf();
                _ = files.insert(relative, std::fs::read(path).unwrap());
            }
        }
    }

    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

fn segment_manifests(snapshot: &BTreeMap<PathBuf, Vec<u8>>) -> BTreeMap<PathBuf, Vec<u8>> {
    snapshot
        .iter()
        .filter(|(path, _)| path.ends_with("manifest.json"))
        .map(|(path, bytes)| (path.clone(), bytes.clone()))
        .collect()
}

async fn query_reduced(
    persistence: &mut OpLogPersistence,
) -> (usize, usize, tinyfs::PlanVisibilityMetricsSnapshot) {
    let tx = persistence.begin_test().await.unwrap();
    let root = tx.root().await.unwrap();
    let node = root
        .get_node_path(Path::new("/reduced/metric/res=1h.series"))
        .await
        .unwrap();
    let file = node.as_file().await.unwrap();
    let handle = file.handle.get_file().await;
    let guard = handle.lock().await;
    let context = tx.state().unwrap().as_provider_context();
    let table = guard
        .as_queryable()
        .expect("temporal-reduce output is queryable")
        .as_table_provider(node.id(), &context)
        .await
        .unwrap();
    drop(guard);
    let batches = context
        .datafusion_session
        .read_table(table)
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows = batches.iter().map(RecordBatch::num_rows).sum();
    let columns = batches.first().map_or(0, RecordBatch::num_columns);
    let metrics = context.plan_visibility_metrics();
    tx.commit_test().await.unwrap();
    (rows, columns, metrics)
}

async fn query_direct_reduced(
    persistence: &mut OpLogPersistence,
) -> (Vec<(f64, i64)>, tinyfs::PlanVisibilityMetricsSnapshot) {
    let tx = persistence.begin_test().await.unwrap();
    let root = tx.root().await.unwrap();
    let node = root.get_node_path(Path::new("/usage/daily")).await.unwrap();
    let file = node.as_file().await.unwrap();
    let handle = file.handle.get_file().await;
    let guard = handle.lock().await;
    let context = tx.state().unwrap().as_provider_context();
    let table = guard
        .as_queryable()
        .expect("direct temporal-reduce output is queryable")
        .as_table_provider(node.id(), &context)
        .await
        .unwrap();
    assert_eq!(
        table
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["timestamp", "gallons", "pump_minutes"]
    );
    drop(guard);
    let batches = context
        .datafusion_session
        .read_table(table)
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let gallons = batch
            .column_by_name("gallons")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let pump_minutes = batch
            .column_by_name("pump_minutes")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| (gallons.value(row), pump_minutes.value(row))));
    }
    let metrics = context.plan_visibility_metrics();
    tx.commit_test().await.unwrap();
    (rows, metrics)
}

#[tokio::test]
async fn direct_reduction_reuses_and_repairs_timestamp_local_dynamic_source() {
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await.unwrap();

    {
        let tx = persistence.begin_test().await.unwrap();
        let root = tx.root().await.unwrap();
        _ = root.create_dir_path("/sources").await.unwrap();
        _ = root.create_dir_path("/usage").await.unwrap();
        _ = root
            .create_series_from_batch(
                "/sources/a.series",
                &source_samples(&[
                    (3_600_000_000, 10.0),
                    (25 * 3_600_000_000, 11.0),
                    (49 * 3_600_000_000, 12.0),
                ]),
                Some("timestamp"),
            )
            .await
            .unwrap();

        let rate = SqlDerivedConfig {
            patterns: HashMap::from([(
                "state".to_owned(),
                provider::Url::parse("series:///sources/a.series").unwrap(),
            )]),
            query: Some(
                "SELECT timestamp, value AS usage_gpm, \
                 CAST(1 AS BIGINT) AS pump_minutes FROM state"
                    .to_owned(),
            ),
            locality: SqlDerivedLocality::TimestampLocal {
                event_time: "timestamp".to_owned(),
            },
            transforms: None,
            pattern_transforms: None,
            scope_prefixes: None,
            provider_wrapper: None,
        };
        _ = root
            .create_dynamic_path(
                "/usage/rate",
                tinyfs::EntryType::FileDynamic,
                "sql-derived-series",
                yaml(&rate),
            )
            .await
            .unwrap();

        let daily = TemporalReduceSeriesConfig {
            in_pattern: provider::Url::parse("series:///usage/rate").unwrap(),
            time_column: "timestamp".to_owned(),
            resolution: "1d".to_owned(),
            aggregations: vec![AggregationConfig {
                agg_type: AggregationType::Sum,
                columns: Some(vec!["usage_gpm".to_owned(), "pump_minutes".to_owned()]),
            }],
            output_aliases: Some(HashMap::from([
                ("usage_gpm.sum".to_owned(), "gallons".to_owned()),
                ("pump_minutes.sum".to_owned(), "pump_minutes".to_owned()),
            ])),
            transforms: None,
            allowed_lateness: Some("1h".to_owned()),
            seal_target_bytes: Some(0),
            max_live_segments: None,
        };
        _ = root
            .create_dynamic_path(
                "/usage/daily",
                tinyfs::EntryType::FileDynamic,
                "temporal-reduce-series",
                yaml(&daily),
            )
            .await
            .unwrap();
        tx.commit_test().await.unwrap();
    }

    let (cold_rows, cold_metrics) = query_direct_reduced(&mut persistence).await;
    assert_eq!(cold_rows, vec![(10.0, 1), (11.0, 1), (12.0, 1)]);
    assert_eq!(cold_metrics.global_plans, 0);
    assert_eq!(cold_metrics.non_incremental_plans, 0);
    assert_eq!(cold_metrics.dynamic_source_executions, 1);
    assert_eq!(cold_metrics.bounded_dynamic_source_executions, 0);

    let cache_dir = Path::new(&store_path).parent().unwrap().join("cache");
    let cold_cache = cache_snapshot(&cache_dir);
    assert!(!segment_manifests(&cold_cache).is_empty());

    let (warm_rows, warm_metrics) = query_direct_reduced(&mut persistence).await;
    assert_eq!(warm_rows, cold_rows);
    assert_eq!(warm_metrics.global_plans, 0);
    assert_eq!(warm_metrics.non_incremental_plans, 0);
    assert_eq!(warm_metrics.dynamic_source_executions, 0);
    assert_eq!(warm_metrics.bounded_dynamic_source_executions, 0);
    assert_eq!(
        cache_snapshot(&cache_dir),
        cold_cache,
        "fresh-context no-change read must reuse every cached byte"
    );

    {
        let tx = persistence.begin_test().await.unwrap();
        let root = tx.root().await.unwrap();
        append_source(&root, "/sources/a.series", 97 * 3_600_000_000, 30.0).await;
        tx.commit_test().await.unwrap();
    }

    let (append_rows, append_metrics) = query_direct_reduced(&mut persistence).await;
    assert_eq!(
        append_rows,
        vec![(10.0, 1), (11.0, 1), (12.0, 1), (30.0, 1)]
    );
    assert_eq!(append_metrics.global_plans, 0);
    assert_eq!(append_metrics.non_incremental_plans, 0);
    assert_eq!(append_metrics.dynamic_source_executions, 1);
    assert_eq!(append_metrics.bounded_dynamic_source_executions, 1);
    let read_lo = append_metrics
        .minimum_dynamic_event_time_lo
        .expect("append repair must carry an event-time lower bound");
    assert!(
        read_lo > 3_600_000_000 && read_lo <= 97 * 3_600_000_000,
        "append repair bound {read_lo} must exclude old history and include the append"
    );
    assert_ne!(
        segment_manifests(&cache_snapshot(&cache_dir)),
        segment_manifests(&cold_cache),
        "bounded append repair must advance the direct reducer manifest"
    );
}

#[tokio::test]
async fn dynamic_join_pivot_reuses_persistent_reduction_lineage() {
    let store_path = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await.unwrap();

    {
        let tx = persistence.begin_test().await.unwrap();
        let root = tx.root().await.unwrap();
        _ = root.create_dir_path("/sources").await.unwrap();
        _ = root.create_dir_path("/combined").await.unwrap();
        _ = root.create_dir_path("/singled").await.unwrap();
        _ = root
            .create_series_from_batch(
                "/sources/a.series",
                &source_history(&[10.0, 11.0, 12.0]),
                Some("timestamp"),
            )
            .await
            .unwrap();
        _ = root
            .create_series_from_batch(
                "/sources/b.series",
                &source_history(&[20.0, 21.0, 22.0]),
                Some("timestamp"),
            )
            .await
            .unwrap();

        _ = root
            .create_dynamic_path(
                "/combined/site",
                tinyfs::EntryType::TableDynamic,
                "timeseries-join",
                yaml(&join_config()),
            )
            .await
            .unwrap();

        let pivot = TimeseriesPivotConfig {
            pattern: provider::Url::parse("file+series:///combined/*").unwrap(),
            columns: vec!["A.value".to_owned(), "B.value".to_owned()],
            time_column: "timestamp".to_owned(),
            transforms: None,
        };
        _ = root
            .create_dynamic_path(
                "/singled/metric",
                tinyfs::EntryType::TableDynamic,
                "timeseries-pivot",
                yaml(&pivot),
            )
            .await
            .unwrap();

        let reduce = TemporalReduceConfig {
            in_pattern: provider::Url::parse("file+series:///singled/*").unwrap(),
            out_pattern: "$0".to_owned(),
            time_column: "timestamp".to_owned(),
            resolutions: vec!["1h".to_owned()],
            aggregations: vec![AggregationConfig {
                agg_type: AggregationType::Avg,
                columns: None,
            }],
            output_aliases: None,
            transforms: None,
            allowed_lateness: Some("1h".to_owned()),
            seal_target_bytes: Some(0),
            max_live_segments: None,
        };
        _ = root
            .create_dynamic_path(
                "/reduced",
                tinyfs::EntryType::DirectoryDynamic,
                "temporal-reduce",
                yaml(&reduce),
            )
            .await
            .unwrap();
        tx.commit_test().await.unwrap();
    }

    let (cold_rows, cold_columns, cold_metrics) = query_reduced(&mut persistence).await;
    assert_eq!(cold_rows, 3);
    assert_eq!(cold_columns, 3);
    assert_eq!(cold_metrics.global_plans, 0);
    assert_eq!(cold_metrics.non_incremental_plans, 0);
    assert_eq!(cold_metrics.dynamic_source_executions, 1);
    assert_eq!(cold_metrics.bounded_dynamic_source_executions, 0);

    let cache_dir = Path::new(&store_path).parent().unwrap().join("cache");
    let cold_cache = cache_snapshot(&cache_dir);
    let cold_manifests = segment_manifests(&cold_cache);
    assert!(
        !cold_manifests.is_empty(),
        "cold query must create persistent segment state"
    );

    let (warm_rows, warm_columns, warm_metrics) = query_reduced(&mut persistence).await;
    assert_eq!(warm_rows, cold_rows);
    assert_eq!(warm_columns, cold_columns);
    assert_eq!(warm_metrics.global_plans, 0);
    assert_eq!(warm_metrics.non_incremental_plans, 0);
    assert_eq!(warm_metrics.dynamic_source_executions, 0);
    assert_eq!(warm_metrics.bounded_dynamic_source_executions, 0);
    assert_eq!(
        cache_snapshot(&cache_dir),
        cold_cache,
        "no-change query must not rewrite persistent cache files"
    );

    {
        let tx = persistence.begin_test().await.unwrap();
        let root = tx.root().await.unwrap();
        append_source(&root, "/sources/a.series", 14_400_000_000, 30.0).await;
        tx.commit_test().await.unwrap();
    }

    let (append_rows, append_columns, append_metrics) = query_reduced(&mut persistence).await;
    assert_eq!(append_rows, 4);
    assert_eq!(append_columns, cold_columns);
    assert_eq!(append_metrics.global_plans, 0);
    assert_eq!(append_metrics.non_incremental_plans, 0);
    assert_eq!(append_metrics.dynamic_source_executions, 1);
    assert_eq!(append_metrics.bounded_dynamic_source_executions, 1);
    let read_lo = append_metrics
        .minimum_dynamic_event_time_lo
        .expect("append source execution must carry an event-time lower bound");
    assert!(
        read_lo > 3_600_000_000 && read_lo <= 14_400_000_000,
        "append read bound {read_lo} must exclude oldest history and include the append"
    );
    let append_cache = cache_snapshot(&cache_dir);
    assert_ne!(
        segment_manifests(&append_cache),
        cold_manifests,
        "append must advance a segment manifest"
    );
    assert_ne!(
        append_cache, cold_cache,
        "append must advance persistent segment state"
    );

    {
        let tx = persistence.begin_test().await.unwrap();
        let root = tx.root().await.unwrap();
        append_source(&root, "/sources/a.series", 13_500_000_000, 40.0).await;
        tx.commit_test().await.unwrap();
    }

    let (disorder_rows, disorder_columns, disorder_metrics) = query_reduced(&mut persistence).await;
    assert_eq!(disorder_rows, 4);
    assert_eq!(disorder_columns, cold_columns);
    assert_eq!(disorder_metrics.global_plans, 0);
    assert_eq!(disorder_metrics.non_incremental_plans, 0);
    assert_eq!(disorder_metrics.dynamic_source_executions, 1);
    assert_eq!(disorder_metrics.bounded_dynamic_source_executions, 1);
    let disorder_lo = disorder_metrics
        .minimum_dynamic_event_time_lo
        .expect("ordinary disorder must carry an event-time lower bound");
    assert!(
        disorder_lo > 3_600_000_000 && disorder_lo <= 13_500_000_000,
        "ordinary-disorder bound {disorder_lo} must exclude sealed history and include the sample"
    );
    let disorder_cache = cache_snapshot(&cache_dir);
    assert_ne!(
        segment_manifests(&disorder_cache),
        segment_manifests(&append_cache),
        "ordinary disorder must advance the affected manifest"
    );

    {
        let tx = persistence.begin_test().await.unwrap();
        let root = tx.root().await.unwrap();
        append_source(&root, "/sources/a.series", 18_000_000_000, 50.0).await;
        tx.commit_test().await.unwrap();
    }

    let (advance_rows, advance_columns, advance_metrics) = query_reduced(&mut persistence).await;
    assert_eq!(advance_rows, 5);
    assert_eq!(advance_columns, cold_columns);
    assert_eq!(advance_metrics.global_plans, 0);
    assert_eq!(advance_metrics.non_incremental_plans, 0);
    assert_eq!(advance_metrics.dynamic_source_executions, 1);
    assert_eq!(advance_metrics.bounded_dynamic_source_executions, 1);
    let advanced_cache = cache_snapshot(&cache_dir);

    {
        let tx = persistence.begin_test().await.unwrap();
        let root = tx.root().await.unwrap();
        append_source(&root, "/sources/a.series", 12_600_000_000, 60.0).await;
        tx.commit_test().await.unwrap();
    }

    let (retro_rows, retro_columns, retro_metrics) = query_reduced(&mut persistence).await;
    assert_eq!(retro_rows, 5);
    assert_eq!(retro_columns, cold_columns);
    assert_eq!(retro_metrics.global_plans, 0);
    assert_eq!(retro_metrics.non_incremental_plans, 0);
    assert_eq!(retro_metrics.dynamic_source_executions, 1);
    assert_eq!(retro_metrics.bounded_dynamic_source_executions, 1);
    let retro_lo = retro_metrics
        .minimum_dynamic_event_time_lo
        .expect("retroactive repair must carry an event-time lower bound");
    assert!(
        retro_lo > 3_600_000_000 && retro_lo <= 12_600_000_000,
        "retroactive bound {retro_lo} must include the late sample"
    );
    assert_ne!(
        segment_manifests(&cache_snapshot(&cache_dir)),
        segment_manifests(&advanced_cache),
        "retroactive repair must advance the affected manifest"
    );

    let pre_membership_cache = cache_snapshot(&cache_dir);
    {
        let tx = persistence.begin_test().await.unwrap();
        let root = tx.root().await.unwrap();
        _ = root
            .create_dynamic_path(
                "/combined/site2",
                tinyfs::EntryType::TableDynamic,
                "timeseries-join",
                yaml(&join_config()),
            )
            .await
            .unwrap();
        tx.commit_test().await.unwrap();
    }

    let (membership_rows, membership_columns, membership_metrics) =
        query_reduced(&mut persistence).await;
    assert_eq!(membership_rows, retro_rows);
    assert!(
        membership_columns > retro_columns,
        "new wildcard member must expand the pivoted output schema"
    );
    assert_eq!(membership_metrics.global_plans, 0);
    assert_eq!(membership_metrics.non_incremental_plans, 0);
    assert_eq!(membership_metrics.dynamic_source_executions, 1);
    assert_eq!(membership_metrics.bounded_dynamic_source_executions, 0);
    assert_eq!(membership_metrics.minimum_dynamic_event_time_lo, None);
    assert!(
        segment_manifests(&cache_snapshot(&cache_dir)).len()
            > segment_manifests(&pre_membership_cache).len(),
        "wildcard membership change must build a distinct aggregate namespace"
    );
}
