// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::persistence::OpLogPersistence;
use arrow_array::{ArrayRef, Float64Array, RecordBatch, TimestampMicrosecondArray};
use provider::factory::temporal_reduce::{
    AggregationConfig, AggregationType, TemporalReduceConfig,
};
use provider::factory::timeseries_join::{TimeseriesInput, TimeseriesJoinConfig};
use provider::factory::timeseries_pivot::TimeseriesPivotConfig;
use std::collections::BTreeMap;
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

fn yaml<T: serde::Serialize>(config: &T) -> Vec<u8> {
    serde_yaml::to_string(config).unwrap().into_bytes()
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
) -> (usize, tinyfs::PlanVisibilityMetricsSnapshot) {
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
    let rows = context
        .datafusion_session
        .read_table(table)
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    let metrics = context.plan_visibility_metrics();
    tx.commit_test().await.unwrap();
    (rows, metrics)
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
                &source_batch(3_600_000_000, 10.0),
                Some("timestamp"),
            )
            .await
            .unwrap();
        _ = root
            .create_series_from_batch(
                "/sources/b.series",
                &source_batch(3_600_000_000, 20.0),
                Some("timestamp"),
            )
            .await
            .unwrap();

        let join = TimeseriesJoinConfig {
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
        };
        _ = root
            .create_dynamic_path(
                "/combined/site",
                tinyfs::EntryType::TableDynamic,
                "timeseries-join",
                yaml(&join),
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

    let (cold_rows, cold_metrics) = query_reduced(&mut persistence).await;
    assert_eq!(cold_rows, 1);
    assert_eq!(cold_metrics.global_plans, 0);
    assert_eq!(cold_metrics.non_incremental_plans, 0);

    let cache_dir = Path::new(&store_path).parent().unwrap().join("cache");
    let cold_cache = cache_snapshot(&cache_dir);
    let cold_manifests = segment_manifests(&cold_cache);
    assert!(
        !cold_manifests.is_empty(),
        "cold query must create persistent segment state"
    );

    let (warm_rows, warm_metrics) = query_reduced(&mut persistence).await;
    assert_eq!(warm_rows, cold_rows);
    assert_eq!(warm_metrics.global_plans, 0);
    assert_eq!(warm_metrics.non_incremental_plans, 0);
    assert_eq!(
        cache_snapshot(&cache_dir),
        cold_cache,
        "no-change query must not rewrite persistent cache files"
    );

    {
        let tx = persistence.begin_test().await.unwrap();
        let root = tx.root().await.unwrap();
        append_source(&root, "/sources/a.series", 7_200_000_000, 30.0).await;
        tx.commit_test().await.unwrap();
    }

    let (append_rows, append_metrics) = query_reduced(&mut persistence).await;
    assert_eq!(append_rows, 2);
    assert_eq!(append_metrics.global_plans, 0);
    assert_eq!(append_metrics.non_incremental_plans, 0);
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
}
