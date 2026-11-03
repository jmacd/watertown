// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use arrow::{
    array::{StringArray, TimestampMicrosecondArray, UInt64Array},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use provider::factory::analytics_rollup::partitions::{
    Inventory, Manifest, PartitionSink, schema, source_id,
};
use std::{collections::BTreeMap, sync::Arc};
use tlogfs::{OpLogPersistence, PondTxnMetadata, PondUserMetadata};

use provider::factory::analytics_rollup::partitions::{DAY_US, HOUR_US};
use provider::factory::analytics_rollup::{AnalyticsRollupConfig, RollupReport, run};
use provider::{ExecutionContext, FactoryContext, Provider};
use site_analytics_collector::{
    config::Config,
    journal::{Event, Journal},
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
type Rows = Vec<(i64, String, u64)>;

async fn rows(context: &tinyfs::ProviderContext, sql: &str) -> Rows {
    let batches = context
        .datafusion_session
        .sql(sql)
        .await
        .expect("reference plan")
        .collect()
        .await
        .expect("reference rows");
    let mut result = Vec::new();
    for batch in batches {
        let times = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("microseconds");
        let paths = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("URIs");
        for i in 0..batch.num_rows() {
            let count = if let Some(counts) = batch.column(2).as_any().downcast_ref::<UInt64Array>()
            {
                counts.value(i)
            } else {
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("counts")
                    .value(i) as u64
            };
            result.push((times.value(i), paths.value(i).to_owned(), count));
        }
    }
    result
}

struct World {
    temp: tempfile::TempDir,
    pond_id: String,
    collector: Config,
    rollup: AnalyticsRollupConfig,
}

impl World {
    async fn new() -> Self {
        let temp = tempfile::tempdir().expect("fixture directory");
        let pond_id = uuid7::uuid7().to_string();
        let _ = OpLogPersistence::create(
            temp.path().join("pond"),
            pond_id.clone(),
            PondUserMetadata::new(vec!["test".into()]),
        )
        .await
        .expect("create pond");
        let collector = Config {
            site_id: "noyo".into(),
            origin: "https://noyocenter.org".into(),
            listen: "127.0.0.1:0".parse().expect("loopback"),
            catalog_dir: temp.path().join("catalogs"),
            log_dir: temp.path().join("logs"),
            endpoint: "/_analytics/view".into(),
            max_log_bytes: 1048576,
            segment_bytes: 4096,
        };
        let rollup = AnalyticsRollupConfig {
            source: "/analytics/raw".into(),
            target: "/analytics/hourly".into(),
            site_id: "noyo".into(),
            repair_days: 2,
            exclude: Default::default(),
        };
        Self {
            temp,
            pond_id,
            collector,
            rollup,
        }
    }

    async fn ingest(&self) {
        let mut pond = OpLogPersistence::open(self.temp.path().join("pond"), self.pond_id.clone())
            .await
            .expect("open pond");
        let meta = PondTxnMetadata::new(
            pond.last_txn_seq_for(&self.pond_id) + 1,
            PondUserMetadata::new(vec!["ingest".into()]),
        );
        let tx = pond.begin_write(&meta).await.expect("ingest transaction");
        let context = tx.state().expect("state").as_provider_context();
        let root = context.filesystem().root().await.expect("root");
        let config = serde_json::json!({
            "active_pattern": self.collector.log_dir.join("views.jsonl"),
            "archived_pattern": self.collector.log_dir.join("views-*.jsonl"),
            "pond_path": self.rollup.source, "timestamp_field": "received_at_us",
            "timestamp_unit": "microseconds", "analytics_site_id": "noyo",
        });
        provider::factory::logfile_ingest::execute(
            config,
            FactoryContext::new(context, root.node_path().id()),
            ExecutionContext::pond_readwriter(vec![]),
        )
        .await
        .expect("strict ingest");
        let _ = tx.commit().await.expect("ingest commit");
    }

    async fn roll(
        &self,
        rebuild: bool,
        approval: Option<&str>,
        commit: bool,
    ) -> TestResult<(RollupReport, Option<Manifest>)> {
        let mut pond =
            OpLogPersistence::open(self.temp.path().join("pond"), self.pond_id.clone()).await?;
        let meta = PondTxnMetadata::new(
            pond.last_txn_seq_for(&self.pond_id) + 1,
            PondUserMetadata::new(vec!["rollup".into()]),
        );
        let tx = pond.begin_write(&meta).await?;
        let context = tx
            .state()?
            .as_provider_context()
            .with_cache_dir(self.temp.path().join("cache"));
        let root = context.filesystem().root().await?;
        let factory = FactoryContext::new(context.clone(), root.node_path().id());
        let report = run(
            &self.rollup,
            &factory,
            rebuild,
            approval,
            10 * DAY_US + HOUR_US,
        )
        .await?;
        let sink = PartitionSink::new(context.clone(), self.rollup.target.clone());
        let manifest = sink.load().await?;
        if let Some(manifest) = &manifest {
            let handle = provider::FactoryRegistry::create_file(
                "analytics-table",
                format!("target: {}\n", self.rollup.target).as_bytes(),
                factory.clone(),
            )
            .await?;
            let file = handle.get_file().await;
            let file = file.lock().await;
            let table = file
                .as_queryable()
                .expect("queryable analytics table")
                .as_table_provider(root.node_path().id(), &context)
                .await?;
            let _ = context.datafusion_session.register_table("counts", table)?;
            if manifest.source.segments().all(|s| s.record_count == 0) {
                assert!(
                    rows(
                        &context,
                        "SELECT * FROM counts ORDER BY timestamp, page_uri"
                    )
                    .await
                    .is_empty()
                );
            } else if !rebuild || approval.is_some() {
                let provider = Provider::with_context(
                    Arc::new(context.filesystem()),
                    Arc::new(context.clone()),
                );
                let table = provider
                    .create_provider_for_url(
                        "site-analytics:///analytics/raw/views-*.jsonl",
                        &context.datafusion_session,
                    )
                    .await?;
                let _ = context.datafusion_session.register_table("events", table)?;
                let excludes = self
                    .rollup
                    .exclude
                    .iter()
                    .map(|s| format!("'{}'", s.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(",");
                let predicate = if excludes.is_empty() {
                    String::new()
                } else {
                    format!("WHERE page_uri NOT IN ({excludes})")
                };
                let reference = rows(&context, &format!(
                    "SELECT date_trunc('hour', timestamp) AS timestamp, page_uri, COUNT(*) AS page_views FROM events {predicate} GROUP BY 1, 2 ORDER BY 1, 2"
                )).await;
                assert_eq!(
                    rows(
                        &context,
                        "SELECT * FROM counts ORDER BY timestamp, page_uri"
                    )
                    .await,
                    reference
                );
            }
        }
        if commit {
            let _ = tx.commit().await?;
        }
        Ok((report, manifest))
    }

    async fn assert_missing_source_blocks_repair(&self) {
        let mut pond = OpLogPersistence::open(self.temp.path().join("pond"), self.pond_id.clone())
            .await
            .expect("open pond");
        let meta = PondTxnMetadata::new(
            pond.last_txn_seq_for(&self.pond_id) + 1,
            PondUserMetadata::new(vec!["missing-source".into()]),
        );
        let tx = pond.begin_write(&meta).await.expect("repair transaction");
        let context = tx.state().expect("state").as_provider_context();
        let root = context.filesystem().root().await.expect("root");
        let sink = PartitionSink::new(context.clone(), self.rollup.target.clone());
        let prior = sink
            .load()
            .await
            .expect("load")
            .expect("published snapshot");
        let name = prior
            .source
            .segments()
            .find(|s| s.bytes > 0)
            .expect("raw segment")
            .filename();
        let dir = root
            .open_dir_path(&self.rollup.source)
            .await
            .expect("raw directory");
        dir.rename_entry(&name, "missing-source.jsonl")
            .await
            .expect("simulate unavailable source");
        let factory = FactoryContext::new(context, root.node_path().id());
        let error = run(&self.rollup, &factory, true, None, 10 * DAY_US)
            .await
            .expect_err("blocked repair");
        assert!(error.to_string().contains("membership"));
        assert_eq!(sink.load().await.expect("unchanged progress"), Some(prior));
        // Dropping this transaction restores the renamed source as well.
    }
}

fn append(journal: &mut Journal, time: i64, page: &str, count: usize) {
    let _ = journal
        .append(&vec![
            Event {
                received_at_us: time,
                page_uri: page.into(),
                catalog_id: "a".repeat(64),
            };
            count
        ])
        .expect("collector append");
}

#[tokio::test]
async fn rollups_match_reference_after_late_data_rotations_rebuild_and_abort() {
    let mut world = World::new().await;
    let mut journal = Journal::open(&world.collector).unwrap();
    world.ingest().await;
    let (empty_report, empty) = world.roll(false, None, true).await.unwrap();
    assert!(empty_report.published);
    assert!(empty.unwrap().days.is_empty());
    append(&mut journal, 10 * DAY_US + 1, "/", 2);
    append(&mut journal, 9 * DAY_US + 1, "/water", 1);
    world.ingest().await;
    let (report, first) = world.roll(false, None, true).await.unwrap();
    assert_eq!(report.records_examined_for_changes, 3);
    assert_eq!(report.days_replaced, vec![9 * DAY_US, 10 * DAY_US]);
    let (idle, same) = world.roll(false, None, true).await.unwrap();
    assert!(!idle.published);
    assert_eq!(idle.records_examined_for_changes, 0);
    assert_eq!(same, first);

    append(&mut journal, 10 * DAY_US + 1, "/", 1);
    world.ingest().await;
    let (late, second) = world.roll(false, None, true).await.unwrap();
    assert_eq!(late.records_examined_for_changes, 1);
    assert_eq!(late.days_replaced, vec![10 * DAY_US]);
    assert_eq!(
        first.as_ref().unwrap().days[&(9 * DAY_US)],
        second.as_ref().unwrap().days[&(9 * DAY_US)]
    );
    append(&mut journal, 9 * DAY_US + 2, "/", 1);
    for _ in 0..3 {
        append(&mut journal, 10 * DAY_US + HOUR_US, "/", 20);
    }
    world.ingest().await;
    let _ = world.roll(false, None, false).await.unwrap();
    let (retried, _) = world.roll(false, None, true).await.unwrap();
    assert_eq!(retried.records_examined_for_changes, 61);
    drop(journal);
    let mut journal = Journal::open(&world.collector).unwrap();
    world.ingest().await;
    let (rotation, _) = world.roll(false, None, true).await.unwrap();
    assert_eq!(rotation.records_examined_for_changes, 0);
    assert!(rotation.days_replaced.is_empty());

    append(&mut journal, 7 * DAY_US, "/", 1);
    world.ingest().await;
    assert!(
        world
            .roll(false, None, true)
            .await
            .unwrap_err()
            .to_string()
            .contains("repair_days")
    );
    let (plan, before) = world.roll(true, None, false).await.unwrap();
    assert!(!plan.published);
    append(&mut journal, 7 * DAY_US, "/", 1);
    world.ingest().await;
    assert!(
        world
            .roll(true, plan.rebuild_token.as_deref(), true)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale")
    );
    let (plan, still_before) = world.roll(true, None, false).await.unwrap();
    assert_eq!(before, still_before);
    let _ = world
        .roll(true, plan.rebuild_token.as_deref(), true)
        .await
        .unwrap();
    let _ = world.rollup.exclude.insert("/".into());
    assert!(
        world
            .roll(false, None, true)
            .await
            .unwrap_err()
            .to_string()
            .contains("recipe")
    );
    let (plan, _) = world.roll(true, None, false).await.unwrap();
    let (_, filtered) = world
        .roll(true, plan.rebuild_token.as_deref(), true)
        .await
        .unwrap();
    assert_eq!(
        filtered.unwrap().days.keys().copied().collect::<Vec<_>>(),
        vec![9 * DAY_US]
    );
    let _ = world.rollup.exclude.insert("/water".into());
    let (plan, _) = world.roll(true, None, false).await.unwrap();
    let (_, empty) = world
        .roll(true, plan.rebuild_token.as_deref(), true)
        .await
        .unwrap();
    assert!(empty.unwrap().days.is_empty());
    world.assert_missing_source_blocks_repair().await;
    let (unchanged, _) = world.roll(false, None, true).await.unwrap();
    assert!(!unchanged.published);
}

fn stream(views: u64) -> SendableRecordBatchStream {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![0]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec!["/"])),
            Arc::new(UInt64Array::from(vec![views])),
        ],
    )
    .expect("fixture batch");
    Box::pin(RecordBatchStreamAdapter::new(
        schema(),
        futures::stream::iter(vec![Ok(batch)]),
    ))
}

fn source() -> Inventory {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1, "site_id": "noyo", "closed": [],
        "active": {
            "segment_id": uuid::Uuid::new_v4(), "predecessor": null, "record_count": 0,
            "bytes": 0, "sha256": site_analytics_collector::catalog::digest(b""),
        },
    }))
    .expect("fixture inventory");
    Inventory::parse(&bytes, "noyo").expect("valid inventory")
}

async fn count(sink: &PartitionSink, context: &tinyfs::ProviderContext) -> u64 {
    let manifest = sink
        .load()
        .await
        .expect("load")
        .expect("committed manifest");
    let table = sink.table(&manifest).await.expect("selected table");
    let _ = context
        .datafusion_session
        .register_table("counts", table)
        .expect("register");
    let batches = context
        .datafusion_session
        .sql("SELECT SUM(page_views) FROM counts")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("query");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("uint count")
        .value(0)
}

#[tokio::test]
async fn partition_selection_and_progress_commit_or_rollback_together() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("pond");
    let id = uuid7::uuid7().to_string();
    let mut pond = OpLogPersistence::create(
        &path,
        id.clone(),
        PondUserMetadata::new(vec!["test".into()]),
    )
    .await
    .unwrap();
    let mut committed = None;
    for (index, publish, commit) in [
        (0, true, true),
        (1, false, false),
        (2, true, false),
        (3, true, true),
    ] {
        let source = source();
        let metadata = PondTxnMetadata::new(
            pond.last_txn_seq_for(&id) + 1,
            PondUserMetadata::new(vec!["test".into()]),
        );
        let tx = pond.begin_write(&metadata).await.unwrap();
        let context = tx.state().unwrap().as_provider_context();
        let sink = PartitionSink::new(context.clone(), "/analytics/hourly".into());
        if index != 0 {
            assert_eq!(sink.load().await.unwrap(), committed);
            assert_eq!(count(&sink, &context).await, 2);
        }
        let partition = sink
            .stage_day(0, stream(if index == 0 { 2 } else { 7 }))
            .await
            .unwrap()
            .unwrap();
        if index == 1 {
            use futures::StreamExt;
            let failing = stream(7).chain(futures::stream::once(async {
                Err(datafusion::error::DataFusionError::Execution(
                    "injected stream failure".into(),
                ))
            }));
            let failing = Box::pin(RecordBatchStreamAdapter::new(schema(), failing));
            assert!(sink.stage_day(0, failing).await.is_err());
            assert_eq!(sink.load().await.unwrap(), committed);
        }
        let manifest = Manifest {
            schema_version: 1,
            source_path: "/analytics/raw".into(),
            recipe_id: format!("recipe-{index}"),
            source_state_id: source_id(&source).unwrap(),
            source: source.clone(),
            published_at_us: index,
            collection_coverage: "unknown".into(),
            days: BTreeMap::from([(0, partition)]),
        };
        if publish {
            sink.publish(&manifest).await.unwrap();
        }
        if commit {
            let _ = tx.commit().await.unwrap();
            committed = Some(manifest);
        } else {
            drop(tx);
        }
        drop(pond);
        pond = OpLogPersistence::open(&path, id.clone()).await.unwrap();
    }
    let metadata = PondTxnMetadata::new(
        pond.last_txn_seq_for(&id) + 1,
        PondUserMetadata::new(vec!["test".into()]),
    );
    let tx = pond.begin_write(&metadata).await.unwrap();
    let context = tx.state().unwrap().as_provider_context();
    let sink = PartitionSink::new(context.clone(), "/analytics/hourly".into());
    assert_eq!(sink.load().await.unwrap(), committed);
    assert_eq!(count(&sink, &context).await, 7);
    let empty = Box::pin(RecordBatchStreamAdapter::new(
        schema(),
        futures::stream::empty(),
    ));
    assert!(sink.stage_day(0, empty).await.unwrap().is_none());
    let mut manifest = committed.unwrap();
    manifest.days.clear();
    manifest.recipe_id = "empty-replacement".into();
    sink.publish(&manifest).await.unwrap();
    let _ = tx.commit().await.unwrap();
    drop(pond);
    let mut pond = OpLogPersistence::open(path, id.clone()).await.unwrap();
    let metadata = PondTxnMetadata::new(
        pond.last_txn_seq_for(&id) + 1,
        PondUserMetadata::new(vec!["test".into()]),
    );
    let tx = pond.begin_write(&metadata).await.unwrap();
    let context = tx.state().unwrap().as_provider_context();
    let sink = PartitionSink::new(context.clone(), "/analytics/hourly".into());
    assert_eq!(sink.load().await.unwrap(), Some(manifest.clone()));
    let _ = context
        .datafusion_session
        .register_table("empty_counts", sink.table(&manifest).await.unwrap())
        .unwrap();
    let batches = context
        .datafusion_session
        .sql("SELECT * FROM empty_counts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}
