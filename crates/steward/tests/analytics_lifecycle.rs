// SPDX-License-Identifier: Apache-2.0

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use provider::{
    ExecutionContext, FactoryContext,
    factory::analytics_rollup::{
        AnalyticsRollupConfig,
        partitions::{HOUR_US, Manifest, PartitionSink, schema},
        run,
    },
};
use site_analytics_collector::{
    config::Config,
    journal::{Event, Journal},
};
use std::{path::Path, sync::Arc};
use steward::{
    ContentSource, LocalPondSource, Ship, build_recovery_capsule, compute_content_tree,
    fetch_object_graph, import_capsule, rebuild_pond,
};
use sync_store::{CapsuleNode, CapsulePayloadKind, ContentRemote};
use tinyfs::arrow::ParquetExt;
use tlogfs::PondUserMetadata;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

fn meta() -> PondUserMetadata {
    PondUserMetadata::new(vec!["analytics-lifecycle".into()])
}

fn config() -> AnalyticsRollupConfig {
    AnalyticsRollupConfig {
        source: "/analytics/raw".into(),
        target: "/analytics/hourly".into(),
        site_id: "noyo".into(),
        repair_days: 1,
        exclude: Default::default(),
    }
}

fn batch(hour: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(
                TimestampMicrosecondArray::from(vec![hour * HOUR_US; 24]).with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(
                (0..24).map(|p| format!("/page/{p:02}")).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(vec![2; 24])),
        ],
    )
    .expect("counts batch")
}

async fn inspect(ship: &mut Ship, export: &Path) -> TestResult<Manifest> {
    let tx = ship.begin_write(&meta()).await?;
    let context = tx.provider_context()?.with_cache_dir(export.join("cache"));
    let root = tx.root().await?;
    let factory = FactoryContext::new(context.clone(), root.node_path().id());
    let report = run(&config(), &factory, false, None, 4 * HOUR_US).await?;
    assert!(
        !report.published,
        "physical changes must not dirty rollup progress"
    );
    assert_eq!(report.records_examined_for_changes, 0);
    let sink = PartitionSink::new(context.clone(), config().target);
    let manifest = sink.load().await?.expect("snapshot");
    let table = sink.table(&manifest).await?;
    let _ = context
        .datafusion_session
        .register_table("counts", table.clone())?;
    let rows = context
        .datafusion_session
        .sql("SELECT * FROM counts ORDER BY timestamp, page_uri")
        .await?
        .collect()
        .await?;
    let rows = arrow::compute::concat_batches(&schema(), &rows)?;
    let expected =
        arrow::compute::concat_batches(&schema(), &(0..4).map(batch).collect::<Vec<_>>())?;
    assert_eq!(rows, expected);

    let output = export.join("public");
    let (outputs, _) = provider::export::export_table_provider_to_parquet(
        table,
        "analytics",
        &output,
        &[],
        &[],
        &output,
        &context,
        "timestamp",
        None,
    )
    .await?;
    assert_eq!(outputs.len(), 1);
    let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(
        output.join(&outputs[0].1.file),
    )?)?;
    assert_eq!(reader.schema(), &schema());
    utilities::parquet_policy::validate_metadata(reader.schema(), reader.metadata())?;
    let exported = reader.build()?.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(arrow::compute::concat_batches(&schema(), &exported)?, rows);
    let _ = tx.commit().await?;
    Ok(manifest)
}

#[allow(clippy::print_stderr)]
fn inspect_capsule(capsule: &steward::CapsuleBuild) -> TestResult<()> {
    let mut tables = 0;
    for entry in &capsule.manifest.entries {
        if let CapsuleNode::Physical {
            payload_kind: CapsulePayloadKind::Table,
            objects,
            ..
        } = &entry.node
        {
            tables += 1;
            for object in objects {
                let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(
                    capsule.payloads.path(object.hash),
                )?)?;
                assert_eq!(reader.schema(), &schema());
                utilities::parquet_policy::validate_metadata(reader.schema(), reader.metadata())?;
                eprintln!(
                    "{}: {} rows, {} physical bytes",
                    entry.path,
                    reader.metadata().file_metadata().num_rows(),
                    object.size
                );
            }
        }
    }
    assert_eq!(
        tables, 2,
        "selected partition and multi-leaf encoding fixture"
    );
    Ok(())
}

fn assert_logical_snapshot(
    expected: &steward::CapsuleBuild,
    actual: &steward::CapsuleBuild,
    preserve_mtimes: bool,
) {
    assert_eq!(
        expected.manifest.entries.len(),
        actual.manifest.entries.len()
    );
    for (before, after) in expected
        .manifest
        .entries
        .iter()
        .zip(&actual.manifest.entries)
    {
        assert_eq!(before.path, after.path);
        assert_eq!(before.entry_type, after.entry_type);
        if let CapsuleNode::Physical {
            logical_root,
            leaves,
            schema_fingerprint,
            ..
        } = &before.node
        {
            let CapsuleNode::Physical {
                logical_root: actual_root,
                leaves: actual_leaves,
                schema_fingerprint: actual_schema,
                ..
            } = &after.node
            else {
                panic!("physical node changed kind");
            };
            assert_eq!(actual_root, logical_root);
            assert_eq!(actual_leaves.len(), leaves.len());
            for (before, after) in leaves.iter().zip(actual_leaves) {
                let mut after = after.clone();
                if !preserve_mtimes {
                    // Native pack descriptors preserve logical identity, not historical mtimes.
                    after.source_timestamp = before.source_timestamp;
                }
                assert_eq!(&after, before);
            }
            assert_eq!(actual_schema, schema_fingerprint);
        }
    }
}

#[allow(clippy::print_stderr)]
async fn inspect_maintained_objects(ship: &Ship, source: &LocalPondSource) -> TestResult<()> {
    let materialized = steward::materialize_content_objects(ship).await?;
    let hash = materialized
        .manifest_records
        .iter()
        .find(|record| record.entry.name == "dictionary-fixture")
        .expect("fixture manifest")
        .entry
        .child_hash;
    let packs = source.list_pack_hashes(hash).await?;
    assert_eq!(packs.len(), 1);
    for pack_hash in packs {
        let bytes = source
            .get_pack_index(hash, pack_hash)
            .await?
            .expect("pack index");
        let index = sync_store::content::PackIndex::decode(&bytes)?;
        assert_eq!(
            index.physical_object_hashes().len(),
            1,
            "four leaves repacked to one object"
        );
        for object in index.physical_object_hashes() {
            let bytes = match source.get_object(*object).await? {
                Some(bytes) => bytes,
                None => {
                    use tokio::io::AsyncReadExt;
                    let mut reader = source
                        .get_blob_reader(*object)
                        .await?
                        .expect("physical pack");
                    let mut bytes = Vec::new();
                    let _ = reader.read_to_end(&mut bytes).await?;
                    bytes
                }
            };
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))?;
            utilities::parquet_policy::validate_metadata(reader.schema(), reader.metadata())?;
            assert_eq!(reader.metadata().file_metadata().num_rows(), 96);
            let rows = reader.build()?.collect::<Result<Vec<_>, _>>()?;
            assert_eq!(
                arrow::compute::concat_batches(&schema(), &rows)?,
                arrow::compute::concat_batches(&schema(), &(0..4).map(batch).collect::<Vec<_>>())?
            );
        }
        eprintln!(
            "maintained four-leaf table: {} physical bytes",
            index.physical_byte_count()
        );
    }
    Ok(())
}

#[tokio::test]
async fn analytics_encoding_and_identity_survive_maintenance_native_restore_and_capsule_import()
-> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let source_path = temp.path().join("source");
    let mut ship = Ship::create_pond(&source_path, "analytics").await?;
    let collector = Config {
        site_id: "noyo".into(),
        origin: "https://noyocenter.org".into(),
        listen: "127.0.0.1:0".parse()?,
        catalog_dir: temp.path().join("catalogs"),
        log_dir: temp.path().join("logs"),
        endpoint: "/_analytics/view".into(),
        max_log_bytes: 1048576,
        segment_bytes: 524288,
    };
    let mut journal = Journal::open(&collector)?;
    for hour in 0..4 {
        let events = (0..24)
            .flat_map(|page| {
                vec![
                    Event {
                        received_at_us: hour * HOUR_US,
                        page_uri: format!("/page/{page:02}"),
                        catalog_id: "a".repeat(64),
                    };
                    2
                ]
            })
            .collect::<Vec<_>>();
        let _ = journal.append(&events)?;
        let tx = ship.begin_write(&meta()).await?;
        let root = tx.root().await?;
        let context = tx.provider_context()?;
        provider::factory::logfile_ingest::execute(
            serde_json::json!({
                "active_pattern": collector.log_dir.join("views.jsonl"),
                "archived_pattern": collector.log_dir.join("views-*.jsonl"),
                "pond_path": config().source, "timestamp_field": "received_at_us",
                "timestamp_unit": "microseconds", "analytics_site_id": "noyo",
            }),
            FactoryContext::new(context, root.node_path().id()),
            ExecutionContext::pond_readwriter(vec![]),
        )
        .await?;
        // Rollup objects have one immutable leaf; this companion series exercises
        // the same schema through actual multi-leaf table maintenance.
        let _ = root
            .write_series_from_batch("/dictionary-fixture", &batch(hour), Some("timestamp"))
            .await?;
        let _ = tx.commit().await?;
    }
    let tx = ship.begin_write(&meta()).await?;
    let context = tx
        .provider_context()?
        .with_cache_dir(temp.path().join("initial-cache"));
    let root = tx.root().await?;
    let report = run(
        &config(),
        &FactoryContext::new(context, root.node_path().id()),
        false,
        None,
        4 * HOUR_US,
    )
    .await?;
    assert!(report.published);
    let _ = tx.commit().await?;
    let original = inspect(&mut ship, &temp.path().join("initial")).await?;
    let before = build_recovery_capsule(&ship).await?;
    inspect_capsule(&before)?;
    let root_before = compute_content_tree(&ship).await?.root_tree_hash;
    let maintenance = ship.collapse_versions(1).await?;
    assert_eq!(
        maintenance.series_repacked, 2,
        "raw events and dictionary fixture"
    );
    assert_eq!(
        compute_content_tree(&ship).await?.root_tree_hash,
        root_before
    );
    assert_eq!(
        inspect(&mut ship, &temp.path().join("maintained")).await?,
        original
    );
    let maintained = build_recovery_capsule(&ship).await?;
    inspect_capsule(&maintained)?;
    assert_logical_snapshot(&before, &maintained, true);
    let second = ship.collapse_versions(1).await?;
    assert_eq!(second.series_repacked, 0);

    let source = LocalPondSource::open(&source_path).await?;
    inspect_maintained_objects(&ship, &source).await?;
    let graph = fetch_object_graph(&source, "").await?;
    let mut rebuilt = Ship::create_pond(temp.path().join("rebuilt"), "rebuild").await?;
    let _ = rebuild_pond(&mut rebuilt, &source, &graph).await?;
    assert_eq!(
        inspect(&mut rebuilt, &temp.path().join("rebuilt-export")).await?,
        original
    );
    let rebuilt_capsule = build_recovery_capsule(&rebuilt).await?;
    inspect_capsule(&rebuilt_capsule)?;
    assert_logical_snapshot(&before, &rebuilt_capsule, false);

    let remote_path = temp.path().join("remote");
    let mut remote = ContentRemote::create_at(
        &remote_path,
        uuid::Uuid::parse_str(ship.data_persistence().pond_id())?,
    )
    .await?;
    let _ = steward::push_content_to_remote(&ship, &mut remote, "main").await?;
    let _ = remote
        .publish_capsule_directory(&maintained.manifest, maintained.payloads.objects_dir())
        .await?;
    let imported_path = temp.path().join("imported");
    let _ = import_capsule(&remote_path, &imported_path, "import").await?;
    let mut imported = Ship::open_pond(&imported_path).await?;
    assert_eq!(
        inspect(&mut imported, &temp.path().join("imported-export")).await?,
        original
    );
    let imported_capsule = build_recovery_capsule(&imported).await?;
    inspect_capsule(&imported_capsule)?;
    assert_logical_snapshot(&before, &imported_capsule, true);

    let tx = imported.begin_write(&meta()).await?;
    let context = tx
        .provider_context()?
        .with_cache_dir(temp.path().join("restore-rebuild-cache"));
    let root = tx.root().await?;
    let factory = FactoryContext::new(context.clone(), root.node_path().id());
    let plan = run(&config(), &factory, true, None, 4 * HOUR_US).await?;
    let rebuilt = run(
        &config(),
        &factory,
        true,
        plan.rebuild_token.as_deref(),
        4 * HOUR_US,
    )
    .await?;
    assert!(rebuilt.published);
    let sink = PartitionSink::new(context.clone(), config().target);
    let manifest = sink.load().await?.expect("rebuilt snapshot");
    let _ = context
        .datafusion_session
        .register_table("counts", sink.table(&manifest).await?)?;
    let rows = context
        .datafusion_session
        .sql("SELECT * FROM counts ORDER BY timestamp, page_uri")
        .await?
        .collect()
        .await?;
    assert_eq!(
        arrow::compute::concat_batches(&schema(), &rows)?,
        arrow::compute::concat_batches(&schema(), &(0..4).map(batch).collect::<Vec<_>>())?
    );
    let _ = tx.commit().await?;
    Ok(())
}
