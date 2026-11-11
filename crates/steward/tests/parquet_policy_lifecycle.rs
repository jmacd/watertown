// SPDX-License-Identifier: Apache-2.0

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use provider::query_foundation_adapter::capture_tinyfs_snapshot;
use query_foundation::{overlap::OverlapPolicy, snapshot::EventTimeContract};
use std::{path::Path, sync::Arc};
use steward::{
    ContentSource, LocalPondSource, Ship, build_recovery_capsule, compute_content_tree,
    fetch_object_graph, import_capsule, rebuild_pond,
};
use sync_store::{CapsuleNode, CapsulePayloadKind, ContentRemote};
use tinyfs::arrow::ParquetExt;
use tlogfs::PondUserMetadata;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
const HOUR_US: i64 = 3_600_000_000;

fn meta() -> PondUserMetadata {
    PondUserMetadata::new(vec!["parquet-policy-lifecycle".into()])
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("category", DataType::Utf8, false).with_metadata(
            std::collections::HashMap::from([(
                utilities::parquet_policy::POLICY_KEY.into(),
                utilities::parquet_policy::DICTIONARY_V1.into(),
            )]),
        ),
        Field::new("count", DataType::UInt64, false),
    ]))
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

async fn inspect(ship: &mut Ship, export: &Path) -> TestResult<()> {
    let tx = ship.begin_write(&meta()).await?;
    let context = tx.provider_context()?.with_cache_dir(export.join("cache"));
    let root = tx.root().await?;
    let id = root.get_node_path("/dictionary-fixture").await?.id();
    let snapshot = capture_tinyfs_snapshot(
        &context,
        id,
        "dictionary-fixture",
        schema(),
        Some(EventTimeContract::new("timestamp")),
        OverlapPolicy::PreserveAll,
    )
    .await?;
    let table = snapshot.table_provider()?;
    let _ = context
        .datafusion_session
        .register_table("counts", table.clone())?;
    let rows = context
        .datafusion_session
        .sql("SELECT * FROM counts ORDER BY timestamp, category")
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
        "dictionary-fixture",
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
    Ok(())
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
    assert_eq!(tables, 1, "multi-leaf encoding fixture");
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
async fn dictionary_encoding_and_identity_survive_maintenance_native_restore_and_capsule_import()
-> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let source_path = temp.path().join("source");
    let mut ship = Ship::create_pond(&source_path, "dictionary").await?;
    for hour in 0..4 {
        let tx = ship.begin_write(&meta()).await?;
        let root = tx.root().await?;
        let _ = root
            .write_series_from_batch("/dictionary-fixture", &batch(hour), Some("timestamp"))
            .await?;
        let _ = tx.commit().await?;
    }
    inspect(&mut ship, &temp.path().join("initial")).await?;
    let before = build_recovery_capsule(&ship).await?;
    inspect_capsule(&before)?;
    let root_before = compute_content_tree(&ship).await?.root_tree_hash;
    let maintenance = ship.collapse_versions(1).await?;
    assert_eq!(maintenance.series_repacked, 1, "dictionary fixture");
    assert_eq!(
        compute_content_tree(&ship).await?.root_tree_hash,
        root_before
    );
    inspect(&mut ship, &temp.path().join("maintained")).await?;
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
    inspect(&mut rebuilt, &temp.path().join("rebuilt-export")).await?;
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
    inspect(&mut imported, &temp.path().join("imported-export")).await?;
    let imported_capsule = build_recovery_capsule(&imported).await?;
    inspect_capsule(&imported_capsule)?;
    assert_logical_snapshot(&before, &imported_capsule, true);

    Ok(())
}
