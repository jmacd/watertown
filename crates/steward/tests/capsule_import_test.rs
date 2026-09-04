// SPDX-License-Identifier: Apache-2.0

//! Coverage for [`steward::import_capsule`], the generic staged importer
//! (`docs/recovery-capsule-design.md`, "Generic staged import").

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use steward::{Ship, build_recovery_capsule, import_capsule};
use sync_store::{CapsuleManifest, CapsuleNode, ContentRemote};
use tempfile::tempdir;
use tinyfs::EntryType;
use tinyfs::arrow::ParquetExt;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::PondUserMetadata;
use tokio::io::AsyncWriteExt;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["capsule-import-test".into(), label.into()])
}

fn table_batch(timestamp: i64, value: &str, note: Option<&str>) -> RecordBatch {
    let mut fields = vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Utf8, false),
    ];
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(TimestampMicrosecondArray::from(vec![timestamp])),
        Arc::new(StringArray::from(vec![value])),
    ];
    if let Some(note) = note {
        fields.push(Field::new("note", DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(vec![Some(note)])));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("table batch")
}
fn assert_logical_projection(expected: &CapsuleManifest, actual: &CapsuleManifest) {
    assert_eq!(expected.entries.len(), actual.entries.len());
    for (expected_entry, actual_entry) in expected.entries.iter().zip(&actual.entries) {
        assert_eq!(expected_entry.path, actual_entry.path);
        assert_eq!(expected_entry.entry_type, actual_entry.entry_type);
        match (&expected_entry.node, &actual_entry.node) {
            (CapsuleNode::Directory, CapsuleNode::Directory) => {}
            (
                CapsuleNode::Symlink { target: expected },
                CapsuleNode::Symlink { target: actual },
            ) => {
                assert_eq!(expected, actual);
            }
            (
                CapsuleNode::Dynamic {
                    recipe: expected, ..
                },
                CapsuleNode::Dynamic { recipe: actual, .. },
            ) => {
                assert_eq!(expected, actual);
            }
            (
                CapsuleNode::Physical {
                    payload_kind: expected_kind,
                    leaves: expected_leaves,
                    ..
                },
                CapsuleNode::Physical {
                    payload_kind: actual_kind,
                    leaves: actual_leaves,
                    ..
                },
            ) => {
                assert_eq!(expected_kind, actual_kind);
                assert_eq!(expected_leaves.len(), actual_leaves.len());
                for (expected_leaf, actual_leaf) in expected_leaves.iter().zip(actual_leaves) {
                    assert_eq!(expected_leaf.logical_count, actual_leaf.logical_count);
                    assert_eq!(expected_leaf.source_timestamp, actual_leaf.source_timestamp);
                    assert_eq!(expected_leaf.min_event_time, actual_leaf.min_event_time);
                    assert_eq!(expected_leaf.max_event_time, actual_leaf.max_event_time);
                    assert_eq!(
                        expected_leaf.logical_attributes,
                        actual_leaf.logical_attributes
                    );
                }
            }
            (expected, actual) => panic!(
                "capsule node kind changed at {:?}: {expected:?} != {actual:?}",
                expected_entry.path
            ),
        }
    }
}

/// Build a small source pond exercising every capsule node kind, publish it
/// as a downloaded-capsule directory on disk, and return that directory
/// alongside the source ship (kept alive so its content tip stays put) and
/// the pre-import manifest for later comparison.
async fn build_source_capsule(
    temporary: &std::path::Path,
) -> (std::path::PathBuf, Ship, CapsuleManifest) {
    let mut ship = Ship::create_pond(temporary.join("source"), "capsule-import-test")
        .await
        .expect("create source pond");

    ship.write_transaction(&meta("content"), async move |transaction| {
        let root = transaction.root().await?;
        let _ = root.create_dir_all("/data").await?;
        let _ = root.create_dir_all("/system/run").await?;
        let _ = create_file_path(&root, "/data/plain.txt", b"plain bytes").await?;
        root.set_extended_attributes(
            "/data/plain.txt",
            HashMap::from([("capsule.test".to_string(), "plain".to_string())]),
        )
        .await?;

        for (index, bytes) in [b"first-".as_slice(), b"second-leaf".as_slice()]
            .into_iter()
            .enumerate()
        {
            let mut writer = root
                .async_writer_path_with_type("/data/log.series", EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(bytes).await?;
            writer.shutdown().await?;
            root.set_extended_attributes(
                "/data/log.series",
                HashMap::from([("capsule.test".to_string(), format!("leaf-{index}"))]),
            )
            .await?;
        }
        for (timestamp, value) in [(100, "a"), (200, "b"), (300, "c")] {
            let batch = table_batch(
                timestamp,
                value,
                (timestamp == 300).then_some("schema-evolved"),
            );
            let _ = root
                .write_series_from_batch("/data/table.series", &batch, Some("timestamp"))
                .await?;
        }
        let _ = root
            .create_symlink_path("/data/link", "/data/plain.txt")
            .await?;
        let _ = root
            .create_dynamic_path(
                "/system/run/10-capsule-import-test",
                EntryType::FileDynamic,
                "no-such-factory-is-registered",
                b"key: value\n".to_vec(),
            )
            .await?;
        Ok(())
    })
    .await
    .expect("write source content");

    let capsule = build_recovery_capsule(&ship)
        .await
        .expect("build recovery capsule");

    let remote_path = temporary.join("remote");
    let pond_id =
        uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("parse source pond id");
    let mut remote = ContentRemote::create_at(&remote_path, pond_id)
        .await
        .expect("create remote");
    let _ = steward::push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("native push");
    let _ = remote
        .publish_capsule_directory(&capsule.manifest, capsule.payloads.objects_dir())
        .await
        .expect("publish capsule directory");

    (remote_path, ship, capsule.manifest)
}
#[tokio::test]
async fn imports_every_node_kind_and_verifies_the_logical_contract() {
    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, source_manifest) = build_source_capsule(temporary.path()).await;

    let target = temporary.path().join("restored");
    let source_table = source_manifest
        .entries
        .iter()
        .find(|entry| entry.path == "/data/table.series")
        .expect("source table series");
    let CapsuleNode::Physical {
        schema_fingerprint,
        leaves,
        ..
    } = &source_table.node
    else {
        panic!("source table series must be physical")
    };
    assert_eq!(*schema_fingerprint, None);
    assert!(leaves.iter().all(|leaf| leaf.schema_fingerprint.is_some()));
    assert_ne!(leaves[0].schema_fingerprint, leaves[2].schema_fingerprint);
    let source_dynamic = source_manifest
        .entries
        .iter()
        .find(|entry| entry.path == "/system/run/10-capsule-import-test")
        .expect("source dynamic entry");
    let CapsuleNode::Dynamic {
        metadata: source_metadata,
        ..
    } = &source_dynamic.node
    else {
        panic!("source dynamic entry must be dynamic")
    };
    assert!(
        source_metadata.is_some(),
        "source dynamic mtime is captured"
    );
    let report = import_capsule(&capsule_dir, &target, "capsule-import-test-target")
        .await
        .expect("import capsule");

    assert_eq!(report.target, target);
    assert_eq!(report.entries, 9);
    assert_eq!(report.directories, 4);
    assert_eq!(report.physical, 3, "plain file, file series, table series");
    assert_eq!(report.symlinks, 1);
    assert_eq!(report.dynamic, 1);
    assert_ne!(report.target_pond_id, report.source_pond_id);

    let provenance_bytes =
        std::fs::read(target.join("CAPSULE_IMPORT_PROVENANCE.json")).expect("read provenance file");
    let provenance: steward::CapsuleImportProvenance =
        serde_json::from_slice(&provenance_bytes).expect("decode provenance");
    assert_eq!(provenance.source_pond_id, report.source_pond_id);
    assert_eq!(provenance.capsule_root, report.capsule_root.to_hex());

    // The restored pond must not appear as a tinyfs entry itself.
    let mut restored = Ship::open_pond(&target).await.expect("open restored pond");
    assert!(
        restored.control_table().post_commit_dispatch_suppressed(),
        "a promoted import must remain inert across reopen"
    );
    let rebuilt = build_recovery_capsule(&restored)
        .await
        .expect("rebuild capsule from restored pond");
    assert_logical_projection(&source_manifest, &rebuilt.manifest);
    let paths: Vec<&str> = rebuilt
        .manifest
        .entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    assert_eq!(
        paths,
        vec![
            "/",
            "/data",
            "/data/link",
            "/data/log.series",
            "/data/plain.txt",
            "/data/table.series",
            "/system",
            "/system/run",
            "/system/run/10-capsule-import-test",
        ]
    );

    restored
        .write_transaction(&meta("read-back"), async move |transaction| {
            let root = transaction.root().await?;
            let bytes = root.read_file_path_to_vec("/data/plain.txt").await?;
            assert_eq!(bytes, b"plain bytes");

            let series_bytes = root.read_file_path_to_vec("/data/log.series").await?;
            assert_eq!(series_bytes, b"first-second-leaf");

            // WD path resolution transparently follows symlinks (POSIX-style),
            // so reading through the recreated symlink must land on the same
            // bytes as reading its target directly.
            let via_symlink = root.read_file_path_to_vec("/data/link").await?;
            assert_eq!(via_symlink, b"plain bytes");
            let _ = create_file_path(&root, "/data/post-import-write.txt", b"still inert").await?;
            Ok(())
        })
        .await
        .expect("read back restored content");
    assert!(
        restored
            .control_table()
            .get_factory_mode("no-such-factory-is-registered")
            .is_none(),
        "the ordinary read-back transaction must not dispatch or default restored factories"
    );

    let dynamic_entry = rebuilt
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == "/system/run/10-capsule-import-test")
        .expect("dynamic entry present");
    let CapsuleNode::Dynamic {
        metadata: rebuilt_metadata,
        ..
    } = &dynamic_entry.node
    else {
        panic!("rebuilt dynamic entry must be dynamic")
    };
    assert_eq!(rebuilt_metadata, source_metadata);

    // The symlink's target payload hash must be preserved exactly, even
    // though the rebuilt manifest carries a fresh source_node_id and pond
    // identity.
    let source_link = source_manifest
        .entries
        .iter()
        .find(|entry| entry.path == "/data/link")
        .expect("source symlink entry present");
    let rebuilt_link = rebuilt
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == "/data/link")
        .expect("rebuilt symlink entry present");
    let (
        CapsuleNode::Symlink {
            target: source_target,
        },
        CapsuleNode::Symlink {
            target: rebuilt_target,
        },
    ) = (&source_link.node, &rebuilt_link.node)
    else {
        panic!("both entries must be symlinks");
    };
    assert_eq!(source_target, rebuilt_target);
}

#[tokio::test]
async fn refuses_to_import_over_an_existing_target() {
    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, _source_manifest) =
        build_source_capsule(temporary.path()).await;

    let target = temporary.path().join("restored");
    std::fs::create_dir_all(&target).expect("pre-create target");

    let error = import_capsule(&capsule_dir, &target, "capsule-import-test-target")
        .await
        .expect_err("import must refuse an existing target");
    assert!(
        error.to_string().contains("already exists"),
        "unexpected error message: {error}"
    );

    // No staging sibling should have been created for a rejection this early.
    let siblings: Vec<_> = std::fs::read_dir(temporary.path())
        .expect("read temp dir")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name())
        .collect();
    assert!(
        siblings
            .iter()
            .all(|name| !name.to_string_lossy().contains("capsule-import-")),
        "target-exists check must run before any staging directory is created: {siblings:?}"
    );
}
