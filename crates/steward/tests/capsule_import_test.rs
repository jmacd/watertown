// SPDX-License-Identifier: Apache-2.0

//! Coverage for [`steward::import_capsule`], the generic staged importer
//! (`docs/recovery-capsule-design.md`, "Generic staged import").

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use steward::{Ship, build_recovery_capsule, import_capsule};
use sync_store::{CapsuleNode, ContentRemote};
use tempfile::tempdir;
use tinyfs::EntryType;
use tinyfs::arrow::ParquetExt;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::PondUserMetadata;
use tokio::io::AsyncWriteExt;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["capsule-import-test".into(), label.into()])
}

fn table_batch(timestamp: i64, value: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![timestamp])),
            Arc::new(StringArray::from(vec![value])),
        ],
    )
    .expect("record batch")
}

/// Build a small source pond exercising every capsule node kind, publish it
/// as a downloaded-capsule directory on disk, and return that directory
/// alongside the source ship (kept alive so its content tip stays put) and
/// the pre-import manifest for later comparison.
async fn build_source_capsule(
    temporary: &std::path::Path,
) -> (std::path::PathBuf, Ship, sync_store::CapsuleManifest) {
    let mut ship = Ship::create_pond(temporary.join("source"), "capsule-import-test")
        .await
        .expect("create source pond");

    ship.write_transaction(&meta("content"), async move |transaction| {
        let root = transaction.root().await?;
        let _ = root.create_dir_all("/data").await?;
        let _ = root.create_dir_all("/system/run").await?;
        let _ = create_file_path(&root, "/data/plain.txt", b"plain bytes").await?;

        for bytes in [b"first-".as_slice(), b"second-leaf".as_slice()] {
            let mut writer = root
                .async_writer_path_with_type("/data/log.series", EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(bytes).await?;
            writer.shutdown().await?;
        }
        for (timestamp, value) in [(100, "a"), (200, "b"), (300, "c")] {
            let _ = root
                .write_series_from_batch(
                    "/data/table.series",
                    &table_batch(timestamp, value),
                    Some("timestamp"),
                )
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
    assert!(matches!(dynamic_entry.node, CapsuleNode::Dynamic { .. }));

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
