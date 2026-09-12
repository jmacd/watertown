// SPDX-License-Identifier: Apache-2.0

//! Coverage for [`steward::import_capsule`], the generic staged importer
//! (`docs/recovery-capsule-design.md`, "Generic staged import").

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use steward::{
    CapsuleImportLimits, CapsuleImportProvenance, Ship, activate_capsule_import,
    build_recovery_capsule, import_capsule, import_capsule_with_limits,
};
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

fn published_capsule_root(capsule_dir: &std::path::Path) -> sync_store::content::ObjectHash {
    let root = std::fs::read_to_string(capsule_dir.join("recovery/refs/latest"))
        .expect("read capsule latest ref");
    sync_store::content::ObjectHash::from_hex(root.trim()).expect("parse capsule root")
}

async fn construct_unjournaled_staging(
    path: &std::path::Path,
    birthplace: &str,
    manifest: &CapsuleManifest,
    capsule_root: sync_store::content::ObjectHash,
) {
    let mut ship = Ship::create_pond(path, birthplace)
        .await
        .expect("construct initialized staging pond");
    ship.control_table_mut()
        .set_setting("post_commit_dispatch", "suppressed")
        .await
        .expect("suppress staged pond");
    drop(ship);
    let provenance = CapsuleImportProvenance {
        format: "pondcapsule.4-import-provenance.1".to_string(),
        source_pond_id: manifest.source.pond_id.clone(),
        source_birthplace: manifest.source.birthplace.clone(),
        source_tip: manifest.source.source_tip.to_hex(),
        capsule_root: capsule_root.to_hex(),
        importer_version: env!("CARGO_PKG_VERSION").to_string(),
        imported_at_micros: 0,
    };
    std::fs::write(
        path.join("CAPSULE_IMPORT_PROVENANCE.json"),
        serde_json::to_vec_pretty(&provenance).unwrap(),
    )
    .expect("write constructed provenance");
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
    assert!(report.batches >= 1);
    assert_ne!(report.target_pond_id, report.source_pond_id);

    let provenance_bytes =
        std::fs::read(target.join("CAPSULE_IMPORT_PROVENANCE.json")).expect("read provenance file");
    let provenance: CapsuleImportProvenance =
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
async fn resumes_multi_batch_import_without_duplicate_series_leaves() {
    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, source_manifest) = build_source_capsule(temporary.path()).await;
    let target = temporary.path().join("restored");
    let limits = CapsuleImportLimits {
        max_units: 1,
        max_logical_count: 1,
    };

    let stopped = import_capsule_with_limits(
        &capsule_dir,
        &target,
        "capsule-import-test-target",
        limits,
        Some(5),
    )
    .await
    .expect("partial import");
    assert!(stopped.is_none());
    assert!(!target.exists());
    let staging = std::fs::read_dir(temporary.path())
        .expect("read staging parent")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().contains(".restored.capsule-import-"))
        })
        .expect("resumable staging directory");
    assert!(
        std::fs::read_dir(&staging)
            .expect("read staging")
            .filter_map(Result::ok)
            .any(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with("CAPSULE_IMPORT_JOURNAL."))
    );
    std::fs::write(
        staging.join(".CAPSULE_IMPORT_CHECKPOINT_STAGING.interrupted"),
        b"{\"truncated\":",
    )
    .expect("construct interrupted staging checkpoint");

    let report = import_capsule_with_limits(
        &capsule_dir,
        &target,
        "capsule-import-test-target",
        limits,
        None,
    )
    .await
    .expect("resume import")
    .expect("completed report");
    assert!(report.batches > 5, "test must force several transactions");
    assert!(
        !target
            .join(".CAPSULE_IMPORT_CHECKPOINT_STAGING.interrupted")
            .exists(),
        "resume must ignore and clean an unpublished staging checkpoint"
    );

    let restored = Ship::open_pond(&target).await.expect("open restored pond");
    let rebuilt = build_recovery_capsule(&restored)
        .await
        .expect("rebuild restored capsule");
    assert_logical_projection(&source_manifest, &rebuilt.manifest);
    let file_leaves = match &rebuilt
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == "/data/log.series")
        .expect("file series")
        .node
    {
        CapsuleNode::Physical { leaves, .. } => leaves,
        other => panic!("unexpected node: {other:?}"),
    };
    assert_eq!(file_leaves.len(), 2);
    let table_leaves = match &rebuilt
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == "/data/table.series")
        .expect("table series")
        .node
    {
        CapsuleNode::Physical { leaves, .. } => leaves,
        other => panic!("unexpected node: {other:?}"),
    };
    assert_eq!(table_leaves.len(), 3);
}

#[tokio::test]
async fn foreign_deterministic_staging_without_journal_is_rejected() {
    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, source_manifest) = build_source_capsule(temporary.path()).await;
    let target = temporary.path().join("restored");
    let capsule_root = published_capsule_root(&capsule_dir);
    let staging = temporary.path().join(format!(
        ".restored.capsule-import-{}",
        capsule_root.to_hex()
    ));
    construct_unjournaled_staging(
        &staging,
        "capsule-import-test-target",
        &source_manifest,
        capsule_root,
    )
    .await;

    let error = import_capsule(&capsule_dir, &target, "capsule-import-test-target")
        .await
        .expect_err("unjournaled matching-name pond must not be adopted");
    assert!(error.to_string().contains("durable journal"));
    assert!(!target.exists());
    assert!(staging.exists());
}

#[tokio::test]
async fn retry_publishes_unjournaled_unique_initialization_directory() {
    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, _source_manifest) =
        build_source_capsule(temporary.path()).await;
    let target = temporary.path().join("restored");
    let capsule_root = published_capsule_root(&capsule_dir);
    let token = "constructed";
    let intent = temporary.path().join(format!(
        ".restored.capsule-init-{}-{token}.json",
        capsule_root.to_hex()
    ));
    std::fs::write(
        &intent,
        serde_json::to_vec_pretty(&serde_json::json!({
            "format": "pondcapsule.4-import-init.1",
            "token": token,
            "capsule_root": capsule_root.to_hex(),
            "target": target.to_str().unwrap(),
            "birthplace": "capsule-import-test-target",
        }))
        .unwrap(),
    )
    .unwrap();
    let container = temporary.path().join(format!(
        ".restored.capsule-init-{}-{token}.work",
        capsule_root.to_hex()
    ));
    std::fs::create_dir(&container).unwrap();
    let initialization = container.join("pond");
    let _ = Ship::create_pond(&initialization, "capsule-import-test-target")
        .await
        .expect("construct interrupted initialization before metadata");

    let report = import_capsule(&capsule_dir, &target, "capsule-import-test-target")
        .await
        .expect("ordinary retry recovers unpublished initialization directory");
    assert_eq!(report.capsule_root, capsule_root);
    assert!(target.exists());
    assert!(!intent.exists());
    assert!(!container.exists());
    assert!(!initialization.exists());
}

#[tokio::test]
async fn retry_recreates_importer_owned_partial_initialization_pond() {
    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, _source_manifest) =
        build_source_capsule(temporary.path()).await;
    let target = temporary.path().join("restored");
    let capsule_root = published_capsule_root(&capsule_dir);
    let token = "partial";
    let intent = temporary.path().join(format!(
        ".restored.capsule-init-{}-{token}.json",
        capsule_root.to_hex()
    ));
    std::fs::write(
        &intent,
        serde_json::to_vec_pretty(&serde_json::json!({
            "format": "pondcapsule.4-import-init.1",
            "token": token,
            "capsule_root": capsule_root.to_hex(),
            "target": target.to_str().unwrap(),
            "birthplace": "capsule-import-test-target",
        }))
        .unwrap(),
    )
    .unwrap();
    let container = temporary.path().join(format!(
        ".restored.capsule-init-{}-{token}.work",
        capsule_root.to_hex()
    ));
    std::fs::create_dir(&container).unwrap();
    let partial = container.join("pond");
    std::fs::create_dir(&partial).unwrap();
    std::fs::write(partial.join("partial"), b"incomplete").unwrap();

    let report = import_capsule(&capsule_dir, &target, "capsule-import-test-target")
        .await
        .expect("owned partial pond is safely recreated");
    assert_eq!(report.capsule_root, capsule_root);
    assert!(target.exists());
    assert!(!intent.exists());
    assert!(!container.exists());
}

#[tokio::test]
async fn foreign_initialization_container_without_intent_is_rejected() {
    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, _source_manifest) =
        build_source_capsule(temporary.path()).await;
    let target = temporary.path().join("restored");
    let capsule_root = published_capsule_root(&capsule_dir);
    let foreign = temporary.path().join(format!(
        ".restored.capsule-init-{}-foreign.work",
        capsule_root.to_hex()
    ));
    std::fs::create_dir(&foreign).unwrap();
    std::fs::write(foreign.join("keep"), b"foreign").unwrap();

    let error = import_capsule(&capsule_dir, &target, "capsule-import-test-target")
        .await
        .expect_err("unowned matching container must be rejected");
    assert!(
        error
            .to_string()
            .contains("no matching importer ownership intent")
    );
    assert_eq!(std::fs::read(foreign.join("keep")).unwrap(), b"foreign");
    assert!(!target.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn initialization_symlink_is_rejected_without_following_it() {
    use std::os::unix::fs::symlink;

    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, _source_manifest) =
        build_source_capsule(temporary.path()).await;
    let target = temporary.path().join("restored");
    let capsule_root = published_capsule_root(&capsule_dir);
    let token = "symlink";
    let intent = temporary.path().join(format!(
        ".restored.capsule-init-{}-{token}.json",
        capsule_root.to_hex()
    ));
    std::fs::write(
        &intent,
        serde_json::to_vec_pretty(&serde_json::json!({
            "format": "pondcapsule.4-import-init.1",
            "token": token,
            "capsule_root": capsule_root.to_hex(),
            "target": target.to_str().unwrap(),
            "birthplace": "capsule-import-test-target",
        }))
        .unwrap(),
    )
    .unwrap();
    let outside = temporary.path().join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("keep"), b"outside").unwrap();
    let container = temporary.path().join(format!(
        ".restored.capsule-init-{}-{token}.work",
        capsule_root.to_hex()
    ));
    symlink(&outside, &container).unwrap();

    let error = import_capsule(&capsule_dir, &target, "capsule-import-test-target")
        .await
        .expect_err("initialization symlink must be rejected");
    assert!(error.to_string().contains("not a regular directory"));
    assert_eq!(std::fs::read(outside.join("keep")).unwrap(), b"outside");
    assert!(!target.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn non_utf8_target_is_rejected_before_creating_import_artifacts() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, _source_manifest) =
        build_source_capsule(temporary.path()).await;
    let target = temporary
        .path()
        .join(OsString::from_vec(b"restored-\xff".to_vec()));
    let before = std::fs::read_dir(temporary.path()).unwrap().count();

    let error = import_capsule(&capsule_dir, &target, "capsule-import-test-target")
        .await
        .expect_err("non-UTF-8 target must be rejected");
    assert!(error.to_string().contains("not valid UTF-8"));
    assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), before);
    assert!(target.symlink_metadata().is_err());
}

#[tokio::test]
async fn table_payload_footer_opens_are_linear_across_import_and_resume() {
    const LEAVES: usize = 24;
    let temporary = tempdir().expect("tempdir");
    let mut source = Ship::create_pond(temporary.path().join("source"), "table-linear")
        .await
        .expect("create source");
    for index in 0..LEAVES {
        let batch = table_batch(index as i64, &format!("value-{index}"), None);
        source
            .write_transaction(&meta("table-leaf"), async move |transaction| {
                let root = transaction.root().await?;
                let _ = root
                    .write_series_from_batch("/table.series", &batch, Some("timestamp"))
                    .await?;
                Ok(())
            })
            .await
            .expect("append table leaf");
    }
    let capsule = build_recovery_capsule(&source)
        .await
        .expect("build capsule");
    let table = capsule
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == "/table.series")
        .expect("table entry");
    let CapsuleNode::Physical {
        objects, leaves, ..
    } = &table.node
    else {
        panic!("table entry must be physical")
    };
    assert_eq!(leaves.len(), LEAVES);
    assert!(
        objects.len() >= LEAVES / 2,
        "test requires many physical objects, got {}",
        objects.len()
    );
    let remote_path = temporary.path().join("remote");
    let pond_id = uuid::Uuid::parse_str(source.data_persistence().pond_id()).unwrap();
    let remote = ContentRemote::create_at(&remote_path, pond_id)
        .await
        .expect("create capsule remote");
    let _ = remote
        .publish_capsule_directory(&capsule.manifest, capsule.payloads.objects_dir())
        .await
        .expect("publish capsule");

    let fresh_target = temporary.path().join("restored-fresh");
    let fresh = import_capsule(&remote_path, &fresh_target, "table-linear-fresh")
        .await
        .expect("fresh import");
    assert!(
        fresh.table_payload_opens <= objects.len() * 2,
        "fresh import used {} opens for {} objects",
        fresh.table_payload_opens,
        objects.len()
    );
    assert!(
        fresh.table_object_range_checks <= LEAVES * 2,
        "{} range checks for {LEAVES} leaves indicates prefix rescanning",
        fresh.table_object_range_checks
    );

    let target = temporary.path().join("restored");
    let limits = CapsuleImportLimits {
        max_units: 1,
        max_logical_count: 1,
    };
    let stopped = import_capsule_with_limits(
        &remote_path,
        &target,
        "table-linear-target",
        limits,
        Some(3),
    )
    .await
    .expect("partial import");
    assert!(stopped.is_none());
    let report =
        import_capsule_with_limits(&remote_path, &target, "table-linear-target", limits, None)
            .await
            .expect("resume import")
            .expect("complete import");
    assert!(
        report.table_payload_opens <= objects.len() * 2,
        "{} opens for {} objects indicates prefix rescanning",
        report.table_payload_opens,
        objects.len()
    );
    assert!(
        report.table_payload_opens >= objects.len(),
        "resume rebuilds one O(objects) metadata index"
    );
    assert!(
        report.table_object_range_checks <= LEAVES * 2,
        "{} range checks for {LEAVES} leaves indicates prefix rescanning",
        report.table_object_range_checks
    );
}

#[tokio::test]
async fn file_payload_range_work_is_linear_across_import_and_resume() {
    const LEAVES: usize = 32;
    let temporary = tempdir().expect("tempdir");
    let mut source = Ship::create_pond(temporary.path().join("source"), "file-linear")
        .await
        .expect("create source");
    for index in 0..LEAVES {
        let bytes = format!("file-leaf-{index:04}-payload").into_bytes();
        source
            .write_transaction(&meta("file-leaf"), async move |transaction| {
                let root = transaction.root().await?;
                let mut writer = root
                    .async_writer_path_with_type("/file.series", EntryType::FilePhysicalSeries)
                    .await?;
                writer.write_all(&bytes).await?;
                writer.shutdown().await?;
                Ok(())
            })
            .await
            .expect("append file leaf");
    }
    let capsule = build_recovery_capsule(&source)
        .await
        .expect("build capsule");
    let file = capsule
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == "/file.series")
        .expect("file entry");
    let CapsuleNode::Physical {
        objects, leaves, ..
    } = &file.node
    else {
        panic!("file entry must be physical")
    };
    assert_eq!(leaves.len(), LEAVES);
    assert!(
        objects.len() >= LEAVES / 2,
        "test requires many physical objects, got {}",
        objects.len()
    );
    let remote_path = temporary.path().join("remote");
    let pond_id = uuid::Uuid::parse_str(source.data_persistence().pond_id()).unwrap();
    let remote = ContentRemote::create_at(&remote_path, pond_id)
        .await
        .expect("create capsule remote");
    let _ = remote
        .publish_capsule_directory(&capsule.manifest, capsule.payloads.objects_dir())
        .await
        .expect("publish capsule");

    let fresh_target = temporary.path().join("restored-fresh");
    let fresh = import_capsule(&remote_path, &fresh_target, "file-linear-fresh")
        .await
        .expect("fresh import");
    assert!(
        fresh.file_payload_opens <= objects.len() * 2,
        "fresh import used {} opens for {} objects",
        fresh.file_payload_opens,
        objects.len()
    );
    assert!(
        fresh.file_object_range_checks <= LEAVES * 2,
        "{} range checks for {LEAVES} leaves indicates prefix rescanning",
        fresh.file_object_range_checks
    );

    let target = temporary.path().join("restored");
    let limits = CapsuleImportLimits {
        max_units: 1,
        max_logical_count: 1,
    };
    let stopped =
        import_capsule_with_limits(&remote_path, &target, "file-linear-target", limits, Some(5))
            .await
            .expect("partial import");
    assert!(stopped.is_none());
    let report =
        import_capsule_with_limits(&remote_path, &target, "file-linear-target", limits, None)
            .await
            .expect("resume import")
            .expect("complete import");
    assert!(
        report.file_payload_opens <= objects.len() * 2,
        "{} opens for {} objects indicates prefix rescanning",
        report.file_payload_opens,
        objects.len()
    );
    assert!(
        report.file_object_range_checks <= LEAVES * 2,
        "{} range checks for {LEAVES} leaves indicates prefix rescanning",
        report.file_object_range_checks
    );
}

#[tokio::test]
async fn resume_rejects_conflicting_capsule_or_parameters() {
    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, _) = build_source_capsule(temporary.path()).await;
    let target = temporary.path().join("restored");
    let limits = CapsuleImportLimits {
        max_units: 1,
        max_logical_count: 1,
    };
    let _ = import_capsule_with_limits(
        &capsule_dir,
        &target,
        "capsule-import-test-target",
        limits,
        Some(1),
    )
    .await
    .expect("partial import");

    let error =
        import_capsule_with_limits(&capsule_dir, &target, "different-birthplace", limits, None)
            .await
            .expect_err("conflicting birthplace must fail");
    assert!(error.to_string().contains("resume parameters conflict"));

    let other_capsule_parent = temporary.path().join("other");
    std::fs::create_dir(&other_capsule_parent).expect("other source parent");
    let (other_capsule, _other_ship, _) = build_source_capsule(&other_capsule_parent).await;
    let error = import_capsule_with_limits(
        &other_capsule,
        &target,
        "capsule-import-test-target",
        limits,
        None,
    )
    .await
    .expect_err("conflicting capsule must fail");
    assert!(
        error
            .to_string()
            .contains("conflicting capsule import staging")
    );
}

#[tokio::test]
async fn resume_does_not_skip_a_corrupt_final_checkpoint() {
    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, _) = build_source_capsule(temporary.path()).await;
    let target = temporary.path().join("restored");
    let limits = CapsuleImportLimits {
        max_units: 1,
        max_logical_count: 1,
    };
    let _ = import_capsule_with_limits(
        &capsule_dir,
        &target,
        "capsule-import-test-target",
        limits,
        Some(1),
    )
    .await
    .expect("partial import");
    let staging = std::fs::read_dir(temporary.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().contains(".restored.capsule-import-"))
        })
        .expect("staging directory");
    std::fs::write(
        staging.join("CAPSULE_IMPORT_JOURNAL.99999999999999999999.json"),
        b"{\"truncated\":",
    )
    .expect("write corrupt final checkpoint");

    let error = import_capsule_with_limits(
        &capsule_dir,
        &target,
        "capsule-import-test-target",
        limits,
        None,
    )
    .await
    .expect_err("corrupt highest final checkpoint must fail");
    assert!(
        error.to_string().contains("decode durable journal"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn activation_refuses_invalid_run_config_and_keeps_pond_inert() {
    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, _) = build_source_capsule(temporary.path()).await;
    let target = temporary.path().join("restored");
    let _ = import_capsule(&capsule_dir, &target, "capsule-import-test-target")
        .await
        .expect("import capsule");

    let error = activate_capsule_import(&target)
        .await
        .expect_err("unknown restored factory must be refused");
    assert!(error.to_string().contains("no-such-factory-is-registered"));
    let restored = Ship::open_pond(&target)
        .await
        .expect("reopen restored pond");
    assert!(restored.control_table().post_commit_dispatch_suppressed());
}

#[tokio::test]
async fn activation_refuses_invalid_remote_config_and_keeps_pond_inert() {
    let temporary = tempdir().expect("tempdir");
    let mut source = Ship::create_pond(temporary.path().join("source"), "source")
        .await
        .expect("create source");
    let write_error = source
        .write_transaction(&meta("bad-remote"), async move |transaction| {
            let root = transaction.root().await?;
            let _ = root.create_dir_all("/sys/remotes").await?;
            let _ = create_file_path(&root, "/sys/remotes/bad", b"url: '[not a url'\n").await?;
            Ok(())
        })
        .await
        .expect_err("invalid remote must fail post-commit auto-push");
    assert!(
        write_error
            .to_string()
            .contains("post-commit auto-push failed after local transaction committed"),
        "the invalid config must remain durable for the activation test: {write_error}"
    );
    let capsule = build_recovery_capsule(&source)
        .await
        .expect("build capsule");
    let remote_path = temporary.path().join("remote");
    let pond_id = uuid::Uuid::parse_str(source.data_persistence().pond_id()).expect("pond id");
    let remote = ContentRemote::create_at(&remote_path, pond_id)
        .await
        .expect("create capsule remote");
    let _ = remote
        .publish_capsule_directory(&capsule.manifest, capsule.payloads.objects_dir())
        .await
        .expect("publish capsule");
    let target = temporary.path().join("restored");
    let _ = import_capsule(&remote_path, &target, "target")
        .await
        .expect("import capsule");

    let error = activate_capsule_import(&target)
        .await
        .expect_err("invalid remote YAML must be refused");
    assert!(error.to_string().contains("remote"));
    let restored = Ship::open_pond(&target)
        .await
        .expect("reopen restored pond");
    assert!(restored.control_table().post_commit_dispatch_suppressed());
}

#[tokio::test]
async fn activation_succeeds_after_safe_empty_preflight() {
    let temporary = tempdir().expect("tempdir");
    let mut source = Ship::create_pond(temporary.path().join("source"), "source")
        .await
        .expect("create source");
    source
        .write_transaction(&meta("content"), async move |transaction| {
            let root = transaction.root().await?;
            let _ = create_file_path(&root, "/content.txt", b"safe").await?;
            Ok(())
        })
        .await
        .expect("write source content");
    let capsule = build_recovery_capsule(&source)
        .await
        .expect("build capsule");
    let remote_path = temporary.path().join("remote");
    let pond_id = uuid::Uuid::parse_str(source.data_persistence().pond_id()).expect("pond id");
    let remote = ContentRemote::create_at(&remote_path, pond_id)
        .await
        .expect("create capsule remote");
    let _ = remote
        .publish_capsule_directory(&capsule.manifest, capsule.payloads.objects_dir())
        .await
        .expect("publish capsule");
    let target = temporary.path().join("restored");
    let _ = import_capsule(&remote_path, &target, "target")
        .await
        .expect("import capsule");

    let report = activate_capsule_import(&target)
        .await
        .expect("activate safe import");
    assert_eq!(report.remotes, 0);
    assert_eq!(report.run_configs, 0);
    let restored = Ship::open_pond(&target)
        .await
        .expect("reopen restored pond");
    assert!(!restored.control_table().post_commit_dispatch_suppressed());
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
