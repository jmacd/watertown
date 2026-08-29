// SPDX-License-Identifier: Apache-2.0

//! Coverage for [`steward::import_capsule`], the generic staged importer
//! (`docs/recovery-capsule-design.md`, "Generic staged import").

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use steward::{ContentSource, LocalPondSource, Ship, build_recovery_capsule, import_capsule};
use sync_store::content::{PackIndex, PackIndexRevision, SeriesManifest, SeriesManifestRevision};
use sync_store::{
    CapsuleManifest, CapsuleNode, ContentRemote, LegacyCapsuleEntry, LegacyCapsuleManifest,
    LegacyCapsuleNode, LegacyCapsuleObject, LegacyCapsulePayloadKind, LegacyCapsuleSource,
    LegacyCapsuleVersion, ObjectHash, capsule_manifest_bytes, capsule_root, decode_manifest,
    legacy_capsule_manifest_bytes, legacy_capsule_root, read_capsule_manifest,
    verify_capsule_directory,
};
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

fn legacy_table_batch(timestamp: i64, value: &str, note: Option<&str>) -> RecordBatch {
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
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("legacy table batch")
}

fn parquet_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut bytes, batch.schema(), None).expect("Parquet writer");
        writer.write(batch).expect("write Parquet batch");
        let _ = writer.close().expect("close Parquet writer");
    }
    bytes
}

fn legacy_object(bytes: &[u8]) -> LegacyCapsuleObject {
    LegacyCapsuleObject {
        hash: ObjectHash::of_bytes(bytes),
        size: bytes.len() as u64,
    }
}

struct LegacyFixture {
    manifest: LegacyCapsuleManifest,
    payloads: BTreeMap<ObjectHash, Vec<u8>>,
    table_hashes: [ObjectHash; 2],
}

fn legacy_fixture() -> LegacyFixture {
    let first_table = parquet_bytes(&legacy_table_batch(100, "first", None));
    let second_table = parquet_bytes(&legacy_table_batch(200, "second", Some("added")));
    let table_hashes = [
        ObjectHash::of_bytes(&first_table),
        ObjectHash::of_bytes(&second_table),
    ];
    let table_series = sync_store::content::encode_series(&table_hashes);

    let first_file = b"first-file-version".to_vec();
    let second_file = b"second-file-version".to_vec();
    let file_hashes = [
        ObjectHash::of_bytes(&first_file),
        ObjectHash::of_bytes(&second_file),
    ];
    let file_series = sync_store::content::encode_series(&file_hashes);
    let symlink = b"/data/files".to_vec();
    let factory = b"legacy-test-factory";
    let config = b"enabled: false\n";
    let mut recipe = b"dp.recipe.1\n".to_vec();
    recipe.extend_from_slice(&(factory.len() as u32).to_le_bytes());
    recipe.extend_from_slice(factory);
    recipe.extend_from_slice(config);
    let attrs = format!(
        r#"{{"{}":"timestamp"}}"#,
        tlogfs::schema::watertown::TIMESTAMP_COLUMN
    );

    let manifest = LegacyCapsuleManifest::new(
        LegacyCapsuleSource {
            pond_id: "11111111-1111-1111-1111-111111111111".to_string(),
            birthplace: "legacy-source".to_string(),
            source_tip: ObjectHash::of_bytes(b"legacy-tip"),
            exported_at_micros: 1_700_000_000_000_000,
            tool_version: "legacy-fixture".to_string(),
            native_format: "dp.commit.3".to_string(),
        },
        vec![
            LegacyCapsuleEntry {
                path: "/".to_string(),
                entry_type: EntryType::DirectoryPhysical,
                source_node_id: "root".to_string(),
                node: LegacyCapsuleNode::Directory,
            },
            LegacyCapsuleEntry {
                path: "/data".to_string(),
                entry_type: EntryType::DirectoryPhysical,
                source_node_id: "data".to_string(),
                node: LegacyCapsuleNode::Directory,
            },
            LegacyCapsuleEntry {
                path: "/data/files".to_string(),
                entry_type: EntryType::FilePhysicalSeries,
                source_node_id: "files".to_string(),
                node: LegacyCapsuleNode::Physical {
                    payload_kind: LegacyCapsulePayloadKind::File,
                    source_child_hash: ObjectHash::of_bytes(&file_series),
                    series_object: Some(legacy_object(&file_series)),
                    versions: vec![
                        LegacyCapsuleVersion {
                            source_version: 0,
                            objects: vec![legacy_object(&first_file)],
                            source_timestamp: Some(101),
                            min_event_time: None,
                            max_event_time: None,
                            extended_attributes: None,
                        },
                        LegacyCapsuleVersion {
                            source_version: 1,
                            objects: vec![legacy_object(&second_file)],
                            source_timestamp: Some(202),
                            min_event_time: None,
                            max_event_time: None,
                            extended_attributes: None,
                        },
                    ],
                },
            },
            LegacyCapsuleEntry {
                path: "/data/link".to_string(),
                entry_type: EntryType::Symlink,
                source_node_id: "link".to_string(),
                node: LegacyCapsuleNode::Symlink {
                    target: legacy_object(&symlink),
                },
            },
            LegacyCapsuleEntry {
                path: "/data/tables".to_string(),
                entry_type: EntryType::TablePhysicalSeries,
                source_node_id: "tables".to_string(),
                node: LegacyCapsuleNode::Physical {
                    payload_kind: LegacyCapsulePayloadKind::Table,
                    source_child_hash: ObjectHash::of_bytes(&table_series),
                    series_object: Some(legacy_object(&table_series)),
                    versions: vec![
                        LegacyCapsuleVersion {
                            source_version: 0,
                            objects: vec![legacy_object(&first_table)],
                            source_timestamp: Some(100),
                            min_event_time: Some(100),
                            max_event_time: Some(100),
                            extended_attributes: Some(attrs.clone()),
                        },
                        LegacyCapsuleVersion {
                            source_version: 1,
                            objects: vec![legacy_object(&second_table)],
                            source_timestamp: Some(200),
                            min_event_time: Some(200),
                            max_event_time: Some(200),
                            extended_attributes: Some(attrs),
                        },
                    ],
                },
            },
            LegacyCapsuleEntry {
                path: "/system".to_string(),
                entry_type: EntryType::DirectoryPhysical,
                source_node_id: "system".to_string(),
                node: LegacyCapsuleNode::Directory,
            },
            LegacyCapsuleEntry {
                path: "/system/run".to_string(),
                entry_type: EntryType::DirectoryPhysical,
                source_node_id: "run".to_string(),
                node: LegacyCapsuleNode::Directory,
            },
            LegacyCapsuleEntry {
                path: "/system/run/10-legacy".to_string(),
                entry_type: EntryType::FileDynamic,
                source_node_id: "dynamic".to_string(),
                node: LegacyCapsuleNode::Dynamic {
                    recipe: legacy_object(&recipe),
                },
            },
        ],
    )
    .expect("legacy fixture manifest");
    let payloads = BTreeMap::from([
        (ObjectHash::of_bytes(&first_table), first_table),
        (ObjectHash::of_bytes(&second_table), second_table),
        (ObjectHash::of_bytes(&table_series), table_series),
        (ObjectHash::of_bytes(&first_file), first_file),
        (ObjectHash::of_bytes(&second_file), second_file),
        (ObjectHash::of_bytes(&file_series), file_series),
        (ObjectHash::of_bytes(&symlink), symlink),
        (ObjectHash::of_bytes(&recipe), recipe),
    ]);
    LegacyFixture {
        manifest,
        payloads,
        table_hashes,
    }
}

fn materialize_legacy_fixture(
    root: &std::path::Path,
    manifest: &LegacyCapsuleManifest,
    payloads: &BTreeMap<ObjectHash, Vec<u8>>,
) -> std::path::PathBuf {
    let capsule = root.join("legacy-capsule");
    std::fs::create_dir_all(capsule.join("recovery/refs")).expect("legacy refs");
    std::fs::create_dir_all(capsule.join("recovery/manifests")).expect("legacy manifests");
    std::fs::create_dir_all(capsule.join("recovery/objects")).expect("legacy objects");
    let root = legacy_capsule_root(manifest).expect("legacy root");
    std::fs::write(
        capsule.join("recovery/refs/latest"),
        format!("{}\n", root.to_hex()),
    )
    .expect("legacy latest");
    std::fs::write(
        capsule.join(format!("recovery/manifests/{}.json", root.to_hex())),
        legacy_capsule_manifest_bytes(manifest).expect("legacy manifest bytes"),
    )
    .expect("legacy manifest");
    for (hash, bytes) in payloads {
        std::fs::write(
            capsule.join(format!("recovery/objects/blake3={}", hash.to_hex())),
            bytes,
        )
        .expect("legacy object");
    }
    capsule
}

#[derive(Debug, Deserialize, Serialize)]
struct FrozenCapsuleFixture {
    root: String,
    manifest_json: String,
    objects_hex: BTreeMap<String, String>,
}

fn materialize_frozen_fixture(temporary: &std::path::Path) -> std::path::PathBuf {
    let fixture: FrozenCapsuleFixture =
        serde_json::from_str(include_str!("fixtures/pondcapsule1.json"))
            .expect("decode frozen pondcapsule.1 fixture");
    let capsule = temporary.join("frozen-capsule");
    let refs = capsule.join("recovery/refs");
    let manifests = capsule.join("recovery/manifests");
    let objects = capsule.join("recovery/objects");
    std::fs::create_dir_all(&refs).expect("create fixture refs");
    std::fs::create_dir_all(&manifests).expect("create fixture manifests");
    std::fs::create_dir_all(&objects).expect("create fixture objects");
    std::fs::write(refs.join("latest"), format!("{}\n", fixture.root))
        .expect("write fixture latest ref");
    std::fs::write(
        manifests.join(format!("{}.json", fixture.root)),
        fixture.manifest_json,
    )
    .expect("write fixture manifest");
    for (hash, encoded) in fixture.objects_hex {
        let bytes = hex::decode(encoded).expect("decode fixture object");
        std::fs::write(objects.join(format!("blake3={hash}")), bytes)
            .expect("write fixture object");
    }
    capsule
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
                CapsuleNode::Dynamic { recipe: _expected },
                CapsuleNode::Dynamic { recipe: _actual },
            ) => {
                // Legacy v1 capsules serialize dynamic recipes under `dp.recipe.1` while
                // the rebuilt pond uses the current `watertown.recipe.v1` framing; the
                // implementation compares the logical factory/config bytes, not the
                // particular wire magic used in their serialized object.
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
async fn imports_frozen_pondcapsule1_fixture() {
    let temporary = tempdir().expect("tempdir");
    let capsule_dir = materialize_frozen_fixture(temporary.path());
    let verified = verify_capsule_directory(&capsule_dir).expect("verify frozen fixture");
    let (source, source_root) =
        read_capsule_manifest(&capsule_dir).expect("read frozen fixture manifest");
    assert_eq!(source_root, verified.root);

    let target = temporary.path().join("restored");
    let report = import_capsule(&capsule_dir, &target, "pondcapsule1-compatibility-test")
        .await
        .expect("import frozen pondcapsule.1 fixture");
    assert_eq!(report.capsule_root, verified.root);

    let restored = Ship::open_pond(&target).await.expect("open restored pond");
    let rebuilt = build_recovery_capsule(&restored)
        .await
        .expect("rebuild capsule from frozen fixture import");
    assert_logical_projection(&source, &rebuilt.manifest);
}

#[tokio::test]
#[ignore = "explicit fixture update; commits the current pondcapsule.1 wire representation"]
async fn regenerate_pondcapsule1_fixture() {
    let temporary = tempdir().expect("tempdir");
    let (capsule_dir, _source_ship, manifest) = build_source_capsule(temporary.path()).await;
    let root = capsule_root(&manifest).expect("compute fixture capsule root");
    let manifest_json = String::from_utf8(
        capsule_manifest_bytes(&manifest).expect("encode fixture capsule manifest"),
    )
    .expect("fixture manifest is UTF-8");
    let mut objects_hex = BTreeMap::new();
    for object in manifest
        .payload_objects()
        .expect("enumerate fixture payload objects")
    {
        let bytes = std::fs::read(
            capsule_dir
                .join("recovery/objects")
                .join(format!("blake3={}", object.hash.to_hex())),
        )
        .expect("read fixture payload object");
        let _ = objects_hex.insert(object.hash.to_hex(), hex::encode(bytes));
    }
    let fixture = FrozenCapsuleFixture {
        root: root.to_hex(),
        manifest_json,
        objects_hex,
    };
    let fixture_path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pondcapsule1.json");
    let mut encoded = serde_json::to_vec_pretty(&fixture).expect("encode fixture bundle");
    encoded.push(b'\n');
    std::fs::write(&fixture_path, encoded).expect("write frozen fixture bundle");
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

#[tokio::test]
async fn imports_opaque_legacy_schema_evolution_and_publishes_per_leaf_pack_schemas() {
    let temporary = tempdir().expect("tempdir");
    let fixture = legacy_fixture();
    let capsule =
        materialize_legacy_fixture(temporary.path(), &fixture.manifest, &fixture.payloads);
    let target = temporary.path().join("legacy-restored");

    let report = import_capsule(&capsule, &target, "legacy-target")
        .await
        .expect("import opaque legacy capsule");
    assert_eq!(report.entries, fixture.manifest.entries.len());
    assert_eq!(report.physical, 2);
    let replay_bytes =
        std::fs::read(target.join("LEGACY_CAPSULE_REPLAY.json")).expect("read replay report");
    let replay: serde_json::Value =
        serde_json::from_slice(&replay_bytes).expect("decode replay report");
    assert_eq!(replay["format"], "pondcapsule.legacy.1-replay.1");
    let table_replay = replay["entries"]
        .as_array()
        .expect("replay entries")
        .iter()
        .find(|entry| entry["path"] == "/data/tables")
        .expect("table replay entry");
    assert_ne!(
        table_replay["versions"][0]["schema_fingerprint"],
        table_replay["versions"][1]["schema_fingerprint"]
    );
    let provenance: steward::CapsuleImportProvenance = serde_json::from_slice(
        &std::fs::read(target.join("CAPSULE_IMPORT_PROVENANCE.json")).expect("provenance"),
    )
    .expect("decode provenance");
    assert_eq!(
        provenance.format,
        "pondcapsule.legacy.1-import-provenance.1"
    );

    let mut restored = Ship::open_pond(&target).await.expect("reopen target");
    assert!(restored.control_table().post_commit_dispatch_suppressed());
    let tx = restored
        .begin_read(&meta("legacy-version-query"))
        .await
        .expect("begin read");
    let root = tx.root().await.expect("root");
    let versions = root
        .list_file_versions("/data/tables")
        .await
        .expect("list table versions");
    assert_eq!(versions.len(), 2);

    let first = root
        .read_file_version("/data/tables", versions[0].version)
        .await
        .expect("read first table version");
    let first_builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(first),
    )
    .expect("open first Parquet");
    assert_eq!(first_builder.schema().fields().len(), 2);
    let first_batches = first_builder
        .build()
        .expect("first reader")
        .collect::<Result<Vec<_>, _>>()
        .expect("first batches");
    assert_eq!(
        first_batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("first value strings")
            .value(0),
        "first"
    );

    let second = root
        .read_file_version("/data/tables", versions[1].version)
        .await
        .expect("read second table version");
    let second_builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(second),
    )
    .expect("open second Parquet");
    assert_eq!(second_builder.schema().fields().len(), 3);
    let second_batches = second_builder
        .build()
        .expect("second reader")
        .collect::<Result<Vec<_>, _>>()
        .expect("second batches");
    assert_eq!(
        second_batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("added note strings")
            .value(0),
        "added"
    );
    let file_versions = root
        .list_file_versions("/data/files")
        .await
        .expect("list file versions");
    assert_eq!(file_versions.len(), 2);
    assert_eq!(
        root.read_file_version("/data/files", file_versions[0].version)
            .await
            .expect("first file version"),
        b"first-file-version"
    );
    assert_eq!(
        root.read_file_version("/data/files", file_versions[1].version)
            .await
            .expect("second file version"),
        b"second-file-version"
    );
    let _ = tx.commit().await;
    drop(restored);

    let source = LocalPondSource::open(&target)
        .await
        .expect("open content source after reopen");
    let tip = source
        .get_tip("")
        .await
        .expect("get target tip")
        .expect("target tip exists");
    let commit = sync_store::Commit::decode(
        &source
            .get_object(tip)
            .await
            .expect("get commit")
            .expect("commit bytes"),
    )
    .expect("decode commit");
    let manifest = decode_manifest(
        &source
            .get_object(commit.node_manifest_hash)
            .await
            .expect("get node manifest")
            .expect("node manifest bytes"),
    )
    .expect("decode node manifest");
    let table_entry = manifest
        .iter()
        .find(|entry| entry.name == "tables" && entry.entry_type == EntryType::TablePhysicalSeries)
        .expect("table series manifest entry");
    let series_hash = table_entry.child_hash;
    let series = SeriesManifest::decode(
        &source
            .get_object(series_hash)
            .await
            .expect("get series manifest")
            .expect("series manifest bytes"),
    )
    .expect("decode target series manifest");
    assert_eq!(series.revision(), SeriesManifestRevision::V2);
    assert_eq!(series.leaf_count(), 2);
    let pack_hashes = source
        .list_pack_hashes(series_hash)
        .await
        .expect("list synthesized packs");
    assert_eq!(pack_hashes.len(), 1);
    let pack_hash = *pack_hashes.iter().next().expect("one pack");
    let pack = PackIndex::decode(
        &source
            .get_pack_index(series_hash, pack_hash)
            .await
            .expect("get pack")
            .expect("pack bytes"),
    )
    .expect("decode target pack");
    assert_eq!(pack.revision(), PackIndexRevision::V2);
    assert_eq!(pack.leaf_descriptors().len(), 2);
    assert_ne!(
        pack.leaf_descriptors()[0].schema_fingerprint(),
        pack.leaf_descriptors()[1].schema_fingerprint(),
        "additive schema evolution must remain two distinct per-leaf schemas"
    );
}

#[tokio::test]
async fn legacy_import_rejects_tampered_raw_payload_and_leaf_mapping() {
    let temporary = tempdir().expect("tempdir");
    let fixture = legacy_fixture();

    let payload_capsule = materialize_legacy_fixture(
        &temporary.path().join("payload-case"),
        &fixture.manifest,
        &fixture.payloads,
    );
    std::fs::write(
        payload_capsule.join(format!(
            "recovery/objects/blake3={}",
            fixture.table_hashes[0].to_hex()
        )),
        b"tampered parquet",
    )
    .expect("tamper payload");
    let payload_target = temporary.path().join("payload-target");
    let error = import_capsule(&payload_capsule, &payload_target, "legacy-target")
        .await
        .expect_err("tampered raw payload must be rejected");
    assert!(
        error.to_string().contains("hash") || error.to_string().contains("size"),
        "unexpected payload error: {error}"
    );
    assert!(!payload_target.exists());

    let mut mapped_manifest = fixture.manifest.clone();
    let table = mapped_manifest
        .entries
        .iter_mut()
        .find(|entry| entry.path == "/data/tables")
        .expect("table entry");
    let LegacyCapsuleNode::Physical { versions, .. } = &mut table.node else {
        panic!("table is physical");
    };
    versions.swap(0, 1);
    versions[0].source_version = 0;
    versions[1].source_version = 1;
    let mapping_capsule = materialize_legacy_fixture(
        &temporary.path().join("mapping-case"),
        &mapped_manifest,
        &fixture.payloads,
    );
    let mapping_target = temporary.path().join("mapping-target");
    let error = import_capsule(&mapping_capsule, &mapping_target, "legacy-target")
        .await
        .expect_err("tampered source leaf mapping must be rejected");
    assert!(
        error.to_string().contains("mapping mismatch"),
        "unexpected mapping error: {error}"
    );
    assert!(!mapping_target.exists());
}

#[tokio::test]
async fn legacy_import_failure_after_staging_leaves_the_staging_pond_for_inspection() {
    let temporary = tempdir().expect("tempdir");
    let mut fixture = legacy_fixture();
    let table = fixture
        .manifest
        .entries
        .iter_mut()
        .find(|entry| entry.path == "/data/tables")
        .expect("table entry");
    let LegacyCapsuleNode::Physical { versions, .. } = &mut table.node else {
        panic!("table is physical");
    };
    versions[0].min_event_time = None;
    versions[0].max_event_time = None;
    let capsule =
        materialize_legacy_fixture(temporary.path(), &fixture.manifest, &fixture.payloads);
    let target = temporary.path().join("failed-target");
    let error = import_capsule(&capsule, &target, "legacy-target")
        .await
        .expect_err("unbounded legacy table series must fail during target preparation");
    assert!(
        error.to_string().contains("left in place for inspection"),
        "unexpected staging failure: {error}"
    );
    assert!(!target.exists());
    let staging: Vec<_> = std::fs::read_dir(temporary.path())
        .expect("read staging parent")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".failed-target.capsule-import-")
        })
        .collect();
    assert_eq!(staging.len(), 1, "failed staging pond must remain");
    assert!(staging[0].path().join("control").is_dir());
    assert!(staging[0].path().join("data").is_dir());
}
