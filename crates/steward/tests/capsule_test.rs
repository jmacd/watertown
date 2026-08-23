// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use steward::{Ship, build_recovery_capsule, build_recovery_capsule_incremental};
use sync_store::{
    CapsuleNode, CapsulePayloadKind, ContentRemote, ObjectHash, capsule_root,
    verify_capsule_directory,
};
use tempfile::tempdir;
use tinyfs::EntryType;
use tinyfs::arrow::ParquetExt;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::PondUserMetadata;
use tokio::io::AsyncWriteExt;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["capsule-test".into(), label.into()])
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

#[tokio::test]
async fn builds_portable_live_inventory_with_ordered_series_leaves() {
    let temporary = tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(temporary.path().join("pond"), "capsule-test")
        .await
        .expect("create pond");

    ship.write_transaction(&meta("content"), async move |transaction| {
        let root = transaction.root().await?;
        let _ = root.create_dir_all("/data").await?;
        let _ = create_file_path(&root, "/data/plain.txt", b"plain").await?;
        let large = vec![0x5a; 256 * 1024];
        let _ = create_file_path(&root, "/data/large.bin", &large).await?;

        for bytes in [b"first".as_slice(), b"second".as_slice()] {
            let mut writer = root
                .async_writer_path_with_type("/data/log.series", EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(bytes).await?;
            writer.shutdown().await?;
        }

        for (timestamp, value) in [(100, "a"), (200, "b")] {
            let _ = root
                .write_series_from_batch(
                    "/data/table.series",
                    &table_batch(timestamp, value),
                    Some("timestamp"),
                )
                .await?;
        }
        Ok(())
    })
    .await
    .expect("write content");

    let capsule = build_recovery_capsule(&ship).await.expect("build capsule");
    let rebuilt = build_recovery_capsule(&ship)
        .await
        .expect("rebuild capsule");
    assert_eq!(
        capsule_root(&capsule.manifest).expect("first root"),
        capsule_root(&rebuilt.manifest).expect("second root"),
        "unchanged source tip must produce one stable capsule generation"
    );
    capsule.manifest.validate().expect("valid manifest");
    assert_eq!(capsule.manifest.source.birthplace, "capsule-test");
    assert_eq!(
        capsule.manifest.payload_objects().expect("objects").len(),
        capsule.payloads.len()
    );
    for object in capsule.payloads.objects() {
        let bytes = std::fs::read(capsule.payloads.path(object.hash)).expect("staged payload");
        assert_eq!(object.hash, ObjectHash::of_bytes(&bytes));
        assert_eq!(object.size, bytes.len() as u64);
    }

    let paths: Vec<&str> = capsule
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
            "/data/large.bin",
            "/data/log.series",
            "/data/plain.txt",
            "/data/table.series"
        ]
    );

    let file_series = capsule
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == "/data/log.series")
        .expect("file series");
    let CapsuleNode::Physical {
        payload_kind,
        objects,
        leaves,
        ..
    } = &file_series.node
    else {
        panic!("file series must be physical")
    };
    assert_eq!(*payload_kind, CapsulePayloadKind::File);
    assert_eq!(objects.len(), 2);
    assert_eq!(leaves.len(), 2);
    assert_eq!(leaves[0].logical_count, 5);
    assert_eq!(leaves[1].logical_count, 6);

    let table_series = capsule
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == "/data/table.series")
        .expect("table series");
    let CapsuleNode::Physical {
        payload_kind,
        schema_fingerprint,
        objects,
        leaves,
        ..
    } = &table_series.node
    else {
        panic!("table series must be physical")
    };
    assert_eq!(*payload_kind, CapsulePayloadKind::Table);
    assert!(schema_fingerprint.is_some());
    assert_eq!(objects.len(), 2);
    assert_eq!(leaves.len(), 2);
    assert_eq!(leaves[0].logical_count, 1);
    assert_eq!(leaves[1].logical_count, 1);
    assert_ne!(leaves[0].logical_hash, leaves[1].logical_hash);

    assert_ne!(
        capsule_root(&capsule.manifest).expect("capsule root"),
        ObjectHash::of_bytes(b"")
    );

    let remote_path = temporary.path().join("remote");
    let pond_id = uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("pond id");
    let mut remote = ContentRemote::create_at(&remote_path, pond_id)
        .await
        .expect("create remote");
    let _ = steward::push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("native push");
    let _ = remote
        .publish_capsule_directory(&capsule.manifest, capsule.payloads.objects_dir())
        .await
        .expect("explicit prototype capsule publication");
    let verified = verify_capsule_directory(&remote_path).expect("verify downloaded capsule");
    assert_eq!(verified.entries, 6);
    assert_eq!(verified.logical_count, 18 + 256 * 1024);

    ship.write_transaction(&meta("incremental"), async move |transaction| {
        let root = transaction.root().await?;
        let _ = create_file_path(&root, "/data/new.txt", b"new").await?;
        Ok(())
    })
    .await
    .expect("append source content");
    let incremental = build_recovery_capsule_incremental(&ship, &capsule.manifest)
        .await
        .expect("incremental capsule");
    assert!(
        incremental.reused_payload_count() > 0,
        "unchanged source versions should inherit prior payloads"
    );
    assert!(
        incremental.payloads.len()
            < incremental
                .manifest
                .payload_objects()
                .expect("incremental payload closure")
                .len(),
        "only changed payloads should be restaged"
    );
    let _ = steward::push_content_to_remote(&ship, &mut remote, "main")
        .await
        .expect("incremental native push");
    let _ = remote
        .publish_capsule_incremental(
            &incremental.manifest,
            incremental.payloads.objects_dir(),
            &capsule.manifest,
        )
        .await
        .expect("explicit incremental prototype publication");
    let verified = verify_capsule_directory(&remote_path).expect("verify incremental capsule");
    assert_eq!(verified.entries, 7);
    assert_eq!(verified.logical_count, 21 + 256 * 1024);
}

#[tokio::test]
async fn rejects_empty_versions_inside_a_series() {
    let temporary = tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(temporary.path().join("pond"), "capsule-test")
        .await
        .expect("create pond");

    ship.write_transaction(&meta("empty-series"), async move |transaction| {
        let root = transaction.root().await?;
        for bytes in [b"first".as_slice(), b"".as_slice(), b"third".as_slice()] {
            let mut writer = root
                .async_writer_path_with_type("/series", EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(bytes).await?;
            writer.shutdown().await?;
        }
        Ok(())
    })
    .await
    .expect("write series");

    let error = build_recovery_capsule(&ship)
        .await
        .expect_err("capsule builder must not silently drop an empty series version");
    assert!(
        error.to_string().contains("empty file version"),
        "unexpected error: {error}"
    );
}
