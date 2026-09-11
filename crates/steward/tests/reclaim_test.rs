// SPDX-License-Identifier: Apache-2.0

//! Native-v2 maintenance keeps user logical series append-only.
//!
//! The only collapsed rows produced by current code belong to the reserved
//! `.pond-node-index`; maintenance may delete those excluded pointers, but it
//! must never rewrite or reclaim user series rows or their payloads.

use steward::{FsckOptions, Ship};
use tempfile::tempdir;
use tinyfs::EntryType;
use tlogfs::PondUserMetadata;
use tokio::io::AsyncWriteExt;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["reclaim-test".into(), label.into()])
}

async fn append_file_series(ship: &mut Ship, bytes: &[u8]) {
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("append"), async move |transaction| {
        let root = transaction.root().await?;
        let mut writer = root
            .async_writer_path_with_type("/events.series", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(&bytes).await?;
        writer.shutdown().await?;
        Ok(())
    })
    .await
    .expect("append series");
}

async fn read_series(ship: &mut Ship) -> Vec<u8> {
    let tx = ship.begin_read(&meta("read")).await.expect("begin read");
    let root = tx.root().await.expect("root");
    let bytes = root
        .read_file_path_to_vec("/events.series")
        .await
        .expect("read series");
    _ = tx.commit().await.expect("close read");
    bytes
}

#[tokio::test]
async fn public_collapsing_writes_reject_both_user_series_kinds() {
    let directory = tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(directory.path().join("pond"), "collapse-retired")
        .await
        .expect("create pond");

    for (path, entry_type) in [
        ("/file.series", EntryType::FilePhysicalSeries),
        ("/table.series", EntryType::TablePhysicalSeries),
    ] {
        let error = ship
            .write_transaction(&meta("retired-collapse"), async move |transaction| {
                let root = transaction.root().await?;
                let _ = root
                    .async_writer_path_collapsing_with_type(path, entry_type)
                    .await?;
                Ok(())
            })
            .await
            .expect_err("native-v2 user-series collapse must be rejected");
        let message = error.to_string();
        assert!(message.contains("collapsing writes for user"));
        assert!(message.contains("append-only"));
        assert!(message.contains("pondcapsule.4"));
    }
}

#[tokio::test]
async fn maintenance_reclaims_only_reserved_index_rows_and_settles() {
    let directory = tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(directory.path().join("pond"), "index-reclaim")
        .await
        .expect("create pond");

    let mut expected = Vec::new();
    for index in 0..12u8 {
        let bytes = vec![b'a' + index; 4096];
        expected.extend_from_slice(&bytes);
        append_file_series(&mut ship, &bytes).await;
    }

    let root_before = steward::compute_content_tree(&ship)
        .await
        .expect("content root before maintenance")
        .root_tree_hash;
    let delta_before = ship.data_persistence().table().version();

    let first = ship
        .collapse_versions(1000)
        .await
        .expect("reserved-index maintenance");
    assert_eq!(first.candidates, 0);
    assert_eq!(
        first.reclaimed.rows_deleted, 11,
        "twelve content commits leave eleven superseded index pointers"
    );
    assert_eq!(read_series(&mut ship).await, expected);
    assert_eq!(
        steward::compute_content_tree(&ship)
            .await
            .expect("content root after maintenance")
            .root_tree_hash,
        root_before
    );
    assert!(
        ship.data_persistence().table().version() > delta_before,
        "reserved-index row deletion is recorded as Delta maintenance"
    );

    let second = ship
        .collapse_versions(1000)
        .await
        .expect("idempotent reserved-index maintenance");
    assert_eq!(second.reclaimed.rows_deleted, 0);
    assert_eq!(read_series(&mut ship).await, expected);

    let report = steward::fsck(&ship, FsckOptions::default())
        .await
        .expect("fsck");
    assert!(report.ok(), "fsck must pass after internal index reclaim");
}
