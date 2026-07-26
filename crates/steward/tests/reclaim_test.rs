// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for reclamation -- the second half of version collapse.
//!
//! Collapse alone bounds a pond's growth RATE but never returns a byte: the
//! superseded rows survive, and each one still references its `_large_files`
//! blob.  These tests pin the two properties that make deleting them safe:
//! only rows no reader can see are removed, and a blob is swept only when NO
//! remaining row names its hash.
//!
//! The failure mode being guarded against is silent: sweeping a live blob
//! leaves a pond that looks healthy until something tries to read it.

use std::collections::HashSet;
use std::path::Path;

use steward::{FsckOptions, Ship};
use tempfile::tempdir;
use tlogfs::PondUserMetadata;
use tokio::io::AsyncWriteExt;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

/// Incompressible bytes, so every version really does exceed
/// `LARGE_FILE_THRESHOLD` and lands as its own blob rather than inline.
fn chunk(seed: u64, len: usize) -> Vec<u8> {
    // The seed must never collide across versions: identical content would
    // dedup to ONE blob and quietly weaken every assertion below.
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

/// Every `blake3=<hash>.parquet` currently on disk, flat or sharded.
fn blobs_on_disk(pond: &Path) -> HashSet<String> {
    let root = pond.join("data").join("_large_files");
    let mut found = HashSet::new();
    let mut dirs = vec![root];
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
                continue;
            }
            if let Some(hash) = entry
                .file_name()
                .to_str()
                .and_then(|n| n.strip_prefix("blake3="))
                .and_then(|n| n.strip_suffix(".parquet"))
            {
                let _ = found.insert(hash.to_string());
            }
        }
    }
    found
}

async fn write_file(ship: &mut Ship, path: &str, bytes: &[u8]) {
    let path = path.to_string();
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("write"), async move |fs| {
        let root = fs.root().await?;
        let _ = tinyfs::async_helpers::convenience::create_file_path(&root, &path, &bytes).await?;
        Ok(())
    })
    .await
    .expect("write transaction");
}

/// Append one version to a `FilePhysicalSeries` -- the kind collapse merges by
/// byte concatenation.
async fn append_series_version(ship: &mut Ship, path: &str, bytes: &[u8]) {
    let path = path.to_string();
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("series"), async move |fs| {
        let root = fs.root().await?;
        let mut writer = root
            .async_writer_path_with_type(&path, tinyfs::EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(&bytes).await?;
        writer.shutdown().await?;
        Ok(())
    })
    .await
    .expect("series transaction");
}

async fn read_bytes(ship: &mut Ship, path: &str) -> Vec<u8> {
    let tx = ship.begin_read(&meta("read")).await.expect("begin read");
    let root = tx.root().await.expect("root");
    root.read_file_path_to_vec(path).await.expect("read")
}

/// A collapse that merges versions must also delete the rows it superseded and
/// free the blobs those rows were the last referrers of -- otherwise collapse
/// changes only the growth rate and the pond never shrinks.
#[tokio::test]
async fn collapse_reclaims_superseded_rows_and_blobs() {
    let tmp = tempdir().expect("tempdir");
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(pond.clone(), "reclaim-src")
        .await
        .expect("create pond");

    // Twelve versions, each its own large blob, so a fanout-10 window has
    // something to merge and a loose tail survives it.
    let chunks: Vec<Vec<u8>> = (0..12).map(|i| chunk(i + 7, 96 * 1024)).collect();
    for bytes in &chunks {
        append_series_version(&mut ship, "/events.series", bytes).await;
    }
    let cumulative: Vec<u8> = chunks.concat();
    assert_eq!(read_bytes(&mut ship, "/events.series").await, cumulative);

    let before = blobs_on_disk(&pond);
    assert!(
        before.len() >= chunks.len(),
        "each version should have externalized its own blob, saw {}",
        before.len()
    );

    let report = ship.collapse_versions(1).await.expect("collapse");
    assert!(report.files_collapsed > 0, "series should have collapsed");
    assert!(
        report.reclaimed.rows_deleted > 0,
        "collapse must delete the rows it superseded"
    );
    assert!(
        report.reclaimed.blobs_removed > 0,
        "deleting the last referrers must free their blobs"
    );
    assert!(report.reclaimed.bytes_freed > 0, "freed bytes must be real");

    let after = blobs_on_disk(&pond);
    assert!(
        after.len() < before.len(),
        "blob count must fall: {} -> {}",
        before.len(),
        after.len()
    );

    // The whole point: the content is unchanged.
    assert_eq!(
        read_bytes(&mut ship, "/events.series").await,
        cumulative,
        "reclamation must not change what the series reads back"
    );

    // And the pond still verifies -- fsck's content pass reads EVERY surviving
    // row and requires its blob to exist, so a premature sweep fails here.
    let report = steward::fsck(&ship, FsckOptions::default())
        .await
        .expect("fsck");
    assert!(report.ok(), "fsck must pass after reclamation");
}

/// Blobs are content-addressed, so one file can back many rows.  Reclamation
/// must therefore mark-sweep by hash: a blob whose series version is superseded
/// is still live if any OTHER row shares its content.
#[tokio::test]
async fn shared_blob_survives_when_one_referrer_is_deleted() {
    let tmp = tempdir().expect("tempdir");
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(pond.clone(), "reclaim-shared")
        .await
        .expect("create pond");

    let chunks: Vec<Vec<u8>> = (0..12).map(|i| chunk(i + 31, 96 * 1024)).collect();
    for bytes in &chunks {
        append_series_version(&mut ship, "/events.series", bytes).await;
    }
    // A plain file with byte-identical content to the series' first version.
    // Both rows name the same blake3, so both name the same blob.
    write_file(&mut ship, "/keep.bin", &chunks[0]).await;

    let shared: HashSet<String> = blobs_on_disk(&pond);
    let report = ship.collapse_versions(1).await.expect("collapse");
    assert!(
        report.reclaimed.rows_deleted > 0,
        "rows should have been cut"
    );

    let survivors = blobs_on_disk(&pond);
    assert!(
        survivors.len() < shared.len(),
        "unshared superseded blobs should still be freed"
    );

    // The shared blob must have survived the sweep even though the version that
    // co-owned it is gone.
    let kept = read_bytes(&mut ship, "/keep.bin").await;
    assert_eq!(
        kept, chunks[0],
        "a blob shared with a live row must not be swept"
    );

    let report = steward::fsck(&ship, FsckOptions::default())
        .await
        .expect("fsck");
    assert!(report.ok(), "fsck must pass with a shared blob retained");
}

/// A second pass has nothing left to reclaim.  This is the direct guard against
/// the catastrophic case -- a sweep that keeps finding "garbage" is a sweep
/// that is eating live data.
#[tokio::test]
async fn reclamation_is_idempotent() {
    let tmp = tempdir().expect("tempdir");
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(pond.clone(), "reclaim-idem")
        .await
        .expect("create pond");

    for i in 0..12 {
        append_series_version(&mut ship, "/events.series", &chunk(i + 101, 96 * 1024)).await;
    }
    // Collapse to a fixed point first: with `max_live = 1` a single pass merges
    // one window, so the pond is only settled once a pass finds nothing.
    loop {
        let pass = ship.collapse_versions(1).await.expect("collapse pass");
        if pass.files_collapsed == 0 {
            break;
        }
    }
    let settled = blobs_on_disk(&pond);

    let second = ship.collapse_versions(1).await.expect("second collapse");
    assert_eq!(
        second.reclaimed.rows_deleted, 0,
        "no live row may be mistaken for garbage on a second pass"
    );
    assert_eq!(
        second.reclaimed.blobs_removed, 0,
        "no live blob may be mistaken for garbage on a second pass"
    );
    assert_eq!(blobs_on_disk(&pond), settled, "blob set must be stable");
}
