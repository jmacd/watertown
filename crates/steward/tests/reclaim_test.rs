// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for reclamation -- the second half of version collapse.
//!
//! Collapse alone bounds a pond's growth RATE but never returns a byte: the
//! superseded rows survive, and each one still references its `_large_files`
//! blob. These tests pin the two properties that make deleting them safe:
//! only rows no reader can see are removed, and a blob is swept only when NO
//! remaining row names its hash.
//!
//! Native writes are now always logical-series-v2 (delivery gate 7): the
//! row-rewriting merge `Ship::collapse_versions` used to perform no longer
//! exists at all -- `tlogfs::collapse_file_series`/`collapse_table_series`
//! themselves now unconditionally return `TLogFSError::CollapseUnsupported`
//! and cannot merge rows at all, in production or in a test.
//! `Ship::collapse_versions` instead performs pack-only physical maintenance
//! (repacking over-threshold series into bounded packs under `_packs/`; see
//! `docs/logical-series-identity-design.md`), which never rewrites/deletes
//! Oplog rows and so can never itself create a superseded row.
//!
//! `Ship::collapse_versions` still runs reclamation unconditionally after
//! pack maintenance -- otherwise reclamation would be permanently
//! unreachable for any pond that always has a real pack-maintenance
//! candidate. These tests build genuinely superseded rows through the one
//! production write path left that can create them,
//! `WD::async_writer_path_collapsing_with_type` (used by content-pull
//! replication to mirror a source pond's own collapse), then drive
//! `Ship::collapse_versions` and assert reclamation actually deletes the
//! superseded rows and sweeps the blobs only they referenced -- not merely
//! that pack maintenance ran.
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

/// Supersede every earlier version of a `FilePhysicalSeries` with one fresh
/// baseline holding `full_content` (the concatenation of everything the
/// series should read back as). This is the one production write path left
/// that can create genuinely superseded rows for reclaim to act on --
/// `WD::async_writer_path_collapsing_with_type`, used by content-pull
/// replication to mirror a source pond's own collapse. Unlike the disabled
/// row-rewriting `collapse_file_series`, the caller supplies the full
/// resulting content directly; nothing is concatenated for it.
async fn mirror_collapse_series(ship: &mut Ship, path: &str, full_content: &[u8]) {
    let path = path.to_string();
    let full_content = full_content.to_vec();
    ship.write_transaction(&meta("mirror-collapse"), async move |fs| {
        let root = fs.root().await?;
        let mut writer = root
            .async_writer_path_collapsing_with_type(&path, tinyfs::EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(&full_content).await?;
        writer.shutdown().await?;
        Ok(())
    })
    .await
    .expect("mirror-collapse transaction");
}

async fn read_bytes(ship: &mut Ship, path: &str) -> Vec<u8> {
    let tx = ship.begin_read(&meta("read")).await.expect("begin read");
    let root = tx.root().await.expect("root");
    root.read_file_path_to_vec(path).await.expect("read")
}

/// A mirror-collapse that supersedes every earlier version must let
/// reclamation actually delete the superseded rows and free the blobs those
/// rows were the last referrers of.
///
/// This is the core reclaim-path regression guard: it must exercise
/// `reclaim_superseded` deleting real rows and sweeping real blobs, not
/// merely observe that the (separately gated) row-rewriting merge refuses to
/// run. The row-rewriting merge itself is pinned unavailable in
/// `crates/steward/src/ship.rs`'s
/// `test_collapse_versions_is_gated_once_a_real_candidate_exists`.
#[tokio::test]
async fn collapse_reclaims_superseded_rows_and_blobs() {
    let tmp = tempdir().expect("tempdir");
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(pond.clone(), "reclaim-src")
        .await
        .expect("create pond");

    // Twelve versions, each its own large blob.
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

    // Mirror-collapse the whole history into one fresh baseline, exactly as
    // content-pull replication would when mirroring a source pond's collapse.
    // Every one of the twelve loose versions above becomes superseded.
    mirror_collapse_series(&mut ship, "/events.series", &cumulative).await;
    assert_eq!(read_bytes(&mut ship, "/events.series").await, cumulative);

    // With only one live version left, this is no longer a collapse
    // candidate, so `collapse_versions` succeeds (Ok) rather than hitting the
    // row-rewriting gate -- but it still runs reclamation, which must delete
    // the twelve now-superseded rows and sweep their now-unreferenced blobs.
    let report = ship
        .collapse_versions(1)
        .await
        .expect("no candidates remain, so collapse_versions must not be gated");
    assert_eq!(
        report.candidates, 0,
        "the merged series has one live version, below any threshold"
    );
    // Reclamation also reclaims the pond's own internal checkpoint/index
    // series (`tinyfs::INDEX_NODE_UUID`), which rolls forward on every commit
    // and so accumulates its own superseded rows across these thirteen write
    // transactions -- `>=`, not `==`, is therefore the correct bound for our
    // series' twelve superseded loose versions.
    assert!(
        report.reclaimed.rows_deleted >= chunks.len(),
        "every superseded loose version of our series must be reclaimed, got {} \
         (>= {} expected)",
        report.reclaimed.rows_deleted,
        chunks.len()
    );
    assert!(
        report.reclaimed.blobs_removed > 0,
        "the swept rows' now-unreferenced blobs must actually be freed"
    );

    let after = blobs_on_disk(&pond);
    assert!(
        after.len() < before.len(),
        "reclamation must have actually shrunk the blob set: before={}, after={}",
        before.len(),
        after.len()
    );

    assert_eq!(
        read_bytes(&mut ship, "/events.series").await,
        cumulative,
        "reclamation must not change what the series reads back"
    );

    let report = steward::fsck(&ship, FsckOptions::default())
        .await
        .expect("fsck");
    assert!(report.ok(), "fsck must pass after a real reclaim");
}

/// Blobs are content-addressed, so one file can back many rows. Reclamation
/// mark-sweeps by hash so a blob whose series version is superseded stays
/// live if any OTHER row shares its content.
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

    let cumulative: Vec<u8> = chunks.concat();
    mirror_collapse_series(&mut ship, "/events.series", &cumulative).await;

    let report = ship
        .collapse_versions(1)
        .await
        .expect("no candidates remain after the mirror-collapse");
    assert!(
        report.reclaimed.rows_deleted > 0,
        "the twelve superseded loose versions must be reclaimed"
    );

    let survivors = blobs_on_disk(&pond);
    let shared_hash = blake3::hash(&chunks[0]).to_hex().to_string();
    assert!(
        survivors.contains(&shared_hash),
        "a blob shared with a live row (/keep.bin) must survive reclamation"
    );

    // The shared blob (and the file that names it) must be untouched.
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

/// A second reclaim-driving call, once nothing further is superseded, must be
/// a stable no-op: no additional row is deleted and no additional blob is
/// swept.
#[tokio::test]
async fn reclamation_is_idempotent() {
    let tmp = tempdir().expect("tempdir");
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(pond.clone(), "reclaim-idem")
        .await
        .expect("create pond");

    let chunks: Vec<Vec<u8>> = (0..12).map(|i| chunk(i + 101, 96 * 1024)).collect();
    for bytes in &chunks {
        append_series_version(&mut ship, "/events.series", bytes).await;
    }
    let cumulative: Vec<u8> = chunks.concat();
    mirror_collapse_series(&mut ship, "/events.series", &cumulative).await;

    let first = ship
        .collapse_versions(1)
        .await
        .expect("first reclaim-driving call");
    assert!(
        first.reclaimed.rows_deleted > 0,
        "the first call must actually reclaim the superseded versions"
    );
    let settled = blobs_on_disk(&pond);

    let second = ship
        .collapse_versions(1)
        .await
        .expect("second reclaim-driving call");
    assert_eq!(
        second.reclaimed.rows_deleted, 0,
        "nothing new is superseded, so a second call must delete nothing"
    );
    assert_eq!(
        blobs_on_disk(&pond),
        settled,
        "blob set must be stable once nothing further is superseded"
    );
}

/// Collapse + reclaim must leave the pond fully readable and internally
/// consistent: every appended byte and an unrelated bystander file both
/// survive, and the pond still verifies.
#[tokio::test]
async fn collapse_and_reclaim_leave_a_consistent_pond() {
    let tmp = tempdir().expect("tempdir");
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(pond.clone(), "reclaim-consistent")
        .await
        .expect("create pond");

    let chunks: Vec<Vec<u8>> = (0..12).map(|i| chunk(i + 7, 96 * 1024)).collect();
    for bytes in &chunks {
        append_series_version(&mut ship, "/events.series", bytes).await;
    }
    write_file(&mut ship, "/bystander.txt", b"untouched").await;

    let cumulative: Vec<u8> = chunks.concat();
    mirror_collapse_series(&mut ship, "/events.series", &cumulative).await;

    let report = ship
        .collapse_versions(1)
        .await
        .expect("no candidates remain after the mirror-collapse");
    assert!(report.reclaimed.rows_deleted > 0);

    let bytes = read_bytes(&mut ship, "/events.series").await;
    assert_eq!(
        bytes.len(),
        12 * 96 * 1024,
        "every appended byte must survive collapse + reclaim"
    );
    assert_eq!(
        read_bytes(&mut ship, "/bystander.txt").await,
        b"untouched",
        "reclamation must not touch an unrelated file"
    );

    let fsck = steward::fsck(&ship, FsckOptions::default())
        .await
        .expect("fsck after collapse + reclaim");
    assert!(
        fsck.errors.is_empty(),
        "fsck must find no dangling blob after collapse + reclaim: {:?}",
        fsck.errors
    );
}

/// Item 6: "Packs and their physical objects are GC roots where applicable
/// before any local pack publication is enabled."
///
/// A native v2 series' rows never gain `collapsed_from`/`collapsed_through`
/// from pack-only maintenance (it never rewrites Oplog rows at all), so
/// reclamation's mark-sweep can never find them "superseded" by
/// [`find_superseded`] and can never touch a live v2 series' rows or the
/// blobs they reference. This test proves that in a single maintenance pass
/// that does real work on *both* halves at once: the native series (3 live
/// versions, never collapsed) is a genuine pack-maintenance candidate and is
/// actually repacked into a new, bounded, disk-published pack, while an
/// unrelated mirror-collapsed legacy series has superseded rows to delete and
/// blobs to sweep. The freshly published native pack -- a GC root -- must
/// survive that same reclaim pass untouched, and the series' live content
/// must be completely unaffected by having been repacked.
#[tokio::test]
async fn reclaim_does_not_orphan_a_native_v2_series_pack() {
    use steward::{ContentSource, LocalPondSource};
    use sync_store::content::{Commit, ObjectHash, PackIndex, decode_manifest};
    use tinyfs::EntryType;
    use tokio::io::AsyncWriteExt;

    let tmp = tempdir().expect("tempdir");
    let pond = tmp.path().join("pond");
    let mut ship = Ship::create_pond(pond.clone(), "reclaim-pack-gc-root")
        .await
        .expect("create pond");

    // The native v2 series whose pack must survive reclaim untouched.
    let native_chunks: Vec<Vec<u8>> = vec![
        b"native-zero".to_vec(),
        b"native-one".to_vec(),
        b"native-two".to_vec(),
    ];
    for bytes in &native_chunks {
        let bytes = bytes.clone();
        ship.write_transaction(&meta("native-append"), async move |fs| {
            let root = fs.root().await?;
            let mut writer = root
                .async_writer_path_with_type("/native.series", EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(&bytes).await?;
            writer.shutdown().await?;
            Ok(())
        })
        .await
        .expect("append native series version");
    }

    // Unrelated genuinely-collapsible content, so reclamation has real work
    // to do in the same pass (not merely a no-op confirming nothing happens
    // anywhere, which would prove nothing about GC roots specifically).
    let legacy_chunks: Vec<Vec<u8>> = (0..12).map(|i| chunk(i + 3, 96 * 1024)).collect();
    for bytes in &legacy_chunks {
        append_series_version(&mut ship, "/legacy.series", bytes).await;
    }
    let legacy_cumulative: Vec<u8> = legacy_chunks.concat();
    mirror_collapse_series(&mut ship, "/legacy.series", &legacy_cumulative).await;

    // Resolve the native series' manifest hash (its stable logical identity)
    // through the same push/pull content-addressed path a remote reader
    // would use.
    async fn native_series_hash(pond: &Path) -> ObjectHash {
        let source = LocalPondSource::open(pond)
            .await
            .expect("open local pond source");
        let tip = source
            .get_tip("")
            .await
            .expect("get tip")
            .expect("a content-changing commit exists");
        let commit_bytes = source
            .get_object(tip)
            .await
            .expect("get tip object")
            .expect("tip commit object is served");
        let commit = Commit::decode(&commit_bytes).expect("decode tip commit");
        let manifest_bytes = source
            .get_object(commit.node_manifest_hash)
            .await
            .expect("get node manifest object")
            .expect("node manifest object is served");
        let manifest_entries = decode_manifest(&manifest_bytes).expect("decode node manifest");
        let series_entry = manifest_entries
            .iter()
            .find(|e| e.name == "native.series" && e.entry_type == EntryType::FilePhysicalSeries)
            .expect("the native series has a manifest entry");
        series_entry.child_hash
    }

    // The one *published* pack advertisement directory for a series
    // (`data/_packs/series=<hex>`), read directly off disk -- distinct from
    // `ContentSource::list_pack_hashes`, which also always reports a
    // synthesized-on-the-fly initial pack regardless of what has been
    // published.
    fn published_pack_hashes(pond: &Path, series_hash: ObjectHash) -> HashSet<ObjectHash> {
        let dir = steward::get_data_path(pond)
            .join(sync_store::pack_keys::PACKS_ROOT)
            .join(sync_store::pack_keys::series_dir_name(series_hash));
        match std::fs::read_dir(&dir) {
            Ok(rd) => rd
                .filter_map(|e| e.ok())
                .filter_map(|e| {
                    sync_store::pack_keys::parse_pack_file_name(e.file_name().to_str()?).ok()
                })
                .collect(),
            Err(_) => HashSet::new(),
        }
    }

    let series_hash_before = native_series_hash(&pond).await;
    // No pack has ever been published for the native series yet -- only the
    // on-the-fly synthesized initial pack answers reads so far.
    assert!(
        published_pack_hashes(&pond, series_hash_before).is_empty(),
        "no real pack should be published before maintenance ever runs"
    );
    let blobs_before = blobs_on_disk(&pond);

    // A single maintenance pass does real, unrelated work on both series at
    // once: the native series is repacked, and the legacy series' superseded
    // rows/blobs are reclaimed.
    let report = ship
        .collapse_versions(1)
        .await
        .expect("pack-only maintenance must succeed and never gate");
    assert!(
        report.series_repacked >= 1,
        "the native series (3 versions, never collapsed) must be a genuine repack candidate: {report}"
    );
    assert_eq!(
        report.unsupported_legacy, 0,
        "neither series carries pre-v2 rows: {report}"
    );

    let blobs_after = blobs_on_disk(&pond);
    assert!(
        blobs_after.len() < blobs_before.len(),
        "reclamation must still have freed the legacy series' superseded blobs even though \
         the native series was independently repacked: before={}, after={}",
        blobs_before.len(),
        blobs_after.len()
    );

    // The native series' logical identity (its manifest hash) is completely
    // unaffected by having its physical layout repacked.
    let series_hash_after = native_series_hash(&pond).await;
    assert_eq!(
        series_hash_before, series_hash_after,
        "the native series' logical identity must be unaffected by pack-only maintenance"
    );

    // A real, bounded pack is now published on disk for the native series --
    // this maintenance run's actual repack -- and it must have strictly
    // fewer physical objects than the three per-append objects it replaces.
    let published_after = published_pack_hashes(&pond, series_hash_after);
    assert!(
        !published_after.is_empty(),
        "maintenance must have published a real pack for the repacked native series"
    );
    let source = LocalPondSource::open(&pond)
        .await
        .expect("reopen local pond source after maintenance + reclaim");
    for &pack_hash in &published_after {
        let pack_bytes = source
            .get_pack_index(series_hash_after, pack_hash)
            .await
            .expect("get published pack index")
            .expect("published pack index is served");
        let pack_index = PackIndex::decode(&pack_bytes).expect("decode published pack index");
        assert!(
            pack_index.physical_object_hashes().len() < native_chunks.len(),
            "the published pack must be more bounded than one physical object per append: {} \
             objects for {} appends",
            pack_index.physical_object_hashes().len(),
            native_chunks.len()
        );
    }

    // And every physical object the pack's own leaves depend on must still
    // be fetchable through the same source, not merely "the pack bytes
    // still parse".
    for bytes in &native_chunks {
        // Small appends are inlined, not externalized as `_large_files`
        // blobs, so what we're really confirming is that the underlying
        // OplogEntry rows (and thus the content the pack's descriptors
        // partition) are still readable post-reclaim.
        let read_back = read_bytes(&mut ship, "/native.series").await;
        assert!(
            read_back
                .windows(bytes.len())
                .any(|w| w == bytes.as_slice()),
            "every native chunk must still be present in the series' content post-reclaim"
        );
    }
    drop(source);

    let fsck = steward::fsck(&ship, FsckOptions::default())
        .await
        .expect("fsck after reclaim");
    assert!(
        fsck.errors.is_empty(),
        "fsck must find no dangling blob after a GC-root-respecting reclaim: {:?}",
        fsck.errors
    );
}
