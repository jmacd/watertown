// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `steward::compute_content_tree`: the read-side
//! content-tree fold that produces a pond's `root_tree_hash` from live state.

use steward::Ship;
use sync_store::ContentRemote;
use tempfile::tempdir;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::PondUserMetadata;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

async fn write_file(ship: &mut Ship, path: &str, bytes: &[u8]) {
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("write"), async move |fs| {
        let root = fs.root().await?;
        let _ = create_file_path(&root, path, &bytes).await?;
        Ok(())
    })
    .await
    .expect("write transaction");
}

async fn mkdir_and_file(ship: &mut Ship, dir: &str, file: &str, bytes: &[u8]) {
    let dir = dir.to_string();
    let file = file.to_string();
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("mkdir"), async move |fs| {
        let root = fs.root().await?;
        let _ = root.create_dir_all(&dir).await?;
        let _ = create_file_path(&root, &file, &bytes).await?;
        Ok(())
    })
    .await
    .expect("mkdir transaction");
}

/// Build a genuine replica of `src`.
///
/// Replication is the only way to obtain a second pond with identical content:
/// a version's metadata -- when it was created, the event-time range it covers
/// -- is data about that immutable version, and the directory entry commits to
/// it.  Two ponds written independently from the same bytes therefore do *not*
/// have identical content.  A replica adopts the source's metadata verbatim,
/// so it does.
async fn replica_of(src: &Ship, label: &str) -> (tempfile::TempDir, tempfile::TempDir, Ship) {
    let pond_id = uuid::Uuid::parse_str(src.data_persistence().pond_id()).expect("pond id");
    let remote_dir = tempdir().expect("remote dir");
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");
    let _ = steward::push_content_to_remote(src, &mut remote, "main")
        .await
        .expect("push");
    let graph = steward::fetch_object_graph(&remote, "main")
        .await
        .expect("fetch");
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), label)
        .await
        .expect("create replica");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");
    (remote_dir, dst_dir, dst)
}

/// The root tree hash is stable across repeated computations and changes when
/// file content changes.
#[tokio::test]
async fn content_tree_root_is_deterministic_and_data_sensitive() {
    let tmp = tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(tmp.path().join("pond"), "content-test")
        .await
        .expect("create pond");

    write_file(&mut ship, "/a.txt", b"hello").await;
    mkdir_and_file(&mut ship, "/sub", "/sub/b.txt", b"world").await;

    let r1 = steward::compute_content_tree(&ship)
        .await
        .expect("compute 1");
    let r2 = steward::compute_content_tree(&ship)
        .await
        .expect("compute 2");
    assert_eq!(
        r1.root_tree_hash, r2.root_tree_hash,
        "root tree hash must be deterministic"
    );
    assert!(r1.nodes_hashed >= 2, "root plus /sub are directories");

    // Adding a new file changes the root tree hash.
    write_file(&mut ship, "/c.txt", b"new file").await;
    let r3 = steward::compute_content_tree(&ship)
        .await
        .expect("compute 3");
    assert_ne!(
        r1.root_tree_hash, r3.root_tree_hash,
        "root tree hash must change when the tree changes"
    );
}

/// A replica produces the same root tree hash as its source, independent of
/// pond identity and lineage (Goal 2: comparison).
#[tokio::test]
async fn replica_yields_identical_root_across_ponds() {
    let tmp_a = tempdir().expect("tempdir a");
    let mut ship_a = Ship::create_pond(tmp_a.path().join("pond"), "pond-a")
        .await
        .expect("create pond a");

    write_file(&mut ship_a, "/a.txt", b"same content").await;
    mkdir_and_file(&mut ship_a, "/d", "/d/c.txt", b"more").await;

    let (_rt, _dt, ship_b) = replica_of(&ship_a, "pond-b").await;

    let ra = steward::compute_content_tree(&ship_a).await.expect("a");
    let rb = steward::compute_content_tree(&ship_b).await.expect("b");
    assert_eq!(
        ra.root_tree_hash, rb.root_tree_hash,
        "identical content must hash identically across ponds with different identities"
    );
}

/// Two ponds differing in one file produce different root tree hashes.
#[tokio::test]
async fn differing_content_yields_differing_root() {
    let tmp_a = tempdir().expect("tempdir a");
    let tmp_b = tempdir().expect("tempdir b");
    let mut ship_a = Ship::create_pond(tmp_a.path().join("pond"), "pond-a")
        .await
        .expect("create pond a");
    let mut ship_b = Ship::create_pond(tmp_b.path().join("pond"), "pond-b")
        .await
        .expect("create pond b");

    write_file(&mut ship_a, "/a.txt", b"alpha").await;
    write_file(&mut ship_b, "/a.txt", b"beta").await;

    let ra = steward::compute_content_tree(&ship_a).await.expect("a");
    let rb = steward::compute_content_tree(&ship_b).await.expect("b");
    assert_ne!(ra.root_tree_hash, rb.root_tree_hash);
}

/// A rich mix of mutation kinds (create, nested dir, overwrite, rename,
/// delete) exercised in sequence.  Every write transaction runs the
/// `StewardTransactionGuard` debug oracle, which asserts the *incremental*
/// spine roots (`root_tree_hash`, `node_manifest_hash`, `node_manifest_root`,
/// and the manifest bytes) are byte-identical to a full fold of the same live
/// state.  Reaching the end without a panic proves incremental-vs-rebuild
/// equivalence for both roots across all of these mutation kinds (Phase 6).
#[tokio::test]
async fn incremental_roots_match_full_fold_over_diverse_mutations() {
    let tmp = tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(tmp.path().join("pond"), "phase6-equiv")
        .await
        .expect("create pond");

    // Create files at the root and in a nested directory.
    write_file(&mut ship, "/a.txt", b"alpha").await;
    mkdir_and_file(&mut ship, "/sub", "/sub/b.txt", b"beta").await;
    mkdir_and_file(&mut ship, "/sub/deep", "/sub/deep/c.txt", b"gamma").await;

    // Overwrite an existing file (new version of the same node).
    ship.write_transaction(&meta("overwrite"), async move |fs| {
        use tokio::io::AsyncWriteExt;
        let root = fs.root().await?;
        let mut w = root
            .async_writer_path_with_type("/a.txt", tinyfs::EntryType::FilePhysicalVersion)
            .await?;
        w.write_all(b"alpha-2")
            .await
            .map_err(|e| steward::StewardError::Aborted(format!("write: {e}")))?;
        w.shutdown()
            .await
            .map_err(|e| steward::StewardError::Aborted(format!("close: {e}")))?;
        Ok(())
    })
    .await
    .expect("overwrite transaction");

    // Rename a file within its directory.
    ship.write_transaction(&meta("rename"), async move |fs| {
        let root = fs.root().await?;
        let sub = root.open_dir_path("/sub").await?;
        sub.rename_entry("b.txt", "b-renamed.txt").await?;
        Ok(())
    })
    .await
    .expect("rename transaction");

    // Delete a file.
    ship.write_transaction(&meta("delete"), async move |fs| {
        let root = fs.root().await?;
        let deep = root.open_dir_path("/sub/deep").await?;
        deep.remove_entry("c.txt").await?;
        Ok(())
    })
    .await
    .expect("delete transaction");

    // The pond still folds to a stable, non-trivial content tree.
    let r1 = steward::compute_content_tree(&ship).await.expect("fold 1");
    let r2 = steward::compute_content_tree(&ship).await.expect("fold 2");
    assert_eq!(
        r1.root_tree_hash, r2.root_tree_hash,
        "root tree hash must be deterministic after diverse mutations"
    );
    assert!(
        r1.nodes_hashed >= 2,
        "root plus /sub survive the delete of /sub/deep/c.txt"
    );
}
