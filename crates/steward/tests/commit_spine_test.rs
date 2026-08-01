// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Phase 2b commit spine: every write transaction
//! stamps its `root_tree_hash`, `parent_commit_hash`, and `commit_hash` into
//! the control table's `DataCommitted` record, and successive commits chain
//! through `parent_commit_hash == previous commit_hash`.

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

/// The persisted `root_tree_hash` for the latest commit equals the read-side
/// fold over current live state.
#[tokio::test]
async fn persisted_root_tree_hash_matches_read_side_fold() {
    let tmp = tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(tmp.path().join("pond"), "spine-test")
        .await
        .expect("create pond");

    write_file(&mut ship, "/a.txt", b"hello").await;

    let seq = ship.last_write_seq();
    let persisted = ship
        .control_table()
        .root_tree_hash_at(seq)
        .await
        .expect("query root_tree_hash")
        .expect("root_tree_hash must be stamped on a user write");

    let live = steward::compute_content_tree(&ship)
        .await
        .expect("compute content tree");

    assert_eq!(
        persisted,
        live.root_tree_hash.to_hex(),
        "persisted root_tree_hash must equal the read-side fold of current state"
    );
}

/// Successive commits form a hash chain: each commit's `parent_commit_hash`
/// equals the previous commit's `commit_hash`.
#[tokio::test]
async fn successive_commits_chain_through_parent_hash() {
    let tmp = tempdir().expect("tempdir");
    let mut ship = Ship::create_pond(tmp.path().join("pond"), "chain-test")
        .await
        .expect("create pond");

    // Three user writes. Each lands at a distinct txn_seq after genesis.
    write_file(&mut ship, "/a.txt", b"one").await;
    let seq_a = ship.last_write_seq();
    write_file(&mut ship, "/b.txt", b"two").await;
    let seq_b = ship.last_write_seq();
    write_file(&mut ship, "/c.txt", b"three").await;
    let seq_c = ship.last_write_seq();

    let ct = ship.control_table();

    let commit_a = ct
        .commit_hash_at(seq_a)
        .await
        .expect("query a")
        .expect("commit a stamped");
    let commit_b = ct
        .commit_hash_at(seq_b)
        .await
        .expect("query b")
        .expect("commit b stamped");
    let parent_b = ct
        .parent_commit_hash_at(seq_b)
        .await
        .expect("query parent b")
        .expect("parent b present");
    let parent_c = ct
        .parent_commit_hash_at(seq_c)
        .await
        .expect("query parent c")
        .expect("parent c present");

    assert_eq!(
        parent_b, commit_a,
        "commit b's parent must be commit a's hash"
    );
    assert_eq!(
        parent_c, commit_b,
        "commit c's parent must be commit b's hash"
    );
}

/// A pond and its replica produce identical persisted `root_tree_hash` values,
/// even though their `commit_hash` values differ (provenance differs by pond_id
/// and time).
#[tokio::test]
async fn replica_same_root_hash_different_commit_hash() {
    let tmp_a = tempdir().expect("tempdir a");
    let mut ship_a = Ship::create_pond(tmp_a.path().join("pond"), "pond-a")
        .await
        .expect("create pond a");

    write_file(&mut ship_a, "/a.txt", b"same content").await;
    let (_rt, _dt, ship_b) = replica_of(&ship_a, "pond-b").await;

    let seq_a = ship_a.last_write_seq();
    let seq_b = ship_b.last_write_seq();

    let root_a = ship_a
        .control_table()
        .root_tree_hash_at(seq_a)
        .await
        .expect("root a")
        .expect("root a stamped");
    let root_b = ship_b
        .control_table()
        .root_tree_hash_at(seq_b)
        .await
        .expect("root b")
        .expect("root b stamped");
    assert_eq!(
        root_a, root_b,
        "identical content yields identical root_tree_hash across ponds"
    );

    let commit_a = ship_a
        .control_table()
        .commit_hash_at(seq_a)
        .await
        .expect("commit a")
        .expect("commit a stamped");
    let commit_b = ship_b
        .control_table()
        .commit_hash_at(seq_b)
        .await
        .expect("commit b")
        .expect("commit b stamped");
    assert_ne!(
        commit_a, commit_b,
        "commit hashes differ because provenance (pond_id, time) differs"
    );
}
