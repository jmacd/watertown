// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `steward::inventory_content_objects`: the reachable
//! content-object set that backs content-addressed sync (design Section 8).

use std::collections::BTreeSet;

use steward::{ObjectKind, Ship, inventory_content_objects};
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

async fn new_pond(label: &str) -> (tempfile::TempDir, Ship) {
    let tmp = tempdir().expect("tempdir");
    let ship = Ship::create_pond(tmp.path().join("pond"), label)
        .await
        .expect("create pond");
    (tmp, ship)
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

/// The inventory contains the root tree plus a tree object per physical
/// directory and a blob per file version.
#[tokio::test]
async fn inventory_covers_trees_and_blobs() {
    let (_t, mut ship) = new_pond("inv").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    mkdir_and_file(&mut ship, "/sub", "/sub/b.txt", b"beta").await;

    let inv = inventory_content_objects(&ship).await.expect("inventory");

    let trees = inv
        .objects
        .values()
        .filter(|k| **k == ObjectKind::Tree)
        .count();
    let blobs = inv
        .objects
        .values()
        .filter(|k| **k == ObjectKind::Blob)
        .count();

    // Root tree + /sub tree.
    assert!(trees >= 2, "expected root and /sub trees, got {trees}");
    // /a.txt and /sub/b.txt blobs.
    assert!(blobs >= 2, "expected two file blobs, got {blobs}");
}

/// A pond and its replica produce identical inventories, so the set difference
/// is empty in both directions (a clone needs nothing transferred).
#[tokio::test]
async fn replica_has_no_object_difference() {
    let (_ta, mut a) = new_pond("pond-a").await;
    write_file(&mut a, "/a.txt", b"same").await;
    mkdir_and_file(&mut a, "/d", "/d/c.txt", b"more").await;
    let (_rt, _dt, b) = replica_of(&a, "pond-b").await;

    let inv_a = inventory_content_objects(&a).await.expect("a");
    let inv_b = inventory_content_objects(&b).await.expect("b");

    assert_eq!(
        inv_a.objects, inv_b.objects,
        "identical content, identical objects"
    );
    assert!(inv_a.missing_from(&inv_b.hashes()).is_empty());
    assert!(inv_b.missing_from(&inv_a.hashes()).is_empty());
}

/// When one pond adds a file, the set difference is exactly the new blob plus
/// the rewritten trees on the path to the root (work proportional to change).
#[tokio::test]
async fn divergent_file_yields_minimal_object_difference() {
    let (_ta, mut a) = new_pond("pond-a").await;
    let (_tb, mut b) = new_pond("pond-b").await;

    // Shared base content.
    for ship in [&mut a, &mut b] {
        write_file(ship, "/shared.txt", b"base").await;
    }
    // Pond A gains one extra top-level file.
    write_file(&mut a, "/extra.txt", b"only-in-a").await;

    let inv_a = inventory_content_objects(&a).await.expect("a");
    let inv_b = inventory_content_objects(&b).await.expect("b");

    // What A must send B: the new blob and A's new root tree (root changed
    // because a top-level entry was added). The shared blob is NOT resent.
    let a_to_b = inv_a.missing_from(&inv_b.hashes());
    let kinds: BTreeSet<ObjectKind> = a_to_b.iter().map(|h| inv_a.objects[h]).collect();
    assert!(
        kinds.contains(&ObjectKind::Blob),
        "the new file blob must be in the difference"
    );
    assert!(
        kinds.contains(&ObjectKind::Tree),
        "A's changed root tree must be in the difference"
    );

    // B is not a strict subset: because A added a top-level entry, A's and B's
    // root trees differ, so B's own root tree is one object A does not have.
    let b_to_a = inv_b.missing_from(&inv_a.hashes());
    let b_kinds: BTreeSet<ObjectKind> = b_to_a.iter().map(|h| inv_b.objects[h]).collect();
    assert_eq!(
        b_to_a.len(),
        1,
        "only B's distinct root tree is unique to B"
    );
    assert_eq!(
        b_kinds,
        BTreeSet::from([ObjectKind::Tree]),
        "B's unique object is its root tree"
    );

    // The shared blob is present in both inventories (deduped by hash).
    let shared: Vec<_> = inv_a
        .hashes()
        .intersection(&inv_b.hashes())
        .copied()
        .collect();
    assert!(
        !shared.is_empty(),
        "the identical /shared.txt blob and base trees must be shared"
    );
}
