// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `steward::materialize_content_objects`: producing the
//! byte-level content objects reachable from a pond's root tree (design
//! Section 8.4 / Decision D7).

use steward::{Ship, inventory_content_objects, materialize_content_objects};
use sync_store::ContentRemote;
use sync_store::content::ObjectHash;
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

/// The content-addressing invariant: every inline object's bytes hash to its
/// own key. This is what lets a consumer verify objects fetched by hash.
#[tokio::test]
async fn every_inline_object_hashes_to_its_key() {
    let (_t, mut ship) = new_pond("mat").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    mkdir_and_file(&mut ship, "/sub", "/sub/b.txt", b"beta").await;

    let mat = materialize_content_objects(&ship)
        .await
        .expect("materialize");
    assert!(!mat.is_empty());
    for (hash, bytes) in &mat.inline {
        assert_eq!(
            ObjectHash::of_bytes(bytes),
            *hash,
            "inline object bytes must hash to their key"
        );
    }
}

/// For a pond with no series, the materialized object set covers exactly the
/// reachability inventory: same hashes, now with bytes attached. Small ponds
/// have no externalized large blobs.
#[tokio::test]
async fn materialized_hashes_match_inventory() {
    let (_t, mut ship) = new_pond("mat").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    write_file(&mut ship, "/c.txt", b"charlie").await;
    mkdir_and_file(&mut ship, "/sub", "/sub/b.txt", b"beta").await;

    let inv = inventory_content_objects(&ship).await.expect("inventory");
    let mat = materialize_content_objects(&ship)
        .await
        .expect("materialize");

    // Small files only -> nothing externalized.
    assert!(mat.external_blobs.is_empty(), "no large blobs expected");

    let mat_hashes: std::collections::BTreeSet<ObjectHash> = mat.inline.keys().copied().collect();
    assert_eq!(
        mat_hashes,
        inv.hashes(),
        "materialized object hashes match the reachability inventory"
    );

    // The root tree object is present and decodes to its own hash.
    let report = steward::compute_content_tree(&ship).await.expect("root");
    assert!(
        mat.inline.contains_key(&report.root_tree_hash),
        "root tree object must be materialized"
    );
}

/// A pond and its replica materialize identical object maps -- the property
/// that lets one clone the other by transferring only missing hashes.
#[tokio::test]
async fn replica_materializes_identical_objects() {
    let (_ta, mut a) = new_pond("pond-a").await;
    write_file(&mut a, "/a.txt", b"same").await;
    mkdir_and_file(&mut a, "/d", "/d/c.txt", b"more").await;
    let (_rt, _dt, b) = replica_of(&a, "pond-b").await;

    let ma = materialize_content_objects(&a).await.expect("a");
    let mb = materialize_content_objects(&b).await.expect("b");
    assert_eq!(ma.inline, mb.inline);
    assert_eq!(ma.external_blobs, mb.external_blobs);
}
