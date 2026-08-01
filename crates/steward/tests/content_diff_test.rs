// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `steward::compare_content_trees`: the content-tree
//! comparison that realizes design Section 6.2 / Goal 2.  Two ponds are equal
//! iff their root tree hashes match; otherwise the diff descends by child hash
//! to the minimal set of divergent paths.

use steward::{DiffKind, Ship, compare_content_trees};
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

/// A replica compares equal to its source with no differences, regardless of
/// pond identity or lineage.
#[tokio::test]
async fn replica_compares_equal_to_source() {
    let (_ta, mut a) = new_pond("pond-a").await;
    write_file(&mut a, "/a.txt", b"hello").await;
    mkdir_and_file(&mut a, "/sub", "/sub/b.txt", b"world").await;

    let (_rt, _dt, b) = replica_of(&a, "pond-b").await;

    let cmp = compare_content_trees(&a, &b).await.expect("compare");
    assert!(
        cmp.equal,
        "a replica must compare equal to its source: {:?}",
        cmp.differences
    );
    assert!(cmp.differences.is_empty());
}

/// Two ponds written independently from the same bytes are not equal, because
/// their versions were created at different times -- and the divergence must be
/// *reported*.  Pruning the descent on `child_hash` alone once let the roots
/// differ while the diff listed nothing, which is the least useful answer a
/// comparison can give.
#[tokio::test]
async fn independently_written_ponds_differ_by_metadata() {
    let (_ta, mut a) = new_pond("pond-a").await;
    let (_tb, mut b) = new_pond("pond-b").await;

    for ship in [&mut a, &mut b] {
        write_file(ship, "/a.txt", b"hello").await;
        mkdir_and_file(ship, "/sub", "/sub/b.txt", b"world").await;
    }

    let cmp = compare_content_trees(&a, &b).await.expect("compare");
    assert!(
        !cmp.equal,
        "independent writes do not share version metadata"
    );
    let paths: Vec<&str> = cmp.differences.iter().map(|d| d.path.as_str()).collect();
    assert!(
        paths.contains(&"/a.txt") && paths.contains(&"/sub/b.txt"),
        "every differing leaf is reported, got {paths:?}"
    );
    assert!(
        cmp.differences.iter().all(|d| d.kind == DiffKind::Modified),
        "same bytes, different metadata is a modification, got {:?}",
        cmp.differences
    );
}

/// A single differing file is reported as exactly one `Modified` path; the
/// shared, identical subtree is pruned.
#[tokio::test]
async fn single_modified_file_is_isolated() {
    let (_ta, mut a) = new_pond("pond-a").await;
    write_file(&mut a, "/shared.txt", b"same").await;
    mkdir_and_file(&mut a, "/sub", "/sub/keep.txt", b"identical").await;
    // The shared base must be *replicated* rather than rewritten, or every
    // common path would differ by its version metadata too.
    let (_rt, _dt, mut b) = replica_of(&a, "pond-b").await;

    // Diverge one nested file only.
    write_file(&mut a, "/sub/diff.txt", b"left").await;
    write_file(&mut b, "/sub/diff.txt", b"right").await;

    let cmp = compare_content_trees(&a, &b).await.expect("compare");
    assert!(!cmp.equal);
    assert_eq!(cmp.differences.len(), 1, "exactly one divergent path");
    assert_eq!(cmp.differences[0].path, "/sub/diff.txt");
    assert_eq!(cmp.differences[0].kind, DiffKind::Modified);
}

/// Added and removed entries are classified from the left pond's perspective.
#[tokio::test]
async fn added_and_removed_entries_are_classified() {
    let (_ta, mut a) = new_pond("pond-a").await;
    // Common base, replicated so it is genuinely common.
    write_file(&mut a, "/common.txt", b"base").await;
    let (_rt, _dt, mut b) = replica_of(&a, "pond-b").await;

    // Only-left and only-right files.
    write_file(&mut a, "/only_left.txt", b"L").await;
    write_file(&mut b, "/only_right.txt", b"R").await;

    let cmp = compare_content_trees(&a, &b).await.expect("compare");
    assert!(!cmp.equal);

    let removed: Vec<_> = cmp
        .differences
        .iter()
        .filter(|d| d.kind == DiffKind::Removed)
        .map(|d| d.path.as_str())
        .collect();
    let added: Vec<_> = cmp
        .differences
        .iter()
        .filter(|d| d.kind == DiffKind::Added)
        .map(|d| d.path.as_str())
        .collect();

    assert_eq!(removed, vec!["/only_left.txt"], "left-only -> Removed");
    assert_eq!(added, vec!["/only_right.txt"], "right-only -> Added");
}
