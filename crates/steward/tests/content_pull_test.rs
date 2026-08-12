// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `steward::fetch_object_graph`: the consumer-side
//! fetch walk over a content-addressed remote (design Section 8.5).

use steward::{Ship, fetch_object_graph, push_content_to_remote};
use sync_store::ContentRemote;
use sync_store::content::ObjectHash;
use tempfile::tempdir;
use tinyfs::arrow::parquet::ParquetExt;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::PondUserMetadata;

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

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

/// A single-row parquet batch with a `timestamp` (microseconds) column and a
/// string `label`, used to append series versions in tests.
fn series_batch(ts_micros: i64, label: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("label", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![ts_micros])),
            Arc::new(StringArray::from(vec![label])),
        ],
    )
    .expect("series batch")
}

/// Append `versions` to a `TablePhysicalSeries` at `path`, creating it on the
/// first version and appending a new version for each subsequent one.
async fn write_series(ship: &mut Ship, path: &str, versions: &[(i64, &str)]) {
    let path = path.to_string();
    let versions: Vec<(i64, String)> = versions
        .iter()
        .map(|(ts, label)| (*ts, (*label).to_string()))
        .collect();
    ship.write_transaction(&meta("series"), async move |fs| {
        let root = fs.root().await?;
        for (ts, label) in &versions {
            let batch = series_batch(*ts, label);
            let _ = root
                .write_series_from_batch(&path, &batch, Some("timestamp"))
                .await?;
        }
        Ok(())
    })
    .await
    .expect("series transaction");
}

/// Append one raw-bytes version to a `FilePhysicalSeries` at `path`, creating
/// it on the first write and appending a new version thereafter.  Unlike
/// `write_series` (a `table:series`), a `file:series` is what
/// `Ship::collapse_versions` compacts.
async fn write_file_series_version(ship: &mut Ship, path: &str, bytes: &[u8]) {
    let path = path.to_string();
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("file-series"), async move |fs| {
        use tokio::io::AsyncWriteExt;
        let root = fs.root().await?;
        let mut writer = root
            .async_writer_path_with_type(&path, tinyfs::EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(&bytes).await?;
        writer.shutdown().await?;
        Ok(())
    })
    .await
    .expect("file-series transaction");
}

async fn write_temporal_file_series_version(
    ship: &mut Ship,
    path: &str,
    bytes: &[u8],
    min: i64,
    max: i64,
) {
    let path = path.to_string();
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("temporal-file-series"), async move |fs| {
        use tokio::io::AsyncWriteExt;
        let root = fs.root().await?;
        let mut writer = root
            .async_writer_path_with_type(&path, tinyfs::EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(&bytes).await?;
        writer.set_temporal_metadata(min, max, "timestamp".to_string());
        writer.shutdown().await?;
        Ok(())
    })
    .await
    .expect("temporal file-series transaction");
}

/// Create a dynamic node (factory + config) at `path` with the given entry
/// type, exercising the recipe path directly without the provider's factory
/// registry (rebuild only needs the stored factory string and config bytes).
async fn write_dynamic(
    ship: &mut Ship,
    path: &str,
    entry_type: tinyfs::EntryType,
    factory: &str,
    config: &[u8],
) {
    let path = path.to_string();
    let factory = factory.to_string();
    let config = config.to_vec();
    ship.write_transaction(&meta("mknod"), async move |fs| {
        let root = fs.root().await?;
        let _ = root
            .create_dynamic_path(&path, entry_type, &factory, config)
            .await?;
        Ok(())
    })
    .await
    .expect("dynamic transaction");
}

/// Rewrite an existing dynamic node with the same factory and config.  The
/// recipe bytes are unchanged, so the node's content hash is unchanged, but the
/// rewrite mints a new version carrying a fresh mtime.
async fn rewrite_dynamic(
    ship: &mut Ship,
    path: &str,
    entry_type: tinyfs::EntryType,
    factory: &str,
    config: &[u8],
) {
    let path = path.to_string();
    let factory = factory.to_string();
    let config = config.to_vec();
    ship.write_transaction(&meta("re-mknod"), async move |fs| {
        let root = fs.root().await?;
        let _ = root
            .create_dynamic_path_with_overwrite(&path, entry_type, &factory, config, true)
            .await?;
        Ok(())
    })
    .await
    .expect("dynamic rewrite transaction");
}

async fn push(ship: &Ship) -> (tempfile::TempDir, ContentRemote) {
    let pond_id = uuid::Uuid::parse_str(ship.data_persistence().pond_id()).expect("pond id");
    let remote_dir = tempdir().expect("remote dir");
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");
    let _ = push_content_to_remote(ship, &mut remote, "main")
        .await
        .expect("push");
    (remote_dir, remote)
}

/// Push again to an already-created remote (used by incremental-pull tests).
async fn repush(ship: &Ship, remote: &mut ContentRemote) {
    let _ = push_content_to_remote(ship, remote, "main")
        .await
        .expect("repush");
}

async fn rename(ship: &mut Ship, old: &str, new: &str) {
    let old = old.to_string();
    let new = new.to_string();
    ship.write_transaction(&meta("rename"), async move |fs| {
        let root = fs.root().await?;
        root.rename_entry(old.trim_start_matches('/'), new.trim_start_matches('/'))
            .await?;
        Ok(())
    })
    .await
    .expect("rename transaction");
}

async fn delete(ship: &mut Ship, path: &str) {
    let path = path.to_string();
    ship.write_transaction(&meta("delete"), async move |fs| {
        let root = fs.root().await?;
        root.remove_entry(path.trim_start_matches('/')).await?;
        Ok(())
    })
    .await
    .expect("delete transaction");
}

async fn read_to_string(ship: &mut Ship, path: &str) -> String {
    let tx = ship.begin_read(&meta("read")).await.expect("begin read");
    let root = tx.root().await.expect("root");
    let bytes = root.read_file_path_to_vec(path).await.expect("read");
    String::from_utf8(bytes).expect("utf8")
}

async fn root_hash(ship: &Ship) -> ObjectHash {
    steward::compute_content_tree(ship)
        .await
        .expect("fold")
        .root_tree_hash
}

async fn foreign_root_hash(ship: &Ship, pond_id: uuid7::Uuid) -> ObjectHash {
    steward::compute_content_tree_for_table(
        ship.data_persistence().table().clone(),
        &pond_id.to_string(),
    )
    .await
    .expect("foreign fold")
    .root_tree_hash
}

/// Fetching a pushed pond returns a verified closure whose tip and root tree
/// are present, and every object's bytes hash to its key.
#[tokio::test]
async fn fetch_returns_verified_closure() {
    let (_t, mut ship) = new_pond("fetch").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    mkdir_and_file(&mut ship, "/sub", "/sub/b.txt", b"beta").await;

    let (_rt, remote) = push(&ship).await;

    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    assert!(!graph.is_empty());
    assert_eq!(graph.tip, remote.get_tip("main").await.expect("tip"));

    // Content-addressing invariant across the whole fetched closure.
    for (hash, bytes) in &graph.bytes {
        assert_eq!(
            ObjectHash::of_bytes(bytes),
            *hash,
            "fetched object must hash to its key"
        );
    }

    // The tip commit's root tree is in the closure.
    let root = graph.root_tree_hash().expect("root tree hash");
    assert!(
        graph.objects.contains_key(&root),
        "root tree must be fetched"
    );

    // The node manifest is fetched and carries one entry per node: the root,
    // both files, and the subdirectory (4 nodes).
    assert_eq!(graph.manifest.len(), 4, "manifest must cover every node");
    assert!(
        graph
            .manifest
            .iter()
            .any(|e| e.parent_node_id.is_empty() && e.name.is_empty()),
        "manifest must contain the root entry"
    );
}

/// Fetching a non-existent ref yields an empty graph, not an error.
#[tokio::test]
async fn fetch_missing_ref_is_empty() {
    let (_t, mut ship) = new_pond("fetch-empty").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    let (_rt, remote) = push(&ship).await;

    let graph = fetch_object_graph(&remote, "does-not-exist")
        .await
        .expect("fetch");
    assert!(graph.is_empty());
    assert!(graph.tip.is_none());
}

/// The fetched closure equals the producer's materialized inline closure plus
/// the commit chain: the consumer fetches exactly what the producer pushed.
#[tokio::test]
async fn fetched_closure_matches_pushed_objects() {
    let (_t, mut ship) = new_pond("fetch-match").await;
    write_file(&mut ship, "/a.txt", b"alpha").await;
    mkdir_and_file(&mut ship, "/sub", "/sub/b.txt", b"beta").await;

    let mat = steward::materialize_content_objects(&ship)
        .await
        .expect("materialize");
    let (_rt, remote) = push(&ship).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    // Every inline materialized object is in the fetched closure.
    for hash in mat.inline.keys() {
        assert!(
            graph.objects.contains_key(hash),
            "materialized object {} must be fetched",
            hash.to_hex()
        );
    }

    // The only fetched objects not in the inline tree closure are commits.
    let commit_hashes: std::collections::BTreeSet<_> =
        graph.commits.iter().map(|(h, _)| *h).collect();
    for hash in graph.objects.keys() {
        assert!(
            mat.inline.contains_key(hash) || commit_hashes.contains(hash),
            "fetched object {} is neither a materialized tree object nor a commit",
            hash.to_hex()
        );
    }
}

/// The full round trip: push a pond, fetch its graph, rebuild into a fresh
/// empty pond, and confirm the rebuilt pond is content-equal to the source
/// (its read-side fold equals the source's root tree hash).
#[tokio::test]
async fn rebuild_reproduces_source_content() {
    let (_t, mut src) = new_pond("src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    write_file(&mut src, "/b.txt", b"beta").await;
    mkdir_and_file(&mut src, "/sub", "/sub/c.txt", b"gamma").await;
    mkdir_and_file(&mut src, "/sub/deep", "/sub/deep/d.txt", b"delta").await;

    let src_root = steward::compute_content_tree(&src)
        .await
        .expect("source fold")
        .root_tree_hash;

    let (_rt, remote) = push(&src).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "dst")
        .await
        .expect("create dst pond");

    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");

    assert_eq!(outcome.root_tree_hash, Some(src_root));
    assert_eq!(outcome.files, 4);
    assert_eq!(outcome.dirs, 2);

    let dst_root = steward::compute_content_tree(&dst)
        .await
        .expect("dst fold")
        .root_tree_hash;
    assert_eq!(
        dst_root, src_root,
        "rebuilt pond must be content-equal to the source"
    );
}

/// A multi-version `table:series` survives the full round trip: the rebuilt
/// pond is content-equal to the source, which requires recreating every series
/// version in order so the read-side fold's `series_hash` matches (Section
/// 8.5.3).
#[tokio::test]
async fn rebuild_reproduces_multi_version_series() {
    let (_t, mut src) = new_pond("series-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    write_series(
        &mut src,
        "/readings.series",
        &[(1_000, "first"), (2_000, "second"), (3_000, "third")],
    )
    .await;

    let src_root = steward::compute_content_tree(&src)
        .await
        .expect("source fold")
        .root_tree_hash;

    let (_rt, remote) = push(&src).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "series-dst")
        .await
        .expect("create dst pond");

    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");

    assert_eq!(outcome.root_tree_hash, Some(src_root));
    assert_eq!(outcome.files, 1);
    assert_eq!(outcome.series, 1);

    let dst_root = steward::compute_content_tree(&dst)
        .await
        .expect("dst fold")
        .root_tree_hash;
    assert_eq!(
        dst_root, src_root,
        "rebuilt pond with a multi-version series must be content-equal to the source"
    );
}

/// A file larger than the large-file threshold is stored out-of-row on the
/// remote (Decision D7): the fetch walk records it as an external blob rather
/// than buffering its bytes, and the rebuild streams it back into the local
/// pond.  The rebuilt pond must still be content-equal to the source.
#[tokio::test]
async fn rebuild_streams_large_external_blob() {
    let (_t, mut src) = new_pond("large-src").await;
    // 256 KiB, comfortably above the 64 KiB large-file threshold, with varied
    // bytes so it does not compress to something tiny.
    let big: Vec<u8> = (0..256 * 1024).map(|i| (i * 31 + 7) as u8).collect();
    write_file(&mut src, "/big.bin", &big).await;
    write_file(&mut src, "/small.txt", b"tiny").await;

    let src_root = steward::compute_content_tree(&src)
        .await
        .expect("source fold")
        .root_tree_hash;

    let (_rt, remote) = push(&src).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    // The large blob is external: its hash is recorded but its bytes are never
    // buffered into the graph.
    assert_eq!(
        graph.external_blobs.len(),
        1,
        "the >64KiB file must be an external blob"
    );
    let big_hash = *graph.external_blobs.iter().next().expect("external hash");
    assert!(
        !graph.bytes.contains_key(&big_hash),
        "external blob bytes must not be buffered in the graph"
    );

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "large-dst")
        .await
        .expect("create dst pond");

    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");
    assert_eq!(outcome.files, 2);

    let dst_root = steward::compute_content_tree(&dst)
        .await
        .expect("dst fold")
        .root_tree_hash;
    assert_eq!(
        dst_root, src_root,
        "rebuilt pond with a streamed large blob must be content-equal to the source"
    );
}

/// A pond containing dynamic nodes (factory + config recipes) survives the
/// round trip: rebuild recreates each recipe and the read-side fold's
/// `recipe_hash` matches the source (Section 8.5.4 / D4).  A dynamic directory
/// is a leaf recipe -- its generated children are recomputed on read and are
/// not part of the graph.
#[tokio::test]
async fn rebuild_reproduces_dynamic_nodes() {
    let (_t, mut src) = new_pond("dyn-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    write_dynamic(
        &mut src,
        "/derived",
        tinyfs::EntryType::TableDynamic,
        "sql-derived-series",
        b"sql: SELECT * FROM source\n",
    )
    .await;
    write_dynamic(
        &mut src,
        "/gen",
        tinyfs::EntryType::DirectoryDynamic,
        "dynamic-dir",
        b"pattern: '*.series'\n",
    )
    .await;

    let src_root = steward::compute_content_tree(&src)
        .await
        .expect("source fold")
        .root_tree_hash;

    let (_rt, remote) = push(&src).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "dyn-dst")
        .await
        .expect("create dst pond");

    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");

    assert_eq!(outcome.root_tree_hash, Some(src_root));
    assert_eq!(outcome.files, 1);
    assert_eq!(outcome.dynamic, 2);

    let dst_root = steward::compute_content_tree(&dst)
        .await
        .expect("dst fold")
        .root_tree_hash;
    assert_eq!(
        dst_root, src_root,
        "rebuilt pond with dynamic nodes must be content-equal to the source"
    );
}

/// Rebuilding from an empty graph is a hard error, not a silent no-op.
#[tokio::test]
async fn rebuild_empty_graph_errors() {
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "dst")
        .await
        .expect("create dst pond");
    let empty = steward::FetchedGraph::default();
    let remote_dir = tempdir().expect("remote dir");
    let remote = ContentRemote::create_at(remote_dir.path().join("remote"), uuid::Uuid::new_v4())
        .await
        .expect("create remote");
    assert!(
        steward::rebuild_pond(&mut dst, &remote, &empty)
            .await
            .is_err()
    );
}

/// Re-pulling an unchanged pond is a no-op: nothing is created and no spurious
/// version is appended.  The second rebuild reports zero creates and the fold
/// still matches the source.
#[tokio::test]
async fn incremental_repull_is_idempotent() {
    let (_t, mut src) = new_pond("idem-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    mkdir_and_file(&mut src, "/sub", "/sub/b.txt", b"beta").await;

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "idem-dst")
        .await
        .expect("create dst");

    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");

    // Push and pull again with no source changes.
    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("re-pull");

    assert_eq!(outcome.dirs, 0);
    assert_eq!(outcome.files, 0);
    assert_eq!(outcome.series, 0);
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

/// Appending a version to a source series is mirrored as a suffix-append, not a
/// recreate: the consumer keeps the prior versions and adds only the new one.
#[tokio::test]
async fn series_repull_appends_only_suffix() {
    let (_t, mut src) = new_pond("ser-src").await;
    write_series(&mut src, "/r.series", &[(1_000, "v1"), (2_000, "v2")]).await;

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "ser-dst")
        .await
        .expect("create dst");

    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");
    assert_eq!(outcome.series, 1);

    // Append a third version, push, and re-pull.
    write_series(&mut src, "/r.series", &[(3_000, "v3")]).await;
    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("re-pull");

    // No new series node is created; the existing one is appended in place.
    assert_eq!(outcome.series, 0);
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

/// Renaming a node in the source preserves its identity on pull: the consumer
/// renames in place rather than deleting and recreating, so no new file node is
/// created.
#[tokio::test]
async fn rename_preserves_node_identity() {
    let (_t, mut src) = new_pond("ren-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "ren-dst")
        .await
        .expect("create dst");

    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");

    rename(&mut src, "/a.txt", "/b.txt").await;
    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("re-pull");

    // A rename is not a create -- a path-keyed mirror would have made a new file.
    assert_eq!(outcome.files, 0);
    assert_eq!(read_to_string(&mut dst, "/b.txt").await, "alpha");
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

/// A name swap between two siblings is a rename cycle: node A takes B's name
/// and B takes A's, each preserving its identity. Applied one at a time the
/// first rename lands on a name the other sibling still holds and the pull
/// would abort; the collision-safe batch stages the cycle through a temporary
/// name so it converges to a row-identical mirror.
#[tokio::test]
async fn swapped_sibling_names_converge() {
    let (_t, mut src) = new_pond("swap-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    write_file(&mut src, "/b.txt", b"beta").await;

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "swap-dst")
        .await
        .expect("create dst");

    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");

    // Swap the two names on the source, preserving each node's identity. The
    // source itself must stage through a temp because rename_entry also rejects
    // an occupied target.
    rename(&mut src, "/a.txt", "/tmp.txt").await;
    rename(&mut src, "/b.txt", "/a.txt").await;
    rename(&mut src, "/tmp.txt", "/b.txt").await;

    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("re-pull with a name swap must not abort");

    // The swap is renames, not creates: no new file node appears.
    assert_eq!(outcome.files, 0);
    assert_eq!(read_to_string(&mut dst, "/a.txt").await, "beta");
    assert_eq!(read_to_string(&mut dst, "/b.txt").await, "alpha");
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

/// A three-way rename rotation (a->b->c->a) is a longer cycle than a swap and
/// still converges: the collision-safe batch breaks it with a single temporary
/// name and then unwinds the chain.
#[tokio::test]
async fn rotated_sibling_names_converge() {
    let (_t, mut src) = new_pond("rot-src").await;
    write_file(&mut src, "/a.txt", b"AAA").await;
    write_file(&mut src, "/b.txt", b"BBB").await;
    write_file(&mut src, "/c.txt", b"CCC").await;

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "rot-dst")
        .await
        .expect("create dst");

    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");

    // Rotate names so node A -> b, node B -> c, node C -> a (content follows its
    // node). Staged through a temp so each single source rename has a free
    // target.
    rename(&mut src, "/a.txt", "/tmp.txt").await; // A: a -> (b)
    rename(&mut src, "/c.txt", "/a.txt").await; // C: c -> a
    rename(&mut src, "/b.txt", "/c.txt").await; // B: b -> c
    rename(&mut src, "/tmp.txt", "/b.txt").await; // A: -> b

    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("re-pull with a rename rotation must not abort");

    assert_eq!(outcome.files, 0);
    assert_eq!(read_to_string(&mut dst, "/a.txt").await, "CCC");
    assert_eq!(read_to_string(&mut dst, "/b.txt").await, "AAA");
    assert_eq!(read_to_string(&mut dst, "/c.txt").await, "BBB");
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

/// Deleting a node in the source propagates on pull: the absent node is
/// unlinked from the mirror.
#[tokio::test]
async fn deletion_propagates() {
    let (_t, mut src) = new_pond("del-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    write_file(&mut src, "/b.txt", b"beta").await;

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "del-dst")
        .await
        .expect("create dst");

    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");

    delete(&mut src, "/b.txt").await;
    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("re-pull");

    assert_eq!(read_to_string(&mut dst, "/a.txt").await, "alpha");
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

/// A COMPACTED `file:series` mirrors its LIVE content, not its superseded
/// history.  After `collapse_versions` merges v1..vN into a single row carrying
/// a `collapsed_through` sentinel, the source table still holds the superseded
/// per-version rows.  The content fold must skip exactly the versions the live
/// series read skips; otherwise the fold materializes the dead blobs, the
/// consumer rebuilds them as live versions, and its series returns the merged
/// data PLUS the pre-merge history -- duplicated content whose fold still
/// equals the source's (both sides fold the same dead rows, so a fold-only
/// check cannot catch it).  This is the regression guard for that bug.
#[tokio::test]
async fn compacted_file_series_mirrors_live_content_not_history() {
    let (_t, mut src) = new_pond("collapse-src").await;

    // Four versions of a file:series; live content is their concatenation.
    let chunks: [&[u8]; 4] = [b"a,1\n", b"b,2\n", b"c,3\n", b"d,4\n"];
    let mut cumulative = String::new();
    for chunk in chunks {
        write_file_series_version(&mut src, "/events.series", chunk).await;
        cumulative.push_str(std::str::from_utf8(chunk).unwrap());
    }
    assert_eq!(read_to_string(&mut src, "/events.series").await, cumulative);

    // Collapse the four versions into one merged row (threshold 1: collapse any
    // series with more than one live version).
    let report = src.collapse_versions(1).await.expect("collapse");
    assert_eq!(report.files_collapsed, 1, "the series must be collapsed");

    // The source's live content is unchanged by the merge.
    assert_eq!(read_to_string(&mut src, "/events.series").await, cumulative);

    // Round-trip the compacted source through a content-addressed remote.
    let (_rt, remote) = push(&src).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "collapse-dst")
        .await
        .expect("create dst");
    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");

    // Exactly one series node is reconstructed, and the folds agree.
    assert_eq!(outcome.series, 1);
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);

    // The decisive check: the mirror's LIVE series content must equal the
    // source's, with no duplicated pre-collapse history.  Before the fix the
    // fold shipped the superseded v1..v4 blobs, the consumer rebuilt them as
    // live versions, and this read returned `cumulative` repeated.
    assert_eq!(
        read_to_string(&mut dst, "/events.series").await,
        cumulative,
        "compacted series must mirror merged content only, not duplicated history"
    );
}

/// A mirror that already replicated a series' pre-collapse versions must
/// converge when the source later compacts it.  This is the regression guard
/// for the bug where the incremental series diff required the held versions to
/// be a prefix of the incoming list and hard-errored on a compaction, leaving
/// the mirror permanently unable to re-pull.
#[tokio::test]
async fn repull_after_source_side_collapse_converges() {
    let (_t, mut src) = new_pond("recollapse-src").await;

    // Four versions; the mirror pulls them before any collapse.
    let chunks: [&[u8]; 4] = [b"a,1\n", b"b,2\n", b"c,3\n", b"d,4\n"];
    let mut cumulative = String::new();
    for chunk in chunks {
        write_file_series_version(&mut src, "/events.series", chunk).await;
        cumulative.push_str(std::str::from_utf8(chunk).unwrap());
    }

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "recollapse-dst")
        .await
        .expect("create dst");
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("initial rebuild");
    // The mirror holds the full pre-collapse history.
    assert_eq!(read_to_string(&mut dst, "/events.series").await, cumulative);
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);

    // The source compacts the four versions into one merged row.
    let report = src.collapse_versions(1).await.expect("collapse");
    assert_eq!(report.files_collapsed, 1, "the series must be collapsed");
    assert_eq!(read_to_string(&mut src, "/events.series").await, cumulative);

    // Re-pull onto the SAME mirror: it must replicate the collapse and converge
    // rather than erroring on the non-prefix version list.
    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("re-pull after collapse");

    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
    assert_eq!(
        read_to_string(&mut dst, "/events.series").await,
        cumulative,
        "mirror must hold the merged live content, not duplicated history"
    );

    // A further re-pull with no source change is a no-op and stays converged.
    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("idempotent re-pull");
    assert_eq!(outcome.series, 0);
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

/// Cross-pond imports also converge after temporal versions stored as external
/// blobs are compacted, matching the shape of the production septic series.
#[tokio::test]
async fn repull_after_temporal_file_series_collapse_converges() {
    let (_t, mut src) = new_pond("temporal-collapse-src").await;
    let src_id = src
        .data_persistence()
        .pond_id()
        .parse::<uuid7::Uuid>()
        .expect("source pond id");

    let chunks: [(Vec<u8>, i64); 4] = [
        (vec![b'a'; 70 * 1024], 1_000_000),
        (vec![b'b'; 70 * 1024], 2_000_000),
        (vec![b'c'; 70 * 1024], 3_000_000),
        (vec![b'd'; 70 * 1024], 4_000_000),
    ];
    for (chunk, timestamp) in chunks {
        write_temporal_file_series_version(
            &mut src,
            "/events.series",
            &chunk,
            timestamp,
            timestamp,
        )
        .await;
    }

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "temporal-collapse-dst")
        .await
        .expect("create dst");
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::import_pond(&mut dst, &remote, &graph, src_id)
        .await
        .expect("initial import");
    assert_eq!(foreign_root_hash(&dst, src_id).await, root_hash(&src).await);

    let report = src.collapse_versions(1).await.expect("collapse");
    assert_eq!(report.files_collapsed, 1);

    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::import_pond(&mut dst, &remote, &graph, src_id)
        .await
        .expect("re-import after temporal collapse");

    assert_eq!(foreign_root_hash(&dst, src_id).await, root_hash(&src).await);
}

/// A collapsing rewrite can retain the exact same series blob list while
/// changing its mtime. A cross-pond retry must replace that series to repair the
/// foreign tree rather than short-circuiting on the unchanged series hash.
#[tokio::test]
async fn repull_after_metadata_only_series_collapse_converges() {
    let (_t, mut src) = new_pond("metadata-collapse-src").await;
    let src_id = src
        .data_persistence()
        .pond_id()
        .parse::<uuid7::Uuid>()
        .expect("source pond id");
    write_file_series_version(&mut src, "/events.series", b"one version\n").await;

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "metadata-collapse-dst")
        .await
        .expect("create dst");
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::import_pond(&mut dst, &remote, &graph, src_id)
        .await
        .expect("initial import");
    let before = root_hash(&src).await;
    assert_eq!(foreign_root_hash(&dst, src_id).await, before);

    src.write_transaction(&meta("metadata-only-collapse"), async move |fs| {
        use tokio::io::AsyncWriteExt;
        let root = fs.root().await?;
        let mut writer = root
            .async_writer_path_collapsing_with_type(
                "/events.series",
                tinyfs::EntryType::FilePhysicalSeries,
            )
            .await?;
        writer.write_all(b"one version\n").await?;
        writer.set_mtime(123);
        writer.shutdown().await?;
        Ok(())
    })
    .await
    .expect("metadata-only collapsing rewrite");
    let after = root_hash(&src).await;
    assert_ne!(
        before, after,
        "single-version collapse must change metadata while retaining its bytes"
    );

    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::import_pond(&mut dst, &remote, &graph, src_id)
        .await
        .expect("metadata-only re-import");

    assert_eq!(foreign_root_hash(&dst, src_id).await, after);
}

/// A source may append fresh versions after a collapse; a mirror that holds the
/// pre-collapse history must adopt the merged baseline AND the later appends,
/// even though it never saw the intermediate versions the source merged away.
#[tokio::test]
async fn repull_after_collapse_then_append_converges() {
    let (_t, mut src) = new_pond("recollapse2-src").await;

    // The mirror pulls only the first two versions.
    write_file_series_version(&mut src, "/events.series", b"a,1\n").await;
    write_file_series_version(&mut src, "/events.series", b"b,2\n").await;

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "recollapse2-dst")
        .await
        .expect("create dst");
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("initial rebuild");
    assert_eq!(
        read_to_string(&mut dst, "/events.series").await,
        "a,1\nb,2\n"
    );

    // The source adds two more versions the mirror never pulls individually,
    // then collapses all four, then appends one more.
    write_file_series_version(&mut src, "/events.series", b"c,3\n").await;
    write_file_series_version(&mut src, "/events.series", b"d,4\n").await;
    let report = src.collapse_versions(1).await.expect("collapse");
    assert_eq!(report.files_collapsed, 1);
    write_file_series_version(&mut src, "/events.series", b"e,5\n").await;
    let expected = "a,1\nb,2\nc,3\nd,4\ne,5\n";
    assert_eq!(read_to_string(&mut src, "/events.series").await, expected);

    // Re-pull: the mirror jumps straight from [v1,v2] to [merged, e] and must
    // converge on the source's exact live content.
    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("re-pull after collapse+append");

    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
    assert_eq!(read_to_string(&mut dst, "/events.series").await, expected);
}

/// A remote whose node manifest is inconsistent with its content tree -- an
/// extra manifest entry reusing a real, in-closure blob hash under a phantom
/// node -- is rejected BEFORE any mutation, so the inconsistent tree is never
/// committed.  Without the pre-mutation check the phantom node would apply, the
/// transaction would commit, and only the post-apply fold would notice.
#[tokio::test]
async fn tampered_manifest_is_rejected_before_commit() {
    let (_t, mut src) = new_pond("tamper-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    write_file(&mut src, "/b.txt", b"beta").await;

    let (_rt, remote) = push(&src).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    // Forge a manifest that reuses a real leaf's content hash under a new
    // node_id and name, as a second child of the root -- structurally
    // inconsistent with the root's tree object.
    let mut tampered = graph.clone();
    let root_id = tampered
        .manifest
        .iter()
        .find(|e| e.parent_node_id.is_empty() && e.name.is_empty())
        .expect("root entry")
        .node_id
        .clone();
    let mut phantom = tampered
        .manifest
        .iter()
        .find(|e| e.parent_node_id == root_id && !e.name.is_empty())
        .expect("a real child leaf")
        .clone();
    phantom.node_id = "phantom-node-id".to_string();
    phantom.name = "phantom.txt".to_string();
    tampered.manifest.push(phantom);

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "dst")
        .await
        .expect("create dst pond");
    let empty_root = root_hash(&dst).await;

    let err = steward::rebuild_pond(&mut dst, &remote, &tampered)
        .await
        .expect_err("tampered manifest must be rejected");
    assert!(
        format!("{err}").contains("inconsistent with its content tree"),
        "unexpected error: {err}"
    );

    // The target pond is untouched: the rejection happened before any write.
    assert_eq!(
        root_hash(&dst).await,
        empty_root,
        "a rejected pull must not mutate the target pond"
    );
}

/// A source-side change to a node's *metadata alone* replicates.
///
/// Rewriting a dynamic node with byte-identical config leaves its content hash
/// untouched but mints a new version with a fresh mtime.  Because the fold
/// commits to version metadata as well as bytes, the source's root tree hash
/// moves; a consumer that planned its diff on content alone would copy nothing,
/// and the post-apply fold -- which runs *after* the import transaction has
/// committed -- would then fail on this and on every subsequent pull, because
/// each retry re-diffs against the same stale metadata.
///
/// This is the failure that stalled a production consumer for days: an
/// unchanging dynamic directory whose mtime kept advancing on the producer.
#[tokio::test]
async fn metadata_only_change_replicates() {
    let (_t, mut src) = new_pond("meta-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    write_dynamic(
        &mut src,
        "/budget",
        tinyfs::EntryType::FileDynamic,
        "rate-limit",
        b"unit: ops/day\nlimit: 10\nburst: 5\n",
    )
    .await;

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "meta-dst")
        .await
        .expect("create dst");
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild");
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);

    // Rewrite with identical config: same recipe bytes, new mtime.
    let before = root_hash(&src).await;
    rewrite_dynamic(
        &mut src,
        "/budget",
        tinyfs::EntryType::FileDynamic,
        "rate-limit",
        b"unit: ops/day\nlimit: 10\nburst: 5\n",
    )
    .await;
    let after = root_hash(&src).await;
    assert_ne!(
        before, after,
        "a metadata-only rewrite must move the source root tree hash, \
         otherwise this test cannot observe the bug it guards"
    );

    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("re-pull after metadata-only change");

    assert_eq!(
        root_hash(&dst).await,
        after,
        "consumer must adopt the source's new metadata, not keep the stale mtime"
    );
}
