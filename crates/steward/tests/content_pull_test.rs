// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `steward::fetch_object_graph`: the consumer-side
//! fetch walk over a content-addressed remote (design Section 8.5).

use async_trait::async_trait;
use steward::{
    BlobReader, ContentSource, FetchedObject, LocalPondSource, Ship, StewardError,
    fetch_object_graph, fetch_object_graph_since, materialize_content_objects,
    push_content_to_remote,
};
use sync_store::ContentRemote;
use sync_store::content::{Commit, ObjectHash, PackIndex};
use tempfile::tempdir;
use tinyfs::arrow::parquet::ParquetExt;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::{PondTxnMetadata, PondUserMetadata};

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use tokio::io::{AsyncRead, ReadBuf};

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

async fn write_file_series_versions(ship: &mut Ship, path: &str, values: &[&[u8]]) {
    let path = path.to_string();
    let values = values
        .iter()
        .map(|value| value.to_vec())
        .collect::<Vec<_>>();
    ship.write_transaction(&meta("file-series-batch"), async move |fs| {
        use tokio::io::AsyncWriteExt;
        let root = fs.root().await?;
        for bytes in values {
            let mut writer = root
                .async_writer_path_with_type(&path, tinyfs::EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(&bytes).await?;
            writer.shutdown().await?;
        }
        Ok(())
    })
    .await
    .expect("file-series batch transaction");
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

#[tokio::test]
async fn incremental_materialization_follows_destination_series_frontiers_across_segmentation() {
    let (_source_dir, mut source) = new_pond("segmented-source").await;
    write_file_series_version(&mut source, "/a.series", b"x").await;
    let remote_dir = tempdir().expect("remote tempdir");
    let pond_id = source.control_table().pond_id_uuid();
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");
    let first = push_content_to_remote(&source, &mut remote, "main")
        .await
        .expect("publish x");

    let target_dir = tempdir().expect("target tempdir");
    let mut target = Ship::create_replica(target_dir.path().join("pond"), pond_id)
        .await
        .expect("create replica");
    let initial = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch initial graph");
    let _ = steward::rebuild_pond(&mut target, &remote, &initial)
        .await
        .expect("materialize initial graph");

    write_file_series_version(&mut source, "/a.series", b"y").await;
    let second = push_content_to_remote(&source, &mut remote, "main")
        .await
        .expect("publish linked xy");
    let xy = fetch_object_graph_since(&remote, "main", Some(first.tip))
        .await
        .expect("fetch xy suffix");
    let _ = steward::rebuild_pond(&mut target, &remote, &xy)
        .await
        .expect("materialize xy suffix");

    // The canonical locator for logical [x,y] is A's suffix [y] -> [x].
    // B publishes the same logical series as one root segment. A new B node
    // must follow the retained canonical chain through the older [x] state.
    write_file_series_versions(&mut source, "/b.series", &[b"x", b"y"]).await;
    let third = push_content_to_remote(&source, &mut remote, "main")
        .await
        .expect("publish alternate xy segmentation");
    let new_node = fetch_object_graph_since(&remote, "main", Some(second.tip))
        .await
        .expect("fetch new B");
    let _ = steward::rebuild_pond(&mut target, &remote, &new_node)
        .await
        .expect("new B must materialize through canonical linked suffix");

    // Install a root [x,y,z] canonical locator through C, then append z to A.
    // A retains the authenticated [x,y] frontier even though the canonical
    // current pack starts at leaf zero rather than at A's retained boundary.
    write_file_series_versions(&mut source, "/c.series", &[b"x", b"y", b"z"]).await;
    let fourth = push_content_to_remote(&source, &mut remote, "main")
        .await
        .expect("publish root xyz segmentation");
    let c_graph = fetch_object_graph_since(&remote, "main", Some(third.tip))
        .await
        .expect("fetch C");
    let _ = steward::rebuild_pond(&mut target, &remote, &c_graph)
        .await
        .expect("materialize C");

    write_file_series_version(&mut source, "/a.series", b"z").await;
    let _fifth = push_content_to_remote(&source, &mut remote, "main")
        .await
        .expect("publish A xyz alternate suffix");
    let retained_prefix = fetch_object_graph_since(&remote, "main", Some(fourth.tip))
        .await
        .expect("fetch A append");
    let _ = steward::rebuild_pond(&mut target, &remote, &retained_prefix)
        .await
        .expect("A retained xy frontier must authenticate within root xyz pack");

    let tx = target
        .begin_read(&meta("verify-segmentation"))
        .await
        .unwrap();
    let root = tx.root().await.unwrap();
    assert_eq!(
        root.read_file_path_to_vec("/a.series").await.unwrap(),
        b"xyz"
    );
    assert_eq!(
        root.read_file_path_to_vec("/b.series").await.unwrap(),
        b"xy"
    );
    assert_eq!(
        root.read_file_path_to_vec("/c.series").await.unwrap(),
        b"xyz"
    );
    let _ = tx.commit().await.unwrap();
}

#[tokio::test]
async fn pull_preserves_payloads_with_structured_and_raw_roles() {
    let (_source_dir, mut source) = new_pond("shared-tree-payload-source").await;
    mkdir_and_file(&mut source, "/sub", "/sub/value.txt", b"value").await;
    let materialized = materialize_content_objects(&source)
        .await
        .expect("materialize source tree");
    let tree_hash = materialized
        .manifest_records
        .iter()
        .find(|record| record.entry.name == "sub")
        .expect("subdirectory manifest record")
        .entry
        .child_hash;
    let tree_bytes = materialized
        .inline
        .get(&tree_hash)
        .expect("subdirectory tree object")
        .bytes
        .clone();
    write_file(&mut source, "/tree-as-file.bin", &tree_bytes).await;

    let remote_dir = tempdir().expect("remote tempdir");
    let pond_id = source.control_table().pond_id_uuid();
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");
    let _ = push_content_to_remote(&source, &mut remote, "main")
        .await
        .expect("publish shared tree/raw payload");
    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch shared tree/raw payload");
    assert!(graph.trees.contains_key(&tree_hash));
    assert!(graph.blob_hashes.contains(&tree_hash));

    let target_dir = tempdir().expect("target tempdir");
    let mut target = Ship::create_replica(target_dir.path().join("pond"), pond_id)
        .await
        .expect("create replica");
    let _ = steward::rebuild_pond(&mut target, &remote, &graph)
        .await
        .expect("materialize shared tree/raw payload");
    let tx = target
        .begin_read(&meta("verify-shared-payload"))
        .await
        .unwrap();
    assert_eq!(
        tx.root()
            .await
            .unwrap()
            .read_file_path_to_vec("/tree-as-file.bin")
            .await
            .unwrap(),
        tree_bytes
    );
    let _ = tx.commit().await.unwrap();
}

#[tokio::test]
async fn pull_buffers_large_dynamic_recipes() {
    let (_source_dir, mut source) = new_pond("large-recipe-source").await;
    let config = vec![b'x'; tlogfs::large_files::LARGE_FILE_THRESHOLD + 1];
    write_dynamic(
        &mut source,
        "/large.dynamic",
        tinyfs::EntryType::FileDynamic,
        "large-recipe-factory",
        &config,
    )
    .await;

    let remote_dir = tempdir().expect("remote tempdir");
    let pond_id = source.control_table().pond_id_uuid();
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");
    let _ = push_content_to_remote(&source, &mut remote, "main")
        .await
        .expect("publish large recipe");
    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch large recipe");
    let target_dir = tempdir().expect("target tempdir");
    let mut target = Ship::create_replica(target_dir.path().join("pond"), pond_id)
        .await
        .expect("create replica");
    let _ = steward::rebuild_pond(&mut target, &remote, &graph)
        .await
        .expect("materialize large recipe");
}

async fn write_foreign_file(ship: &mut Ship, pond_id: uuid7::Uuid, path: &str, bytes: &[u8]) {
    let tx = ship
        .begin_write(&meta("write-foreign"))
        .await
        .expect("begin write");
    let foreign_node = tx.foreign_root_node(pond_id).await.expect("foreign root");
    let foreign_path = tinyfs::NodePath {
        node: foreign_node,
        path: "/".into(),
    };
    let root = tx
        .wd(&foreign_path, foreign_path.clone())
        .await
        .expect("foreign wd");
    let _ = create_file_path(&root, path, bytes)
        .await
        .expect("write foreign file");
    let _ = tx.commit().await.expect("commit foreign write");
}

async fn point_mount_at_foreign_child(
    ship: &mut Ship,
    pond_id: uuid7::Uuid,
    mount_parent: &str,
    mount_name: &str,
    child_name: &str,
) {
    let tx = ship
        .begin_write(&meta("mispoint-mount"))
        .await
        .expect("begin write");
    let foreign_root = tx.foreign_root_node(pond_id).await.expect("foreign root");
    let foreign_path = tinyfs::NodePath {
        node: foreign_root,
        path: "/".into(),
    };
    let foreign_wd = tx
        .wd(&foreign_path, foreign_path.clone())
        .await
        .expect("foreign wd");
    let child = foreign_wd
        .get(child_name)
        .await
        .expect("lookup child")
        .expect("foreign child")
        .node;
    let root = tx.root().await.expect("local root");
    let parent = root
        .open_dir_path(mount_parent)
        .await
        .expect("mount parent");
    parent.remove_entry(mount_name).await.expect("remove mount");
    let _ = parent
        .insert_node(mount_name, child)
        .await
        .expect("insert wrong mount");
    let _ = tx.commit().await.expect("commit wrong mount");
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

fn series_batch_rows(first_ts_micros: i64, rows: usize, label: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("label", DataType::Utf8, false),
    ]));
    let timestamps = (0..rows)
        .map(|offset| first_ts_micros + offset as i64)
        .collect::<Vec<_>>();
    let labels = std::iter::repeat_n(label, rows).collect::<Vec<_>>();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(StringArray::from(labels)),
        ],
    )
    .expect("multi-row series batch")
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

async fn write_series_batch(ship: &mut Ship, path: &str, batch: RecordBatch) {
    let path = path.to_string();
    ship.write_transaction(&meta("series-batch"), async move |fs| {
        let root = fs.root().await?;
        let _ = root
            .write_series_from_batch(&path, &batch, Some("timestamp"))
            .await?;
        Ok(())
    })
    .await
    .expect("series batch transaction");
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

#[derive(Debug, Default)]
struct ReadCounts {
    blob_bytes: AtomicU64,
    blob_requests: Mutex<Vec<ObjectHash>>,
    object_bytes: AtomicU64,
    object_requests: Mutex<Vec<ObjectHash>>,
    object_point_requests: AtomicU64,
    object_batches: Mutex<Vec<Vec<ObjectHash>>>,
    commit_index_requests: AtomicU64,
    publication_record_requests: AtomicU64,
    pack_index_bytes: AtomicU64,
}

struct CountingReader {
    inner: BlobReader,
    counts: Arc<ReadCounts>,
}

impl AsyncRead for CountingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) {
            let read = buf.filled().len().saturating_sub(before) as u64;
            let _ = self.counts.blob_bytes.fetch_add(read, Ordering::Relaxed);
        }
        result
    }
}

struct CountingSource<'a> {
    inner: &'a dyn ContentSource,
    counts: Arc<ReadCounts>,
}

struct AdvancingStateSource<'a> {
    inner: &'a dyn ContentSource,
    first: sync_store::PublicationState,
    calls: AtomicU64,
}

enum BatchFault {
    Omit(ObjectHash),
    Corrupt(ObjectHash),
    Unexpected(ObjectHash, Vec<u8>),
}

enum BlobFault {
    Corrupt(ObjectHash),
}

struct FaultingSource<'a> {
    inner: &'a dyn ContentSource,
    batch_fault: Option<BatchFault>,
    blob_fault: Option<BlobFault>,
}

struct CorruptingReader {
    inner: BlobReader,
    corrupted: bool,
}

impl AsyncRead for CorruptingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && !self.corrupted {
            let after = buf.filled().len();
            if after > before {
                buf.filled_mut()[before] ^= 0xff;
                self.corrupted = true;
            }
        }
        result
    }
}

#[async_trait]
impl ContentSource for FaultingSource<'_> {
    fn pond_id(&self) -> uuid::Uuid {
        self.inner.pond_id()
    }

    async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>, StewardError> {
        self.inner.get_tip(ref_name).await
    }

    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        self.inner.get_object(hash).await
    }

    async fn object_size(&self, hash: ObjectHash) -> Result<Option<u64>, StewardError> {
        self.inner.object_size(hash).await
    }

    async fn get_objects(
        &self,
        hashes: &[ObjectHash],
    ) -> Result<HashMap<ObjectHash, Vec<u8>>, StewardError> {
        let mut values = self.inner.get_objects(hashes).await?;
        match &self.batch_fault {
            Some(BatchFault::Omit(hash)) => {
                let _ = values.remove(hash);
            }
            Some(BatchFault::Corrupt(hash)) => {
                if let Some(bytes) = values.get_mut(hash) {
                    if let Some(first) = bytes.first_mut() {
                        *first ^= 0xff;
                    } else {
                        bytes.push(0xff);
                    }
                }
            }
            Some(BatchFault::Unexpected(hash, bytes)) => {
                let _ = values.insert(*hash, bytes.clone());
            }
            None => {}
        }
        Ok(values)
    }

    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError> {
        self.inner.has_blob(hash).await
    }

    async fn list_blobs(&self) -> Result<HashSet<ObjectHash>, StewardError> {
        self.inner.list_blobs().await
    }

    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError> {
        let reader = self.inner.get_blob_reader(hash).await?;
        match (&self.blob_fault, reader) {
            (Some(BlobFault::Corrupt(target)), Some(reader)) if *target == hash => {
                Ok(Some(Box::new(CorruptingReader {
                    inner: reader,
                    corrupted: false,
                })))
            }
            (_, reader) => Ok(reader),
        }
    }

    async fn get_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<sync_store::content::PackDescriptor>, StewardError> {
        self.inner.get_series_pack(series_hash).await
    }

    async fn get_consolidated_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<sync_store::content::PackDescriptor>, StewardError> {
        self.inner.get_consolidated_series_pack(series_hash).await
    }

    async fn list_pack_hashes(
        &self,
        series_hash: ObjectHash,
    ) -> Result<HashSet<ObjectHash>, StewardError> {
        self.inner.list_pack_hashes(series_hash).await
    }

    async fn get_pack_index(
        &self,
        series_hash: ObjectHash,
        pack_hash: ObjectHash,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        self.inner.get_pack_index(series_hash, pack_hash).await
    }
}

impl<'a> CountingSource<'a> {
    fn new(inner: &'a dyn ContentSource) -> Self {
        Self {
            inner,
            counts: Arc::new(ReadCounts::default()),
        }
    }
}

#[async_trait]
impl ContentSource for AdvancingStateSource<'_> {
    fn pond_id(&self) -> uuid::Uuid {
        self.inner.pond_id()
    }

    async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>, StewardError> {
        Ok(self
            .get_publication_state(ref_name)
            .await?
            .map(|state| state.snapshot_tip))
    }

    async fn get_publication_state(
        &self,
        ref_name: &str,
    ) -> Result<Option<sync_store::PublicationState>, StewardError> {
        if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            Ok(Some(self.first.clone()))
        } else {
            self.inner.get_publication_state(ref_name).await
        }
    }

    async fn get_publication_record(
        &self,
        hash: ObjectHash,
    ) -> Result<Option<sync_store::content::PublicationRecord>, StewardError> {
        self.inner.get_publication_record(hash).await
    }

    async fn get_publication_pack(
        &self,
        descriptor: sync_store::content::PackDescriptor,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        self.inner.get_publication_pack(descriptor).await
    }

    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        self.inner.get_object(hash).await
    }

    async fn object_size(&self, hash: ObjectHash) -> Result<Option<u64>, StewardError> {
        self.inner.object_size(hash).await
    }

    async fn get_objects(
        &self,
        hashes: &[ObjectHash],
    ) -> Result<HashMap<ObjectHash, Vec<u8>>, StewardError> {
        self.inner.get_objects(hashes).await
    }

    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError> {
        self.inner.has_blob(hash).await
    }

    async fn list_blobs(&self) -> Result<HashSet<ObjectHash>, StewardError> {
        self.inner.list_blobs().await
    }

    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError> {
        self.inner.get_blob_reader(hash).await
    }

    async fn get_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<sync_store::content::PackDescriptor>, StewardError> {
        self.inner.get_series_pack(series_hash).await
    }

    async fn get_consolidated_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<sync_store::content::PackDescriptor>, StewardError> {
        self.inner.get_consolidated_series_pack(series_hash).await
    }

    async fn list_pack_hashes(
        &self,
        series_hash: ObjectHash,
    ) -> Result<HashSet<ObjectHash>, StewardError> {
        self.inner.list_pack_hashes(series_hash).await
    }

    async fn get_pack_index(
        &self,
        series_hash: ObjectHash,
        pack_hash: ObjectHash,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        self.inner.get_pack_index(series_hash, pack_hash).await
    }
}

#[async_trait]
impl ContentSource for CountingSource<'_> {
    fn pond_id(&self) -> uuid::Uuid {
        self.inner.pond_id()
    }

    async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>, StewardError> {
        ContentSource::get_tip(self.inner, ref_name).await
    }

    async fn get_publication_state(
        &self,
        ref_name: &str,
    ) -> Result<Option<sync_store::PublicationState>, StewardError> {
        ContentSource::get_publication_state(self.inner, ref_name).await
    }

    async fn get_publication_record(
        &self,
        hash: ObjectHash,
    ) -> Result<Option<sync_store::content::PublicationRecord>, StewardError> {
        let _ = self
            .counts
            .publication_record_requests
            .fetch_add(1, Ordering::Relaxed);
        let _ = self.counts.commit_index_requests.compare_exchange(
            0,
            1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        ContentSource::get_publication_record(self.inner, hash).await
    }

    async fn get_publication_pack(
        &self,
        descriptor: sync_store::content::PackDescriptor,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        let value = ContentSource::get_publication_pack(self.inner, descriptor).await?;
        if let Some(bytes) = &value {
            let _ = self
                .counts
                .pack_index_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(value)
    }

    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        self.counts
            .object_requests
            .lock()
            .expect("object_requests lock")
            .push(hash);
        let value = ContentSource::get_object(self.inner, hash).await?;
        if let Some(bytes) = &value {
            let _ = self
                .counts
                .object_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(value)
    }

    async fn object_size(&self, hash: ObjectHash) -> Result<Option<u64>, StewardError> {
        self.inner.object_size(hash).await
    }

    async fn get_objects(
        &self,
        hashes: &[ObjectHash],
    ) -> Result<HashMap<ObjectHash, Vec<u8>>, StewardError> {
        self.counts
            .object_batches
            .lock()
            .expect("object_batches lock")
            .push(hashes.to_vec());
        self.counts
            .object_requests
            .lock()
            .expect("object_requests lock")
            .extend(hashes.iter().copied());
        let values = ContentSource::get_objects(self.inner, hashes).await?;
        let bytes = values.values().map(Vec::len).sum::<usize>() as u64;
        let _ = self.counts.object_bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(values)
    }

    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError> {
        ContentSource::has_blob(self.inner, hash).await
    }

    async fn list_blobs(&self) -> Result<HashSet<ObjectHash>, StewardError> {
        Err(StewardError::Content(
            "incremental pulls must not list the complete blob partition".to_string(),
        ))
    }

    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError> {
        let reader = ContentSource::get_blob_reader(self.inner, hash).await?;
        let Some(reader) = reader else {
            return Ok(None);
        };
        self.counts
            .blob_requests
            .lock()
            .expect("blob_requests lock")
            .push(hash);
        Ok(Some(Box::new(CountingReader {
            inner: reader,
            counts: Arc::clone(&self.counts),
        })))
    }

    async fn get_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<sync_store::content::PackDescriptor>, StewardError> {
        ContentSource::get_series_pack(self.inner, series_hash).await
    }

    async fn get_consolidated_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<sync_store::content::PackDescriptor>, StewardError> {
        ContentSource::get_consolidated_series_pack(self.inner, series_hash).await
    }

    async fn list_pack_hashes(
        &self,
        series_hash: ObjectHash,
    ) -> Result<HashSet<ObjectHash>, StewardError> {
        ContentSource::list_pack_hashes(self.inner, series_hash).await
    }

    async fn get_pack_index(
        &self,
        series_hash: ObjectHash,
        pack_hash: ObjectHash,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        let value = ContentSource::get_pack_index(self.inner, series_hash, pack_hash).await?;
        if let Some(bytes) = &value {
            let _ = self
                .counts
                .pack_index_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(value)
    }
}

fn only_fetched_series(graph: &steward::FetchedGraph) -> &steward::FetchedSeriesV2 {
    let mut all_series = graph.objects.values().filter_map(|object| match object {
        FetchedObject::SeriesV2(series) => Some(series.as_ref()),
        _ => None,
    });
    let fetched_series = all_series.next().expect("one fetched series");
    assert!(
        all_series.next().is_none(),
        "fixture must contain one series"
    );
    fetched_series
}

fn only_series_pack(graph: &steward::FetchedGraph) -> &PackIndex {
    let fetched_series = only_fetched_series(graph);
    assert_eq!(
        fetched_series.packs.len(),
        1,
        "fixture must select one pack"
    );
    &fetched_series.packs[0].1
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

    let mat = materialize_content_objects(&ship)
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

#[tokio::test]
async fn initial_fetch_does_not_scan_complete_commit_history() {
    let (_t, mut src) = new_pond("push-ancestry-src").await;
    write_file(&mut src, "/v1.txt", b"one").await;
    let (_rt, mut remote) = push(&src).await;
    let old_tip = remote
        .get_tip("main")
        .await
        .expect("read old tip")
        .expect("old tip");

    write_file(&mut src, "/v2.txt", b"two").await;
    write_file(&mut src, "/v3.txt", b"three").await;
    repush(&src, &mut remote).await;

    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    assert_eq!(
        graph.commits.len(),
        1,
        "initial clone needs only the current tip"
    );
    assert_ne!(graph.commits[0].0, old_tip);
}

#[tokio::test]
async fn incremental_fetch_bounds_ancestry_at_known_tip() {
    const LAG_COMMITS: usize = 8;

    let (_t, mut src) = new_pond("bounded-ancestry-src").await;
    write_file(&mut src, "/initial.txt", b"initial").await;
    write_file(&mut src, "/baseline.txt", b"baseline").await;
    let (_rt, mut remote) = push(&src).await;
    let old_tip = remote
        .get_tip("main")
        .await
        .expect("read old tip")
        .expect("old tip");
    let old_commit_bytes = remote
        .get_object(old_tip)
        .await
        .expect("read old commit")
        .expect("old commit");
    let old_parent = Commit::decode(&old_commit_bytes)
        .expect("decode old commit")
        .parent_commit_hash
        .expect("old parent");

    for index in 0..LAG_COMMITS {
        write_file(
            &mut src,
            &format!("/next-{index}.txt"),
            format!("next-{index}").as_bytes(),
        )
        .await;
    }
    repush(&src, &mut remote).await;

    let source = CountingSource::new(&remote);
    let graph = fetch_object_graph_since(&source, "main", Some(old_tip))
        .await
        .expect("fetch bounded ancestry");

    assert_eq!(
        graph.commits.len(),
        LAG_COMMITS + 1,
        "every lagging commit plus the known boundary must be fetched"
    );
    assert_eq!(
        graph.commits.last().map(|(hash, _)| *hash),
        Some(old_tip),
        "the known ancestor must be included as the fast-forward proof boundary"
    );
    assert!(
        !source
            .counts
            .object_requests
            .lock()
            .unwrap()
            .contains(&old_parent),
        "no commit older than the durable prior tip may be requested"
    );
    assert_eq!(
        source.counts.object_point_requests.load(Ordering::Relaxed),
        0,
        "indexed ancestry must issue no object point queries"
    );
    assert_eq!(
        source.counts.commit_index_requests.load(Ordering::Relaxed),
        1,
        "bounded ancestry must use the publication-record channel"
    );
    assert_eq!(
        source
            .counts
            .object_batches
            .lock()
            .expect("object_batches lock")
            .len(),
        0,
        "native-v2 pulls do not query a Delta object partition"
    );
}

#[tokio::test]
async fn exact_publication_fetch_does_not_reread_an_advancing_head() {
    let (_t, mut source_pond) = new_pond("advancing-head-source").await;
    write_file(&mut source_pond, "/a.txt", b"a").await;
    let (_remote_dir, mut remote) = push(&source_pond).await;
    let first = remote
        .get_publication_state("main")
        .await
        .expect("read first state")
        .expect("first publication");
    write_file(&mut source_pond, "/b.txt", b"b").await;
    repush(&source_pond, &mut remote).await;
    let second = remote
        .get_publication_state("main")
        .await
        .expect("read second state")
        .expect("second publication");
    assert_ne!(first.snapshot_tip, second.snapshot_tip);

    let source = AdvancingStateSource {
        inner: &remote,
        first: first.clone(),
        calls: AtomicU64::new(0),
    };
    let selected = source
        .get_publication_state("main")
        .await
        .expect("select publication")
        .expect("selected publication");
    let graph = steward::fetch_object_graph_at_publication(&source, selected.clone(), None)
        .await
        .expect("fetch exact selected publication");
    assert_eq!(
        source.calls.load(Ordering::Relaxed),
        1,
        "fetching the selected graph must not reread a mutable active head"
    );
    assert_eq!(graph.publication_state, Some(selected));
    assert_eq!(graph.tip, Some(first.snapshot_tip));
}

#[tokio::test]
async fn incremental_fetch_rejects_unknown_ancestry_boundary() {
    let (_t, mut src) = new_pond("missing-ancestry-boundary-src").await;
    write_file(&mut src, "/initial.txt", b"initial").await;
    write_file(&mut src, "/next.txt", b"next").await;
    let (_rt, remote) = push(&src).await;
    let unrelated = ObjectHash::of_bytes(b"not a commit in this source");

    let error = fetch_object_graph_since(&remote, "main", Some(unrelated))
        .await
        .expect_err("unknown publication baseline must fail");
    assert!(
        error
            .to_string()
            .contains("does not contain known snapshot")
    );
}

#[tokio::test]
async fn malformed_publication_chain_cannot_exceed_advertised_generation_reads() {
    let remote_dir = tempdir().expect("remote dir");
    let pond_id = uuid::Uuid::new_v4();
    let mut remote = ContentRemote::create_at(remote_dir.path().join("remote"), pond_id)
        .await
        .expect("create remote");
    let mut parent = None;
    let mut first = None;
    let mut head = None;
    for index in 0..10 {
        let snapshot = ObjectHash::of_bytes(format!("malformed-snapshot-{index}").as_bytes());
        let root = ObjectHash::of_bytes(format!("malformed-root-{index}").as_bytes());
        let record = sync_store::content::PublicationRecord::new(
            pond_id,
            "main",
            snapshot,
            root,
            parent,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("publication record");
        let record_hash = record.hash();
        let _ = remote
            .put_publication_record(&record)
            .await
            .expect("store publication record");
        parent = Some(record_hash);
        let _ = first.get_or_insert((snapshot, root, record_hash));
        head = Some((snapshot, root, record_hash));
    }
    let (first_snapshot, first_root, first_record) = first.expect("first");
    let first_state = sync_store::PublicationState::new(
        pond_id,
        "main",
        first_snapshot,
        first_root,
        first_record,
        1,
        0,
    )
    .expect("first state");
    let _ = remote
        .compare_and_swap_publication(sync_store::PublicationExpectation::Missing, first_state)
        .await
        .expect("publish initial state");
    let (snapshot, root, publication_record) = head.expect("head");
    let state = sync_store::PublicationState::new(
        pond_id,
        "main",
        snapshot,
        root,
        publication_record,
        2,
        1,
    )
    .expect("state");
    let _ = remote
        .compare_and_swap_publication(
            sync_store::PublicationExpectation::Existing {
                generation: 1,
                publication_record: first_record,
            },
            state,
        )
        .await
        .expect("publish malformed head");

    let source = CountingSource::new(&remote);
    let missing = ObjectHash::of_bytes(b"missing advertised-generation boundary");
    let error = fetch_object_graph_since(&source, "main", Some(missing))
        .await
        .expect_err("malformed overlong history must fail at its advertised bound");
    assert!(
        error.to_string().contains("within advertised generation 2"),
        "{error}"
    );
    assert_eq!(
        source
            .counts
            .publication_record_requests
            .load(Ordering::Relaxed),
        2,
        "a generation-2 active row permits at most two publication-record reads"
    );
}

#[tokio::test]
async fn local_source_synthesizes_bounded_publication_history() {
    let (src_dir, mut src) = new_pond("local-sequential-ancestry").await;
    let src_path = src_dir.path().join("pond");
    write_file(&mut src, "/initial.txt", b"initial").await;
    let old_seq = src
        .control_table()
        .latest_spine_seq()
        .await
        .expect("read old spine")
        .expect("old spine");
    let old_tip = ObjectHash::from_hex(
        &src.control_table()
            .commit_hash_at(old_seq)
            .await
            .expect("read old hash")
            .expect("old hash"),
    )
    .expect("decode old hash");
    for index in 0..3 {
        write_file(
            &mut src,
            &format!("/local-{index}.txt"),
            format!("local-{index}").as_bytes(),
        )
        .await;
    }
    drop(src);

    let local = LocalPondSource::open(&src_path)
        .await
        .expect("open local source");
    let source = CountingSource::new(&local);
    let graph = fetch_object_graph_since(&source, "main", Some(old_tip))
        .await
        .expect("local sequential ancestry");

    assert_eq!(graph.commits.len(), 4, "three new commits plus boundary");
    assert_eq!(
        source.counts.commit_index_requests.load(Ordering::Relaxed),
        1,
        "the local source exposes publication records"
    );
    assert_eq!(
        source.counts.object_point_requests.load(Ordering::Relaxed),
        0,
        "the local source needs no legacy commit-index fallback"
    );
}

#[tokio::test]
async fn push_uses_log_tip_when_control_spine_is_stale() {
    let (_t, mut src) = new_pond("push-log-tip-src").await;
    write_file(&mut src, "/v1.txt", b"one").await;
    let old_seq = src.last_write_seq();
    let old_spine = steward::CommitSpine {
        root_tree_hash: src
            .control_table()
            .root_tree_hash_at(old_seq)
            .await
            .expect("read old root")
            .expect("old root"),
        parent_commit_hash: src
            .control_table()
            .parent_commit_hash_at(old_seq)
            .await
            .expect("read old parent"),
        commit_hash: src
            .control_table()
            .commit_hash_at(old_seq)
            .await
            .expect("read old hash")
            .expect("old hash"),
        commit_object: src
            .control_table()
            .commit_object_at(old_seq)
            .await
            .expect("read old object")
            .expect("old object"),
    };

    write_file(&mut src, "/v2.txt", b"two").await;
    let current_seq = src.last_write_seq();
    let authoritative_tip = src
        .control_table()
        .commit_hash_at(current_seq)
        .await
        .expect("read current hash")
        .expect("current hash");
    let fake_meta = PondTxnMetadata::new(current_seq + 100, meta("stale-control-spine"));
    let data_version = src
        .data_persistence()
        .table()
        .version()
        .expect("data version");
    src.control_table_mut()
        .record_data_committed(
            &fake_meta,
            steward::TransactionType::Write,
            data_version,
            0,
            Some(old_spine),
        )
        .await
        .expect("inject stale control spine");

    let (_rt, remote) = push(&src).await;
    assert_eq!(
        remote
            .get_tip("main")
            .await
            .expect("read pushed tip")
            .expect("pushed tip")
            .to_hex(),
        authoritative_tip
    );
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

/// A multi-version (multi-leaf) native `watertown.series.v3` table series survives
/// the full round trip: the rebuilt pond is content-equal to the source,
/// materializing every logical leaf in order so the read-side fold's root
/// tree hash matches (design Section 8.5.3, release blocker item 1 --
/// `docs/logical-series-identity-design.md`).
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

    let (_rt, remote) = push(&src).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "series-dst")
        .await
        .expect("create dst pond");

    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild must materialize the native v2 series");
    assert_eq!(outcome.series, 1);

    assert_eq!(
        root_hash(&dst).await,
        root_hash(&src).await,
        "rebuilt pond must be content-equal to the source, including its v2 series"
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
    let big: Vec<u8> = (0..256 * 1024_usize)
        .map(|i| ((i * 31 + 7) & 0xff) as u8)
        .collect();
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

#[tokio::test]
async fn external_blob_validation_failure_leaves_target_unchanged() {
    let (_t, mut src) = new_pond("large-abort-src").await;
    let big: Vec<u8> = (0..256 * 1024).map(|i| (i * 31 + 7) as u8).collect();
    write_file(&mut src, "/big.bin", &big).await;

    let (_rt, remote) = push(&src).await;
    let valid = fetch_object_graph(&remote, "main").await.expect("fetch");
    assert_eq!(valid.external_blobs.len(), 1);
    let mut invalid = valid.clone();
    invalid.commits[0].1.manifest_root = ObjectHash::of_bytes(b"wrong external-blob manifest root");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "large-abort-dst")
        .await
        .expect("create dst");
    let version_before = dst.data_persistence().table().version();
    let root_before = root_hash(&dst).await;

    let _ = steward::rebuild_pond(&mut dst, &remote, &invalid)
        .await
        .expect_err("bad root must abort after streaming the external blob");
    assert_eq!(dst.data_persistence().table().version(), version_before);
    assert_eq!(root_hash(&dst).await, root_before);

    let _ = steward::rebuild_pond(&mut dst, &remote, &valid)
        .await
        .expect("valid retry");
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
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
    let source = CountingSource::new(&remote);
    let graph = fetch_object_graph(&source, "main").await.expect("fetch");
    let metadata_batches = source.counts.object_batches.lock().unwrap().len();
    let outcome = steward::rebuild_pond(&mut dst, &source, &graph)
        .await
        .expect("re-pull");

    assert_eq!(outcome.dirs, 0);
    assert_eq!(outcome.files, 0);
    assert_eq!(outcome.series, 0);
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
    assert_eq!(
        source.counts.object_batches.lock().unwrap().len(),
        metadata_batches,
        "an unchanged series suffix plan must skip the payload batch"
    );
    assert_eq!(
        source.counts.object_point_requests.load(Ordering::Relaxed),
        0,
        "an unchanged pull must issue no object point queries"
    );
}

/// Appending a version to a source series used to be mirrored as a
/// Appending a version to a source series is mirrored as a suffix-append,
/// not a recreate: the consumer keeps the leaves it already materialized
/// and writes only the new one(s) (design Section 8.5.3, release blocker
/// item 1 -- `docs/logical-series-identity-design.md`).
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
        .expect("initial rebuild must materialize the v2 series");
    assert_eq!(outcome.series, 1);
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);

    // Append a third version at the source, then re-push and re-pull: only
    // the new leaf should be written, and the mirror must converge again.
    write_series(&mut src, "/r.series", &[(3_000, "v3")]).await;
    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("re-fetch");
    let outcome = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("re-pull must append only the new leaf");
    // The node already existed, so this is an append (not a create): no new
    // dirs/files/series nodes are counted, only the leaf itself is written.
    assert_eq!(outcome.dirs, 0);
    assert_eq!(outcome.files, 0);
    assert_eq!(outcome.series, 0);
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

#[tokio::test]
async fn renamed_series_reuses_its_canonical_pack() {
    let (_t, mut src) = new_pond("renamed-series-src").await;
    write_series(
        &mut src,
        "/before.series",
        &[(1_000, "first"), (2_000, "second")],
    )
    .await;

    let (_rt, mut remote) = push(&src).await;
    let first_state = remote
        .current_publication("main")
        .await
        .expect("publication state")
        .expect("initial publication");
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "renamed-series-dst")
        .await
        .expect("create dst");
    let initial = fetch_object_graph(&remote, "main")
        .await
        .expect("initial fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &initial)
        .await
        .expect("initial rebuild");

    rename(&mut src, "/before.series", "/after.series").await;
    repush(&src, &mut remote).await;
    let incremental = fetch_object_graph_since(&remote, "main", Some(first_state.snapshot_tip))
        .await
        .expect("renamed series fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &incremental)
        .await
        .expect("renamed series rebuild");

    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

#[tokio::test]
async fn unchanged_series_under_a_moved_directory_remains_fetchable() {
    let (_t, mut src) = new_pond("moved-series-src").await;
    src.write_transaction(&meta("mkdir-series-parent"), async move |transaction| {
        let root = transaction.root().await?;
        let _ = root.create_dir_all("/before").await?;
        Ok(())
    })
    .await
    .expect("create series parent");
    write_series(
        &mut src,
        "/before/observations.series",
        &[(1_000, "first"), (2_000, "second")],
    )
    .await;

    let (_rt, mut remote) = push(&src).await;
    let first_state = remote
        .current_publication("main")
        .await
        .expect("publication state")
        .expect("initial publication");
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "moved-series-dst")
        .await
        .expect("create dst");
    let initial = fetch_object_graph(&remote, "main")
        .await
        .expect("initial fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &initial)
        .await
        .expect("initial rebuild");

    rename(&mut src, "/before", "/after").await;
    repush(&src, &mut remote).await;
    let incremental = fetch_object_graph_since(&remote, "main", Some(first_state.snapshot_tip))
        .await
        .expect("moved series fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &incremental)
        .await
        .expect("moved series rebuild");

    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

#[tokio::test]
async fn unrelated_incremental_change_keeps_unchanged_series() {
    let (_t, mut src) = new_pond("unchanged-series-src").await;
    write_series(&mut src, "/observations.series", &[(1_000, "first")]).await;
    let (_rt, mut remote) = push(&src).await;
    let first_state = remote
        .current_publication("main")
        .await
        .expect("publication state")
        .expect("initial publication");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "unchanged-series-dst")
        .await
        .expect("create dst");
    let initial = fetch_object_graph(&remote, "main")
        .await
        .expect("initial fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &initial)
        .await
        .expect("initial rebuild");

    write_file(&mut src, "/unrelated.txt", b"new").await;
    repush(&src, &mut remote).await;
    let incremental = fetch_object_graph_since(&remote, "main", Some(first_state.snapshot_tip))
        .await
        .expect("incremental fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &incremental)
        .await
        .expect("apply unrelated change beside unchanged series");

    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

#[tokio::test]
async fn pinned_mirror_change_does_not_emit_local_limiter_usage() {
    let (_t, mut src) = new_pond("pinned-usage-src").await;
    write_file(&mut src, "/value.txt", b"first").await;
    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "pinned-usage-dst")
        .await
        .expect("create dst");
    let initial = fetch_object_graph(&remote, "main")
        .await
        .expect("initial fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &initial)
        .await
        .expect("initial rebuild");

    let sample = steward::UsageSample {
        at_us: 1,
        limiter: "/sys/limits/pull-bytes".to_string(),
        unit: "bytes".to_string(),
        amount: 1,
        observed: 1,
        used: 1,
        limit: 1024,
        window_us: 60_000_000,
    };
    steward::limiter_usage::queue(dst.control_table_mut(), std::slice::from_ref(&sample)).await;

    write_file(&mut src, "/other.txt", b"second").await;
    repush(&src, &mut remote).await;
    let changed = fetch_object_graph(&remote, "main")
        .await
        .expect("changed fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &changed)
        .await
        .expect("pinned rebuild must ignore local telemetry");
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
    assert_eq!(
        steward::limiter_usage::read_pending(dst.control_table()).await,
        vec![sample],
        "pinned replication must leave local telemetry queued"
    );
}

#[tokio::test]
async fn incremental_inline_series_prefetches_exact_suffix_in_one_batch() {
    let (_t, mut src) = new_pond("inline-prefetch-src").await;
    let baseline = [b"baseline-one\n".as_slice(), b"baseline-two\n".as_slice()];
    for bytes in baseline {
        write_file_series_version(&mut src, "/history.series", bytes).await;
    }

    let (_rt, mut remote) = push(&src).await;
    let baseline_state = remote
        .current_publication("main")
        .await
        .expect("baseline publication")
        .expect("baseline state");
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "inline-prefetch-dst")
        .await
        .expect("create dst");
    let initial = fetch_object_graph(&remote, "main")
        .await
        .expect("initial fetch");
    let historical_objects = only_fetched_series(&initial)
        .physical_object_hashes
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let _ = steward::rebuild_pond(&mut dst, &remote, &initial)
        .await
        .expect("initial materialization");

    for bytes in [
        b"suffix-one\n".as_slice(),
        b"suffix-two\n".as_slice(),
        b"suffix-three\n".as_slice(),
    ] {
        write_file_series_version(&mut src, "/history.series", bytes).await;
    }
    repush(&src, &mut remote).await;

    let source = CountingSource::new(&remote);
    let graph = fetch_object_graph_since(&source, "main", Some(baseline_state.snapshot_tip))
        .await
        .expect("incremental metadata fetch");
    let metadata_batches = source.counts.object_batches.lock().unwrap().len();
    let fetched = only_fetched_series(&graph);
    assert_eq!(fetched.leaf_start, baseline.len() as u64);
    assert_eq!(fetched.leaf_hashes.len(), 3);
    let suffix_objects = fetched
        .packs
        .iter()
        .flat_map(|(_, pack)| pack.object_spans())
        .map(|span| span.object_hash())
        .collect::<HashSet<_>>();
    assert!(
        suffix_objects.len() >= 2,
        "fixture must require multiple distinct inline physical objects"
    );

    let _ = steward::rebuild_pond(&mut dst, &source, &graph)
        .await
        .expect("incremental inline materialization");

    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
    assert_eq!(
        source.counts.object_point_requests.load(Ordering::Relaxed),
        0,
        "pack payload materialization must never issue object point queries"
    );
    assert!(
        source.counts.blob_requests.lock().unwrap().is_empty(),
        "small physical objects must be served by the inline exact batch"
    );
    let batches = source.counts.object_batches.lock().unwrap();
    assert_eq!(
        batches.len(),
        metadata_batches + 1,
        "all required inline suffix objects must use one additional exact batch"
    );
    let payload_batch = batches.last().expect("payload batch");
    assert_eq!(
        payload_batch.iter().copied().collect::<HashSet<_>>(),
        suffix_objects,
        "the payload batch must contain exactly the suffix-intersecting physical objects"
    );
    assert!(
        payload_batch
            .iter()
            .all(|hash| !historical_objects.contains(hash)),
        "objects wholly inside the durable prefix must not be requested"
    );
}

#[tokio::test]
async fn incremental_file_series_pull_reads_only_the_bounded_physical_suffix() {
    const MIB: usize = 1024 * 1024;
    const HISTORICAL_BYTES: u64 = 6 * MIB as u64;

    let (_t, mut src) = new_pond("bounded-series-src").await;
    write_file_series_version(&mut src, "/history.series", &vec![0x11; 2 * MIB]).await;
    write_file_series_version(&mut src, "/history.series", &vec![0x22; 2 * MIB]).await;
    write_file_series_version(&mut src, "/history.series", &vec![0x33; 2 * MIB]).await;
    let initial_maintenance = src
        .collapse_versions(1)
        .await
        .expect("build bounded initial pack");
    assert_eq!(initial_maintenance.series_repacked, 1);

    let (_rt, mut remote) = push(&src).await;
    let baseline_state = remote
        .current_publication("main")
        .await
        .expect("baseline publication")
        .expect("baseline state");
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "bounded-series-dst")
        .await
        .expect("create dst");

    let historical_objects = {
        let source = CountingSource::new(&remote);
        let graph = fetch_object_graph(&source, "main")
            .await
            .expect("initial metadata fetch must not preload all objects");
        assert_eq!(
            source.counts.blob_bytes.load(Ordering::Relaxed),
            0,
            "metadata discovery must not read pack payload bytes"
        );
        assert!(
            source.counts.blob_requests.lock().unwrap().is_empty(),
            "metadata discovery must not request pack payload objects"
        );
        let expected_initial_bytes: u64 = only_series_pack(&graph)
            .object_spans()
            .iter()
            .map(|span| span.physical_len())
            .sum();
        let historical_objects = only_series_pack(&graph)
            .physical_object_hashes()
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let _ = steward::rebuild_pond(&mut dst, &source, &graph)
            .await
            .expect("initial materialization");
        assert_eq!(
            source.counts.blob_bytes.load(Ordering::Relaxed),
            expected_initial_bytes,
            "initial materialization must stream each physical pack object exactly once"
        );
        historical_objects
    };
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);

    let appended = vec![0x44; 128 * 1024];
    write_file_series_version(&mut src, "/history.series", &appended).await;
    let incremental_maintenance = src
        .collapse_versions(1)
        .await
        .expect("refresh bounded pack after append");
    assert_eq!(incremental_maintenance.series_repacked, 1);
    repush(&src, &mut remote).await;

    let source = CountingSource::new(&remote);
    let graph = fetch_object_graph_since(&source, "main", Some(baseline_state.snapshot_tip))
        .await
        .expect("incremental metadata fetch must not preload all objects");
    assert_eq!(
        source.counts.blob_bytes.load(Ordering::Relaxed),
        0,
        "incremental metadata discovery must not read pack payload bytes"
    );
    let pack = only_series_pack(&graph);
    assert_eq!(pack.leaf_start(), 3);
    let expected_suffix_bytes: u64 = pack
        .object_spans()
        .iter()
        .map(|span| span.physical_len())
        .sum();
    let suffix_objects: HashSet<ObjectHash> = pack
        .object_spans()
        .iter()
        .map(|span| span.object_hash())
        .collect();
    let batches_before_rebuild = source.counts.object_batches.lock().unwrap().len();

    let _ = steward::rebuild_pond(&mut dst, &source, &graph)
        .await
        .expect("incremental materialization");

    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
    assert_eq!(
        source.counts.blob_bytes.load(Ordering::Relaxed),
        expected_suffix_bytes,
        "incremental pull must read only objects intersecting the missing suffix"
    );
    assert!(
        source
            .counts
            .blob_requests
            .lock()
            .unwrap()
            .iter()
            .all(|hash| !historical_objects.contains(hash)),
        "objects wholly before the durable local frontier must never be requested"
    );
    let requests = source.counts.blob_requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        suffix_objects.len(),
        "each required physical object must be requested exactly once"
    );
    assert_eq!(
        requests.iter().copied().collect::<HashSet<_>>(),
        suffix_objects,
        "materialization must request exactly the objects intersecting the suffix"
    );
    assert_eq!(
        source.counts.object_point_requests.load(Ordering::Relaxed),
        0,
        "external pack objects must not fall back to inline point queries"
    );
    let batches = source.counts.object_batches.lock().unwrap();
    assert_eq!(
        batches.len(),
        batches_before_rebuild,
        "external candidates bypass the inline payload batch"
    );
    assert!(
        expected_suffix_bytes < HISTORICAL_BYTES / 2,
        "the fixture must distinguish bounded suffix I/O from a historical replay"
    );
}

#[tokio::test]
async fn incremental_table_series_pull_reads_only_the_bounded_physical_suffix() {
    const ROWS_PER_LEAF: usize = 40_000;
    const HISTORICAL_ROWS: u64 = (3 * ROWS_PER_LEAF) as u64;

    let (src_dir, mut src) = new_pond("bounded-table-src").await;
    let src_path = src_dir.path().join("pond");
    write_series_batch(
        &mut src,
        "/history.series",
        series_batch_rows(1_000_000, ROWS_PER_LEAF, "first"),
    )
    .await;
    write_series_batch(
        &mut src,
        "/history.series",
        series_batch_rows(2_000_000, ROWS_PER_LEAF, "second"),
    )
    .await;
    write_series_batch(
        &mut src,
        "/history.series",
        series_batch_rows(3_000_000, ROWS_PER_LEAF, "third"),
    )
    .await;
    let initial_maintenance = src
        .collapse_versions(1)
        .await
        .expect("build bounded initial table pack");
    assert_eq!(initial_maintenance.series_repacked, 1);

    let (_rt, remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "bounded-table-dst")
        .await
        .expect("create dst");
    let initial = fetch_object_graph(&remote, "main")
        .await
        .expect("initial metadata fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &initial)
        .await
        .expect("initial table materialization");

    write_series_batch(
        &mut src,
        "/history.series",
        series_batch_rows(4_000_000, 10, "suffix"),
    )
    .await;
    let incremental_maintenance = src
        .collapse_versions(1)
        .await
        .expect("refresh bounded table pack after append");
    assert_eq!(incremental_maintenance.series_repacked, 1);
    let expected_root = root_hash(&src).await;
    drop(src);

    let local = LocalPondSource::open(&src_path)
        .await
        .expect("open maintained local source");
    let source = CountingSource::new(&local);
    let graph = fetch_object_graph(&source, "main")
        .await
        .expect("incremental table metadata fetch");
    assert_eq!(
        source.counts.blob_bytes.load(Ordering::Relaxed),
        0,
        "table metadata discovery must not read pack payload bytes"
    );
    let pack = only_series_pack(&graph);
    assert!(
        pack.object_spans().iter().any(|span| {
            span.logical_start() < HISTORICAL_ROWS && span.logical_end() > HISTORICAL_ROWS
        }),
        "the fixture must exercise a bounded object crossing the suffix frontier"
    );
    let historical_objects: HashSet<ObjectHash> = pack
        .object_spans()
        .iter()
        .filter(|span| span.logical_end() <= HISTORICAL_ROWS)
        .map(|span| span.object_hash())
        .collect();
    assert!(
        !historical_objects.is_empty(),
        "the fixture must contain a wholly historical physical object"
    );
    let suffix_objects: HashSet<ObjectHash> = pack
        .object_spans()
        .iter()
        .filter(|span| span.logical_end() > HISTORICAL_ROWS)
        .map(|span| span.object_hash())
        .collect();
    let expected_suffix_bytes: u64 = pack
        .object_spans()
        .iter()
        .filter(|span| span.logical_end() > HISTORICAL_ROWS)
        .map(|span| span.physical_len())
        .sum();

    let _ = steward::rebuild_pond(&mut dst, &source, &graph)
        .await
        .expect("incremental table materialization");

    assert_eq!(root_hash(&dst).await, expected_root);
    assert_eq!(
        source.counts.blob_bytes.load(Ordering::Relaxed),
        expected_suffix_bytes,
        "incremental table pull must read only suffix-intersecting objects"
    );
    let requests = source.counts.blob_requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .all(|hash| !historical_objects.contains(hash)),
        "wholly historical table objects must never be requested"
    );
    assert_eq!(
        requests.iter().copied().collect::<HashSet<_>>(),
        suffix_objects,
        "table materialization must request exactly the suffix objects"
    );
    assert_eq!(
        requests.len(),
        suffix_objects.len(),
        "each required table object must be requested exactly once"
    );
}

#[tokio::test]
async fn duplicate_series_references_fetch_each_physical_object_once() {
    let (_t, mut src) = new_pond("duplicate-series-src").await;
    let body = vec![0x5a; 128 * 1024];
    write_file_series_version(&mut src, "/first.series", &body).await;
    write_file_series_version(&mut src, "/second.series", &body).await;

    let (_rt, remote) = push(&src).await;
    let source = CountingSource::new(&remote);
    let graph = fetch_object_graph(&source, "main")
        .await
        .expect("fetch shared series metadata");
    let pack = only_series_pack(&graph);
    let expected_objects = pack
        .object_spans()
        .iter()
        .map(|span| span.object_hash())
        .collect::<HashSet<_>>();

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "duplicate-series-dst")
        .await
        .expect("create dst");
    let _ = steward::rebuild_pond(&mut dst, &source, &graph)
        .await
        .expect("materialize both references");

    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
    let requests = source.counts.blob_requests.lock().unwrap();
    assert_eq!(
        requests.iter().copied().collect::<HashSet<_>>(),
        expected_objects
    );
    assert_eq!(
        requests.len(),
        expected_objects.len(),
        "transaction-wide caching must prevent duplicate payload reads"
    );
}

#[tokio::test]
async fn divergent_local_series_prefix_fails_before_remote_payload_reads() {
    let (_t, mut src) = new_pond("divergent-prefix-src").await;
    write_file_series_version(&mut src, "/history.series", b"shared-prefix").await;
    let _ = src.collapse_versions(0).await.expect("build initial pack");

    let (_rt, mut remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "divergent-prefix-dst")
        .await
        .expect("create dst");
    let initial = fetch_object_graph(&remote, "main")
        .await
        .expect("initial fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &initial)
        .await
        .expect("initial materialization");

    write_file_series_version(&mut dst, "/history.series", b"local-divergence").await;
    write_file_series_version(&mut src, "/history.series", b"remote-append").await;
    let _ = src.collapse_versions(0).await.expect("refresh source pack");
    repush(&src, &mut remote).await;

    let version_before = dst.data_persistence().table().version();
    let source = CountingSource::new(&remote);
    let graph = fetch_object_graph(&source, "main")
        .await
        .expect("metadata remains valid");
    let batches_before = source.counts.object_batches.lock().unwrap().len();
    let err = steward::rebuild_pond(&mut dst, &source, &graph)
        .await
        .expect_err("a divergent local prefix must be rejected");

    assert!(
        format!("{err}").contains("diverged"),
        "failure must identify the prefix divergence: {err}"
    );
    assert_eq!(
        source.counts.blob_bytes.load(Ordering::Relaxed),
        0,
        "prefix validation must happen before any pack payload is read"
    );
    assert!(
        source.counts.blob_requests.lock().unwrap().is_empty(),
        "no physical object may be requested after prefix validation fails"
    );
    assert_eq!(
        source.counts.object_batches.lock().unwrap().len(),
        batches_before,
        "prefix validation must fail before the exact payload batch"
    );
    assert_eq!(
        source.counts.object_point_requests.load(Ordering::Relaxed),
        0,
        "prefix rejection must not issue a point lookup"
    );
    assert_eq!(
        dst.data_persistence().table().version(),
        version_before,
        "a rejected prefix must not commit any destination changes"
    );
}

#[tokio::test]
async fn inline_pack_prefetch_corruption_and_missing_data_fail_without_point_lookup() {
    let (_t, mut src) = new_pond("inline-prefetch-failure-src").await;
    write_file_series_version(&mut src, "/events.series", b"small inline payload\n").await;
    let (_rt, remote) = push(&src).await;
    let source = CountingSource::new(&remote);
    let graph = fetch_object_graph(&source, "main")
        .await
        .expect("fetch inline series metadata");
    let object_hash = only_series_pack(&graph)
        .object_spans()
        .first()
        .expect("physical object")
        .object_hash();
    assert!(
        ContentSource::get_objects(&remote, &[object_hash])
            .await
            .expect("probe inline object")
            .contains_key(&object_hash),
        "fixture physical object must be inline"
    );

    for (label, batch_fault, expected_error) in [
        (
            "corrupt",
            BatchFault::Corrupt(object_hash),
            "fetched object hashes to",
        ),
        (
            "missing",
            BatchFault::Omit(object_hash),
            "was omitted from the exact payload fetch",
        ),
    ] {
        let dst_dir = tempdir().expect("dst dir");
        let mut dst = Ship::create_pond(
            dst_dir.path().join("pond"),
            &format!("inline-prefetch-{label}-dst"),
        )
        .await
        .expect("create dst");
        let version_before = dst.data_persistence().table().version();
        let batches_before = source.counts.object_batches.lock().unwrap().len();
        let faulting = FaultingSource {
            inner: &source,
            batch_fault: Some(batch_fault),
            blob_fault: None,
        };
        let error = steward::rebuild_pond(&mut dst, &faulting, &graph)
            .await
            .expect_err("faulted inline payload must fail closed");
        assert!(
            error.to_string().contains(expected_error),
            "{label} failure must identify the payload fault: {error}"
        );
        assert_eq!(
            dst.data_persistence().table().version(),
            version_before,
            "{label} payload failure must not commit destination changes"
        );
        assert_eq!(
            source.counts.object_batches.lock().unwrap().len(),
            batches_before + 1,
            "{label} failure must perform exactly one payload batch"
        );
        assert_eq!(
            source.counts.object_point_requests.load(Ordering::Relaxed),
            0,
            "{label} failure must not fall back to get_object"
        );
    }
}

#[tokio::test]
async fn inline_pack_prefetch_rejects_unexpected_keys() {
    let (_t, mut src) = new_pond("inline-unexpected-src").await;
    write_file_series_version(&mut src, "/events.series", b"small inline payload\n").await;
    let (_rt, remote) = push(&src).await;
    let source = CountingSource::new(&remote);
    let graph = fetch_object_graph(&source, "main")
        .await
        .expect("fetch inline series metadata");
    let unexpected_bytes = b"not requested".to_vec();
    let unexpected_hash = ObjectHash::of_bytes(&unexpected_bytes);
    let faulting = FaultingSource {
        inner: &source,
        batch_fault: Some(BatchFault::Unexpected(unexpected_hash, unexpected_bytes)),
        blob_fault: None,
    };

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "inline-unexpected-dst")
        .await
        .expect("create dst");
    let version_before = dst.data_persistence().table().version();
    let error = steward::rebuild_pond(&mut dst, &faulting, &graph)
        .await
        .expect_err("unexpected batch key must fail closed");

    assert!(
        error.to_string().contains("unexpected key"),
        "failure must identify the unexpected key: {error}"
    );
    assert_eq!(dst.data_persistence().table().version(), version_before);
    assert_eq!(
        source.counts.object_point_requests.load(Ordering::Relaxed),
        0,
        "unexpected batch data must not trigger a point lookup"
    );
}

#[tokio::test]
async fn external_pack_object_corruption_still_streams_and_fails_closed() {
    let (_t, mut src) = new_pond("external-pack-corrupt-src").await;
    write_file_series_version(&mut src, "/events.series", &vec![0x5a; 256 * 1024]).await;
    let (_rt, remote) = push(&src).await;
    let source = CountingSource::new(&remote);
    let graph = fetch_object_graph(&source, "main")
        .await
        .expect("fetch external series metadata");
    let object_hash = only_series_pack(&graph)
        .object_spans()
        .first()
        .expect("physical object")
        .object_hash();
    assert!(
        ContentSource::object_size(&remote, object_hash)
            .await
            .expect("probe object size")
            .is_some_and(|size| size >= tlogfs::large_files::LARGE_FILE_THRESHOLD as u64),
        "fixture physical object must exercise the streaming path"
    );
    let faulting = FaultingSource {
        inner: &source,
        batch_fault: None,
        blob_fault: Some(BlobFault::Corrupt(object_hash)),
    };

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "external-pack-corrupt-dst")
        .await
        .expect("create dst");
    let version_before = dst.data_persistence().table().version();
    let error = steward::rebuild_pond(&mut dst, &faulting, &graph)
        .await
        .expect_err("corrupt external payload must fail closed");

    assert!(
        error.to_string().contains("external object hashes to"),
        "external hash failure must be explicit: {error}"
    );
    assert_eq!(dst.data_persistence().table().version(), version_before);
    assert_eq!(
        source.counts.blob_requests.lock().unwrap().as_slice(),
        &[object_hash],
        "external physical object must be opened exactly once through the streaming path"
    );
    assert_eq!(
        source.counts.object_point_requests.load(Ordering::Relaxed),
        0,
        "external corruption must not fall back to get_object"
    );
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

#[tokio::test]
async fn rename_cycle_avoids_a_real_temporary_name_collision() {
    let (_t, mut src) = new_pond("swap-temp-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    write_file(&mut src, "/b.txt", b"beta").await;
    let (_rt, mut remote) = push(&src).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let a_id = graph
        .manifest
        .iter()
        .find(|entry| entry.name == "a.txt")
        .expect("a.txt manifest entry")
        .node_id
        .clone();
    let collision_path = format!("/.pull-rename-tmp-{a_id}");
    write_file(&mut src, &collision_path, b"keep").await;
    repush(&src, &mut remote).await;

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "swap-temp-dst")
        .await
        .expect("create dst");
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("initial rebuild");

    rename(&mut src, "/a.txt", "/tmp.txt").await;
    rename(&mut src, "/b.txt", "/a.txt").await;
    rename(&mut src, "/tmp.txt", "/b.txt").await;
    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rename cycle with occupied default temp");

    assert_eq!(read_to_string(&mut dst, "/a.txt").await, "beta");
    assert_eq!(read_to_string(&mut dst, "/b.txt").await, "alpha");
    assert_eq!(read_to_string(&mut dst, &collision_path).await, "keep");
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

/// A COMPACTED `file:series` used to mirror its LIVE content, not its
/// superseded history: the old row-rewriting `collapse_versions` would merge
/// v1..vN into a single row carrying a `collapsed_through` sentinel, and the
/// fold had to skip exactly the versions the live series read skipped.
///
/// Row-rewriting collapse no longer exists at all for logical-series-v2
/// ponds (design doc, delivery gate 7): merging several rows into one cannot
/// represent each merged row's immutable per-append logical leaf.
/// `Ship::collapse_versions` now performs pack-only physical maintenance
/// instead -- it never merges/rewrites Oplog rows, so there is no
/// live-content-diverges-from-history bug left for this scenario to trigger.
/// This test is now the push/pull-path regression guard for that fact:
/// pack-only maintenance succeeds, and the series' live content (and thus
/// what a mirror replicates) is completely unaffected.
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

    // Pack-only maintenance repacks the four small physical objects into a
    // bounded pack; it never touches Oplog rows/logical content.
    let report = src
        .collapse_versions(1)
        .await
        .expect("pack-only maintenance must succeed");
    assert_eq!(report.candidates, 1);
    assert_eq!(report.series_repacked, 1);

    // The source's live content is untouched by pack-only maintenance.
    assert_eq!(read_to_string(&mut src, "/events.series").await, cumulative);
}

/// A mirror that already replicated a series' pre-collapse versions used to
/// need to converge when the source later compacted it. Row-rewriting
/// collapse no longer exists at all for v2 series (see
/// `compacted_file_series_mirrors_live_content_not_history` above; pack-only
/// maintenance never changes logical content), so this is simplified to what
/// it always also needed to prove: a multi-version native v2 series
/// round-trips through push/fetch/rebuild and the destination mirror is
/// content-equal to the source.
#[tokio::test]
async fn repull_after_source_side_collapse_converges() {
    let (_t, mut src) = new_pond("recollapse-src").await;

    let chunks: [&[u8]; 4] = [b"a,1\n", b"b,2\n", b"c,3\n", b"d,4\n"];
    let mut cumulative = String::new();
    for chunk in chunks {
        write_file_series_version(&mut src, "/events.series", chunk).await;
        cumulative.push_str(std::str::from_utf8(chunk).unwrap());
    }
    assert_eq!(read_to_string(&mut src, "/events.series").await, cumulative);

    let (_rt, remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "recollapse-dst")
        .await
        .expect("create dst");
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("rebuild must materialize the native v2 series");
    assert_eq!(read_to_string(&mut dst, "/events.series").await, cumulative);
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

/// Cross-pond imports must converge for temporal versions stored as external
/// blobs, matching the shape of the production septic series.  Collapse is
/// now gated for v2 series, so this exercises the import path directly on
/// the uncollapsed, multi-version, externally-stored series.
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
    let mut cumulative = Vec::new();
    for (chunk, timestamp) in &chunks {
        write_temporal_file_series_version(
            &mut src,
            "/events.series",
            chunk,
            *timestamp,
            *timestamp,
        )
        .await;
        cumulative.extend_from_slice(chunk);
    }
    assert_eq!(
        {
            let tx = src.begin_read(&meta("read")).await.expect("begin read");
            let root = tx.root().await.expect("root");
            root.read_file_path_to_vec("/events.series")
                .await
                .expect("read source series")
        },
        cumulative,
        "sanity: source series content is the concatenation of its versions"
    );

    let (_rt, remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "temporal-collapse-dst")
        .await
        .expect("create dst");
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::import_pond(&mut dst, &remote, &graph, src_id)
        .await
        .expect("import must materialize the native v2 series, including external leaves");
    assert_eq!(
        foreign_root_hash(&dst, src_id).await,
        root_hash(&src).await,
        "imported foreign tree must be content-equal to the source, including external leaves"
    );
}

/// A single-version series must still import cleanly and be content-equal on
/// the destination; this is the degenerate (one-leaf) case of the same v2
/// materialization path exercised by the multi-version tests above.
#[tokio::test]
async fn repull_after_metadata_only_series_collapse_converges() {
    let (_t, mut src) = new_pond("metadata-collapse-src").await;
    let src_id = src
        .data_persistence()
        .pond_id()
        .parse::<uuid7::Uuid>()
        .expect("source pond id");
    write_file_series_version(&mut src, "/events.series", b"one version\n").await;

    let (_rt, remote) = push(&src).await;
    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "metadata-collapse-dst")
        .await
        .expect("create dst");
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let _ = steward::import_pond(&mut dst, &remote, &graph, src_id)
        .await
        .expect("import must materialize the single-leaf native v2 series");
    assert_eq!(foreign_root_hash(&dst, src_id).await, root_hash(&src).await);
}

/// A source can append fresh versions after a mirror already pulled a
/// prefix; the mirror must adopt just the later appends without
/// re-materializing the leaves it already has.  (Collapse-then-append is
/// gated for v2 series, so this now covers the append-only-suffix shape on a
/// raw `FilePhysicalSeries` rather than a genuine collapse.)
#[tokio::test]
async fn repull_after_collapse_then_append_converges() {
    let (_t, mut src) = new_pond("recollapse2-src").await;

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
        .expect("initial rebuild must materialize the prefix");
    assert_eq!(
        read_to_string(&mut dst, "/events.series").await,
        "a,1\nb,2\n"
    );

    write_file_series_version(&mut src, "/events.series", b"c,3\n").await;
    repush(&src, &mut remote).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("re-fetch");
    let _ = steward::rebuild_pond(&mut dst, &remote, &graph)
        .await
        .expect("re-pull must append only the new leaf");
    assert_eq!(
        read_to_string(&mut dst, "/events.series").await,
        "a,1\nb,2\nc,3\n"
    );
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
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

/// A mismatch discovered only after applying the import plan is still rejected
/// before the Delta transaction commits. The failed attempt leaves no foreign
/// root behind, and the unmodified graph can be imported immediately afterward.
#[tokio::test]
async fn precommit_manifest_root_mismatch_leaves_target_unchanged() {
    let (_t, mut src) = new_pond("precommit-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    let src_id = src
        .data_persistence()
        .pond_id()
        .parse::<uuid7::Uuid>()
        .expect("source pond id");

    let (_rt, remote) = push(&src).await;
    let valid = fetch_object_graph(&remote, "main").await.expect("fetch");
    let mut invalid = valid.clone();
    invalid.commits[0].1.manifest_root = ObjectHash::of_bytes(b"wrong manifest root");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "precommit-dst")
        .await
        .expect("create dst");
    let version_before = dst.data_persistence().table().version();

    let err = steward::import_pond(&mut dst, &remote, &invalid, src_id)
        .await
        .expect_err("advertised manifest-root mismatch must abort");
    let message = err.to_string();
    assert!(
        message.contains("would produce root"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("expected root"),
        "unexpected bounded validation diagnosis: {message}"
    );
    assert_eq!(
        dst.data_persistence().table().version(),
        version_before,
        "failed validation must not advance the data Delta table"
    );
    assert!(
        steward::compute_content_tree_for_table(
            dst.data_persistence().table().clone(),
            &src_id.to_string(),
        )
        .await
        .is_err(),
        "failed first import must leave no foreign root"
    );

    let _ = steward::import_pond(&mut dst, &remote, &valid, src_id)
        .await
        .expect("valid retry");
    assert_eq!(foreign_root_hash(&dst, src_id).await, root_hash(&src).await);
}

/// Foreign data, the local mount, and its pin share one Delta transaction.
/// Validation failure leaves none of them behind; success commits all three.
#[tokio::test]
async fn graft_import_is_atomic_with_mount_and_pin() {
    let (_t, mut src) = new_pond("atomic-graft-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;
    let src_id = src
        .data_persistence()
        .pond_id()
        .parse::<uuid7::Uuid>()
        .expect("source pond id");

    let (_rt, remote) = push(&src).await;
    let valid = fetch_object_graph(&remote, "main").await.expect("fetch");
    let mut invalid = valid.clone();
    invalid.commits[0].1.manifest_root = ObjectHash::of_bytes(b"wrong manifest root");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "atomic-graft-dst")
        .await
        .expect("create dst");
    let version_before = dst.data_persistence().table().version();

    let _ = steward::import_graft(
        &mut dst,
        &remote,
        &invalid,
        src_id,
        "upstream",
        "/imports/upstream",
    )
    .await
    .expect_err("invalid graft must abort");
    assert_eq!(dst.data_persistence().table().version(), version_before);
    let tx = dst.begin_read(&meta("inspect-abort")).await.expect("read");
    assert!(
        !tx.root()
            .await
            .expect("root")
            .exists(&steward::GraftPin::pin_path("upstream"))
            .await
    );
    let _ = tx.commit().await.expect("close read");

    let _ = steward::import_graft(
        &mut dst,
        &remote,
        &valid,
        src_id,
        "upstream",
        "/imports/upstream",
    )
    .await
    .expect("valid graft");
    assert_eq!(
        dst.data_persistence().table().version(),
        version_before.map(|version| version + 1),
        "foreign rows, mount, and pin must land in one Delta commit"
    );
    assert_eq!(
        read_to_string(&mut dst, "/imports/upstream/a.txt").await,
        "alpha"
    );
    let pin_yaml = read_to_string(&mut dst, &steward::GraftPin::pin_path("upstream")).await;
    let pin = steward::GraftPin::from_yaml_bytes(pin_yaml.as_bytes()).expect("parse pin");
    assert_eq!(pin.foreign_pond_id, src_id.to_string());
    assert_eq!(pin.pinned_tip, valid.tip.expect("tip").to_hex());

    let committed_version = dst.data_persistence().table().version();
    let _ = steward::import_graft(
        &mut dst,
        &remote,
        &valid,
        src_id,
        "upstream",
        "/imports/upstream",
    )
    .await
    .expect("idempotent retry");
    assert_eq!(
        dst.data_persistence().table().version(),
        committed_version,
        "retry after a missing watermark must not create another data commit"
    );
}

#[tokio::test]
async fn exact_identity_noop_authenticates_remote_head_and_mirror_roots() {
    let (_source_dir, mut source) = new_pond("noop-auth-source").await;
    write_file(&mut source, "/a.txt", b"alpha").await;
    let (_remote_dir, remote) = push(&source).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let state = graph.publication_state.clone().expect("publication state");

    let destination_dir = tempdir().expect("destination");
    let mut destination = Ship::create_pond(destination_dir.path().join("pond"), "noop-auth-dst")
        .await
        .expect("create destination");
    let _ = steward::rebuild_pond(&mut destination, &remote, &graph)
        .await
        .expect("initial mirror");
    let commit = steward::authenticated_publication_head_commit(&remote, &state)
        .await
        .expect("authenticate head");
    let local_pond_id = destination.data_persistence().pond_id().to_string();
    steward::authenticate_destination_publication_head(
        &destination,
        &state,
        &commit,
        &local_pond_id,
    )
    .await
    .expect("matching destination roots");

    let stale_cursor = sync_store::content::encode_manifest_root(state.manifest_root);
    write_file(&mut destination, "/tampered.txt", b"tampered").await;
    let cursor = steward::get_data_path(destination.pond_path())
        .join("_content/v2/state/manifest-root")
        .join(format!("pond={local_pond_id}"));
    std::fs::write(&cursor, stale_cursor).expect("restore stale matching cursor");
    let error = steward::authenticate_destination_publication_head(
        &destination,
        &state,
        &commit,
        &local_pond_id,
    )
    .await
    .expect_err("stale acknowledgement must not hide destination divergence");
    assert!(error.to_string().contains("manifest root"));
}

#[tokio::test]
async fn exact_identity_graft_noop_authenticates_foreign_roots_mount_and_pin() {
    let (_source_dir, mut source) = new_pond("graft-noop-auth-source").await;
    write_file(&mut source, "/a.txt", b"alpha").await;
    let source_id = source
        .data_persistence()
        .pond_id()
        .parse::<uuid7::Uuid>()
        .expect("source id");
    let (_remote_dir, remote) = push(&source).await;
    let graph = fetch_object_graph(&remote, "main").await.expect("fetch");
    let state = graph.publication_state.clone().expect("publication state");

    let destination_dir = tempdir().expect("destination");
    let mut destination =
        Ship::create_pond(destination_dir.path().join("pond"), "graft-noop-auth-dst")
            .await
            .expect("create destination");
    let _ = steward::import_graft(
        &mut destination,
        &remote,
        &graph,
        source_id,
        "upstream",
        "/imports/upstream",
    )
    .await
    .expect("import graft");
    let commit = steward::authenticated_publication_head_commit(&remote, &state)
        .await
        .expect("authenticate head");
    steward::authenticate_graft_publication_head(
        &mut destination,
        &state,
        &commit,
        "upstream",
        "/imports/upstream",
    )
    .await
    .expect("matching graft identity");

    destination
        .write_transaction(&meta("remove-pin"), async move |transaction| {
            let root = transaction.root().await?;
            let grafts = root.open_dir_path(steward::SYS_GRAFTS_DIR).await?;
            grafts.remove_entry("upstream").await?;
            Ok(())
        })
        .await
        .expect("remove pin");
    let error = steward::authenticate_graft_publication_head(
        &mut destination,
        &state,
        &commit,
        "upstream",
        "/imports/upstream",
    )
    .await
    .expect_err("missing pin must prevent the no-op");
    assert!(error.to_string().contains("graft pin"));
}

#[tokio::test]
async fn graft_noop_rejects_advanced_foreign_index_behind_stale_cursor() {
    let (_source_dir, mut source) = new_pond("graft-stale-cursor-source").await;
    write_file(&mut source, "/a.txt", b"alpha").await;
    let source_id = source
        .data_persistence()
        .pond_id()
        .parse::<uuid7::Uuid>()
        .expect("source id");
    let (_remote_dir, mut remote) = push(&source).await;
    let first_graph = fetch_object_graph(&remote, "main")
        .await
        .expect("first graph");
    let first_state = first_graph.publication_state.clone().expect("first state");
    let first_commit = steward::authenticated_publication_head_commit(&remote, &first_state)
        .await
        .expect("first commit");

    let destination_dir = tempdir().expect("destination");
    let mut destination = Ship::create_pond(
        destination_dir.path().join("pond"),
        "graft-stale-cursor-dst",
    )
    .await
    .expect("create destination");
    let _ = steward::import_graft(
        &mut destination,
        &remote,
        &first_graph,
        source_id,
        "upstream",
        "/imports/upstream",
    )
    .await
    .expect("import first graft");

    write_file(&mut source, "/b.txt", b"beta").await;
    repush(&source, &mut remote).await;
    let second_graph = fetch_object_graph_since(&remote, "main", Some(first_state.snapshot_tip))
        .await
        .expect("second graph");
    let _ = steward::import_pond(&mut destination, &remote, &second_graph, source_id)
        .await
        .expect("advance foreign partition without advancing graft metadata");

    let cursor = steward::get_data_path(destination.pond_path())
        .join("_content/v2/state/manifest-root")
        .join(format!("pond={}", first_state.pond_id));
    std::fs::write(
        cursor,
        sync_store::content::encode_manifest_root(first_state.manifest_root),
    )
    .expect("stage stale matching foreign cursor");
    let error = steward::authenticate_graft_publication_head(
        &mut destination,
        &first_state,
        &first_commit,
        "upstream",
        "/imports/upstream",
    )
    .await
    .expect_err("authoritative foreign index must expose the advanced content");
    assert!(error.to_string().contains("manifest root"), "{error}");
}

/// Scoped replacement rebuilds exactly one foreign partition and validates the
/// replacement before commit, leaving unrelated grafts intact.
#[tokio::test]
async fn replace_graft_is_scoped_and_atomic() {
    let (_t1, mut src1) = new_pond("replace-src-1").await;
    write_file(&mut src1, "/one.txt", b"one").await;
    let src1_id = src1
        .data_persistence()
        .pond_id()
        .parse::<uuid7::Uuid>()
        .expect("source 1 pond id");
    let (_r1, remote1) = push(&src1).await;
    let valid1 = fetch_object_graph(&remote1, "main").await.expect("fetch 1");

    let (_t2, mut src2) = new_pond("replace-src-2").await;
    write_file(&mut src2, "/two.txt", b"two").await;
    let src2_id = src2
        .data_persistence()
        .pond_id()
        .parse::<uuid7::Uuid>()
        .expect("source 2 pond id");
    let (_r2, remote2) = push(&src2).await;
    let valid2 = fetch_object_graph(&remote2, "main").await.expect("fetch 2");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "replace-dst")
        .await
        .expect("create dst");
    let _ = steward::import_graft(&mut dst, &remote1, &valid1, src1_id, "one", "/imports/one")
        .await
        .expect("import first graft");
    let _ = steward::import_graft(&mut dst, &remote2, &valid2, src2_id, "two", "/imports/two")
        .await
        .expect("import second graft");
    write_foreign_file(&mut dst, src1_id, "/poison.txt", b"poison").await;
    let poisoned_root = foreign_root_hash(&dst, src1_id).await;
    let unrelated_root = foreign_root_hash(&dst, src2_id).await;
    point_mount_at_foreign_child(&mut dst, src1_id, "/imports", "one", "one.txt").await;

    let mut invalid1 = valid1.clone();
    invalid1.commits[0].1.manifest_root = ObjectHash::of_bytes(b"wrong replacement root");
    let version_before = dst.data_persistence().table().version();
    let _ = steward::replace_graft(
        &mut dst,
        &remote1,
        &invalid1,
        src1_id,
        "one",
        "/imports/one",
    )
    .await
    .expect_err("invalid replacement must abort");
    assert_eq!(dst.data_persistence().table().version(), version_before);
    assert_eq!(foreign_root_hash(&dst, src1_id).await, poisoned_root);
    assert_eq!(foreign_root_hash(&dst, src2_id).await, unrelated_root);

    let _ = steward::replace_graft(&mut dst, &remote1, &valid1, src1_id, "one", "/imports/one")
        .await
        .expect("replace first graft");
    assert_eq!(
        foreign_root_hash(&dst, src1_id).await,
        root_hash(&src1).await
    );
    assert_eq!(foreign_root_hash(&dst, src2_id).await, unrelated_root);
    assert_eq!(
        read_to_string(&mut dst, "/imports/one/one.txt").await,
        "one"
    );
    assert_eq!(
        read_to_string(&mut dst, "/imports/two/two.txt").await,
        "two"
    );

    dst.write_transaction(&meta("local-mount-collision"), async |fs| {
        let root = fs.root().await?;
        root.open_dir_path("/imports")
            .await?
            .remove_entry("one")
            .await?;
        let _ = create_file_path(&root, "/imports/one", b"local").await?;
        Ok(())
    })
    .await
    .expect("create local collision");
    let collision_version = dst.data_persistence().table().version();
    let _ = steward::replace_graft(&mut dst, &remote1, &valid1, src1_id, "one", "/imports/one")
        .await
        .expect_err("local content must not be replaced");
    assert_eq!(dst.data_persistence().table().version(), collision_version);
    assert_eq!(read_to_string(&mut dst, "/imports/one").await, "local");

    dst.write_transaction(&meta("foreign-mount-collision"), async |fs| {
        let root = fs.root().await?;
        let imports = root.open_dir_path("/imports").await?;
        imports.remove_entry("one").await?;
        let other_root = fs.foreign_root_node(src2_id).await?;
        let _ = imports.insert_node("one", other_root).await?;
        Ok(())
    })
    .await
    .expect("create unrelated graft collision");
    let collision_version = dst.data_persistence().table().version();
    let _ = steward::replace_graft(&mut dst, &remote1, &valid1, src1_id, "one", "/imports/one")
        .await
        .expect_err("another graft must not be replaced");
    assert_eq!(dst.data_persistence().table().version(), collision_version);
    assert_eq!(
        read_to_string(&mut dst, "/imports/one/two.txt").await,
        "two"
    );
}

/// Local rebuilds use the same precommit gate as foreign imports: a bad
/// advertised root aborts without advancing Delta and does not poison retry.
#[tokio::test]
async fn rebuild_manifest_root_mismatch_leaves_target_unchanged() {
    let (_t, mut src) = new_pond("rebuild-precommit-src").await;
    write_file(&mut src, "/a.txt", b"alpha").await;

    let (_rt, remote) = push(&src).await;
    let valid = fetch_object_graph(&remote, "main").await.expect("fetch");
    let mut invalid = valid.clone();
    invalid.commits[0].1.manifest_root = ObjectHash::of_bytes(b"wrong manifest root");

    let dst_dir = tempdir().expect("dst dir");
    let mut dst = Ship::create_pond(dst_dir.path().join("pond"), "rebuild-precommit-dst")
        .await
        .expect("create dst");
    let version_before = dst.data_persistence().table().version();
    let root_before = root_hash(&dst).await;

    let err = steward::rebuild_pond(&mut dst, &remote, &invalid)
        .await
        .expect_err("advertised manifest-root mismatch must abort");
    assert!(
        err.to_string().contains("precommit manifest root"),
        "unexpected error: {err}"
    );
    assert_eq!(
        dst.data_persistence().table().version(),
        version_before,
        "failed validation must not advance the data Delta table"
    );
    assert_eq!(
        root_hash(&dst).await,
        root_before,
        "failed validation must leave local content unchanged"
    );

    let _ = steward::rebuild_pond(&mut dst, &remote, &valid)
        .await
        .expect("valid retry");
    assert_eq!(root_hash(&dst).await, root_hash(&src).await);
}

#[tokio::test]
async fn dropped_and_aborted_writes_reuse_their_sequence() {
    let (_tmp, mut ship) = new_pond("sequence-reuse").await;
    let before = ship.last_write_seq();

    let tx = ship
        .begin_write(&meta("drop"))
        .await
        .expect("begin dropped write");
    assert_eq!(tx.txn_meta().txn_seq, before + 1);
    drop(tx);
    assert_eq!(ship.last_write_seq(), before);

    let _ = ship
        .write_transaction(&meta("abort"), async |_fs| {
            Err(StewardError::Content("injected failure".to_string()))
        })
        .await
        .expect_err("callback failure must abort");
    assert_eq!(ship.last_write_seq(), before);

    write_file(&mut ship, "/after.txt", b"after").await;
    assert_eq!(ship.last_write_seq(), before + 1);
}

#[tokio::test]
async fn failed_replay_reuses_its_sequence() {
    let (_tmp, mut ship) = new_pond("replay-sequence-reuse").await;
    let before = ship.last_write_seq();
    let replay_meta = PondTxnMetadata::new(before + 1, meta("replay"));

    let _ = ship
        .replay_transaction(&replay_meta, |_guard, _fs| {
            Box::pin(async {
                Err::<(), _>(StewardError::Content("injected replay failure".to_string()))
            })
        })
        .await
        .expect_err("replay callback failure must abort");
    assert_eq!(ship.last_write_seq(), before);

    ship.replay_transaction(&replay_meta, |_guard, fs| {
        Box::pin(async move {
            let root = fs.root().await?;
            let _ = create_file_path(&root, "/replayed.txt", b"ok").await?;
            Ok(())
        })
    })
    .await
    .expect("retry replay at the same sequence");
    assert_eq!(ship.last_write_seq(), before + 1);
    assert_eq!(read_to_string(&mut ship, "/replayed.txt").await, "ok");
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
