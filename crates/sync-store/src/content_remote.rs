// SPDX-License-Identifier: Apache-2.0

//! [`ContentRemote`]: the delta-managed content-addressed remote.
//!
//! This is the single replication backend described in the design doc
//! Section 8 (Decision D6).  It replaces the bundle/frontier remote: there
//! is no `(pond_id, seq)` frontier, no per-bundle manifest, and no
//! per-partition checksum list.
//!
//! The remote is one Delta table (a [`Store`]) whose rows are content
//! objects keyed by their content hash, plus a distinguished ref row holding
//! the tip commit hash:
//!
//! ```text
//! partition = "objects", item_key = <hex object hash>, value = object bytes
//! partition = "refs",    item_key = <ref name>,        value = 32-byte tip hash
//! ```
//!
//! Because object `value` is the exact object bytes, the store's own
//! `value_blake3` column equals the object hash -- the storage key and the
//! integrity digest agree.
//!
//! **Atomicity comes from Delta, not from object ordering.**  A push is one
//! [`Store::apply_batch`] -- a single Delta commit -- that writes the new
//! object rows *and* advances the tip ref together.  The tip can therefore
//! never point at an incomplete object closure, with no "objects-before-ref"
//! two-phase write and no separate compare-and-swap: delta-rs over
//! `object_store` provides the commit atomicity the Delta protocol already
//! requires on S3.

use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use uuid::Uuid;

use crate::content::{
    CapsuleManifest, ObjectHash, capsule_manifest_bytes, capsule_root, decode_capsule_manifest,
    verify_capsule_payload_directory, verify_capsule_payloads,
    verify_incremental_capsule_payload_directory,
};
use crate::error::{Result, StoreError};
use crate::store::{Op, Store};

/// Partition holding content objects, keyed by hex object hash.
const OBJECTS_PARTITION: &str = "objects";
/// Partition holding refs, keyed by ref name; value is the 32-byte tip hash.
const REFS_PARTITION: &str = "refs";
/// Partition holding remote metadata; the source pond_id is stored here under
/// the nil pond partition so a consumer can discover it without knowing it.
const META_PARTITION: &str = "meta";
const POND_ID_KEY: &str = "pond_id";
const CAPSULE_PREFIX: &str = "recovery";
const CAPSULE_HISTORY_LIMIT: usize = 3;

/// Result of publishing one verified recovery-capsule generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapsulePublishOutcome {
    /// Capsule manifest root and generation identifier.
    pub root: ObjectHash,
    /// Number of previously absent payload objects uploaded.
    pub payloads_uploaded: usize,
    /// Total distinct payload objects referenced by the generation.
    pub payloads_total: usize,
}

/// The delta-managed content-addressed remote for one source pond.
///
/// All rows are written under the source pond's `pond_id`, matching the
/// store's per-`pond_id` physical partitioning.  Object hashes are
/// content-only and lineage-independent, so two ponds with identical content
/// produce identical object bytes under identical keys.  A node's content
/// includes the metadata its directory entry commits to, so a pond and its
/// replica share keys while two independently written ponds share only their
/// blobs.
pub struct ContentRemote {
    store: Store,
    pond_id: Uuid,
    /// Optional in-memory snapshot of the entire `objects` partition, keyed by
    /// hex object hash.  [`Self::preload_objects`] fills it with a single
    /// [`Store::list`] scan so a bulk read (e.g. cloning the object graph)
    /// resolves each object from memory instead of a full-table Delta scan per
    /// hash -- the difference between O(objects x table-size) and one scan.
    ///
    /// It is a *per-operation* snapshot, not a durable cache: a caller preloads
    /// at the start of a bulk read and [`Self::clear_object_cache`]s at the end,
    /// so a later read (or a re-pull after new commits) never sees stale bytes.
    /// `None` means "not preloaded"; [`Self::get_object`] then falls back to a
    /// per-hash store lookup, preserving prior behavior.
    object_cache: RwLock<Option<HashMap<String, Vec<u8>>>>,
}

impl ContentRemote {
    /// Multipart providers require non-final parts of at least 5 MiB.
    const MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;
    /// Maximum in-flight multipart part uploads allowed while streaming a large
    /// blob to the remote (see [`Self::put_blob`]).  Bounds staged upload memory
    /// to this many [`Self::MULTIPART_PART_SIZE`] parts when the reader outpaces
    /// the network.
    const MAX_INFLIGHT_UPLOAD_PARTS: usize = 16;

    /// Create a fresh remote at `path`.  Errors if a Delta table already
    /// exists there.
    pub async fn create_at(path: impl AsRef<Path>, pond_id: Uuid) -> Result<Self> {
        let store = Store::create(path).await?;
        let mut me = Self {
            store,
            pond_id,
            object_cache: RwLock::new(None),
        };
        me.write_pond_id().await?;
        Ok(me)
    }

    /// Open an existing remote at `path`.
    pub async fn open_at(path: impl AsRef<Path>, pond_id: Uuid) -> Result<Self> {
        let store = Store::open(path).await?;
        Ok(Self {
            store,
            pond_id,
            object_cache: RwLock::new(None),
        })
    }

    /// Create a fresh remote at `url` with `storage_options` (e.g. S3 creds),
    /// recording `pond_id`.  Errors if a table already exists.
    pub async fn create_at_url(
        url: &str,
        pond_id: Uuid,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let store = Store::create_at_url(url, storage_options).await?;
        let mut me = Self {
            store,
            pond_id,
            object_cache: RwLock::new(None),
        };
        me.write_pond_id().await?;
        Ok(me)
    }

    /// Open an existing remote at `url`, discovering its source pond_id from
    /// the recorded metadata.
    pub async fn open_at_url(
        url: &str,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let store = Store::open_at_url(url, storage_options).await?;
        let bytes = store
            .get(Uuid::nil(), META_PARTITION, POND_ID_KEY)
            .await?
            .ok_or_else(|| StoreError::Invariant("remote has no recorded pond_id".to_string()))?;
        let s = String::from_utf8(bytes)
            .map_err(|e| StoreError::Invariant(format!("pond_id not utf8: {e}")))?;
        let pond_id =
            Uuid::parse_str(&s).map_err(|e| StoreError::Invariant(format!("bad pond_id: {e}")))?;
        Ok(Self {
            store,
            pond_id,
            object_cache: RwLock::new(None),
        })
    }

    async fn write_pond_id(&mut self) -> Result<()> {
        let _ = self
            .store
            .put(
                Uuid::nil(),
                META_PARTITION,
                POND_ID_KEY,
                self.pond_id.to_string().into_bytes(),
            )
            .await?;
        Ok(())
    }

    /// The URL this remote lives at, which is the identity its storage budget
    /// is bound to.
    pub fn url(&self) -> String {
        self.store.url()
    }

    /// The pond whose objects this remote holds.
    pub fn pond_id(&self) -> Uuid {
        self.pond_id
    }

    /// Push a commit: write `objects` and advance `ref_name` to `tip` in a
    /// single atomic Delta commit.  `objects` should be the closure the
    /// remote lacks (typically the producer's `missing_from` set); already
    /// present objects may be included harmlessly, since a re-put of an
    /// identical hash is idempotent.
    ///
    /// Returns the `txn_seq` allocated for the commit.
    pub async fn push_commit(
        &mut self,
        objects: &[(ObjectHash, Vec<u8>)],
        ref_name: &str,
        tip: ObjectHash,
    ) -> Result<i64> {
        let mut ops: Vec<Op> = Vec::with_capacity(objects.len() + 1);
        for (hash, bytes) in objects {
            ops.push(Op::Put {
                partition: OBJECTS_PARTITION.to_string(),
                key: hash.to_hex(),
                value: bytes.clone(),
            });
        }
        ops.push(Op::Put {
            partition: REFS_PARTITION.to_string(),
            key: ref_name.to_string(),
            value: tip.as_bytes().to_vec(),
        });

        let txn_seq = self.store.last_txn_seq(self.pond_id).await? + 1;
        let ts = chrono::Utc::now().timestamp_micros();
        self.store
            .apply_batch(self.pond_id, txn_seq, ts, ops)
            .await?;
        Ok(txn_seq)
    }

    /// Read the tip commit hash for `ref_name`, or `None` if the ref does not
    /// exist.
    pub async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>> {
        let Some(bytes) = self
            .store
            .get(self.pond_id, REFS_PARTITION, ref_name)
            .await?
        else {
            return Ok(None);
        };
        let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            StoreError::Invariant(format!(
                "ref '{}' value is {} bytes, expected 32",
                ref_name,
                bytes.len()
            ))
        })?;
        Ok(Some(ObjectHash::from_bytes(arr)))
    }

    /// Read the bytes of the object with the given hash, or `None` if absent.
    pub async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>> {
        // When preloaded, the snapshot is authoritative for the whole `objects`
        // partition: a hit returns the bytes, and a miss definitively means the
        // hash is not an inline object (e.g. it is a large external blob).
        // Either way we skip the per-hash Delta scan entirely.
        {
            let guard = self
                .object_cache
                .read()
                .map_err(|_| StoreError::Invariant("object_cache lock poisoned".into()))?;
            if let Some(cache) = guard.as_ref() {
                return Ok(cache.get(&hash.to_hex()).cloned());
            }
        }
        self.store
            .get(self.pond_id, OBJECTS_PARTITION, &hash.to_hex())
            .await
    }

    /// Snapshot the entire `objects` partition into memory with one
    /// [`Store::list`] scan, so subsequent [`Self::get_object`] / [`Self::has_object`]
    /// calls resolve from memory instead of one full-table Delta scan per hash.
    ///
    /// This is the fast path for bulk reads such as cloning the object graph.
    /// The snapshot is *per-operation*: each call re-scans and replaces any
    /// prior snapshot, and callers must [`Self::clear_object_cache`] when the
    /// bulk read finishes so no later read sees stale bytes.  Only inline
    /// objects live in the `objects` partition (large blobs are external,
    /// Decision D7), so the snapshot is bounded and does not buffer bulk content.
    pub async fn preload_objects(&self) -> Result<()> {
        let rows = self.store.list(self.pond_id, OBJECTS_PARTITION).await?;
        let map: HashMap<String, Vec<u8>> = rows.into_iter().collect();
        let mut guard = self
            .object_cache
            .write()
            .map_err(|_| StoreError::Invariant("object_cache lock poisoned".into()))?;
        *guard = Some(map);
        Ok(())
    }

    /// Drop any snapshot taken by [`Self::preload_objects`], restoring per-hash
    /// store lookups.  Call this when a bulk read finishes.
    pub fn clear_object_cache(&self) {
        if let Ok(mut guard) = self.object_cache.write() {
            *guard = None;
        }
    }

    /// True if the object with the given hash is present on the remote.
    pub async fn has_object(&self, hash: ObjectHash) -> Result<bool> {
        Ok(self.get_object(hash).await?.is_some())
    }

    /// Object-store key for an external blob, sibling to the Delta log.  Large
    /// blobs (>64KB) live here rather than as inline `objects` rows so a
    /// multi-gigabyte value never lands in the Delta table (Decision D7).
    fn blob_path(hash: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!("_blobs/blob={}", hash.to_hex()))
    }

    /// Every external blob the remote holds, as one listing of the `_blobs/`
    /// prefix.
    ///
    /// This exists because presence is asked about in bulk.  A push must know,
    /// for each blob its content closure references, whether the remote already
    /// holds it -- and [`Self::has_blob`] answers that with one `HEAD` per
    /// blob.  That cost is proportional to the pond's accumulated history
    /// rather than to the work being done: a pond holding 180 blobs pays 180
    /// billed requests on every push, including a push that transfers nothing.
    /// Measured on a staging pond, hourly pushes came to ~4300 requests a day
    /// purely to re-confirm blobs that had not changed.
    ///
    /// One listing answers the same question. `object_store` paginates
    /// internally at 1000 keys per request, so this is a single request for any
    /// realistic blob count, and it reads live remote state exactly as the
    /// per-blob `HEAD`s did -- it is not a cache.
    pub async fn list_blobs(&self) -> Result<std::collections::HashSet<ObjectHash>> {
        use futures::StreamExt;

        let prefix = object_store::path::Path::from("_blobs");
        let mut stream = self.store.object_store().list(Some(&prefix));
        let mut out = std::collections::HashSet::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| StoreError::Invariant(format!("list blobs: {e}")))?;
            // Keys are `_blobs/blob=<hex>`; anything else under the prefix is
            // not ours and is skipped rather than guessed at.
            let Some(name) = meta.location.filename() else {
                continue;
            };
            let Some(hex) = name.strip_prefix("blob=") else {
                continue;
            };
            if let Ok(hash) = ObjectHash::from_hex(hex) {
                let _ = out.insert(hash);
            }
        }
        Ok(out)
    }

    /// True if the external blob `hash` is already present in the remote blob
    /// store, so a producer can skip re-uploading it.
    ///
    /// Prefer [`Self::list_blobs`] when asking about more than a couple of
    /// blobs: this is one billed request per call.
    pub async fn has_blob(&self, hash: ObjectHash) -> Result<bool> {
        match self.store.object_store().head(&Self::blob_path(hash)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(StoreError::Invariant(format!("blob head: {e}"))),
        }
    }

    /// Stream a large blob's raw bytes from `reader` into the remote blob store,
    /// keyed by `hash`.  Chunks flow through a bounded buffer to a multipart
    /// upload; the bytes are hashed as they pass so a value can never be stored
    /// under a key it does not equal.  Never collects the whole blob in memory.
    ///
    /// Backpressure is applied so a fast local reader cannot outrun a slow
    /// upload.  Keeping the multipart handle here, rather than delegating the
    /// trailing-part flush to `WriteMultipart::finish`, also guarantees every
    /// failed or refused part can be followed by an explicit abort.
    pub async fn put_blob<R>(&self, hash: ObjectHash, mut reader: R) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        self.put_hashed_object(Self::blob_path(hash), hash, None, &mut reader)
            .await
    }

    async fn put_hashed_object<R>(
        &self,
        path: object_store::path::Path,
        hash: ObjectHash,
        expected_size: Option<u64>,
        mut reader: R,
    ) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        use tokio::io::AsyncReadExt;
        if expected_size == Some(0) {
            let mut probe = [0u8; 1];
            let count = reader.read(&mut probe).await.map_err(|error| {
                StoreError::Invariant(format!("read empty streamed object: {error}"))
            })?;
            let empty_hash = ObjectHash::of_bytes(&[]);
            if count != 0 || hash != empty_hash {
                return Err(StoreError::Invariant(format!(
                    "streamed object is not the declared empty payload {hash}"
                )));
            }
            self.store
                .object_store()
                .put(&path, Vec::new().into())
                .await
                .map_err(|error| {
                    StoreError::Invariant(format!("publish empty streamed object: {error}"))
                })?;
            return Ok(());
        }
        let upload = self
            .store
            .object_store()
            .put_multipart(&path)
            .await
            .map_err(|e| StoreError::Invariant(format!("blob put_multipart: {e}")))?;
        let mut upload = upload;
        let mut parts = FuturesUnordered::new();
        let mut hasher = blake3::Hasher::new();
        let mut size = 0u64;

        loop {
            if parts.len() >= Self::MAX_INFLIGHT_UPLOAD_PARTS {
                match parts.next().await {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        drop(parts);
                        let abort = upload.abort().await;
                        return Err(StoreError::Invariant(match abort {
                            Ok(()) => format!("blob upload part: {error}"),
                            Err(abort_error) => {
                                format!("blob upload part: {error}; abort failed: {abort_error}")
                            }
                        }));
                    }
                    None => {}
                }
            }

            let mut part = vec![0u8; Self::MULTIPART_PART_SIZE];
            let mut filled = 0;
            while filled < part.len() {
                let n = match reader.read(&mut part[filled..]).await {
                    Ok(n) => n,
                    Err(error) => {
                        drop(parts);
                        let abort = upload.abort().await;
                        return Err(StoreError::Invariant(match abort {
                            Ok(()) => format!("blob read: {error}"),
                            Err(abort_error) => {
                                format!("blob read: {error}; abort failed: {abort_error}")
                            }
                        }));
                    }
                };
                if n == 0 {
                    break;
                }
                hasher.update(&part[filled..filled + n]);
                size = match size.checked_add(n as u64) {
                    Some(size) => size,
                    None => {
                        drop(parts);
                        let abort = upload.abort().await;
                        return Err(StoreError::Invariant(match abort {
                            Ok(()) => "streamed object exceeds u64::MAX".to_string(),
                            Err(abort_error) => format!(
                                "streamed object exceeds u64::MAX; abort failed: {abort_error}"
                            ),
                        }));
                    }
                };
                filled += n;
            }
            if filled == 0 {
                break;
            }
            part.truncate(filled);
            parts.push(upload.put_part(part.into()));
        }

        // Verify the streamed content matches its claimed key BEFORE completing
        // the multipart upload.  A multipart object only becomes visible on
        // `finish()`, so aborting here discards the staged parts and a value is
        // never stored under a key it does not equal -- no temporary key needed.
        let computed = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
        if computed != hash || expected_size.is_some_and(|expected| expected != size) {
            drop(parts);
            upload
                .abort()
                .await
                .map_err(|e| StoreError::Invariant(format!("blob abort: {e}")))?;
            return Err(StoreError::Invariant(format!(
                "streamed object has hash {} and size {size}, expected hash {} and size {:?}",
                computed.to_hex(),
                hash.to_hex(),
                expected_size
            )));
        }

        while let Some(result) = parts.next().await {
            if let Err(error) = result {
                drop(parts);
                let abort = upload.abort().await;
                return Err(StoreError::Invariant(match abort {
                    Ok(()) => format!("blob upload part: {error}"),
                    Err(abort_error) => {
                        format!("blob upload part: {error}; abort failed: {abort_error}")
                    }
                }));
            }
        }
        if let Err(error) = upload.complete().await {
            let abort = upload.abort().await;
            return Err(StoreError::Invariant(match abort {
                Ok(()) => format!("blob complete: {error}"),
                Err(abort_error) => {
                    format!("blob complete: {error}; abort failed: {abort_error}")
                }
            }));
        }
        Ok(())
    }

    /// Open a streaming reader over a large blob's raw bytes by hash, or `None`
    /// if absent.  The body streams from object storage chunk by chunk; the
    /// caller re-hashes as it consumes so a multi-gigabyte blob never lands in a
    /// single buffer.  Unlike an in-memory fetch, integrity is the consumer's
    /// responsibility -- it must verify the streamed bytes hash to `hash`.
    pub async fn get_blob_reader(
        &self,
        hash: ObjectHash,
    ) -> Result<Option<Box<dyn tokio::io::AsyncRead + Unpin + Send>>> {
        let path = Self::blob_path(hash);
        let res = match self.store.object_store().get(&path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(StoreError::Invariant(format!("blob get: {e}"))),
        };
        let stream = futures::TryStreamExt::map_err(res.into_stream(), std::io::Error::other);
        let reader = tokio_util::io::StreamReader::new(stream);
        Ok(Some(Box::new(reader)))
    }

    /// Publish a complete portable recovery-capsule generation.
    ///
    /// Plain payload objects are written first, followed by the manifest and
    /// generated recovery artifacts. `recovery/refs/latest` is updated last,
    /// so interruption leaves the previous verified generation current.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest is invalid, the supplied payload
    /// closure differs from the manifest, any payload bytes disagree with
    /// their key/declared size, or object-store publication fails.
    pub async fn publish_capsule(
        &self,
        manifest: &CapsuleManifest,
        payloads: &std::collections::BTreeMap<ObjectHash, Vec<u8>>,
    ) -> Result<CapsulePublishOutcome> {
        let root = capsule_root(manifest).map_err(StoreError::Invariant)?;
        let manifest_bytes = capsule_manifest_bytes(manifest).map_err(StoreError::Invariant)?;
        let declared = manifest.payload_objects().map_err(StoreError::Invariant)?;
        verify_capsule_payloads(manifest, payloads).map_err(StoreError::Invariant)?;
        if declared.len() != payloads.len() {
            return Err(StoreError::Invariant(format!(
                "capsule declares {} payloads but publisher supplied {}",
                declared.len(),
                payloads.len()
            )));
        }

        let present = self.list_capsule_payloads().await?;
        let mut uploaded = 0usize;
        for object in &declared {
            let bytes = payloads.get(&object.hash).ok_or_else(|| {
                StoreError::Invariant(format!("capsule payload {} was not supplied", object.hash))
            })?;
            if ObjectHash::of_bytes(bytes) != object.hash {
                return Err(StoreError::Invariant(format!(
                    "capsule payload bytes do not hash to {}",
                    object.hash
                )));
            }
            if u64::try_from(bytes.len()).ok() != Some(object.size) {
                return Err(StoreError::Invariant(format!(
                    "capsule payload {} has size {}, expected {}",
                    object.hash,
                    bytes.len(),
                    object.size
                )));
            }
            let path = Self::capsule_payload_path(object.hash);
            if present.contains(&object.hash) {
                continue;
            }
            self.store
                .object_store()
                .put(&path, bytes.clone().into())
                .await
                .map_err(|error| {
                    StoreError::Invariant(format!(
                        "publish capsule payload {}: {error}",
                        object.hash
                    ))
                })?;
            uploaded += 1;
        }

        self.finish_capsule_publication(root, manifest_bytes, &declared, uploaded)
            .await
    }

    /// Publish a capsule whose payload closure is staged as
    /// `blake3=<hash>` files in `objects_dir`.
    ///
    /// Payload files are verified before publication and loaded one at a time,
    /// avoiding retention of the complete capsule closure in memory.
    pub async fn publish_capsule_directory(
        &self,
        manifest: &CapsuleManifest,
        objects_dir: &Path,
    ) -> Result<CapsulePublishOutcome> {
        let root = capsule_root(manifest).map_err(StoreError::Invariant)?;
        let manifest_bytes = capsule_manifest_bytes(manifest).map_err(StoreError::Invariant)?;
        let declared = manifest.payload_objects().map_err(StoreError::Invariant)?;
        verify_capsule_payload_directory(manifest, objects_dir).map_err(StoreError::Invariant)?;

        let present = self.list_capsule_payloads().await?;
        let mut uploaded = 0usize;
        for object in &declared {
            if present.contains(&object.hash) {
                continue;
            }
            let source = objects_dir.join(format!("blake3={}", object.hash.to_hex()));
            let file = tokio::fs::File::open(&source).await.map_err(|error| {
                StoreError::Invariant(format!(
                    "open staged capsule payload {}: {error}",
                    object.hash
                ))
            })?;
            let path = Self::capsule_payload_path(object.hash);
            self.put_hashed_object(path, object.hash, Some(object.size), file)
                .await
                .map_err(|error| {
                    StoreError::Invariant(format!(
                        "publish capsule payload {}: {error}",
                        object.hash
                    ))
                })?;
            uploaded += 1;
        }

        self.finish_capsule_publication(root, manifest_bytes, &declared, uploaded)
            .await
    }

    /// Publish a capsule that inherits unstaged payloads from `prior`.
    ///
    /// Every unstaged descriptor must occur identically in the current remote
    /// generation and its object key must still be present. Newly staged
    /// payloads are streamed and verified as usual. Publication is refused if
    /// `prior` is no longer the current generation.
    pub async fn publish_capsule_incremental(
        &self,
        manifest: &CapsuleManifest,
        objects_dir: &Path,
        prior: &CapsuleManifest,
    ) -> Result<CapsulePublishOutcome> {
        let root = capsule_root(manifest).map_err(StoreError::Invariant)?;
        let manifest_bytes = capsule_manifest_bytes(manifest).map_err(StoreError::Invariant)?;
        let declared = manifest.payload_objects().map_err(StoreError::Invariant)?;
        let prior_root = capsule_root(prior).map_err(StoreError::Invariant)?;
        verify_incremental_capsule_payload_directory(manifest, prior, objects_dir)
            .map_err(StoreError::Invariant)?;
        let prior_objects = prior
            .payload_objects()
            .map_err(StoreError::Invariant)?
            .into_iter()
            .map(|object| (object.hash, object.size))
            .collect::<std::collections::HashMap<_, _>>();
        self.require_current_capsule(prior_root).await?;

        let present = self.list_capsule_payloads().await?;
        let mut uploaded = 0usize;
        for object in &declared {
            let source = objects_dir.join(format!("blake3={}", object.hash.to_hex()));
            let staged = tokio::fs::try_exists(&source).await.map_err(|error| {
                StoreError::Invariant(format!(
                    "inspect staged capsule payload {}: {error}",
                    object.hash
                ))
            })?;
            if !staged {
                if prior_objects.get(&object.hash) != Some(&object.size) {
                    return Err(StoreError::Invariant(format!(
                        "unstaged capsule payload {} is not inherited from the prior generation",
                        object.hash
                    )));
                }
                if !present.contains(&object.hash) {
                    return Err(StoreError::Invariant(format!(
                        "inherited capsule payload {} is missing remotely",
                        object.hash
                    )));
                }
                continue;
            }
            if present.contains(&object.hash) {
                continue;
            }
            let file = tokio::fs::File::open(&source).await.map_err(|error| {
                StoreError::Invariant(format!(
                    "open staged capsule payload {}: {error}",
                    object.hash
                ))
            })?;
            self.put_hashed_object(
                Self::capsule_payload_path(object.hash),
                object.hash,
                Some(object.size),
                file,
            )
            .await
            .map_err(|error| {
                StoreError::Invariant(format!("publish capsule payload {}: {error}", object.hash))
            })?;
            uploaded += 1;
        }

        self.require_current_capsule(prior_root).await?;
        self.finish_capsule_publication(root, manifest_bytes, &declared, uploaded)
            .await
    }

    async fn require_current_capsule(&self, expected: ObjectHash) -> Result<()> {
        let current = self.latest_capsule().await?.ok_or_else(|| {
            StoreError::Invariant(
                "incremental capsule publication requires a current generation".to_string(),
            )
        })?;
        if current.0 != expected {
            return Err(StoreError::Invariant(format!(
                "capsule generation changed during incremental publication: expected {expected}, current {}",
                current.0
            )));
        }
        Ok(())
    }

    async fn finish_capsule_publication(
        &self,
        root: ObjectHash,
        manifest_bytes: Vec<u8>,
        declared: &[crate::content::CapsuleObject],
        uploaded: usize,
    ) -> Result<CapsulePublishOutcome> {
        let manifest_path = Self::capsule_manifest_path(root);
        self.store
            .object_store()
            .put(&manifest_path, manifest_bytes.into())
            .await
            .map_err(|error| StoreError::Invariant(format!("publish capsule manifest: {error}")))?;

        let object_list = capsule_object_list(root, declared);
        let checksums = capsule_checksums(declared);
        let artifacts = [
            ("objects.list", object_list.into_bytes()),
            ("checksums", checksums.into_bytes()),
            ("RUNBOOK.txt", capsule_runbook(root).into_bytes()),
            ("download-az.sh", capsule_az_script(root).into_bytes()),
            ("download-mc.sh", capsule_mc_script(root).into_bytes()),
        ];
        for (name, bytes) in artifacts {
            let path = Self::capsule_generation_path(root, name);
            self.store
                .object_store()
                .put(&path, bytes.into())
                .await
                .map_err(|error| {
                    StoreError::Invariant(format!("publish capsule artifact {name}: {error}"))
                })?;
        }

        let mut history = self.capsule_roots().await?;
        history.retain(|prior| *prior != root);
        history.insert(0, root);
        history.truncate(CAPSULE_HISTORY_LIMIT);
        let history_bytes = history
            .iter()
            .map(ObjectHash::to_hex)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        self.store
            .object_store()
            .put(
                &Self::capsule_history_path(),
                history_bytes.into_bytes().into(),
            )
            .await
            .map_err(|error| {
                StoreError::Invariant(format!("publish capsule history ref: {error}"))
            })?;

        // Ref last: readers cannot discover this root before every dependency
        // and recovery artifact above is durable.
        self.store
            .object_store()
            .put(
                &Self::capsule_latest_path(),
                format!("{}\n", root.to_hex()).into_bytes().into(),
            )
            .await
            .map_err(|error| {
                StoreError::Invariant(format!("publish capsule latest ref: {error}"))
            })?;

        Ok(CapsulePublishOutcome {
            root,
            payloads_uploaded: uploaded,
            payloads_total: declared.len(),
        })
    }

    /// Read and validate the latest capsule manifest, if one is published.
    pub async fn latest_capsule(&self) -> Result<Option<(ObjectHash, CapsuleManifest)>> {
        let reference = match self
            .store
            .object_store()
            .get(&Self::capsule_latest_path())
            .await
        {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|error| StoreError::Invariant(format!("read capsule ref: {error}")))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => {
                return Err(StoreError::Invariant(format!(
                    "get capsule latest ref: {error}"
                )));
            }
        };
        let root_text = std::str::from_utf8(&reference)
            .map_err(|error| StoreError::Invariant(format!("capsule ref is not UTF-8: {error}")))?
            .trim_end();
        let root = ObjectHash::from_hex(root_text).map_err(StoreError::Invariant)?;
        let bytes = self
            .store
            .object_store()
            .get(&Self::capsule_manifest_path(root))
            .await
            .map_err(|error| StoreError::Invariant(format!("get capsule manifest: {error}")))?
            .bytes()
            .await
            .map_err(|error| StoreError::Invariant(format!("read capsule manifest: {error}")))?;
        let manifest = decode_capsule_manifest(&bytes).map_err(StoreError::Invariant)?;
        let computed = capsule_root(&manifest).map_err(StoreError::Invariant)?;
        if computed != root {
            return Err(StoreError::Invariant(format!(
                "capsule manifest hashes to {computed}, latest ref names {root}"
            )));
        }
        Ok(Some((root, manifest)))
    }

    /// Retained verified capsule roots, newest first.
    pub async fn capsule_roots(&self) -> Result<Vec<ObjectHash>> {
        let bytes = match self
            .store
            .object_store()
            .get(&Self::capsule_history_path())
            .await
        {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|error| StoreError::Invariant(format!("read capsule history: {error}")))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(Vec::new()),
            Err(error) => {
                return Err(StoreError::Invariant(format!(
                    "get capsule history: {error}"
                )));
            }
        };
        let text = std::str::from_utf8(&bytes).map_err(|error| {
            StoreError::Invariant(format!("capsule history is not UTF-8: {error}"))
        })?;
        let mut roots = Vec::new();
        for line in text.lines() {
            if line.is_empty() {
                return Err(StoreError::Invariant(
                    "capsule history contains an empty root".to_string(),
                ));
            }
            if line.bytes().any(|byte| byte.is_ascii_uppercase()) {
                return Err(StoreError::Invariant(
                    "capsule history root is not lowercase hexadecimal".to_string(),
                ));
            }
            let root = ObjectHash::from_hex(line).map_err(StoreError::Invariant)?;
            if roots.contains(&root) {
                return Err(StoreError::Invariant(format!(
                    "capsule history repeats root {root}"
                )));
            }
            roots.push(root);
        }
        if roots.len() > CAPSULE_HISTORY_LIMIT {
            return Err(StoreError::Invariant(format!(
                "capsule history contains {} roots, limit is {CAPSULE_HISTORY_LIMIT}",
                roots.len()
            )));
        }
        Ok(roots)
    }

    fn capsule_payload_path(hash: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!("{CAPSULE_PREFIX}/objects/blake3={}", hash.to_hex()))
    }

    async fn list_capsule_payloads(&self) -> Result<std::collections::HashSet<ObjectHash>> {
        let prefix = object_store::path::Path::from(format!("{CAPSULE_PREFIX}/objects"));
        let mut stream = self.store.object_store().list(Some(&prefix));
        let mut hashes = std::collections::HashSet::new();
        while let Some(result) = stream.next().await {
            let metadata = result.map_err(|error| {
                StoreError::Invariant(format!("list capsule payloads: {error}"))
            })?;
            let Some(name) = metadata.location.filename() else {
                continue;
            };
            let Some(hex) = name.strip_prefix("blake3=") else {
                continue;
            };
            if let Ok(hash) = ObjectHash::from_hex(hex) {
                let _ = hashes.insert(hash);
            }
        }
        Ok(hashes)
    }

    fn capsule_manifest_path(root: ObjectHash) -> object_store::path::Path {
        object_store::path::Path::from(format!("{CAPSULE_PREFIX}/manifests/{}.json", root.to_hex()))
    }

    fn capsule_generation_path(root: ObjectHash, name: &str) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{CAPSULE_PREFIX}/generations/{}/{name}",
            root.to_hex()
        ))
    }

    fn capsule_latest_path() -> object_store::path::Path {
        object_store::path::Path::from(format!("{CAPSULE_PREFIX}/refs/latest"))
    }

    fn capsule_history_path() -> object_store::path::Path {
        object_store::path::Path::from(format!("{CAPSULE_PREFIX}/refs/history"))
    }
}

fn capsule_object_list(root: ObjectHash, objects: &[crate::content::CapsuleObject]) -> String {
    let mut output = format!("{CAPSULE_PREFIX}/manifests/{}.json\n", root.to_hex());
    for object in objects {
        output.push_str(&format!(
            "{CAPSULE_PREFIX}/objects/blake3={}\n",
            object.hash.to_hex()
        ));
    }
    output
}

fn capsule_checksums(objects: &[crate::content::CapsuleObject]) -> String {
    let mut output = String::new();
    for object in objects {
        output.push_str(&format!(
            "{}  {CAPSULE_PREFIX}/objects/blake3={}\n",
            object.hash.to_hex(),
            object.hash.to_hex()
        ));
    }
    output
}

fn capsule_runbook(root: ObjectHash) -> String {
    format!(
        "Watertown recovery capsule {}\n\n\
         1. Download and review download-az.sh or download-mc.sh.\n\
         2. Authenticate with managed identity or your normal client environment.\n\
         3. Run the reviewed script; it embeds no credentials.\n\
         4. Run: pond capsule verify <download-directory>\n\
         5. Import only into an empty target with pond capsule import.\n\
         6. Never delete the source namespace as part of import.\n",
        root.to_hex()
    )
}

fn capsule_az_script(root: ObjectHash) -> String {
    format!(
        "#!/bin/sh\nset -eu\n\
         : \"${{AZURE_CONTAINER:?set AZURE_CONTAINER}}\"\n\
         DEST=${{DEST:-capsule-{root}}}\n\
         mkdir -p \"$DEST/recovery/generations/{root}\" \"$DEST/recovery/manifests\" \"$DEST/recovery/objects\" \"$DEST/recovery/refs\"\n\
         printf '%s\\n' '{root}' > \"$DEST/recovery/refs/latest\"\n\
         az storage blob download --auth-mode login --container-name \"$AZURE_CONTAINER\" --name \"recovery/generations/{root}/objects.list\" --file \"$DEST/recovery/generations/{root}/objects.list\" --overwrite\n\
         while IFS= read -r key; do\n\
           case \"$key\" in\n\
             \"recovery/manifests/{root}.json\") ;;\n\
             recovery/objects/blake3=*) digest=${{key#recovery/objects/blake3=}}; case \"$digest\" in *[!0-9a-f]*|'') exit 1;; esac; [ \"${{#digest}}\" -eq 64 ] || exit 1 ;;\n\
             *) exit 1 ;;\n\
           esac\n\
           mkdir -p \"$DEST/$(dirname \"$key\")\"\n\
           az storage blob download --auth-mode login --container-name \"$AZURE_CONTAINER\" --name \"$key\" --file \"$DEST/$key\" --overwrite\n\
         done < \"$DEST/recovery/generations/{root}/objects.list\"\n",
        root = root.to_hex()
    )
}

fn capsule_mc_script(root: ObjectHash) -> String {
    format!(
        "#!/bin/sh\nset -eu\n\
         : \"${{MC_SOURCE:?set MC_SOURCE to alias/bucket-or-prefix}}\"\n\
         DEST=${{DEST:-capsule-{root}}}\n\
         mkdir -p \"$DEST/recovery/generations/{root}\" \"$DEST/recovery/manifests\" \"$DEST/recovery/objects\" \"$DEST/recovery/refs\"\n\
         printf '%s\\n' '{root}' > \"$DEST/recovery/refs/latest\"\n\
         mc cp \"$MC_SOURCE/recovery/generations/{root}/objects.list\" \"$DEST/recovery/generations/{root}/objects.list\"\n\
         while IFS= read -r key; do\n\
           case \"$key\" in\n\
             \"recovery/manifests/{root}.json\") ;;\n\
             recovery/objects/blake3=*) digest=${{key#recovery/objects/blake3=}}; case \"$digest\" in *[!0-9a-f]*|'') exit 1;; esac; [ \"${{#digest}}\" -eq 64 ] || exit 1 ;;\n\
             *) exit 1 ;;\n\
           esac\n\
           mkdir -p \"$DEST/$(dirname \"$key\")\"\n\
           mc cp \"$MC_SOURCE/$key\" \"$DEST/$key\"\n\
         done < \"$DEST/recovery/generations/{root}/objects.list\"\n",
        root = root.to_hex()
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::content::{
        CapsuleEntry, CapsuleLeaf, CapsuleNode, CapsuleObject, CapsulePayloadKind, CapsuleSource,
        capsule_leaf_hash, capsule_series_root, verify_capsule_directory,
    };
    use tempfile::tempdir;
    use tinyfs::EntryType;

    #[tokio::test]
    async fn url_remote_persists_and_discovers_pond_id() {
        let dir = tempdir().unwrap();
        let url = format!("file://{}/remote", dir.path().display());
        let pond = Uuid::new_v4();
        let _ = ContentRemote::create_at_url(&url, pond, Default::default())
            .await
            .unwrap();
        let opened = ContentRemote::open_at_url(&url, Default::default())
            .await
            .unwrap();
        assert_eq!(opened.pond_id(), pond);
    }

    fn test_capsule(payload: &[u8]) -> (CapsuleManifest, BTreeMap<ObjectHash, Vec<u8>>) {
        let payload_hash = ObjectHash::of_bytes(payload);
        let leaf = CapsuleLeaf {
            logical_hash: capsule_leaf_hash(
                CapsulePayloadKind::File,
                None,
                payload.len() as u64,
                payload,
                None,
                None,
                None,
            )
            .unwrap(),
            logical_count: payload.len() as u64,
            source_timestamp: 1_700_000_000_000_000,
            min_event_time: None,
            max_event_time: None,
            logical_attributes: None,
        };
        let manifest = CapsuleManifest::new(
            CapsuleSource {
                pond_id: Uuid::nil().to_string(),
                birthplace: "test".to_string(),
                source_tip: ObjectHash::of_bytes(b"tip"),
                exported_at_micros: 1_700_000_000_000_000,
                tool_version: "test".to_string(),
            },
            vec![
                CapsuleEntry {
                    path: "/".to_string(),
                    entry_type: EntryType::DirectoryPhysical,
                    source_node_id: "root".to_string(),
                    node: CapsuleNode::Directory,
                },
                CapsuleEntry {
                    path: "/data".to_string(),
                    entry_type: EntryType::FilePhysicalVersion,
                    source_node_id: "data".to_string(),
                    node: CapsuleNode::Physical {
                        payload_kind: CapsulePayloadKind::File,
                        schema_fingerprint: None,
                        logical_root: capsule_series_root(
                            CapsulePayloadKind::File,
                            None,
                            std::slice::from_ref(&leaf),
                        ),
                        objects: vec![CapsuleObject {
                            hash: payload_hash,
                            size: payload.len() as u64,
                        }],
                        leaves: vec![leaf],
                    },
                },
            ],
        )
        .unwrap();
        (manifest, BTreeMap::from([(payload_hash, payload.to_vec())]))
    }

    fn append_capsule_file(
        prior: &CapsuleManifest,
        path: &str,
        payload: &[u8],
    ) -> (CapsuleManifest, CapsuleObject) {
        let object = CapsuleObject {
            hash: ObjectHash::of_bytes(payload),
            size: payload.len() as u64,
        };
        let leaf = CapsuleLeaf {
            logical_hash: capsule_leaf_hash(
                CapsulePayloadKind::File,
                None,
                object.size,
                payload,
                None,
                None,
                None,
            )
            .unwrap(),
            logical_count: object.size,
            source_timestamp: 1_700_000_000_000_001,
            min_event_time: None,
            max_event_time: None,
            logical_attributes: None,
        };
        let mut entries = prior.entries.clone();
        entries.push(CapsuleEntry {
            path: path.to_string(),
            entry_type: EntryType::FilePhysicalVersion,
            source_node_id: path.to_string(),
            node: CapsuleNode::Physical {
                payload_kind: CapsulePayloadKind::File,
                schema_fingerprint: None,
                logical_root: capsule_series_root(
                    CapsulePayloadKind::File,
                    None,
                    std::slice::from_ref(&leaf),
                ),
                objects: vec![object.clone()],
                leaves: vec![leaf],
            },
        });
        let mut source = prior.source.clone();
        source.source_tip = ObjectHash::of_bytes(path.as_bytes());
        (
            CapsuleManifest::new(source, entries).expect("extended capsule"),
            object,
        )
    }

    #[tokio::test]
    async fn capsule_publication_is_verified_reference_last_and_idempotent() {
        let dir = tempdir().unwrap();
        let remote_path = dir.path().join("remote");
        let remote = ContentRemote::create_at(&remote_path, Uuid::new_v4())
            .await
            .unwrap();
        let (manifest, payloads) = test_capsule(b"portable");

        let first = remote.publish_capsule(&manifest, &payloads).await.unwrap();
        assert_eq!(first.payloads_uploaded, 1);
        assert_eq!(first.payloads_total, 1);
        let (latest_root, latest) = remote.latest_capsule().await.unwrap().unwrap();
        assert_eq!(latest_root, first.root);
        assert_eq!(latest, manifest);
        assert_eq!(remote.capsule_roots().await.unwrap(), vec![first.root]);
        let report = verify_capsule_directory(&remote_path).unwrap();
        assert_eq!(report.root, first.root);
        assert_eq!(report.entries, 2);
        assert_eq!(report.payload_objects, 1);
        assert_eq!(report.logical_count, 8);

        let second = remote.publish_capsule(&manifest, &payloads).await.unwrap();
        assert_eq!(second.root, first.root);
        assert_eq!(second.payloads_uploaded, 0);
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            first.root
        );
    }

    #[tokio::test]
    async fn refused_capsule_does_not_replace_latest() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let (manifest, payloads) = test_capsule(b"good");
        let published = remote.publish_capsule(&manifest, &payloads).await.unwrap();

        let mut corrupt = payloads;
        *corrupt.values_mut().next().unwrap() = b"bad".to_vec();
        assert!(remote.publish_capsule(&manifest, &corrupt).await.is_err());
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            published.root
        );

        let (mut invalid_manifest, valid_payloads) = test_capsule(b"logically-invalid");
        let CapsuleNode::Physical {
            payload_kind,
            schema_fingerprint,
            logical_root,
            leaves,
            ..
        } = &mut invalid_manifest.entries[1].node
        else {
            panic!("physical test entry");
        };
        leaves[0].logical_hash = ObjectHash::of_bytes(b"wrong logical leaf");
        *logical_root = capsule_series_root(*payload_kind, *schema_fingerprint, leaves);
        assert!(
            remote
                .publish_capsule(&invalid_manifest, &valid_payloads)
                .await
                .is_err()
        );
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            published.root
        );
    }

    #[test]
    fn capsule_download_scripts_reject_unexpected_object_list_paths() {
        let root = ObjectHash::of_bytes(b"capsule");
        for script in [capsule_az_script(root), capsule_mc_script(root)] {
            assert!(script.contains("\"recovery/manifests/"));
            assert!(script.contains("recovery/objects/blake3=*"));
            assert!(script.contains("*[!0-9a-f]*|'') exit 1"));
            assert!(script.contains("\"${#digest}\" -eq 64"));
            assert!(script.contains("*) exit 1"));
        }
    }

    #[tokio::test]
    async fn capsule_history_retains_three_newest_roots() {
        let dir = tempdir().unwrap();
        let remote = ContentRemote::create_at(dir.path().join("remote"), Uuid::new_v4())
            .await
            .unwrap();
        let mut published = Vec::new();
        for payload in [b"one".as_slice(), b"two", b"three", b"four"] {
            let (manifest, payloads) = test_capsule(payload);
            published.push(
                remote
                    .publish_capsule(&manifest, &payloads)
                    .await
                    .unwrap()
                    .root,
            );
        }
        assert_eq!(
            remote.capsule_roots().await.unwrap(),
            vec![published[3], published[2], published[1]]
        );
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            published[3]
        );
    }

    #[tokio::test]
    async fn incremental_capsule_requires_every_inherited_remote_payload() {
        let dir = tempdir().unwrap();
        let remote_path = dir.path().join("remote");
        let remote = ContentRemote::create_at(&remote_path, Uuid::new_v4())
            .await
            .unwrap();
        let (first, first_payloads) = test_capsule(b"first");
        let first_root = remote
            .publish_capsule(&first, &first_payloads)
            .await
            .unwrap()
            .root;

        let (second, second_object) = append_capsule_file(&first, "/second", b"second");
        let staging = tempdir().unwrap();
        std::fs::write(
            staging
                .path()
                .join(format!("blake3={}", second_object.hash.to_hex())),
            b"second",
        )
        .unwrap();
        let second_root = remote
            .publish_capsule_incremental(&second, staging.path(), &first)
            .await
            .unwrap()
            .root;
        assert_ne!(second_root, first_root);

        let (mut malicious, malicious_object) =
            append_capsule_file(&second, "/malicious", b"malicious");
        let CapsuleNode::Physical {
            payload_kind,
            schema_fingerprint,
            logical_root,
            leaves,
            ..
        } = &mut malicious.entries[1].node
        else {
            panic!("physical inherited entry");
        };
        leaves[0].logical_hash = ObjectHash::of_bytes(b"forged logical leaf");
        *logical_root = capsule_series_root(*payload_kind, *schema_fingerprint, leaves);
        let malicious_staging = tempdir().unwrap();
        std::fs::write(
            malicious_staging
                .path()
                .join(format!("blake3={}", malicious_object.hash.to_hex())),
            b"malicious",
        )
        .unwrap();
        assert!(
            remote
                .publish_capsule_incremental(&malicious, malicious_staging.path(), &second)
                .await
                .is_err()
        );
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            second_root
        );

        let inherited = first.payload_objects().unwrap()[0].hash;
        std::fs::remove_file(
            remote_path
                .join("recovery/objects")
                .join(format!("blake3={}", inherited.to_hex())),
        )
        .unwrap();
        let (third, third_object) = append_capsule_file(&second, "/third", b"third");
        let staging = tempdir().unwrap();
        std::fs::write(
            staging
                .path()
                .join(format!("blake3={}", third_object.hash.to_hex())),
            b"third",
        )
        .unwrap();
        assert!(
            remote
                .publish_capsule_incremental(&third, staging.path(), &second)
                .await
                .is_err()
        );
        assert_eq!(
            remote.latest_capsule().await.unwrap().unwrap().0,
            second_root
        );
    }
}
