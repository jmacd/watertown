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

use crate::content::ObjectHash;
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
        use tokio::io::AsyncReadExt;
        let path = Self::blob_path(hash);
        let upload = self
            .store
            .object_store()
            .put_multipart(&path)
            .await
            .map_err(|e| StoreError::Invariant(format!("blob put_multipart: {e}")))?;
        let mut upload = upload;
        let mut parts = FuturesUnordered::new();
        let mut hasher = blake3::Hasher::new();

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
        if computed != hash {
            drop(parts);
            upload
                .abort()
                .await
                .map_err(|e| StoreError::Invariant(format!("blob abort: {e}")))?;
            return Err(StoreError::Invariant(format!(
                "blob bytes hash to {} but were offered under {}",
                computed.to_hex(),
                hash.to_hex()
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
}
