// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! [`ContentSource`]: the read side of a content-addressed pond, abstracted so
//! the fetch/import path ([`crate::content_pull`]) can pull from either a
//! remote content store ([`ContentRemote`], backed by S3 or a `file://` Delta
//! store) or a **local sibling pond** ([`LocalPondSource`]).
//!
//! The local source exists for the develop-and-preview workflow: clone a group
//! of ponds locally, point a consumer's cross-pond import at a producer clone
//! on disk (a `pond://<path>` URL), then edit the producer and re-pull without
//! any S3 round-trip or an intermediate `file://` content store.  A
//! `LocalPondSource` serves exactly the payload a `pond push` would send -- the
//! tip commit, the node manifest, the inline object closure, and the external
//! `_large_files` blobs -- read directly from the producer clone's on-disk
//! state, so the consumer rebuilds a byte-identical foreign subtree.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use async_trait::async_trait;
use sync_store::ContentRemote;
use sync_store::content::{Commit, ObjectHash};
use uuid::Uuid;

use crate::content_tree::materialize_content_objects;
use crate::{Steward, StewardError};

/// A streaming reader over a large external blob's bytes.
pub type BlobReader = Box<dyn tokio::io::AsyncRead + Unpin + Send>;

/// The read side of a content-addressed pond: enough to walk a tip commit's
/// object closure and stream its external blobs.  Implemented by both
/// [`ContentRemote`] (S3 / `file://` Delta store) and [`LocalPondSource`] (a
/// producer clone on local disk).
#[async_trait]
pub trait ContentSource: Send + Sync {
    /// The pond whose content this source holds.
    fn pond_id(&self) -> Uuid;

    /// The tip commit hash for `ref_name`, or `None` if the ref is absent.
    async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>, StewardError>;

    /// The bytes of the inline object with `hash`, or `None` if it is not an
    /// inline object (e.g. it is a large external blob).
    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError>;

    /// True if the source holds the external blob with `hash`.
    ///
    /// Prefer [`Self::list_blobs`] when asking about more than a couple of
    /// blobs: against a remote store this is one billed request per call.
    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError>;

    /// Every external blob the source holds, as one listing.
    ///
    /// Presence is asked about in bulk -- once per blob in a content closure --
    /// and answering it per blob costs a request proportional to the pond's
    /// accumulated history rather than to the work being done.  One listing
    /// answers the whole question, and reads live state exactly as the
    /// per-blob probes did.
    async fn list_blobs(&self) -> Result<std::collections::HashSet<ObjectHash>, StewardError>;

    /// A bounded streaming reader over the external blob with `hash`, or `None`
    /// if the source does not hold it.
    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError>;

    /// Snapshot the object partition into memory for a bulk read.  Default: a
    /// no-op (sources that already resolve objects from memory need nothing).
    async fn preload_objects(&self) -> Result<(), StewardError> {
        Ok(())
    }

    /// Drop any snapshot taken by [`Self::preload_objects`].  Default: a no-op.
    fn clear_object_cache(&self) {}
}

#[async_trait]
impl ContentSource for ContentRemote {
    fn pond_id(&self) -> Uuid {
        ContentRemote::pond_id(self)
    }

    async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>, StewardError> {
        ContentRemote::get_tip(self, ref_name)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))
    }

    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        ContentRemote::get_object(self, hash)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))
    }

    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError> {
        ContentRemote::has_blob(self, hash)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))
    }

    async fn list_blobs(&self) -> Result<std::collections::HashSet<ObjectHash>, StewardError> {
        ContentRemote::list_blobs(self)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))
    }

    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError> {
        ContentRemote::get_blob_reader(self, hash)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))
    }

    async fn preload_objects(&self) -> Result<(), StewardError> {
        ContentRemote::preload_objects(self)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))
    }

    fn clear_object_cache(&self) {
        ContentRemote::clear_object_cache(self);
    }
}

/// A [`ContentSource`] backed by a **producer pond clone on local disk**.
///
/// On [`open`](Self::open) it resolves the producer's current tip commit (the
/// highest content-changing spine seq, exactly as `push` does) and materializes
/// the reachable inline object closure and node manifest into memory; external
/// `_large_files` blobs are streamed on demand from the clone's own store.  The
/// opened [`Steward`] is held for the lifetime of the source so blob streaming
/// can read the clone's persistence layer.
///
/// The served graph is byte-identical to what a `pond push` of the same clone
/// would place on a remote, so the consumer's import path is unchanged.
pub struct LocalPondSource {
    /// The opened producer clone, held so [`Self::get_blob_reader`] can stream
    /// external blobs from its persistence layer.
    steward: Steward,
    pond_id: Uuid,
    tip: ObjectHash,
    /// Inline objects reachable from the tip (trees, series, symlinks, recipes,
    /// small blobs), plus the node manifest and the tip commit object.
    objects: BTreeMap<ObjectHash, Vec<u8>>,
    /// Hashes of the large blobs that transfer via the external path.
    external_blobs: BTreeSet<ObjectHash>,
}

impl LocalPondSource {
    /// Open the producer pond at `pond_path` and materialize its current
    /// content closure so it can be served as a [`ContentSource`].
    ///
    /// # Errors
    ///
    /// Returns an error if the path is not a pond, has no content-changing
    /// commit to serve, or its commit spine is missing/corrupt.
    pub async fn open<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        let steward = Steward::open_pond(pond_path).await?;
        let ship = steward.as_pond().ok_or_else(|| {
            StewardError::Content("pond:// source path is not a pond steward".to_string())
        })?;

        let pond_id = ship.control_table().pond_id_uuid();
        let commit_log = crate::content_tree::read_log_leaves(
            ship.data_persistence().table().clone(),
            &pond_id.to_string(),
        )
        .await?;
        let tip = Commit::decode(commit_log.last().ok_or_else(|| {
            StewardError::Content("pond:// source has no content-changing commit".to_string())
        })?)
        .map_err(|e| StewardError::Content(format!("decode commit-log tip: {e}")))?
        .hash();

        let materialized = materialize_content_objects(ship).await?;
        let mut objects: BTreeMap<ObjectHash, Vec<u8>> = materialized.inline;
        let (manifest_hash, manifest_bytes) = materialized.manifest.ok_or_else(|| {
            StewardError::Content("materialized objects carry no node manifest".to_string())
        })?;
        let _ = objects.insert(manifest_hash, manifest_bytes);
        // Serve the authoritative commit-log chain, not only the tip. A
        // consumer uses this lineage to reject stale/out-of-order refs while
        // still accepting an ordinary producer fast-forward.
        for bytes in commit_log {
            let commit = Commit::decode(&bytes)
                .map_err(|e| StewardError::Content(format!("decode commit-log leaf: {e}")))?;
            let hash = commit.hash();
            let _ = objects.insert(hash, bytes);
        }
        let external_blobs = materialized.external_blobs;

        Ok(Self {
            steward,
            pond_id,
            tip,
            objects,
            external_blobs,
        })
    }
}

#[async_trait]
impl ContentSource for LocalPondSource {
    fn pond_id(&self) -> Uuid {
        self.pond_id
    }

    async fn get_tip(&self, _ref_name: &str) -> Result<Option<ObjectHash>, StewardError> {
        // A local clone serves a single logical ref (its current tip).
        Ok(Some(self.tip))
    }

    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        Ok(self.objects.get(&hash).cloned())
    }

    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError> {
        Ok(self.external_blobs.contains(&hash))
    }

    async fn list_blobs(&self) -> Result<std::collections::HashSet<ObjectHash>, StewardError> {
        // Already resolved in memory when the producer clone was opened.
        Ok(self.external_blobs.iter().copied().collect())
    }

    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError> {
        if !self.external_blobs.contains(&hash) {
            return Ok(None);
        }
        let ship = self.steward.as_pond().ok_or_else(|| {
            StewardError::Content("pond:// source path is not a pond steward".to_string())
        })?;
        let reader = ship
            .data_persistence()
            .open_large_file_reader_by_hash(&hash.to_hex())
            .await
            .map_err(|e| {
                StewardError::Content(format!("open external blob {}: {e}", hash.to_hex()))
            })?;
        Ok(Some(Box::new(reader)))
    }
}
