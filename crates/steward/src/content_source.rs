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
//! current publication state, immutable per-push record, persistent manifest
//! map, changed content objects, and large payload streams -- read directly
//! from the producer clone's on-disk state, so the consumer rebuilds a
//! byte-identical foreign subtree.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sync_store::ContentRemote;
use sync_store::PublicationState;
use sync_store::content::{Commit, ObjectHash, PackDescriptor, PackIndex, PublicationRecord};
use uuid::Uuid;

use crate::content_tree::materialize_content_objects;
use crate::{Steward, StewardError, get_data_path};

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

    /// Fixed-size current publication state for `ref_name`.
    async fn get_publication_state(
        &self,
        ref_name: &str,
    ) -> Result<Option<PublicationState>, StewardError> {
        let _ = ref_name;
        Err(StewardError::Content(
            "content source does not expose native-v2 publication state".to_string(),
        ))
    }

    /// One immutable per-push publication record.
    async fn get_publication_record(
        &self,
        hash: ObjectHash,
    ) -> Result<Option<PublicationRecord>, StewardError> {
        let _ = hash;
        Ok(None)
    }

    /// One immutable pack index named by a publication record.
    async fn get_publication_pack(
        &self,
        descriptor: PackDescriptor,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        self.get_pack_index(descriptor.series_hash, descriptor.pack_hash)
            .await
    }

    /// The bytes of the immutable object with `hash`, or `None` when absent.
    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError>;

    /// Receipt-authenticated byte length when available.
    async fn object_size(&self, hash: ObjectHash) -> Result<Option<u64>, StewardError> {
        Ok(self.get_object(hash).await?.map(|bytes| bytes.len() as u64))
    }

    /// Read exactly the requested inline objects.
    ///
    /// Remote implementations should batch this into one physical query.
    /// Missing hashes are omitted. The default preserves correct behavior for
    /// sources whose objects are already local or in memory.
    async fn get_objects(
        &self,
        hashes: &[ObjectHash],
    ) -> Result<HashMap<ObjectHash, Vec<u8>>, StewardError> {
        let mut objects = HashMap::with_capacity(hashes.len());
        for &hash in hashes {
            if objects.contains_key(&hash) {
                continue;
            }
            if let Some(bytes) = self.get_object(hash).await? {
                let _ = objects.insert(hash, bytes);
            }
        }
        Ok(objects)
    }

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

    /// Preferred immutable linked pack segment for one exact series state.
    ///
    /// This is a fixed-key point lookup keyed by `series_hash`, never a pack
    /// prefix listing. A full-range consolidated segment may replace traversal
    /// of older linked segments without mutating them.
    async fn get_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>, StewardError>;

    /// Optional whole-range consolidated segment for a fresh clone.
    ///
    /// Incremental consumers deliberately ignore this and use
    /// [`Self::get_series_pack`] so explicit maintenance cannot make suffix
    /// metadata cost proportional to the complete series.
    async fn get_consolidated_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>, StewardError> {
        let _ = series_hash;
        Ok(None)
    }

    /// Explicit diagnostic listing of pack hashes for one
    /// `watertown.series.v3` state.
    ///
    /// Ordinary native fetch never calls this method; it uses the fixed-key
    /// [`Self::get_series_pack`] locator and linked segments. Implementations
    /// may use a real prefix listing here for tests, maintenance, or audit.
    async fn list_pack_hashes(
        &self,
        series_hash: ObjectHash,
    ) -> Result<std::collections::HashSet<ObjectHash>, StewardError>;

    /// Fetch one pack advertisement's raw `watertown.series-pack.v4` bytes by the
    /// series it claims and its own content address, or `None` if absent.
    ///
    /// Implementations validate the returned bytes before handing them back:
    /// they must hash to `pack_hash` and must decode to a
    /// [`sync_store::content::PackIndex`] whose own `series_hash` agrees
    /// with `series_hash`, rejecting a pack advertised under the wrong
    /// series by mistake or by attack.
    async fn get_pack_index(
        &self,
        series_hash: ObjectHash,
        pack_hash: ObjectHash,
    ) -> Result<Option<Vec<u8>>, StewardError>;
}

#[async_trait]
impl ContentSource for ContentRemote {
    fn pond_id(&self) -> Uuid {
        ContentRemote::pond_id(self)
    }

    async fn get_tip(&self, ref_name: &str) -> Result<Option<ObjectHash>, StewardError> {
        ContentRemote::current_publication(self, ref_name)
            .await
            .map(|state| state.map(|state| state.snapshot_tip))
            .map_err(|e| StewardError::Content(e.to_string()))
    }

    async fn get_publication_state(
        &self,
        ref_name: &str,
    ) -> Result<Option<PublicationState>, StewardError> {
        ContentRemote::current_publication(self, ref_name)
            .await
            .map_err(|error| StewardError::Content(error.to_string()))
    }

    async fn get_publication_record(
        &self,
        hash: ObjectHash,
    ) -> Result<Option<PublicationRecord>, StewardError> {
        ContentRemote::get_publication_record(self, hash)
            .await
            .map_err(|error| StewardError::Content(error.to_string()))
    }

    async fn get_publication_pack(
        &self,
        descriptor: PackDescriptor,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        ContentRemote::get_immutable_pack(self, descriptor)
            .await
            .map_err(|error| StewardError::Content(error.to_string()))
    }

    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        ContentRemote::get_immutable_object(self, hash)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))
    }

    async fn object_size(&self, hash: ObjectHash) -> Result<Option<u64>, StewardError> {
        ContentRemote::immutable_object_size(self, hash)
            .await
            .map_err(|error| StewardError::Content(error.to_string()))
    }

    async fn get_objects(
        &self,
        hashes: &[ObjectHash],
    ) -> Result<HashMap<ObjectHash, Vec<u8>>, StewardError> {
        let mut objects = HashMap::new();
        for hash in hashes.iter().copied().collect::<BTreeSet<_>>() {
            if let Some(bytes) = ContentRemote::get_immutable_object(self, hash)
                .await
                .map_err(|error| StewardError::Content(error.to_string()))?
            {
                let _ = objects.insert(hash, bytes);
            }
        }
        Ok(objects)
    }

    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError> {
        Ok(ContentRemote::get_immutable_object(self, hash)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))?
            .is_some())
    }

    async fn list_blobs(&self) -> Result<std::collections::HashSet<ObjectHash>, StewardError> {
        Err(StewardError::Content(
            "native-v2 consumers do not list cumulative blob inventories".to_string(),
        ))
    }

    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError> {
        ContentRemote::get_immutable_object_reader(self, hash)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))
    }

    async fn get_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>, StewardError> {
        ContentRemote::canonical_pack_for_series(self, series_hash)
            .await
            .map_err(|error| StewardError::Content(error.to_string()))
    }

    async fn get_consolidated_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>, StewardError> {
        ContentRemote::consolidated_pack_for_series(self, series_hash)
            .await
            .map_err(|error| StewardError::Content(error.to_string()))
    }

    async fn list_pack_hashes(
        &self,
        series_hash: ObjectHash,
    ) -> Result<std::collections::HashSet<ObjectHash>, StewardError> {
        Ok(ContentRemote::canonical_pack_for_series(self, series_hash)
            .await
            .map_err(|error| StewardError::Content(error.to_string()))?
            .map(|descriptor| std::collections::HashSet::from([descriptor.pack_hash]))
            .unwrap_or_default())
    }

    async fn get_pack_index(
        &self,
        series_hash: ObjectHash,
        pack_hash: ObjectHash,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        ContentRemote::get_immutable_pack(self, PackDescriptor::new(series_hash, pack_hash))
            .await
            .map_err(|e| StewardError::Content(e.to_string()))
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
    publication_state: PublicationState,
    publication_records: HashMap<ObjectHash, PublicationRecord>,
    /// The producer clone's root directory, so explicit maintenance packs can
    /// be read from its `_packs/` directory.
    pond_path: PathBuf,
    /// Inline objects reachable from the tip (trees, series, symlinks, recipes,
    /// small blobs), plus the node manifest and the tip commit object.
    objects: BTreeMap<ObjectHash, Vec<u8>>,
    /// Hashes of the large blobs that transfer via the external path.
    external_blobs: BTreeSet<ObjectHash>,
    /// Preferred linked pack segment for each immutable series state. Commit
    /// deltas supply ordinary append segments.
    series_pack_heads: HashMap<ObjectHash, PackDescriptor>,
    /// Optional full-range explicit-maintenance pack for a current series
    /// state, kept separate so incremental consumers can ignore it.
    consolidated_pack_heads: HashMap<ObjectHash, PackDescriptor>,
    publication_pack_bytes: HashMap<ObjectHash, Vec<u8>>,
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
        let pond_path = ship.pond_path().to_path_buf();

        let pond_id = ship.control_table().pond_id_uuid();
        let commit_log = crate::content_tree::read_log_leaves(
            ship.data_persistence().table().clone(),
            &pond_id.to_string(),
        )
        .await?;
        let tip_commit = Commit::decode(commit_log.last().ok_or_else(|| {
            StewardError::Content("pond:// source has no content-changing commit".to_string())
        })?)
        .map_err(|e| StewardError::Content(format!("decode commit-log tip: {e}")))?;
        let tip = tip_commit.hash();

        let materialized = materialize_content_objects(ship).await?;
        let manifest_root = materialized.manifest_root.ok_or_else(|| {
            StewardError::Content("materialized objects carry no manifest root".to_string())
        })?;
        if manifest_root != tip_commit.manifest_root {
            return Err(StewardError::Content(format!(
                "pond:// manifest root {manifest_root} disagrees with tip {}",
                tip_commit.manifest_root
            )));
        }
        let series_material = materialized.series_material.clone();
        let mut objects: BTreeMap<ObjectHash, Vec<u8>> = materialized
            .inline
            .iter()
            .map(|(hash, object)| (*hash, object.bytes.clone()))
            .collect();
        let external_blobs = materialized.external_blobs;
        let mut publication_pack_bytes = HashMap::new();
        let mut packs = Vec::new();
        for material in &series_material {
            for (descriptor, bytes) in current_publication_packs(&pond_path, material).await? {
                packs.push(descriptor);
                let _ = publication_pack_bytes.insert(descriptor.pack_hash, bytes);
            }
        }
        let local_store = crate::local_content::LocalContentStore::new(&pond_path);
        let mut publication_records = HashMap::new();
        let mut series_pack_heads = HashMap::new();
        let consolidated_pack_heads = packs
            .iter()
            .map(|descriptor| (descriptor.series_hash, *descriptor))
            .collect::<HashMap<_, _>>();
        let mut parent_record = None;
        let mut last_record = None;
        for bytes in commit_log {
            let commit = Commit::decode(&bytes)
                .map_err(|e| StewardError::Content(format!("decode commit-log leaf: {e}")))?;
            let hash = commit.hash();
            let _ = objects.insert(hash, bytes);
            let mut introduced = commit.introduced_objects.clone();
            introduced.push(sync_store::content::ObjectDescriptor::new(
                hash,
                sync_store::content::ContentObjectKind::Commit,
            ));
            for descriptor in &commit.introduced_packs {
                let _ = series_pack_heads.insert(descriptor.series_hash, *descriptor);
            }

            for descriptor in &commit.introduced_packs {
                if !publication_pack_bytes.contains_key(&descriptor.pack_hash)
                    && let Some(bytes) = local_store.read_optional(descriptor.pack_hash)?
                {
                    let _ = publication_pack_bytes.insert(descriptor.pack_hash, bytes);
                }
            }
            let record = PublicationRecord::new(
                pond_id,
                "main",
                hash,
                commit.manifest_root,
                parent_record,
                introduced,
                commit.introduced_packs.clone(),
                commit.manifest_changes.clone(),
            )
            .map_err(StewardError::Content)?;
            let record_hash = record.hash();
            let _ = publication_records.insert(record_hash, record);
            parent_record = Some(record_hash);
            last_record = Some(record_hash);
        }
        let publication_record = last_record.ok_or_else(|| {
            StewardError::Content("pond:// source has no publication record".to_string())
        })?;
        let publication_state = PublicationState::new(
            pond_id,
            "main",
            tip,
            manifest_root,
            publication_record,
            i64::try_from(publication_records.len()).map_err(|_| {
                StewardError::Content("pond:// publication generation exceeds i64".to_string())
            })?,
            tip_commit.provenance.time_micros,
        )
        .map_err(|error| StewardError::Content(error.to_string()))?;

        Ok(Self {
            steward,
            pond_id,
            tip,
            publication_state,
            publication_records,
            pond_path,
            objects,
            external_blobs,
            series_pack_heads,
            consolidated_pack_heads,
            publication_pack_bytes,
        })
    }

    /// The `_packs/v4/series=<hex>` directory for one series, beside
    /// `_large_files/` in the pond's physical data directory
    /// (`docs/logical-series-identity-design.md` delivery gate 3). Ordinary
    /// commit-produced segments live in the pond-local immutable object
    /// cache; only explicit maintenance packs need this sidecar directory.
    fn pack_series_dir(&self, series_hash: ObjectHash) -> PathBuf {
        get_data_path(&self.pond_path)
            .join(sync_store::pack_keys::PACK_INDEX_ROOT)
            .join(sync_store::pack_keys::series_dir_name(series_hash))
    }

    /// Read one persisted pack advertisement from this producer's own
    /// `_packs/v4/series=<hex>` directory, decoding and validating it exactly
    /// as [`Self::get_pack_index`] does. Returns `None` when no file with
    /// that name exists.
    ///
    /// Shared by [`Self::get_pack_index`] and explicit diagnostic listing.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but its content hash, decoding,
    /// or declared series hash do not match -- a real corruption/placement
    /// bug, never silently ignored.
    async fn read_advertised_pack(
        &self,
        series_hash: ObjectHash,
        pack_hash: ObjectHash,
    ) -> Result<Option<(Vec<u8>, PackIndex)>, StewardError> {
        let path = self
            .pack_series_dir(series_hash)
            .join(sync_store::pack_keys::pack_file_name(pack_hash));
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(StewardError::Content(format!(
                    "read pack advertisement {}: {e}",
                    path.display()
                )));
            }
        };
        let computed = ObjectHash::of_bytes(&bytes);
        if computed != pack_hash {
            return Err(StewardError::Content(format!(
                "pack advertisement at {} hashes to {} (content-address mismatch)",
                path.display(),
                computed.to_hex()
            )));
        }
        let decoded = PackIndex::decode(&bytes).map_err(|e| {
            StewardError::Content(format!("decode pack advertisement {}: {e}", path.display()))
        })?;
        if decoded.series_hash() != series_hash {
            return Err(StewardError::Content(format!(
                "pack advertisement {} declares series_hash {} but was found under series={} (cross-series index)",
                path.display(),
                decoded.series_hash().to_hex(),
                series_hash.to_hex()
            )));
        }
        Ok(Some((bytes, decoded)))
    }
}

async fn current_publication_packs(
    pond_path: &Path,
    material: &crate::content_tree::SeriesPackMaterial,
) -> Result<Vec<(PackDescriptor, Vec<u8>)>, StewardError> {
    let directory = get_data_path(pond_path)
        .join(sync_store::pack_keys::PACK_INDEX_ROOT)
        .join(sync_store::pack_keys::series_dir_name(material.series_hash));
    let mut candidates = Vec::new();
    match tokio::fs::read_dir(&directory).await {
        Ok(mut entries) => {
            while let Some(entry) = entries.next_entry().await.map_err(|error| {
                StewardError::Content(format!(
                    "list pack advertisements under {}: {error}",
                    directory.display()
                ))
            })? {
                let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                let Ok(pack_hash) = sync_store::pack_keys::parse_pack_file_name(&name) else {
                    continue;
                };
                let bytes = tokio::fs::read(entry.path()).await.map_err(|error| {
                    StewardError::Content(format!(
                        "read pack advertisement {}: {error}",
                        entry.path().display()
                    ))
                })?;
                if ObjectHash::of_bytes(&bytes) != pack_hash {
                    return Err(StewardError::Content(format!(
                        "pack advertisement {} has a content-address mismatch",
                        entry.path().display()
                    )));
                }
                let pack = PackIndex::decode(&bytes).map_err(|error| {
                    StewardError::Content(format!(
                        "decode pack advertisement {}: {error}",
                        entry.path().display()
                    ))
                })?;
                if pack.series_hash() != material.series_hash {
                    return Err(StewardError::Content(format!(
                        "pack advertisement {} names the wrong series",
                        entry.path().display()
                    )));
                }
                candidates.push((pack_hash, pack, bytes));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(StewardError::Content(format!(
                "list pack advertisements under {}: {error}",
                directory.display()
            )));
        }
    }
    let best = candidates
        .into_iter()
        .filter(|(_, pack, _)| {
            pack.parent_series_hash().is_none()
                && pack.leaf_start() == 0
                && pack.leaf_end() == material.manifest.leaf_count()
                && pack.total_leaf_count() == material.manifest.leaf_count()
        })
        .min_by_key(|(hash, pack, _)| (pack.physical_object_hashes().len(), *hash));
    Ok(best
        .map(|(hash, _, bytes)| vec![(PackDescriptor::new(material.series_hash, hash), bytes)])
        .unwrap_or_default())
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

    async fn get_publication_state(
        &self,
        _ref_name: &str,
    ) -> Result<Option<PublicationState>, StewardError> {
        Ok(Some(self.publication_state.clone()))
    }

    async fn get_publication_record(
        &self,
        hash: ObjectHash,
    ) -> Result<Option<PublicationRecord>, StewardError> {
        Ok(self.publication_records.get(&hash).cloned())
    }

    async fn get_publication_pack(
        &self,
        descriptor: PackDescriptor,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        Ok(self
            .publication_pack_bytes
            .get(&descriptor.pack_hash)
            .cloned())
    }

    async fn get_object(&self, hash: ObjectHash) -> Result<Option<Vec<u8>>, StewardError> {
        if let Some(bytes) = self.objects.get(&hash) {
            return Ok(Some(bytes.clone()));
        }
        crate::local_content::LocalContentStore::new(&self.pond_path).read_optional(hash)
    }

    async fn object_size(&self, hash: ObjectHash) -> Result<Option<u64>, StewardError> {
        if let Some(bytes) = self.objects.get(&hash) {
            return Ok(Some(bytes.len() as u64));
        }
        Ok(None)
    }

    async fn has_blob(&self, hash: ObjectHash) -> Result<bool, StewardError> {
        if self.external_blobs.contains(&hash) {
            return Ok(true);
        }
        // A maintenance-published physical pack object is not part of the
        // ordinary v1 blob closure captured at `open()` time -- it lives in
        // this pond's own shared `_packs/objects/` sidecar, written only by
        // `pond maintain --collapse-versions` (`crate::pack_store`). Check
        // it too, so a repacked series' physical objects are fetchable
        // through the same `ContentSource` surface as any other blob.
        crate::pack_store::has_pack_object(&self.pond_path, hash).await
    }

    async fn list_blobs(&self) -> Result<std::collections::HashSet<ObjectHash>, StewardError> {
        // Already resolved in memory when the producer clone was opened,
        // plus every physical pack object this pond has published locally
        // (see `Self::has_blob`'s doc comment) -- both are unioned so
        // `fetch_blob`'s cached presence index (built from a single
        // `list_blobs` call) recognizes a maintained pack's physical
        // objects, not only ordinary externalized `_large_files` blobs.
        let mut blobs: std::collections::HashSet<ObjectHash> =
            self.external_blobs.iter().copied().collect();
        blobs.extend(crate::pack_store::list_pack_object_hashes(&self.pond_path).await?);
        Ok(blobs)
    }

    async fn get_blob_reader(&self, hash: ObjectHash) -> Result<Option<BlobReader>, StewardError> {
        if let Some(reader) =
            crate::pack_store::open_pack_object_reader(&self.pond_path, hash).await?
        {
            return Ok(Some(Box::new(reader)));
        }
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

    async fn get_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>, StewardError> {
        Ok(self.series_pack_heads.get(&series_hash).copied())
    }

    async fn get_consolidated_series_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<PackDescriptor>, StewardError> {
        Ok(self.consolidated_pack_heads.get(&series_hash).copied())
    }

    async fn list_pack_hashes(
        &self,
        series_hash: ObjectHash,
    ) -> Result<std::collections::HashSet<ObjectHash>, StewardError> {
        let dir = self.pack_series_dir(series_hash);
        let mut out: std::collections::HashSet<ObjectHash> = std::collections::HashSet::new();
        match tokio::fs::read_dir(&dir).await {
            Ok(mut entries) => loop {
                let entry = entries.next_entry().await.map_err(|e| {
                    StewardError::Content(format!(
                        "list pack advertisements under {}: {e}",
                        dir.display()
                    ))
                })?;
                let Some(entry) = entry else { break };
                let name = entry.file_name();
                let name = name.to_str().ok_or_else(|| {
                    StewardError::Content(format!(
                        "non-utf8 pack advertisement filename under {}",
                        dir.display()
                    ))
                })?;
                // A stray temp file from an interrupted write, this pack's
                // own layout-marker sidecar, or harmless OS filesystem
                // metadata (e.g. `.DS_Store`) is not a pack advertisement
                // at all -- ignore it exactly as `pack_store`'s own local
                // listing does (requirement 8), rather than failing this
                // list over an artifact of a crash mid-publish, a table
                // repack's marker, or browsing this directory in a file
                // manager.
                if sync_store::pack_keys::is_ignorable_directory_entry(name) {
                    continue;
                }
                if sync_store::pack_keys::parse_layout_marker_file_name(name)
                    .map_err(|e| {
                        StewardError::Content(format!(
                            "malformed pack advertisement {}/{name}: {e}",
                            dir.display()
                        ))
                    })?
                    .is_some()
                {
                    continue;
                }
                // A pack's stale-generation sentinel (`pack_store`'s
                // one-maintenance-cycle deletion grace period, see
                // `crate::pack_store::retain_selected_pack_only`): the pack
                // it marks is still a fully valid, currently-selectable
                // advertisement -- only the sentinel filename itself is
                // skipped, so a concurrent reader keeps seeing (and can
                // keep fetching) an advertisement pack maintenance has only
                // provisionally superseded.
                if sync_store::pack_keys::parse_stale_marker_file_name(name)
                    .map_err(|e| {
                        StewardError::Content(format!(
                            "malformed pack advertisement {}/{name}: {e}",
                            dir.display()
                        ))
                    })?
                    .is_some()
                {
                    continue;
                }
                let hash = sync_store::pack_keys::parse_pack_file_name(name).map_err(|e| {
                    StewardError::Content(format!(
                        "malformed pack advertisement {}/{name}: {e}",
                        dir.display()
                    ))
                })?;
                if !out.insert(hash) {
                    return Err(StewardError::Content(format!(
                        "duplicate pack advertisement listing entry: {hash}"
                    )));
                }
            },
            // No explicit maintenance directory is not an error. Ordinary
            // append segments live in the pond-local immutable object cache
            // and are added below through `series_pack_heads`.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(StewardError::Content(format!(
                    "list pack advertisements under {}: {e}",
                    dir.display()
                )));
            }
        }
        if let Some(descriptor) = self.consolidated_pack_heads.get(&series_hash) {
            out.clear();
            let _ = out.insert(descriptor.pack_hash);
        } else if let Some(descriptor) = self.series_pack_heads.get(&series_hash) {
            let _ = out.insert(descriptor.pack_hash);
        }
        Ok(out)
    }

    async fn get_pack_index(
        &self,
        series_hash: ObjectHash,
        pack_hash: ObjectHash,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        if let Some((bytes, _)) = self.read_advertised_pack(series_hash, pack_hash).await? {
            return Ok(Some(bytes));
        }
        let descriptor = PackDescriptor::new(series_hash, pack_hash);
        if self
            .series_pack_heads
            .get(&series_hash)
            .is_some_and(|head| *head == descriptor)
            || self
                .consolidated_pack_heads
                .get(&series_hash)
                .is_some_and(|head| *head == descriptor)
            || self
                .publication_records
                .values()
                .any(|record| record.introduced_packs.contains(&descriptor))
        {
            return crate::local_content::LocalContentStore::new(&self.pond_path)
                .read_optional(pack_hash);
        }
        Ok(None)
    }
}
