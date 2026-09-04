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
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sync_store::ContentRemote;
use sync_store::content::{Commit, ObjectHash, PackIndex};
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

    /// Every pack hash advertised for the `watertown.series.v2` series named
    /// `series_hash`, as one listing
    /// (`docs/logical-series-identity-design.md` delivery gate 3).
    ///
    /// Pack indexes are derived storage metadata excluded from the logical
    /// content tree, so they are never reachable through
    /// [`Self::get_object`]/[`Self::get_tip`]'s commit/tree closure; this is
    /// the dedicated, uniform discovery entry point every source
    /// implements. A series with no advertised packs yet -- true of every
    /// v1 series, and of any v2 series before its first publication --
    /// returns an empty set, not an error.
    async fn list_pack_hashes(
        &self,
        series_hash: ObjectHash,
    ) -> Result<std::collections::HashSet<ObjectHash>, StewardError>;

    /// Fetch one pack advertisement's raw `watertown.series-pack.v2` bytes by the
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

    async fn list_pack_hashes(
        &self,
        series_hash: ObjectHash,
    ) -> Result<std::collections::HashSet<ObjectHash>, StewardError> {
        ContentRemote::list_pack_hashes(self, series_hash)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))
    }

    async fn get_pack_index(
        &self,
        series_hash: ObjectHash,
        pack_hash: ObjectHash,
    ) -> Result<Option<Vec<u8>>, StewardError> {
        ContentRemote::get_pack_index_bytes(self, series_hash, pack_hash)
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

/// Result of [`LocalPondSource::on_disk_packs_form_full_cover`]: whether real
/// on-disk pack advertisements alone already exactly tile a series' leaf
/// range, so the synthesized initial pack (finding 1: BLOCKING selection
/// bug) can stay a true fallback rather than always being offered.
enum FullCoverStatus {
    /// The real, on-disk advertisements already exactly cover the series'
    /// leaf range (or the series has no leaves at all): synthesis must be
    /// suppressed so only maintained physical hashes are ever selected.
    Covered,
    /// The real, on-disk advertisements do not by themselves cover the
    /// range (empty, or only a partial cover): the synthesized fallback
    /// must be offered as a candidate so a cover can still be found,
    /// potentially combined with any surviving real partial packs.
    NotCovered,
    /// This series has no captured native pack material at all (a v1
    /// series, or one this pond does not itself track): synthesis is not
    /// possible regardless, so the on-disk set alone is used as-is.
    NoMaterial,
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
    /// The producer clone's root directory, so pack advertisements can be
    /// read from its `_packs/` directory (see [`Self::list_pack_hashes`]).
    pond_path: PathBuf,
    /// Inline objects reachable from the tip (trees, series, symlinks, recipes,
    /// small blobs), plus the node manifest and the tip commit object.
    objects: BTreeMap<ObjectHash, Vec<u8>>,
    /// Hashes of the large blobs that transfer via the external path.
    external_blobs: BTreeSet<ObjectHash>,
    /// Every `watertown.series.v2` series' manifest and ordered live versions
    /// captured at [`Self::open`] time, exactly as a push would.  Backs
    /// [`Self::list_pack_hashes`]/[`Self::get_pack_index`]'s on-demand pack
    /// materialization: a native v2 series in this unpushed local pond has
    /// no persisted `_packs/` advertisement (nothing writes there on a
    /// bare `Ship::write_transaction`), so it is otherwise unfetchable
    /// through a `pond://` source the same way it would be through a
    /// `ContentRemote` before its first push publishes an initial pack.
    series_material: Vec<crate::content_tree::SeriesPackMaterial>,
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
        let tip = Commit::decode(commit_log.last().ok_or_else(|| {
            StewardError::Content("pond:// source has no content-changing commit".to_string())
        })?)
        .map_err(|e| StewardError::Content(format!("decode commit-log tip: {e}")))?
        .hash();

        let materialized = materialize_content_objects(ship).await?;
        let series_material = materialized.series_material;
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
            pond_path,
            objects,
            external_blobs,
            series_material,
        })
    }

    /// The `_packs/series=<hex>` directory for one series, beside
    /// `_large_files/` in the pond's physical data directory
    /// (`docs/logical-series-identity-design.md` delivery gate 3).  A
    /// producer that has actually published (via `pond push`) writes real
    /// files here; [`Self::list_pack_hashes`]/[`Self::get_pack_index`] also
    /// consult [`Self::series_material`] to synthesize the initial pack
    /// on-demand for a series that has never been pushed, so this directory
    /// existing is never required for a fresh native series to be
    /// discoverable.
    fn pack_series_dir(&self, series_hash: ObjectHash) -> PathBuf {
        get_data_path(&self.pond_path)
            .join(sync_store::pack_keys::PACKS_ROOT)
            .join(sync_store::pack_keys::series_dir_name(series_hash))
    }

    /// Read one persisted pack advertisement from this producer's own
    /// `_packs/series=<hex>` directory, decoding and validating it exactly
    /// as [`Self::get_pack_index`] does. Returns `None` when no file with
    /// that name exists (never persisted, or synthesized-only).
    ///
    /// Shared by [`Self::get_pack_index`] (the public fetch path) and
    /// [`Self::list_pack_hashes`] (which must decode every on-disk
    /// advertisement to check whether they already form a full exact cover
    /// before deciding whether on-demand synthesis is needed at all).
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

    /// Whether `on_disk_hashes` -- every real pack advertisement currently
    /// published on disk for `series_hash` -- already forms a full exact
    /// cover of that series' leaf range on its own, with no help from the
    /// synthesized fallback.
    ///
    /// This is what [`Self::list_pack_hashes`] (finding 1: BLOCKING
    /// selection bug) uses to decide whether the synthesized initial pack
    /// needs to be offered as a candidate at all: a producer that has
    /// actually run `pond maintain --collapse-versions` and published a
    /// maintained pack set must have its real, physical hashes selected by
    /// [`sync_store::content::select_exact_cover`] -- never a synthesized
    /// stand-in -- whenever those real advertisements already exactly tile
    /// `[0, leaf_count)`. A merely *partial* on-disk cover must **not**
    /// suppress synthesis, so a stray/partial maintained pack can still be
    /// combined with the synthesized fallback to complete a cover.
    async fn on_disk_packs_form_full_cover(
        &self,
        series_hash: ObjectHash,
        on_disk_hashes: &std::collections::HashSet<ObjectHash>,
    ) -> Result<FullCoverStatus, StewardError> {
        let Some(material) = self
            .series_material
            .iter()
            .find(|m| m.series_hash == series_hash)
        else {
            // No captured native series material at all (e.g. a v1 series,
            // or a series this pond doesn't itself track): there is nothing
            // to synthesize regardless, so the on-disk set is used as-is.
            return Ok(FullCoverStatus::NoMaterial);
        };
        let total_leaf_count = material.manifest.leaf_count();
        if total_leaf_count == 0 {
            // No leaves at all: the trivial empty cover already holds, so
            // there is nothing a synthesized pack could ever add.
            return Ok(FullCoverStatus::Covered);
        }
        if on_disk_hashes.is_empty() {
            return Ok(FullCoverStatus::NotCovered);
        }
        let mut decoded: Vec<(ObjectHash, PackIndex)> = Vec::with_capacity(on_disk_hashes.len());
        for &pack_hash in on_disk_hashes {
            let Some((_, pack)) = self.read_advertised_pack(series_hash, pack_hash).await? else {
                // Vanished between the directory listing and this read
                // (e.g. concurrent prune): treat exactly like any other
                // absent candidate -- it simply cannot contribute to a
                // cover -- rather than failing this discovery call.
                continue;
            };
            decoded.push((pack_hash, pack));
        }
        match sync_store::content::select_exact_cover(series_hash, total_leaf_count, &decoded) {
            Ok(_) => Ok(FullCoverStatus::Covered),
            Err(_) => Ok(FullCoverStatus::NotCovered),
        }
    }

    /// Deterministically synthesize the initial whole-range pack for
    /// `series_hash` from [`Self::series_material`], the exact same
    /// construction [`crate::content_tree::publish_initial_series_packs`]
    /// uses at push time -- so an unpushed local pond and a freshly pushed
    /// remote advertise byte-identical pack bytes for the same series
    /// state. Returns `None` when no captured series matches `series_hash`
    /// or the matching series has no logical leaves yet (needs no cover).
    ///
    /// Pure and side-effect free: nothing is written to disk. Idempotent by
    /// construction, since [`crate::content_tree::build_initial_pack_index`]
    /// is a pure function of already-persisted rows.
    ///
    /// # Errors
    ///
    /// Returns an error if building the pack index from persisted rows
    /// fails (an internal-bug class error, not user error -- see
    /// [`crate::content_tree::build_initial_pack_index`]'s docs).
    fn synthesize_initial_pack(
        &self,
        series_hash: ObjectHash,
    ) -> Result<Option<(ObjectHash, Vec<u8>)>, StewardError> {
        let Some(material) = self
            .series_material
            .iter()
            .find(|m| m.series_hash == series_hash)
        else {
            return Ok(None);
        };
        let Some(pack) = crate::content_tree::build_initial_pack_index(material)? else {
            return Ok(None);
        };
        let bytes = pack.encode();
        let hash = ObjectHash::of_bytes(&bytes);
        Ok(Some((hash, bytes)))
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
        if self.external_blobs.contains(&hash) {
            return Ok(true);
        }
        // A maintenance-published physical pack object is not part of the
        // ordinary v1 blob closure captured at `open()` time -- it lives in
        // this pond's own `_packs/objects/` sidecar, written only by
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
            // No `_packs/series=<hex>` directory at all means no pack has
            // ever been published for this series -- true of every v1
            // series, and of any never-pushed v2 series -- which is not an
            // error: the on-demand synthesis below still answers it.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(StewardError::Content(format!(
                    "list pack advertisements under {}: {e}",
                    dir.display()
                )));
            }
        }
        // Only fall back to synthesizing the initial whole-range pack when
        // the real on-disk advertisements do not themselves already form a
        // full exact cover of this series' leaf range: a producer that has
        // actually published a maintained pack set (whole or partial) must
        // have its real, maintained physical hashes selected -- never a
        // synthesized stand-in -- so long as those real advertisements
        // already exactly tile [0, leaf_count). A merely *partial* cover
        // (e.g. one real pack covering only part of the range) is *not*
        // sufficient to suppress synthesis: the synthesized initial pack
        // must still be offered as a candidate so `select_exact_cover` can
        // combine it with the surviving real partials, exactly as it would
        // combine any other set of non-overlapping candidates.
        let needs_synthesis = match self
            .on_disk_packs_form_full_cover(series_hash, &out)
            .await?
        {
            FullCoverStatus::Covered => false,
            FullCoverStatus::NotCovered | FullCoverStatus::NoMaterial => true,
        };
        if needs_synthesis
            && let Some((pack_hash, _)) = self.synthesize_initial_pack(series_hash)?
        {
            let _ = out.insert(pack_hash);
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
        // No persisted advertisement on disk (true of every never-pushed
        // v2 series, and of any pack range the synthesized fallback alone
        // covers): fall back to synthesizing the exact same initial pack a
        // push would publish, directly from this clone's persisted rows,
        // without writing anything.
        match self.synthesize_initial_pack(series_hash)? {
            Some((synthesized_hash, bytes)) if synthesized_hash == pack_hash => Ok(Some(bytes)),
            _ => Ok(None),
        }
    }
}
