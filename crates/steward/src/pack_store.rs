// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Local, on-disk `_packs` namespace: the physical-pack-object sidecar and
//! series/pack advertisement directory a pond's own `pond maintain
//! --collapse-versions` writes into (`docs/logical-series-identity-design.md`,
//! pack-only physical maintenance).
//!
//! This is distinct from [`sync_store::ContentRemote`]'s pack advertisement
//! (an `object_store` remote, written only by `pond push`): this module is
//! the read/write side for the pond's OWN root directory, exactly the
//! `_packs/series=<hex>/pack=<hex>` layout
//! [`crate::content_source::LocalPondSource`] already reads from
//! (`sync_store::pack_keys`). Physical pack objects (the bounded, repacked
//! byte ranges / Parquet objects a pack index names) live in a second,
//! dedicated sidecar directory here -- `_packs/objects/<hex>` -- kept
//! deliberately separate from `_large_files/`: `tlogfs::large_files`'s own
//! sweep is keyed off Oplog-row `blake3` hashes and knows nothing about a
//! repacked physical object, so writing pack objects anywhere the ordinary
//! large-file sweep might reason about them risks that sweep deleting a
//! freshly-published pack object as an "orphan". Keeping this directory
//! wholly separate means the existing `_large_files` sweep is entirely
//! unaffected by pack maintenance, and this module owns its own orphan
//! sweep ([`sweep_unreferenced_pack_objects`]) against its own namespace.
//!
//! Every write here is content-addressed and idempotent (an existing file at
//! a content-addressed path is definitionally already the right bytes, so a
//! repeat call is a cheap no-op) and uses a temp-file-then-rename pattern so
//! a crash never leaves a partially-written file at its final path.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use sync_store::content::{ObjectHash, PackIndex};

use crate::{StewardError, get_data_path};

/// Directory holding every repacked physical pack object, content-addressed
/// by its own hash: `data/_packs/objects/<hex>`.
const PACK_OBJECTS_DIR: &str = "objects";

/// The `_packs` root under a pond's data directory.
#[must_use]
pub(crate) fn packs_root(pond_root: &Path) -> PathBuf {
    get_data_path(pond_root).join(sync_store::pack_keys::PACKS_ROOT)
}

/// The physical pack object sidecar directory: `data/_packs/objects`.
#[must_use]
pub(crate) fn pack_objects_dir(pond_root: &Path) -> PathBuf {
    packs_root(pond_root).join(PACK_OBJECTS_DIR)
}

/// One series' pack advertisement directory: `data/_packs/series=<hex>`.
#[must_use]
pub(crate) fn pack_series_dir(pond_root: &Path, series_hash: ObjectHash) -> PathBuf {
    packs_root(pond_root).join(sync_store::pack_keys::series_dir_name(series_hash))
}

/// One physical pack object's on-disk path: `data/_packs/objects/<hex>`.
#[must_use]
fn pack_object_path(pond_root: &Path, object_hash: ObjectHash) -> PathBuf {
    pack_objects_dir(pond_root).join(object_hash.to_hex())
}

/// One pack advertisement's on-disk path: `data/_packs/series=<hex>/pack=<hex>`.
#[must_use]
fn pack_index_path(pond_root: &Path, series_hash: ObjectHash, pack_hash: ObjectHash) -> PathBuf {
    pack_series_dir(pond_root, series_hash).join(sync_store::pack_keys::pack_file_name(pack_hash))
}

/// Write `bytes` to `final_path` atomically: a uniquely-named temp file in
/// the same directory, flushed and fsynced, then renamed into place. A
/// crash before the rename leaves only the harmless, never-referenced temp
/// file (never swept -- see [`sweep_unreferenced_pack_objects`]'s hex-name
/// filter); a crash after the rename leaves the complete file, never a
/// partial one, at `final_path`.
///
/// Durability does not stop at the renamed file's own bytes: `fsync`ing a
/// file guarantees its *data* (and, per POSIX, its own inode metadata) is
/// durable, but the *directory entry* a `rename` creates/replaces lives in a
/// separate durability domain -- the containing directory's own data -- and
/// is only guaranteed durable once that directory itself is `fsync`ed. A
/// crash between `rename` returning and this function's caller believing
/// the write "done" could otherwise, on some filesystems/journaling modes,
/// resurrect the old directory entry (or none at all) even though the new
/// file's bytes survived perfectly intact on disk. So every directory this
/// touches -- `parent` (for the rename below) and, before that, `parent`
/// and its own parent (in case `create_dir_all` just created a brand new
/// `series=<hex>` directory entry) -- is `fsync`ed too.
async fn write_atomic(final_path: &Path, bytes: &[u8]) -> Result<(), StewardError> {
    use tokio::io::AsyncWriteExt;

    let parent = final_path.parent().ok_or_else(|| {
        StewardError::Content(format!(
            "pack path {} has no parent directory",
            final_path.display()
        ))
    })?;
    tokio::fs::create_dir_all(parent).await?;
    // `create_dir_all` may have just created `parent` itself (a brand new
    // `series=<hex>` directory entry under `_packs/`, or `_packs/objects`
    // itself on a fresh pond): fsync it and its own parent unconditionally
    // (idempotent and cheap even when both already existed) so that new
    // directory's existence can never be lost to a crash independent of
    // whatever file is about to be written into it.
    fsync_dir(parent).await?;
    if let Some(grandparent) = parent.parent() {
        fsync_dir(grandparent).await?;
    }
    let tmp_path = parent.join(format!(
        ".tmp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    {
        let mut f = tokio::fs::File::create(&tmp_path).await?;
        f.write_all(bytes).await?;
        f.flush().await?;
        f.sync_all().await?;
    }
    tokio::fs::rename(&tmp_path, final_path).await?;
    // The rename's own directory-entry durability (see this function's own
    // doc comment above) -- fsync `parent` again, now that the entry it
    // names actually exists.
    fsync_dir(parent).await?;
    Ok(())
}

/// `fsync` a directory (not a file): durably publish every directory-entry
/// change (create, rename, remove) made under it so far. Cheap and
/// idempotent to call on a directory that has not changed at all.
///
/// # Errors
/// Returns an error if the directory cannot be opened or `fsync`ed.
async fn fsync_dir(dir: &Path) -> Result<(), StewardError> {
    let f = tokio::fs::File::open(dir).await?;
    f.sync_all().await?;
    Ok(())
}

/// Content-address and durably write one physical pack object, unless it is
/// already present at its content-addressed path (identical bytes always
/// live at an identical path, so an existing file need not be rewritten).
///
/// Returns `(hash, true)` when this call actually wrote the object, or
/// `(hash, false)` when it was already present.
pub(crate) async fn write_pack_object(
    pond_root: &Path,
    bytes: &[u8],
) -> Result<(ObjectHash, bool), StewardError> {
    let hash = ObjectHash::of_bytes(bytes);
    let path = pack_object_path(pond_root, hash);
    if tokio::fs::try_exists(&path).await? {
        return Ok((hash, false));
    }
    write_atomic(&path, bytes).await?;
    Ok((hash, true))
}

/// Open a streaming reader over a physical pack object's bytes, or `None`
/// if not present at `object_hash`'s content-addressed path.
pub(crate) async fn open_pack_object_reader(
    pond_root: &Path,
    object_hash: ObjectHash,
) -> Result<Option<tokio::fs::File>, StewardError> {
    let path = pack_object_path(pond_root, object_hash);
    match tokio::fs::File::open(&path).await {
        Ok(f) => Ok(Some(f)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StewardError::Io(e)),
    }
}

/// True if a physical pack object exists at `object_hash`'s content-addressed
/// path.
pub(crate) async fn has_pack_object(
    pond_root: &Path,
    object_hash: ObjectHash,
) -> Result<bool, StewardError> {
    let path = pack_object_path(pond_root, object_hash);
    Ok(tokio::fs::try_exists(&path).await?)
}

/// Every physical pack object hash currently durable under
/// `data/_packs/objects/` -- parsed from each entry's own hex filename,
/// exactly the content-addressed key [`write_pack_object`] wrote it under.
///
/// Used by [`crate::content_source::LocalPondSource`] to answer
/// `ContentSource::list_blobs`-style presence for a maintained pack's
/// physical objects, the same way it already answers presence for ordinary
/// `_large_files` external blobs -- so a `pond://` reader can fetch a
/// repacked series' physical objects, not only its pack index bytes.
///
/// A stray temp file from an interrupted [`write_atomic`] has no
/// hex-shaped name and is skipped here, exactly as
/// [`sweep_unreferenced_pack_objects`] already skips it.
///
/// # Errors
/// Returns an error if the objects directory exists but cannot be listed,
/// or if an entry's file type cannot be read.
pub(crate) async fn list_pack_object_hashes(
    pond_root: &Path,
) -> Result<HashSet<ObjectHash>, StewardError> {
    let dir = pack_objects_dir(pond_root);
    let mut out = HashSet::new();
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(StewardError::Io(e)),
    };
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Ok(hash) = ObjectHash::from_hex(name) {
            let _ = out.insert(hash);
        }
    }
    Ok(out)
}

/// Publish one pack advertisement at its own content-addressed key
/// (`data/_packs/series=<hex>/pack=<hex>`), unless it already exists (the
/// key is content-addressed, so an existing file is necessarily
/// byte-identical). Returns the pack's own hash.
///
/// # Errors
/// Returns an error if `index` declares a `series_hash` other than
/// `series_hash` (refusing to publish a cross-series advertisement under
/// the wrong directory), or if the write fails.
pub(crate) async fn publish_pack_index(
    pond_root: &Path,
    series_hash: ObjectHash,
    index: &PackIndex,
) -> Result<ObjectHash, StewardError> {
    if index.series_hash() != series_hash {
        return Err(StewardError::Content(format!(
            "refusing to publish pack under series={} for a pack index declaring series_hash={} \
             (cross-series index)",
            series_hash.to_hex(),
            index.series_hash().to_hex()
        )));
    }
    let bytes = index.encode();
    let hash = ObjectHash::of_bytes(&bytes);
    let path = pack_index_path(pond_root, series_hash, hash);
    if tokio::fs::try_exists(&path).await? {
        return Ok(hash);
    }
    write_atomic(&path, &bytes).await?;
    Ok(hash)
}

/// Read and strictly validate one pack advertisement by its series and own
/// content address: bytes must hash to `pack_hash`, decode as a
/// [`PackIndex`], and declare `series_hash` -- exactly the validation
/// [`crate::content_source::LocalPondSource::get_pack_index`] already
/// performs, factored out so maintenance's own discovery/GC can reuse it
/// without a second, possibly-divergent implementation.
///
/// # Errors
/// Returns an error if the file exists but fails any of those checks
/// (content-address mismatch, undecodable bytes, or cross-series
/// advertisement) -- never silently ignored, since a caller doing GC must
/// not treat a malformed advertisement as if it named no objects.
pub(crate) async fn read_and_verify_pack_index(
    pond_root: &Path,
    series_hash: ObjectHash,
    pack_hash: ObjectHash,
) -> Result<Option<PackIndex>, StewardError> {
    let path = pack_index_path(pond_root, series_hash, pack_hash);
    let bytes = match tokio::fs::read(&path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(StewardError::Io(e)),
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
            "pack advertisement {} declares series_hash {} but was found under series={} \
             (cross-series index)",
            path.display(),
            decoded.series_hash().to_hex(),
            series_hash.to_hex()
        )));
    }
    Ok(Some(decoded))
}

/// Every pack hash advertised under one series' directory, parsed from
/// `pack=<hex>` filenames.
///
/// Ignores exactly two kinds of non-advertisement entries, neither of which
/// can hide a malformed advertisement: a stray `.tmp-` temp file from an
/// interrupted [`write_atomic`] (never referenced by anything, never
/// swept as an object either), and this module's own versioned
/// maintenance-layout marker sidecars (`pack=<hex>.layout`, see
/// [`read_table_layout_marker`]) -- a marker names the *same* pack hash its
/// sibling `pack=<hex>` file already does, so skipping it here never
/// changes which pack hashes this reports, only avoids double-reporting
/// one hash under two filenames. Any other unrecognized entry (including
/// common OS filesystem metadata, deliberately *not* ignored inside a
/// pack-series directory specifically -- see
/// [`sync_store::pack_keys::is_ignorable_directory_entry`]'s callers
/// elsewhere for where that narrower allowance actually applies) still
/// fails loudly.
///
/// # Errors
/// Returns an error if the directory cannot be listed, or if it holds an
/// entry whose name does not parse as `pack=<hex>` (a foreign/corrupt file
/// under a pack-series directory is refused rather than silently skipped).
pub(crate) async fn list_local_pack_hashes(
    pond_root: &Path,
    series_hash: ObjectHash,
) -> Result<Vec<ObjectHash>, StewardError> {
    let dir = pack_series_dir(pond_root, series_hash);
    let mut out = Vec::new();
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(StewardError::Io(e)),
    };
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(StewardError::Content(format!(
                "non-utf8 pack advertisement filename under {}",
                dir.display()
            )));
        };
        // A stray temp file from an interrupted write, or a harmless piece
        // of OS filesystem metadata (e.g. `.DS_Store`), never matches
        // `pack=<hex>` and is not a pack advertisement at all -- ignore it
        // here exactly as the sweep below does, rather than failing
        // discovery/GC over an artifact of a crash mid-publish or of
        // browsing this directory in a file manager.
        if sync_store::pack_keys::is_ignorable_directory_entry(name) {
            continue;
        }
        // This pack's own layout-marker sidecar, if any: names the same
        // hash its `pack=<hex>` sibling already will, so skip it rather
        // than fail on the unexpected `.layout` suffix.
        if sync_store::pack_keys::parse_layout_marker_file_name(name)
            .map_err(|e| {
                StewardError::Content(format!(
                    "malformed layout marker {}/{name}: {e}",
                    dir.display()
                ))
            })?
            .is_some()
        {
            continue;
        }
        // This pack's stale-generation sentinel, if any (see
        // `retain_selected_pack_only`): also names the same hash its
        // `pack=<hex>` sibling already does, and a stale-but-not-yet-second-
        // generation advertisement is still a fully valid, selectable pack
        // -- skip only the sentinel filename itself, never the pack it
        // marks.
        if sync_store::pack_keys::parse_stale_marker_file_name(name)
            .map_err(|e| {
                StewardError::Content(format!(
                    "malformed stale marker {}/{name}: {e}",
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
        out.push(hash);
    }
    Ok(out)
}

/// Every series hash with at least one advertisement under `_packs/`, parsed
/// from `series=<hex>` directory names.
///
/// # Errors
/// Returns an error if the `_packs` root cannot be listed, or if it holds
/// an entry (other than [`PACK_OBJECTS_DIR`] or common, harmless OS
/// filesystem metadata such as `.DS_Store` -- see
/// [`sync_store::pack_keys::is_ignorable_directory_entry`]) whose name does
/// not parse as `series=<hex>`.
pub(crate) async fn list_local_series_hashes(
    pond_root: &Path,
) -> Result<Vec<ObjectHash>, StewardError> {
    let root = packs_root(pond_root);
    let mut out = Vec::new();
    let mut entries = match tokio::fs::read_dir(&root).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(StewardError::Io(e)),
    };
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(StewardError::Content(format!(
                "non-utf8 directory name under {}",
                root.display()
            )));
        };
        if !entry.file_type().await?.is_dir() {
            // A stray non-directory entry directly under `_packs/` is
            // either a harmless piece of OS filesystem metadata (ignored,
            // exactly as inside a pack-series directory), a whole series
            // directory's stale-generation sentinel (see
            // `prune_obsolete_series_dirs`, `series=<hex>.stale`), or
            // something suspicious; only the former two are silently
            // skipped.
            if sync_store::pack_keys::is_ignorable_directory_entry(name) {
                continue;
            }
            if sync_store::pack_keys::parse_stale_series_marker_file_name(name)
                .map_err(|e| {
                    StewardError::Content(format!(
                        "malformed stale series marker {}/{name}: {e}",
                        root.display()
                    ))
                })?
                .is_some()
            {
                continue;
            }
            return Err(StewardError::Content(format!(
                "unexpected non-directory entry under {}: {name:?}",
                root.display()
            )));
        }
        if name == PACK_OBJECTS_DIR {
            continue;
        }
        let hash = sync_store::pack_keys::parse_series_dir_name(name).map_err(|e| {
            StewardError::Content(format!(
                "malformed pack-series directory {}/{name}: {e}",
                root.display()
            ))
        })?;
        out.push(hash);
    }
    Ok(out)
}

/// Every valid, decoded local pack advertisement across the whole pond:
/// `(series_hash, pack_hash, index)`.
///
/// Fails loudly the moment any advertisement under
/// `_packs/series=*/pack=*` does not verify (content-address mismatch or
/// cross-series index) -- a caller doing GC must never guess that a
/// corrupt advertisement names no objects, since that could sweep an
/// object something else still depends on.
///
/// # Errors
/// Propagates [`list_local_series_hashes`]'s, [`list_local_pack_hashes`]'s,
/// and [`read_and_verify_pack_index`]'s errors.
pub(crate) async fn all_local_pack_indexes(
    pond_root: &Path,
) -> Result<Vec<(ObjectHash, ObjectHash, PackIndex)>, StewardError> {
    let mut out = Vec::new();
    for series_hash in list_local_series_hashes(pond_root).await? {
        for pack_hash in list_local_pack_hashes(pond_root, series_hash).await? {
            let Some(index) = read_and_verify_pack_index(pond_root, series_hash, pack_hash).await?
            else {
                continue;
            };
            out.push((series_hash, pack_hash, index));
        }
    }
    Ok(out)
}

/// One pack's versioned maintenance-layout marker sidecar: the exact
/// bounded-layout parameters `crate::pack_maintenance`'s table repack used
/// to produce it, kept deliberately outside the pack's own content-addressed
/// bytes/hash (`PackIndex` carries nothing about *how* it was laid out,
/// only the layout's result) so this can be consulted -- or safely ignored
/// by anything that predates it -- without ever perturbing a pack's
/// identity.
///
/// A repeated maintenance run compares a live series' current best
/// full-range pack's marker (if any) against these same constants to decide
/// settlement without re-decoding a single physical object: an exact match
/// means that pack was already produced by this exact bounded-layout
/// algorithm and is therefore already at its achievable floor, no matter
/// what a cheap `ceil(rows / cap)` estimate (necessarily blind to the
/// per-object byte safeguard's actual effect) would suggest. Changing
/// either cap constant, or the layout version, invalidates every existing
/// marker automatically (a mismatch simply falls back to a fresh repack
/// determination) -- never a stale false-positive settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TableLayoutMarker {
    pub(crate) layout_version: u32,
    pub(crate) row_cap: u64,
    pub(crate) byte_safeguard_cap: u64,
}

impl TableLayoutMarker {
    const ENCODED_LEN: usize = 4 + 8 + 8;

    fn encode(self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[0..4].copy_from_slice(&self.layout_version.to_le_bytes());
        out[4..12].copy_from_slice(&self.row_cap.to_le_bytes());
        out[12..20].copy_from_slice(&self.byte_safeguard_cap.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; Self::ENCODED_LEN] = bytes.try_into().ok()?;
        Some(Self {
            layout_version: u32::from_le_bytes(bytes[0..4].try_into().ok()?),
            row_cap: u64::from_le_bytes(bytes[4..12].try_into().ok()?),
            byte_safeguard_cap: u64::from_le_bytes(bytes[12..20].try_into().ok()?),
        })
    }
}

fn layout_marker_path(pond_root: &Path, series_hash: ObjectHash, pack_hash: ObjectHash) -> PathBuf {
    pack_series_dir(pond_root, series_hash)
        .join(sync_store::pack_keys::layout_marker_file_name(pack_hash))
}

/// One pack advertisement's stale-generation sentinel path (see
/// `sync_store::pack_keys`'s `STALE_MARKER_SUFFIX` doc):
/// `data/_packs/series=<hex>/pack=<hex>.stale`.
fn stale_pack_marker_path(
    pond_root: &Path,
    series_hash: ObjectHash,
    pack_hash: ObjectHash,
) -> PathBuf {
    pack_series_dir(pond_root, series_hash)
        .join(sync_store::pack_keys::stale_marker_file_name(pack_hash))
}

/// One whole series directory's stale-generation sentinel path, living
/// directly under `_packs/` beside the `series=<hex>` directory it marks:
/// `data/_packs/series=<hex>.stale`.
fn stale_series_marker_path(pond_root: &Path, series_hash: ObjectHash) -> PathBuf {
    packs_root(pond_root).join(sync_store::pack_keys::stale_series_marker_file_name(
        series_hash,
    ))
}

/// Durably write `marker` beside the already-published `pack_hash`
/// advertisement under `series_hash`'s directory. Content-addressed by the
/// pack it describes (the filename embeds `pack_hash`), so this is just as
/// idempotent as [`write_pack_object`]/[`publish_pack_index`] for a repeat
/// run producing byte-identical output.
///
/// # Errors
/// Returns an error if the durable write fails.
pub(crate) async fn write_table_layout_marker(
    pond_root: &Path,
    series_hash: ObjectHash,
    pack_hash: ObjectHash,
    marker: TableLayoutMarker,
) -> Result<(), StewardError> {
    let path = layout_marker_path(pond_root, series_hash, pack_hash);
    write_atomic(&path, &marker.encode()).await
}

/// Read and decode `pack_hash`'s layout marker under `series_hash`, or
/// `None` if absent or unreadable as one (an older pack this maintenance
/// code never wrote one for, or a marker from a since-changed encoding --
/// either way, safely treated as "no marker", never an error: the caller
/// falls back to a fresh bounded-layout determination).
pub(crate) async fn read_table_layout_marker(
    pond_root: &Path,
    series_hash: ObjectHash,
    pack_hash: ObjectHash,
) -> Option<TableLayoutMarker> {
    let path = layout_marker_path(pond_root, series_hash, pack_hash);
    let bytes = tokio::fs::read(&path).await.ok()?;
    TableLayoutMarker::decode(&bytes)
}

/// Delete every local pack advertisement, and its layout marker sidecar (if
/// any), under `series_hash`'s directory *except* `keep_pack_hash` -- but
/// only once each has survived one full maintenance generation as
/// superseded, then fsyncs the directory once done.
///
/// Used once a repack has published a fresh, deterministic full-range pack
/// for a still-live series: any other advertisement left over in that same
/// directory (from a superseded layout, an alternate cover, or a stale
/// partial-range pack) no longer represents the series' selected physical
/// layout and would otherwise accumulate forever across repeated
/// append-then-maintain cycles.
///
/// **Concurrent-reader-availability grace period** (`docs/logical-series-identity-design.md`):
/// a `pond://` reader ([`crate::content_source::LocalPondSource`]) may have
/// already listed `series_hash`'s advertisements -- including one this call
/// is about to find no longer selected -- moments before this maintenance
/// run started, and may still be mid-fetch of it. Deleting it out from
/// under that fetch immediately would be a real (if narrow) race. Instead:
/// the *first* time a still-valid, non-selected advertisement is seen here,
/// it is only marked stale (a `pack=<hex>.stale` sentinel written beside
/// it, never touching the advertisement or its object bytes); only on a
/// *later* call that finds it *already* marked stale (i.e. it survived at
/// least one full maintenance generation as superseded, so any reader that
/// listed it before the marking run has had a whole generation to finish)
/// is it actually deleted. This bounds growth to at most one extra
/// generation's worth of superseded advertisements per series -- never
/// unbounded -- while giving every in-flight fetch a full cycle to
/// complete. Any reader unlucky enough to still be mid-fetch after that
/// (a fetch spanning two entire maintenance cycles) must retry: see
/// [`crate::content_source::LocalPondSource`]'s own doc comment for this
/// residual retry semantics note.
///
/// Deletion order, once an advertisement is actually removed, is the
/// reverse of publication: the layout marker sidecar (published *after*
/// the index it describes) is deleted *first*, then the pack index itself
/// -- so a crash between the two deletes can only ever leave an orphaned
/// marker with no corresponding index (harmless: [`list_local_pack_hashes`]
/// and [`read_table_layout_marker`]'s callers only ever look up a marker
/// keyed off a pack hash an index listing already produced, so a marker
/// with no sibling index is simply never looked up), never an orphaned
/// index missing its marker (which would wrongly look like a table pack
/// that has never had its bounded layout determined).
///
/// Every advertisement considered here is independently validated
/// ([`read_and_verify_pack_index`]) before this decides whether it is the
/// one to keep -- so a malformed entry fails this call loudly rather than
/// being silently swept or silently kept.
///
/// # Errors
/// Returns an error if the directory cannot be listed, if any advertisement
/// in it fails validation, or if a delete fails.
pub(crate) async fn retain_selected_pack_only(
    pond_root: &Path,
    series_hash: ObjectHash,
    keep_pack_hash: ObjectHash,
) -> Result<RetentionStats, StewardError> {
    let dir = pack_series_dir(pond_root, series_hash);
    let mut stats = RetentionStats::default();
    for pack_hash in list_local_pack_hashes(pond_root, series_hash).await? {
        if pack_hash == keep_pack_hash {
            // Currently selected: clear any stale marking left over from a
            // pack hash that was superseded and then, unusually, became
            // selected again (e.g. content reverted to a byte-identical
            // prior layout) -- it must get a fresh grace period if it is
            // ever superseded again, not be deleted on sight.
            let stale_path = stale_pack_marker_path(pond_root, series_hash, pack_hash);
            if tokio::fs::try_exists(&stale_path).await? {
                tokio::fs::remove_file(&stale_path).await?;
            }
            continue;
        }
        // Validate before doing anything: a malformed advertisement must
        // fail this outright, never be silently marked stale or swept as
        // if it were merely superseded.
        let _ = read_and_verify_pack_index(pond_root, series_hash, pack_hash)
            .await?
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "pack advertisement {pack_hash} listed under series={series_hash} vanished \
                     mid-prune"
                ))
            })?;
        let stale_path = stale_pack_marker_path(pond_root, series_hash, pack_hash);
        if tokio::fs::try_exists(&stale_path).await? {
            // Already superseded as of a previous maintenance run: this
            // generation's worth of grace period has elapsed, safe to
            // actually delete now. Marker first, then index (see this
            // function's own doc comment).
            let marker_path = layout_marker_path(pond_root, series_hash, pack_hash);
            if tokio::fs::try_exists(&marker_path).await? {
                tokio::fs::remove_file(&marker_path).await?;
            }
            tokio::fs::remove_file(&stale_path).await?;
            let path = pack_index_path(pond_root, series_hash, pack_hash);
            tokio::fs::remove_file(&path).await?;
            stats.removed += 1;
        } else {
            // First generation as non-selected: only mark it, so any
            // reader that already listed it can still fetch it this cycle.
            write_atomic(&stale_path, &[]).await?;
            stats.marked_stale += 1;
        }
    }
    stats.orphan_markers_removed += sweep_orphan_layout_markers(pond_root, series_hash).await?;
    fsync_dir(&dir).await?;
    Ok(stats)
}

/// Delete any layout marker sidecar under `series_hash`'s directory whose
/// `pack=<hex>` advertisement it names no longer exists -- a defensive
/// cleanup for an orphan a crash (or, before finding 5's fix, this same
/// function's own now-corrected index-before-marker deletion order) could
/// have left behind: an index-then-marker deletion order can leave the
/// marker behind if a crash lands between the two deletes, and unlike a
/// dangling index (this repository's crash-safety rule always publishes a
/// marker only *after* its index, so a marker can never legitimately
/// outlive its index) an orphan marker is always safe to remove outright,
/// never something another caller could still depend on -- see
/// [`list_local_pack_hashes`]'s doc comment for why it is already ignored
/// by ordinary discovery, and [`crate::pack_maintenance::table_pack_is_current_deterministic_layout`]
/// for why nothing looks a marker up except by a pack hash an index
/// listing has already produced.
///
/// # Errors
/// Returns an error if the directory cannot be listed or a delete fails.
async fn sweep_orphan_layout_markers(
    pond_root: &Path,
    series_hash: ObjectHash,
) -> Result<usize, StewardError> {
    let dir = pack_series_dir(pond_root, series_hash);
    let mut removed = 0usize;
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(StewardError::Io(e)),
    };
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(pack_hash) =
            sync_store::pack_keys::parse_layout_marker_file_name(name).unwrap_or(None)
        else {
            continue;
        };
        let index_path = pack_index_path(pond_root, series_hash, pack_hash);
        if !tokio::fs::try_exists(&index_path).await? {
            tokio::fs::remove_file(entry.path()).await?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// What one [`retain_selected_pack_only`] (or
/// [`prune_obsolete_series_dirs`]) pass did: how many superseded
/// advertisements/directories were newly marked stale (deferred one more
/// generation) versus how many already-stale ones were actually removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetentionStats {
    pub(crate) marked_stale: usize,
    pub(crate) removed: usize,
    /// Orphan layout marker sidecars removed (their pack index was already
    /// gone) -- always 0 for [`prune_obsolete_series_dirs`], which never
    /// leaves an orphan marker behind since it removes a whole directory
    /// at once.
    pub(crate) orphan_markers_removed: usize,
}

/// Delete every `_packs/series=<hex>` directory whose `<hex>` is not in
/// `live_series_hashes` -- the other half of pack-backed local maintenance
/// alongside [`sweep_unreferenced_pack_objects`] -- but only once each has
/// survived one full maintenance generation as no-longer-live.
///
/// A `watertown.series.v1` series hash is the fold of *all* of a series' current
/// live leaf hashes, so any append at all mints a brand new series hash;
/// without this, every append-then-repack cycle would leave the previous
/// cycle's now-superseded `series=<oldhash>` directory (and its
/// advertisement) behind forever, growing this pond's local `_packs/`
/// namespace without bound even though nothing ever reads through an old
/// series hash again (`crate::content_source::LocalPondSource` only ever
/// serves the *current* live tip's manifest, hence only ever asks about a
/// live series' *current* hash).
///
/// `live_series_hashes` must already be every native v2 series' current
/// manifest hash across the *whole* pond (not merely this run's
/// over-threshold candidates -- see
/// `crate::pack_maintenance::all_live_v2_series_hashes`), so a series that
/// legitimately still exists but happened not to need a repack this run is
/// never mistaken for obsolete.
///
/// **Concurrent-reader-availability grace period**, exactly the same
/// one-generation deferral [`retain_selected_pack_only`] gives individual
/// superseded pack advertisements: a reader may have resolved a now-old
/// manifest to this series hash and be mid-fetch of it when this series
/// stops being live. The first time a series hash is found no longer live,
/// this only writes a `series=<hex>.stale` sentinel *beside* (not inside)
/// its directory, leaving the directory and everything under it untouched;
/// only a *later* call that finds the sentinel already present (the
/// directory survived a whole generation as non-live) actually removes the
/// directory. If the series hash becomes live again before that (e.g. a
/// revert), the sentinel is cleared and the directory keeps its full
/// grace period should it become non-live again later.
///
/// Every candidate-for-removal directory's advertisements are independently
/// validated (content-address and cross-series checked, via
/// [`read_and_verify_pack_index`]) before anything is deleted: a malformed
/// advertisement under a would-be-pruned directory fails this call outright
/// rather than being silently swept, since a corrupt local index is exactly
/// the situation where guessing "it's fine to delete" could destroy
/// something a caller elsewhere still depends on.
///
/// # Errors
/// Returns an error if directory listing/validation fails, or if a
/// directory cannot be removed.
pub(crate) async fn prune_obsolete_series_dirs(
    pond_root: &Path,
    live_series_hashes: &HashSet<ObjectHash>,
) -> Result<RetentionStats, StewardError> {
    let mut stats = RetentionStats::default();
    let root = packs_root(pond_root);
    for series_hash in list_local_series_hashes(pond_root).await? {
        let stale_path = stale_series_marker_path(pond_root, series_hash);
        if live_series_hashes.contains(&series_hash) {
            // Live again (or still live): clear any leftover stale marking
            // so a future non-live episode gets a fresh grace period.
            if tokio::fs::try_exists(&stale_path).await? {
                tokio::fs::remove_file(&stale_path).await?;
                fsync_dir(&root).await?;
            }
            continue;
        }
        // Fail-safe on malformed state: every advertisement under a
        // to-be-pruned directory must independently validate before this
        // proceeds to mark or delete the directory that holds it.
        for pack_hash in list_local_pack_hashes(pond_root, series_hash).await? {
            let _ = read_and_verify_pack_index(pond_root, series_hash, pack_hash)
                .await?
                .ok_or_else(|| {
                    StewardError::Content(format!(
                        "pack advertisement {pack_hash} listed under series={series_hash} \
                         vanished mid-prune"
                    ))
                })?;
        }
        if tokio::fs::try_exists(&stale_path).await? {
            // Already non-live as of a previous maintenance run: safe to
            // actually remove now.
            let dir = pack_series_dir(pond_root, series_hash);
            tokio::fs::remove_dir_all(&dir).await?;
            tokio::fs::remove_file(&stale_path).await?;
            fsync_dir(&root).await?;
            stats.removed += 1;
        } else {
            write_atomic(&stale_path, &[]).await?;
            stats.marked_stale += 1;
        }
    }
    Ok(stats)
}

/// What one [`sweep_unreferenced_pack_objects`] pass freed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PackObjectSweepStats {
    pub(crate) removed: usize,
    pub(crate) bytes_freed: u64,
}

/// Delete every physical pack object under `data/_packs/objects/` whose
/// hash is not in `referenced` -- the GC half of pack-backed local
/// maintenance.
///
/// `referenced` must already include every physical object hash named by
/// every retained, valid local pack advertisement (see
/// [`all_local_pack_indexes`]); this function only sweeps what is not in
/// that set, it never decides what belongs in it.
///
/// # Errors
/// Returns an error if the objects directory cannot be listed, or if an
/// entry's metadata cannot be read or it cannot be deleted.
pub(crate) async fn sweep_unreferenced_pack_objects(
    pond_root: &Path,
    referenced: &HashSet<ObjectHash>,
) -> Result<PackObjectSweepStats, StewardError> {
    let dir = pack_objects_dir(pond_root);
    let mut stats = PackObjectSweepStats::default();
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(stats),
        Err(e) => return Err(StewardError::Io(e)),
    };
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // A stray temp file from an interrupted write has no hash-shaped
        // name and, by construction, is never named by any pack index
        // (which only ever names a successfully-renamed final path); skip
        // it here rather than reject the whole sweep over a crash artifact.
        let Ok(hash) = ObjectHash::from_hex(name) else {
            continue;
        };
        if referenced.contains(&hash) {
            continue;
        }
        let len = entry.metadata().await?.len();
        tokio::fs::remove_file(entry.path()).await?;
        stats.removed += 1;
        stats.bytes_freed += len;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_and_read_pack_object_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bytes = b"hello pack object".to_vec();
        let (hash, wrote) = write_pack_object(dir.path(), &bytes).await.expect("write");
        assert!(wrote);
        assert!(has_pack_object(dir.path(), hash).await.expect("has"));

        // Idempotent: writing the identical bytes again is a no-op.
        let (hash2, wrote2) = write_pack_object(dir.path(), &bytes)
            .await
            .expect("write again");
        assert_eq!(hash, hash2);
        assert!(!wrote2);

        let mut reader = open_pack_object_reader(dir.path(), hash)
            .await
            .expect("open")
            .expect("present");
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let _ = reader.read_to_end(&mut buf).await.expect("read");
        assert_eq!(buf, bytes);
    }

    #[tokio::test]
    async fn sweep_removes_only_unreferenced_objects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (keep_hash, _) = write_pack_object(dir.path(), b"keep me")
            .await
            .expect("write");
        let (drop_hash, _) = write_pack_object(dir.path(), b"drop me")
            .await
            .expect("write");

        let mut referenced = HashSet::new();
        let _ = referenced.insert(keep_hash);
        let stats = sweep_unreferenced_pack_objects(dir.path(), &referenced)
            .await
            .expect("sweep");
        assert_eq!(stats.removed, 1);

        assert!(has_pack_object(dir.path(), keep_hash).await.expect("has"));
        assert!(!has_pack_object(dir.path(), drop_hash).await.expect("has"));
    }

    #[tokio::test]
    async fn read_and_verify_pack_index_rejects_content_address_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let series_hash = ObjectHash::of_bytes(b"series");
        let pack_hash = ObjectHash::of_bytes(b"pack");
        let path = pack_index_path(dir.path(), series_hash, pack_hash);
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .expect("mkdir");
        tokio::fs::write(&path, b"not the right bytes")
            .await
            .expect("write");
        let err = read_and_verify_pack_index(dir.path(), series_hash, pack_hash)
            .await
            .expect_err("must fail loudly on content-address mismatch");
        assert!(matches!(err, StewardError::Content(_)));
    }

    /// Build a trivial, self-consistent single-leaf whole-range pack index
    /// for `series_hash`, naming one physical object `object_hash` -- enough
    /// to exercise directory-level plumbing (publish/list/prune/retain) that
    /// does not care about a pack's actual payload semantics.
    fn trivial_pack_index(series_hash: ObjectHash, object_hash: ObjectHash) -> PackIndex {
        use sync_store::content::{PackLeafDescriptor, generate_range_proof, merkle_root};
        let leaf_hash = ObjectHash::of_bytes(b"one-leaf");
        let root = merkle_root(&[leaf_hash]);
        let proof = generate_range_proof(&[leaf_hash], 0, 1).expect("range proof");
        let descriptor = PackLeafDescriptor::new(1, None, None, None).expect("descriptor");
        PackIndex::new(
            series_hash,
            0,
            1,
            1,
            root,
            proof,
            vec![object_hash],
            1,
            1,
            vec![descriptor],
        )
        .expect("pack index")
    }

    #[tokio::test]
    async fn list_local_pack_hashes_ignores_layout_markers_and_os_metadata_but_not_junk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let series_hash = ObjectHash::of_bytes(b"series");
        let object_hash = ObjectHash::of_bytes(b"object");
        let index = trivial_pack_index(series_hash, object_hash);
        let pack_hash = publish_pack_index(dir.path(), series_hash, &index)
            .await
            .expect("publish");
        write_table_layout_marker(
            dir.path(),
            series_hash,
            pack_hash,
            TableLayoutMarker {
                layout_version: 1,
                row_cap: 100_000,
                byte_safeguard_cap: 8 * 1024 * 1024,
            },
        )
        .await
        .expect("write marker");
        let series_dir = pack_series_dir(dir.path(), series_hash);
        tokio::fs::write(series_dir.join(".DS_Store"), b"finder junk")
            .await
            .expect("write ds_store");
        tokio::fs::write(
            series_dir.join(sync_store::pack_keys::stale_marker_file_name(pack_hash)),
            b"",
        )
        .await
        .expect("write stale sentinel");

        let hashes = list_local_pack_hashes(dir.path(), series_hash)
            .await
            .expect("list");
        assert_eq!(
            hashes,
            vec![pack_hash],
            "only the real advertisement counts, whether or not it also has a stale sentinel"
        );

        tokio::fs::write(series_dir.join("suspicious-file"), b"???")
            .await
            .expect("write junk");
        let err = list_local_pack_hashes(dir.path(), series_hash)
            .await
            .expect_err("an unrecognized entry must fail loudly, not be silently ignored");
        assert!(matches!(err, StewardError::Content(_)));
    }

    #[tokio::test]
    async fn table_layout_marker_round_trips_and_absent_is_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let series_hash = ObjectHash::of_bytes(b"series");
        let pack_hash = ObjectHash::of_bytes(b"pack");
        assert!(
            read_table_layout_marker(dir.path(), series_hash, pack_hash)
                .await
                .is_none()
        );
        let marker = TableLayoutMarker {
            layout_version: 3,
            row_cap: 42,
            byte_safeguard_cap: 999,
        };
        write_table_layout_marker(dir.path(), series_hash, pack_hash, marker)
            .await
            .expect("write marker");
        let read_back = read_table_layout_marker(dir.path(), series_hash, pack_hash)
            .await
            .expect("marker present");
        assert_eq!(read_back, marker);
    }

    #[tokio::test]
    async fn retain_selected_pack_only_defers_deletion_one_generation_then_removes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let series_hash = ObjectHash::of_bytes(b"series");
        let object_a = ObjectHash::of_bytes(b"object-a");
        let object_b = ObjectHash::of_bytes(b"object-b");

        // Two distinct full-range pack indexes for the same series (a
        // stand-in for "a superseded layout left behind"): distinguish them
        // by naming different physical objects.
        let index_a = trivial_pack_index(series_hash, object_a);
        let index_b = trivial_pack_index(series_hash, object_b);
        let pack_a = publish_pack_index(dir.path(), series_hash, &index_a)
            .await
            .expect("publish a");
        let pack_b = publish_pack_index(dir.path(), series_hash, &index_b)
            .await
            .expect("publish b");
        assert_ne!(pack_a, pack_b);
        write_table_layout_marker(
            dir.path(),
            series_hash,
            pack_a,
            TableLayoutMarker {
                layout_version: 1,
                row_cap: 1,
                byte_safeguard_cap: 1,
            },
        )
        .await
        .expect("marker a");

        // First maintenance generation: `pack_a` is no longer selected, but
        // a reader that already listed it moments ago must still be able
        // to fetch it -- so this call only marks it stale, it must not yet
        // be deleted (finding 4's concurrent-reader grace period).
        let first = retain_selected_pack_only(dir.path(), series_hash, pack_b)
            .await
            .expect("retain b only (first generation)");
        assert_eq!(first.marked_stale, 1);
        assert_eq!(first.removed, 0);
        let mut remaining = list_local_pack_hashes(dir.path(), series_hash)
            .await
            .expect("list after first generation");
        remaining.sort();
        let mut expected = vec![pack_a, pack_b];
        expected.sort();
        assert_eq!(
            remaining, expected,
            "the superseded pack must still be listed (and fetchable) after one generation"
        );
        assert!(
            read_table_layout_marker(dir.path(), series_hash, pack_a)
                .await
                .is_some(),
            "the superseded pack's marker must survive its first stale generation"
        );

        // Second maintenance generation: `pack_a` is still not selected and
        // was already marked stale by the call above, so this is safe to
        // actually delete now.
        let second = retain_selected_pack_only(dir.path(), series_hash, pack_b)
            .await
            .expect("retain b only (second generation)");
        assert_eq!(second.marked_stale, 0);
        assert_eq!(second.removed, 1);
        let remaining = list_local_pack_hashes(dir.path(), series_hash)
            .await
            .expect("list after second generation");
        assert_eq!(remaining, vec![pack_b]);
        assert!(
            read_table_layout_marker(dir.path(), series_hash, pack_a)
                .await
                .is_none(),
            "the superseded pack's marker must be removed along with it"
        );
    }

    #[tokio::test]
    async fn retain_selected_pack_only_cleans_up_an_orphan_layout_marker() {
        // A layout marker with no corresponding pack index left over on
        // disk (e.g. from a crash between the two deletes, or predating
        // finding 5's marker-before-index deletion order): safe to remove
        // outright, since a marker can never legitimately outlive the
        // index it describes.
        let dir = tempfile::tempdir().expect("tempdir");
        let series_hash = ObjectHash::of_bytes(b"series");
        let object_hash = ObjectHash::of_bytes(b"object");
        let index = trivial_pack_index(series_hash, object_hash);
        let pack_hash = publish_pack_index(dir.path(), series_hash, &index)
            .await
            .expect("publish");
        write_table_layout_marker(
            dir.path(),
            series_hash,
            pack_hash,
            TableLayoutMarker {
                layout_version: 1,
                row_cap: 1,
                byte_safeguard_cap: 1,
            },
        )
        .await
        .expect("write marker");

        // Simulate the orphan directly: delete only the index, leaving the
        // marker behind.
        tokio::fs::remove_file(pack_index_path(dir.path(), series_hash, pack_hash))
            .await
            .expect("remove index, simulating a crash between marker/index deletes");
        assert!(
            read_table_layout_marker(dir.path(), series_hash, pack_hash)
                .await
                .is_some(),
            "orphan marker present before cleanup"
        );

        // A different, still-live pack to keep, so `retain_selected_pack_only`
        // has something to run against this series' directory at all.
        let other_object = ObjectHash::of_bytes(b"object-2");
        let keep_pack = publish_pack_index(
            dir.path(),
            series_hash,
            &trivial_pack_index(series_hash, other_object),
        )
        .await
        .expect("publish keeper");

        let stats = retain_selected_pack_only(dir.path(), series_hash, keep_pack)
            .await
            .expect("retain");
        assert_eq!(stats.orphan_markers_removed, 1);
        assert!(
            read_table_layout_marker(dir.path(), series_hash, pack_hash)
                .await
                .is_none(),
            "the orphan marker must be cleaned up"
        );
    }

    #[tokio::test]
    async fn prune_obsolete_series_dirs_defers_removal_one_generation_then_removes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let live_series = ObjectHash::of_bytes(b"live-series");
        let stale_series = ObjectHash::of_bytes(b"stale-series");
        let object_hash = ObjectHash::of_bytes(b"object");

        let _ = publish_pack_index(
            dir.path(),
            live_series,
            &trivial_pack_index(live_series, object_hash),
        )
        .await
        .expect("publish live");
        let _ = publish_pack_index(
            dir.path(),
            stale_series,
            &trivial_pack_index(stale_series, object_hash),
        )
        .await
        .expect("publish stale");

        let mut live = HashSet::new();
        let _ = live.insert(live_series);

        // First generation: only marked, directory (and any concurrent
        // reader's in-flight fetch of it) survives.
        let first = prune_obsolete_series_dirs(dir.path(), &live)
            .await
            .expect("prune (first generation)");
        assert_eq!(first.marked_stale, 1);
        assert_eq!(first.removed, 0);
        let mut remaining = list_local_series_hashes(dir.path()).await.expect("list");
        remaining.sort();
        let mut expected = vec![live_series, stale_series];
        expected.sort();
        assert_eq!(
            remaining, expected,
            "the no-longer-live series directory must survive its first stale generation"
        );

        // Second generation: still not live, and already marked stale --
        // safe to actually remove now.
        let second = prune_obsolete_series_dirs(dir.path(), &live)
            .await
            .expect("prune (second generation)");
        assert_eq!(second.marked_stale, 0);
        assert_eq!(second.removed, 1);
        let remaining = list_local_series_hashes(dir.path()).await.expect("list");
        assert_eq!(remaining, vec![live_series]);
    }

    #[tokio::test]
    async fn prune_obsolete_series_dirs_fails_loudly_on_a_malformed_stale_advertisement() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stale_series = ObjectHash::of_bytes(b"stale-series");
        let pack_hash = ObjectHash::of_bytes(b"pack");
        let path = pack_index_path(dir.path(), stale_series, pack_hash);
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .expect("mkdir");
        tokio::fs::write(&path, b"not the right bytes")
            .await
            .expect("write corrupt advertisement");

        let live: HashSet<ObjectHash> = HashSet::new();
        let err = prune_obsolete_series_dirs(dir.path(), &live)
            .await
            .expect_err("a malformed advertisement under a stale series must fail the prune");
        assert!(matches!(err, StewardError::Content(_)));
        // Fail-safe: the directory must still exist, never half-deleted.
        assert!(
            tokio::fs::try_exists(pack_series_dir(dir.path(), stale_series))
                .await
                .expect("check existence")
        );
    }
}
