// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Generic staged import: materialize a downloaded `pondcapsule.1` capsule
//! into a brand-new pond (`docs/recovery-capsule-design.md`, "Generic staged
//! import").
//!
//! [`import_capsule`] implements the coherent core of the twelve-step design:
//!
//! 1. the target must not exist; a private sibling staging directory is
//!    created next to it;
//! 2. a fresh pond identity is minted at the staging path;
//! 3. immutable provenance (source pond, source tip, capsule root, importer
//!    version) is written alongside the staged pond's `data/`/`control/`, not
//!    inside its tinyfs namespace;
//! 4. every write transaction used while staging suppresses post-commit
//!    factory execution and remote auto-push
//!    ([`crate::Ship::begin_write_suppressed`]), so a capsule that carries
//!    `/system/run/*` configs or `/sys/remotes/*` attachments cannot dispatch
//!    before the operator seals and unsuppresses the target;
//! 5. entries are recreated in the capsule's canonical path order, which is
//!    already parent-before-child (a parent path is always a strict string
//!    prefix of its children's paths, and prefixes sort first);
//! 6. file bytes and decoded table rows are streamed leaf-by-leaf rather than
//!    buffered whole;
//! 7. series leaves are recreated in original order carrying their
//!    `source_timestamp`/event-time bounds/logical attributes as
//!    [`sync_store::VersionMeta`], reusing the same version-finalization
//!    helper the content-addressed puller uses
//!    ([`crate::content_pull::finalize_writer`]);
//! 9. the staged pond is re-read from scratch with the existing capsule
//!    builder ([`crate::build_recovery_capsule`]) and compared against the
//!    source manifest by logical projection (paths, entry types, payload
//!    kind, schema fingerprint, and ordered leaves -- deliberately ignoring
//!    `source_node_id` and physical object boundaries, which the design
//!    explicitly allows to change); and
//!
//! Finally, the staging directory is renamed atomically onto the target only
//! after that comparison succeeds (design steps 11-12).
//!
//! Not yet implemented (see module-level `# Remaining` docs below and
//! `docs/recovery-capsule-design.md`): bounded/resumable per-batch
//! transactions with content-addressed journal checkpoints (design steps 6/8
//! describe finer-grained batching than the single write transaction used
//! here), and active-remote resolution/preflight before sealing (design step
//! 10). A capsule whose entries include `/sys/remotes/*` attachments is
//! imported as inert namespace content (dispatch suppressed per step 4), but
//! this module does not yet validate or reject an unsafe restored
//! destination -- that is left to the operator until the preflight lands.
//!
//! # Errors
//!
//! On any failure after the staging directory is created, the staging
//! directory is deliberately left in place (never silently removed) so the
//! operator can inspect it, resume by hand, or delete it once the capsule and
//! source have been re-checked. The error message always names the staging
//! path.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use sync_store::{
    CapsuleEntry, CapsuleLeaf, CapsuleManifest, CapsuleNode, CapsuleObject, CapsulePayloadKind,
    ObjectHash, VersionMeta, canonicalize_schema, decode_recipe, read_capsule_manifest,
    verify_capsule_payload_directory,
};
use tinyfs::{EntryType, WD};
use tokio::io::AsyncWriteExt;

use crate::content_pull::finalize_writer;
use crate::control_table::{POST_COMMIT_DISPATCH_SETTING, POST_COMMIT_DISPATCH_SUPPRESSED};
use crate::{PondUserMetadata, Ship, StewardError};

/// Provenance filename written at the pond directory's top level (a sibling
/// of `data/` and `control/`), so it survives the atomic rename to the
/// target without becoming a tinyfs namespace entry that the post-write
/// logical comparison would have to special-case.
const IMPORT_PROVENANCE_FILE: &str = "CAPSULE_IMPORT_PROVENANCE.json";
const IMPORT_PROVENANCE_FORMAT: &str = "pondcapsule.1-import-provenance.1";

/// Immutable provenance recorded for a staged import (design step 3).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapsuleImportProvenance {
    /// Always [`IMPORT_PROVENANCE_FORMAT`].
    pub format: String,
    /// Source pond identity named by the capsule.
    pub source_pond_id: String,
    /// Source pond birthplace label named by the capsule.
    pub source_birthplace: String,
    /// Source content tip the capsule was exported from, lowercase hex.
    pub source_tip: String,
    /// Capsule manifest root that was imported, lowercase hex.
    pub capsule_root: String,
    /// Exact importer version that performed the import.
    pub importer_version: String,
    /// Import time in microseconds since the Unix epoch.
    pub imported_at_micros: i64,
}

/// Outcome of a successful [`import_capsule`] run.
#[derive(Debug, Clone)]
pub struct CapsuleImportReport {
    /// Target pond path the staged directory was renamed to.
    pub target: PathBuf,
    /// Fresh pond identity minted for the target.
    pub target_pond_id: String,
    /// Source pond identity recorded in the capsule.
    pub source_pond_id: String,
    /// Source content tip the capsule was exported from.
    pub source_tip: ObjectHash,
    /// Capsule manifest root that was imported.
    pub capsule_root: ObjectHash,
    /// Number of live namespace entries recreated.
    pub entries: usize,
    /// Number of directories recreated.
    pub directories: usize,
    /// Number of physical files/tables recreated (single-version or series).
    pub physical: usize,
    /// Number of symlinks recreated.
    pub symlinks: usize,
    /// Number of dynamic-node recipes recreated.
    pub dynamic: usize,
    /// Sum of logical bytes and rows across every leaf, as declared by the
    /// source capsule.
    pub logical_count: u64,
}

/// Materialize a downloaded `pondcapsule.1` capsule at `capsule_dir` into a
/// brand-new pond at `target`.
///
/// `capsule_dir` is the directory containing the capsule's `recovery/` tree
/// (as documented by `docs/recovery-capsule-design.md`, "Downloaded capsule
/// layout"); it is only ever opened for reading. `target` must not already
/// exist; `birthplace` is the immutable label recorded for the freshly
/// minted target pond identity (see [`Ship::create_pond`]).
///
/// # Errors
///
/// Returns an error if `target` already exists, its parent directory is
/// missing, the source capsule fails its logical/physical verification, any
/// write into the staged pond fails, or the staged pond does not match the
/// capsule's logical contract byte-for-byte on re-read. In every failure case
/// after staging begins, the staging directory is left on disk (never
/// silently removed) and named in the error.
pub async fn import_capsule(
    capsule_dir: &Path,
    target: &Path,
    birthplace: impl Into<String>,
) -> Result<CapsuleImportReport, StewardError> {
    if target.symlink_metadata().is_ok() {
        return Err(StewardError::Content(format!(
            "capsule import target {} already exists; import only bootstraps a fresh pond",
            target.display()
        )));
    }
    let parent = target.parent().filter(|p| !p.as_os_str().is_empty());
    let Some(parent) = parent else {
        return Err(StewardError::Content(format!(
            "capsule import target {} has no parent directory to stage a private sibling in",
            target.display()
        )));
    };
    if !parent.is_dir() {
        return Err(StewardError::Content(format!(
            "capsule import target {}'s parent directory {} does not exist",
            target.display(),
            parent.display()
        )));
    }

    // Everything below only opens files under `capsule_dir` for reading: the
    // source capsule is never written to.
    let (manifest, capsule_root_hash) = read_capsule_manifest(capsule_dir)
        .map_err(|error| StewardError::Content(format!("read source capsule manifest: {error}")))?;
    let objects_dir = capsule_dir.join("recovery").join("objects");
    let verify_report = verify_capsule_payload_directory(&manifest, &objects_dir)
        .map_err(|error| StewardError::Content(format!("verify source capsule: {error}")))?;

    let staging = pick_staging_path(target, parent)?;
    if staging.symlink_metadata().is_ok() {
        return Err(StewardError::Content(format!(
            "capsule import staging path {} already exists; retry the import",
            staging.display()
        )));
    }

    let mut ship = Ship::create_pond(&staging, birthplace)
        .await
        .map_err(|error| {
            StewardError::Content(format!(
                "create staged pond at {}: {error}",
                staging.display()
            ))
        })?;

    ship.control_table_mut()
        .set_setting(
            POST_COMMIT_DISPATCH_SETTING,
            POST_COMMIT_DISPATCH_SUPPRESSED,
        )
        .await
        .map_err(|error| {
            StewardError::Content(format!(
                "disable post-commit dispatch for staged pond at {}: {error}",
                staging.display()
            ))
        })?;

    write_provenance(&staging, &manifest, capsule_root_hash).map_err(|error| {
        StewardError::Content(format!(
            "capsule import failed writing provenance at {} (left in place for inspection): \
             {error}",
            staging.display()
        ))
    })?;

    if let Err(error) = write_entries(&mut ship, &manifest, &objects_dir).await {
        return Err(StewardError::Content(format!(
            "capsule import failed while staging at {} (left in place for inspection): {error}",
            staging.display()
        )));
    }

    let rebuilt = crate::build_recovery_capsule(&ship)
        .await
        .map_err(|error| {
            StewardError::Content(format!(
                "capsule import failed re-reading the staged pond at {} for verification (left in \
             place for inspection): {error}",
                staging.display()
            ))
        })?;
    if let Err(error) = assert_logical_match(&manifest, &rebuilt.manifest) {
        return Err(StewardError::Content(format!(
            "staged pond at {} does not match the capsule's logical contract (left in place for \
             inspection): {error}",
            staging.display()
        )));
    }

    let target_pond_id = ship.control_table().pond_id_uuid().to_string();
    drop(ship);

    sync_tree(&staging).map_err(|error| {
        StewardError::Content(format!(
            "sync verified staged pond at {} before promotion (left in place for inspection): \
             {error}",
            staging.display()
        ))
    })?;

    rename_no_replace(&staging, target).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            StewardError::Content(format!(
                "capsule import target {} was created concurrently during staging; the verified \
                 staged pond remains at {}",
                target.display(),
                staging.display()
            ))
        } else {
            StewardError::Content(format!(
                "rename verified staged pond {} to target {}: {error}",
                staging.display(),
                target.display()
            ))
        }
    })?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            StewardError::Content(format!(
                "capsule import was renamed to {} but syncing parent directory {} failed; \
                 promotion durability is not confirmed: {error}",
                target.display(),
                parent.display()
            ))
        })?;

    let (directories, physical, symlinks, dynamic) = count_kinds(&manifest);
    Ok(CapsuleImportReport {
        target: target.to_path_buf(),
        target_pond_id,
        source_pond_id: manifest.source.pond_id.clone(),
        source_tip: manifest.source.source_tip,
        capsule_root: capsule_root_hash,
        entries: manifest.entries.len(),
        directories,
        physical,
        symlinks,
        dynamic,
        logical_count: verify_report.logical_count,
    })
}

#[cfg(any(target_vendor = "apple", target_os = "linux", target_os = "android"))]
fn rename_no_replace(source: &Path, target: &Path) -> std::io::Result<()> {
    Ok(rustix::fs::renameat_with(
        rustix::fs::CWD,
        source,
        rustix::fs::CWD,
        target,
        rustix::fs::RenameFlags::NOREPLACE,
    )?)
}

#[cfg(windows)]
fn rename_no_replace(source: &Path, target: &Path) -> std::io::Result<()> {
    std::fs::rename(source, target)
}

#[cfg(not(any(
    target_vendor = "apple",
    target_os = "linux",
    target_os = "android",
    windows
)))]
fn rename_no_replace(_source: &Path, _target: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic no-replace directory rename is unsupported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::{canonicalize_table_batch, rename_no_replace};
    use arrow_array::{DictionaryArray, RecordBatch, StringArray, types::UInt16Type};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use sync_store::canonicalize_schema;

    #[test]
    fn promotion_never_replaces_an_existing_target() {
        let temporary = tempfile::tempdir().unwrap();
        let staging = temporary.path().join("staging");
        let target = temporary.path().join("target");
        std::fs::create_dir(&staging).unwrap();
        std::fs::write(staging.join("staged"), b"staged").unwrap();
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("existing"), b"existing").unwrap();

        let error = rename_no_replace(&staging, &target).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(target.join("existing")).unwrap(), b"existing");
        assert_eq!(std::fs::read(staging.join("staged")).unwrap(), b"staged");
    }

    #[test]
    fn canonicalizes_plain_and_dictionary_batches_to_one_schema() {
        let plain_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let dictionary_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            false,
        )]));
        let plain = RecordBatch::try_new(
            plain_schema.clone(),
            vec![Arc::new(StringArray::from(vec!["plain"]))],
        )
        .unwrap();
        let dictionary: DictionaryArray<UInt16Type> =
            vec![Some("dictionary")].into_iter().collect();
        let dictionary =
            RecordBatch::try_new(dictionary_schema, vec![Arc::new(dictionary)]).unwrap();

        let canonical_schema = canonicalize_schema(plain_schema.as_ref()).unwrap();
        let plain = canonicalize_table_batch(plain, &canonical_schema, "/table").unwrap();
        let dictionary = canonicalize_table_batch(dictionary, &canonical_schema, "/table").unwrap();

        assert_eq!(plain.schema(), canonical_schema);
        assert_eq!(dictionary.schema(), canonical_schema);
        assert_eq!(
            dictionary
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "dictionary"
        );
    }
}

/// Choose a private, hidden sibling staging path next to `target`, named with
/// a random suffix so retries after a failed prior attempt (whose staging
/// directory was deliberately left behind) do not collide.
fn pick_staging_path(target: &Path, parent: &Path) -> Result<PathBuf, StewardError> {
    let name = target.file_name().ok_or_else(|| {
        StewardError::Content(format!(
            "capsule import target {} has no file name component",
            target.display()
        ))
    })?;
    let suffix = uuid::Uuid::new_v4();
    Ok(parent.join(format!(
        ".{}.capsule-import-{suffix}",
        name.to_string_lossy()
    )))
}

fn write_provenance(
    staging: &Path,
    manifest: &CapsuleManifest,
    capsule_root_hash: ObjectHash,
) -> Result<(), StewardError> {
    let provenance = CapsuleImportProvenance {
        format: IMPORT_PROVENANCE_FORMAT.to_string(),
        source_pond_id: manifest.source.pond_id.clone(),
        source_birthplace: manifest.source.birthplace.clone(),
        source_tip: manifest.source.source_tip.to_hex(),
        capsule_root: capsule_root_hash.to_hex(),
        importer_version: env!("CARGO_PKG_VERSION").to_string(),
        imported_at_micros: chrono::Utc::now().timestamp_micros(),
    };
    let bytes = serde_json::to_vec_pretty(&provenance)
        .map_err(|error| StewardError::Content(format!("encode import provenance: {error}")))?;
    let path = staging.join(IMPORT_PROVENANCE_FILE);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| StewardError::Content(format!("create import provenance: {error}")))?;
    file.write_all(&bytes)
        .map_err(|error| StewardError::Content(format!("write import provenance: {error}")))?;
    file.sync_all()
        .map_err(|error| StewardError::Content(format!("sync import provenance: {error}")))
}

/// Flush every staged file and directory before the final rename. Directory
/// metadata is synced after its children, then the parent is synced again
/// after rename so both the staged contents and promoted name are durable.
fn sync_tree(path: &Path) -> std::io::Result<()> {
    let metadata = path.symlink_metadata()?;
    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("refusing to follow filesystem symlink {}", path.display()),
        ));
    }
    if metadata.is_file() {
        return File::open(path)?.sync_all();
    }
    if !metadata.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported filesystem entry {}", path.display()),
        ));
    }
    for entry in std::fs::read_dir(path)? {
        sync_tree(&entry?.path())?;
    }
    File::open(path)?.sync_all()
}

/// Recreate every capsule entry (except the root, already created by
/// [`Ship::create_pond`]) inside one suppressed write transaction.
///
/// Canonical manifest order is already parent-before-child (see module
/// docs), so a single left-to-right pass never needs to look ahead.
async fn write_entries(
    ship: &mut Ship,
    manifest: &CapsuleManifest,
    objects_dir: &Path,
) -> Result<(), StewardError> {
    let meta = PondUserMetadata::new(vec!["capsule".to_string(), "import".to_string()]);
    let tx = ship.begin_write_suppressed(&meta).await?;
    let apply_result: Result<(), StewardError> = async {
        let root_wd = tx.root().await?;
        let mut dirs: std::collections::HashMap<String, WD> = std::collections::HashMap::new();
        let _ = dirs.insert("/".to_string(), root_wd);
        for entry in manifest.entries.iter().filter(|entry| entry.path != "/") {
            let (parent_path, name) = split_path(&entry.path)?;
            let parent_wd = dirs
                .get(&parent_path)
                .ok_or_else(|| {
                    StewardError::Content(format!(
                        "capsule path {:?} has no recreated parent {parent_path:?}",
                        entry.path
                    ))
                })?
                .clone();
            write_entry(&parent_wd, name, entry, objects_dir, &mut dirs, &entry.path).await?;
        }
        Ok(())
    }
    .await;
    match apply_result {
        Ok(()) => tx.commit().await.map(|_| ()),
        Err(error) => Err(tx.abort_preserving(error).await),
    }
}

async fn write_entry(
    parent_wd: &WD,
    name: &str,
    entry: &CapsuleEntry,
    objects_dir: &Path,
    dirs: &mut std::collections::HashMap<String, WD>,
    path: &str,
) -> Result<(), StewardError> {
    match &entry.node {
        CapsuleNode::Directory => {
            let wd = parent_wd.create_dir_path(name).await?;
            let _ = dirs.insert(path.to_string(), wd);
        }
        CapsuleNode::Symlink { target } => {
            let bytes = read_object(objects_dir, target)?;
            let target_str = std::str::from_utf8(&bytes).map_err(|error| {
                StewardError::Content(format!("symlink target for {path:?} is not utf-8: {error}"))
            })?;
            let _ = parent_wd.create_symlink_path(name, target_str).await?;
        }
        CapsuleNode::Dynamic { recipe } => {
            let bytes = read_object(objects_dir, recipe)?;
            let (factory, config) = decode_recipe(&bytes).map_err(|error| {
                StewardError::Content(format!("decode dynamic recipe for {path:?}: {error}"))
            })?;
            let _ = parent_wd
                .create_dynamic_path(name, entry.entry_type, &factory, config)
                .await?;
        }
        CapsuleNode::Physical {
            payload_kind,
            objects,
            leaves,
            ..
        } => {
            if leaves.is_empty() {
                write_empty_versions(parent_wd, name, entry.entry_type, objects, objects_dir)
                    .await?;
            } else {
                match payload_kind {
                    CapsulePayloadKind::File => {
                        write_file_entry(
                            parent_wd,
                            name,
                            entry.entry_type,
                            objects,
                            leaves,
                            objects_dir,
                        )
                        .await?;
                    }
                    CapsulePayloadKind::Table => {
                        write_table_entry(
                            parent_wd,
                            name,
                            entry.entry_type,
                            objects,
                            leaves,
                            objects_dir,
                        )
                        .await?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Split an absolute canonical capsule path into its parent path and final
/// name component.
fn split_path(path: &str) -> Result<(String, &str), StewardError> {
    let index = path
        .rfind('/')
        .ok_or_else(|| StewardError::Content(format!("capsule path {path:?} is not absolute")))?;
    let parent = if index == 0 {
        "/".to_string()
    } else {
        path[..index].to_string()
    };
    let name = &path[index + 1..];
    if name.is_empty() {
        return Err(StewardError::Content(format!(
            "capsule path {path:?} has an empty name component"
        )));
    }
    Ok((parent, name))
}

/// Read one payload object's bytes from the capsule's objects directory.
///
/// The source capsule was already deeply verified by
/// [`verify_capsule_payload_directory`] before staging began, so this only
/// re-checks size (cheap) rather than repeating a BLAKE3 pass; the
/// independent post-write comparison against a freshly rebuilt manifest
/// (see [`assert_logical_match`]) is what actually re-validates the staged
/// result.
fn read_object(objects_dir: &Path, object: &CapsuleObject) -> Result<Vec<u8>, StewardError> {
    let path = objects_dir.join(format!("blake3={}", object.hash.to_hex()));
    let bytes = std::fs::read(&path).map_err(|error| {
        StewardError::Content(format!("read capsule payload {}: {error}", object.hash))
    })?;
    if bytes.len() as u64 != object.size {
        return Err(StewardError::Content(format!(
            "capsule payload {} has {} bytes on disk, manifest declares {}",
            object.hash,
            bytes.len(),
            object.size
        )));
    }
    Ok(bytes)
}

fn leaf_meta(leaf: &CapsuleLeaf) -> VersionMeta {
    VersionMeta {
        timestamp: Some(leaf.source_timestamp),
        min_event_time: leaf.min_event_time,
        max_event_time: leaf.max_event_time,
        extended_attributes: leaf.logical_attributes.clone(),
    }
}

/// Recreate a physical node whose every source version was empty (zero bytes
/// or zero rows) -- a state `pondcapsule.1` cannot represent as a logical
/// leaf (`capsule_leaf_hash` rejects an empty leaf; see
/// `docs/recovery-capsule-design.md`). Any schema-carrying Parquet objects
/// the source still declared are copied verbatim, one per version, so at
/// least the physical schema survives; a node with no objects at all (a
/// plain empty file) gets a single empty version so its path and entry type
/// still exist.
async fn write_empty_versions(
    parent_wd: &WD,
    name: &str,
    entry_type: EntryType,
    objects: &[CapsuleObject],
    objects_dir: &Path,
) -> Result<(), StewardError> {
    if objects.is_empty() {
        let writer = parent_wd
            .async_writer_path_with_type(name, entry_type)
            .await?;
        return finalize_writer(writer, entry_type, &VersionMeta::default()).await;
    }
    for object in objects {
        let bytes = read_object(objects_dir, object)?;
        let mut writer = parent_wd
            .async_writer_path_with_type(name, entry_type)
            .await?;
        writer.write_all(&bytes).await.map_err(|error| {
            StewardError::Content(format!(
                "write empty-leaf payload {} for {name:?}: {error}",
                object.hash
            ))
        })?;
        finalize_writer(writer, entry_type, &VersionMeta::default()).await?;
    }
    Ok(())
}

/// Stream a `File`-kind physical node's payload objects into fresh pond
/// versions, splitting the concatenated byte stream at each leaf's declared
/// byte count and preserving exact bytes per leaf.
async fn write_file_entry(
    parent_wd: &WD,
    name: &str,
    entry_type: EntryType,
    objects: &[CapsuleObject],
    leaves: &[CapsuleLeaf],
    objects_dir: &Path,
) -> Result<(), StewardError> {
    let mut leaf_index = 0usize;
    let mut remaining = leaves[0].logical_count;
    let mut writer = parent_wd
        .async_writer_path_with_type(name, entry_type)
        .await?;
    let mut buffer = vec![0u8; 1024 * 1024];
    for object in objects {
        let path = objects_dir.join(format!("blake3={}", object.hash.to_hex()));
        let mut file = File::open(&path).map_err(|error| {
            StewardError::Content(format!(
                "open capsule payload {} for {name:?}: {error}",
                object.hash
            ))
        })?;
        loop {
            let count = std::io::Read::read(&mut file, &mut buffer).map_err(|error| {
                StewardError::Content(format!(
                    "read capsule payload {} for {name:?}: {error}",
                    object.hash
                ))
            })?;
            if count == 0 {
                break;
            }
            let mut offset = 0usize;
            while offset < count {
                if remaining == 0 {
                    finalize_writer(writer, entry_type, &leaf_meta(&leaves[leaf_index])).await?;
                    leaf_index += 1;
                    let leaf = leaves.get(leaf_index).ok_or_else(|| {
                        StewardError::Content(format!(
                            "file {name:?} payload stream has bytes after its final logical leaf"
                        ))
                    })?;
                    remaining = leaf.logical_count;
                    writer = parent_wd
                        .async_writer_path_with_type(name, entry_type)
                        .await?;
                }
                let take = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(count - offset);
                writer
                    .write_all(&buffer[offset..offset + take])
                    .await
                    .map_err(|error| {
                        StewardError::Content(format!(
                            "write file {name:?} leaf {leaf_index}: {error}"
                        ))
                    })?;
                offset += take;
                remaining -= take as u64;
            }
        }
    }
    if remaining != 0 || leaf_index + 1 != leaves.len() {
        return Err(StewardError::Content(format!(
            "file {name:?} payload stream ended after leaf {leaf_index} of {} declared logical \
             leaves",
            leaves.len()
        )));
    }
    finalize_writer(writer, entry_type, &leaf_meta(&leaves[leaf_index])).await
}

/// Stream a `Table`-kind physical node's Parquet payload objects into fresh
/// pond versions, decoding rows across object boundaries and re-encoding one
/// standalone Parquet file per leaf at its declared row count. Physical
/// object boundaries need not (and in general will not) align with leaf
/// boundaries; only the ordered rows and the schema carry logical identity.
async fn write_table_entry(
    parent_wd: &WD,
    name: &str,
    entry_type: EntryType,
    objects: &[CapsuleObject],
    leaves: &[CapsuleLeaf],
    objects_dir: &Path,
) -> Result<(), StewardError> {
    let mut leaf_index = 0usize;
    let mut collected = 0u64;
    let mut pending: Vec<RecordBatch> = Vec::new();
    let mut schema: Option<Arc<Schema>> = None;

    for object in objects {
        let path = objects_dir.join(format!("blake3={}", object.hash.to_hex()));
        let file = File::open(&path).map_err(|error| {
            StewardError::Content(format!(
                "open capsule payload {} for {name:?}: {error}",
                object.hash
            ))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            StewardError::Content(format!(
                "open Parquet payload {} for {name:?}: {error}",
                object.hash
            ))
        })?;
        let object_schema = canonicalize_schema(builder.schema().as_ref()).map_err(|error| {
            StewardError::Content(format!(
                "canonicalize Parquet schema for payload {} in {name:?}: {error}",
                object.hash
            ))
        })?;
        if let Some(schema) = &schema {
            if schema.as_ref() != object_schema.as_ref() {
                return Err(StewardError::Content(format!(
                    "Parquet payload {} for {name:?} has a different logical schema",
                    object.hash
                )));
            }
        } else {
            schema = Some(object_schema);
        }
        let reader = builder.build().map_err(|error| {
            StewardError::Content(format!("build Parquet reader for {name:?}: {error}"))
        })?;
        for batch in reader {
            let batch = batch.map_err(|error| {
                StewardError::Content(format!("read Parquet rows for {name:?}: {error}"))
            })?;
            let batch = canonicalize_table_batch(
                batch,
                schema.as_ref().expect("schema established by first object"),
                name,
            )?;
            let mut offset = 0usize;
            while offset < batch.num_rows() {
                let leaf = leaves.get(leaf_index).ok_or_else(|| {
                    StewardError::Content(format!(
                        "table {name:?} payload stream has rows after its final logical leaf"
                    ))
                })?;
                let remaining = leaf.logical_count.checked_sub(collected).ok_or_else(|| {
                    StewardError::Content(format!(
                        "table {name:?} leaf {leaf_index} row count overflow"
                    ))
                })?;
                let take = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(batch.num_rows() - offset);
                if take > 0 {
                    pending.push(batch.slice(offset, take));
                }
                collected += take as u64;
                offset += take;
                if collected == leaf.logical_count {
                    write_table_leaf(
                        parent_wd,
                        name,
                        entry_type,
                        schema.as_ref().expect("schema established by first object"),
                        &pending,
                        leaf,
                    )
                    .await?;
                    pending.clear();
                    collected = 0;
                    leaf_index += 1;
                }
            }
        }
    }
    if leaf_index != leaves.len() || collected != 0 || !pending.is_empty() {
        return Err(StewardError::Content(format!(
            "table {name:?} payload stream ended after {leaf_index} of {} declared logical \
             leaves",
            leaves.len()
        )));
    }
    Ok(())
}

fn canonicalize_table_batch(
    batch: RecordBatch,
    schema: &Arc<Schema>,
    name: &str,
) -> Result<RecordBatch, StewardError> {
    if batch.num_columns() != schema.fields().len() {
        return Err(StewardError::Content(format!(
            "Parquet batch for {name:?} has {} columns, expected {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .enumerate()
        .map(|(index, (column, field))| {
            if column.data_type() == field.data_type() {
                return Ok(column.clone());
            }
            arrow::compute::cast(column, field.data_type()).map_err(|error| {
                StewardError::Content(format!(
                    "canonicalize Parquet column {index} ({:?}) for {name:?}: {error}",
                    field.name()
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|error| {
        StewardError::Content(format!(
            "build canonical Parquet batch for {name:?}: {error}"
        ))
    })
}

async fn write_table_leaf(
    parent_wd: &WD,
    name: &str,
    entry_type: EntryType,
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    leaf: &CapsuleLeaf,
) -> Result<(), StewardError> {
    let mut buffer = Vec::new();
    {
        let properties = WriterProperties::builder().build();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(properties))
            .map_err(|error| {
                StewardError::Content(format!("open Parquet encoder for {name:?} leaf: {error}"))
            })?;
        for batch in batches {
            arrow_writer.write(batch).map_err(|error| {
                StewardError::Content(format!("encode Parquet rows for {name:?} leaf: {error}"))
            })?;
        }
        let _: parquet::file::metadata::ParquetMetaData =
            arrow_writer.close().map_err(|error| {
                StewardError::Content(format!("finish Parquet encoder for {name:?} leaf: {error}"))
            })?;
    }
    let mut writer = parent_wd
        .async_writer_path_with_type(name, entry_type)
        .await?;
    writer.write_all(&buffer).await.map_err(|error| {
        StewardError::Content(format!("write Parquet bytes for {name:?} leaf: {error}"))
    })?;
    finalize_writer(writer, entry_type, &leaf_meta(leaf)).await
}

/// Compare a capsule manifest rebuilt from the staged pond against the
/// source capsule manifest by logical projection.
///
/// Deliberately ignored: `source_node_id` (fresh identities are allowed to
/// differ, design "Node IDs ... may change") and, within `Physical` nodes,
/// the `objects` vector (physical repacking is allowed; `logical_root`
/// already commits to every ordered leaf, so comparing it -- and the leaves
/// themselves, for a precise diagnostic -- is the strongest correct check).
fn assert_logical_match(source: &CapsuleManifest, rebuilt: &CapsuleManifest) -> Result<(), String> {
    if source.entries.len() != rebuilt.entries.len() {
        return Err(format!(
            "staged pond has {} live entries, capsule declares {}",
            rebuilt.entries.len(),
            source.entries.len()
        ));
    }
    for (expected, actual) in source.entries.iter().zip(&rebuilt.entries) {
        if expected.path != actual.path {
            return Err(format!(
                "entry order mismatch: expected {:?}, found {:?}",
                expected.path, actual.path
            ));
        }
        if expected.entry_type != actual.entry_type {
            return Err(format!(
                "{:?} changed entry type from {:?} to {:?}",
                expected.path, expected.entry_type, actual.entry_type
            ));
        }
        match (&expected.node, &actual.node) {
            (CapsuleNode::Directory, CapsuleNode::Directory) => {}
            (CapsuleNode::Symlink { target: want }, CapsuleNode::Symlink { target: got }) => {
                if want != got {
                    return Err(format!("{:?} symlink target changed", expected.path));
                }
            }
            (CapsuleNode::Dynamic { recipe: want }, CapsuleNode::Dynamic { recipe: got }) => {
                if want != got {
                    return Err(format!("{:?} dynamic recipe changed", expected.path));
                }
            }
            (
                CapsuleNode::Physical {
                    payload_kind: want_kind,
                    schema_fingerprint: want_schema,
                    logical_root: want_root,
                    leaves: want_leaves,
                    ..
                },
                CapsuleNode::Physical {
                    payload_kind: got_kind,
                    schema_fingerprint: got_schema,
                    logical_root: got_root,
                    leaves: got_leaves,
                    ..
                },
            ) => {
                if want_kind != got_kind
                    || want_schema != got_schema
                    || want_root != got_root
                    || want_leaves != got_leaves
                {
                    return Err(format!(
                        "{:?} logical content changed (payload kind, schema, series root, or \
                         leaves differ)",
                        expected.path
                    ));
                }
            }
            _ => {
                return Err(format!("{:?} node kind changed", expected.path));
            }
        }
    }
    Ok(())
}

fn count_kinds(manifest: &CapsuleManifest) -> (usize, usize, usize, usize) {
    let mut directories = 0;
    let mut physical = 0;
    let mut symlinks = 0;
    let mut dynamic = 0;
    for entry in &manifest.entries {
        match &entry.node {
            CapsuleNode::Directory => directories += 1,
            CapsuleNode::Physical { .. } => physical += 1,
            CapsuleNode::Symlink { .. } => symlinks += 1,
            CapsuleNode::Dynamic { .. } => dynamic += 1,
        }
    }
    (directories, physical, symlinks, dynamic)
}
