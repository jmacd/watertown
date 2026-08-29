// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Generic staged import: materialize a downloaded `pondcapsule.1`,
//! `pondcapsule.2`, or opaque `pondcapsule.legacy.1` capsule
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

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_cast::cast as cast_array;
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::context::SessionContext;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use sync_store::content::{PayloadKind, SeriesManifest, merkle_root, recipe_hash};
use sync_store::{
    CapsuleEntry, CapsuleLeaf, CapsuleManifest, CapsuleNode, CapsuleObject, CapsulePayloadKind,
    IncrementalFileLeafHasher, IncrementalTableLeafHasher, LEGACY_CAPSULE_FORMAT,
    LegacyCapsuleEntry, LegacyCapsuleManifest, LegacyCapsuleNode, LegacyCapsuleObject,
    LegacyCapsulePayloadKind, LegacyCapsuleVersion, ManifestEntry, ObjectHash, VersionMeta,
    canonicalize_schema, decode_manifest, decode_recipe, encode_canonical_attributes,
    encode_canonical_batch_rows, legacy_capsule_root, read_capsule_manifest,
    read_legacy_capsule_manifest, schema_fingerprint, verify_capsule_payload_directory,
    verify_legacy_capsule_payload_directory,
};
use tinyfs::{EntryType, WD};
use tlogfs::schema::OplogEntry;
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
const LEGACY_IMPORT_PROVENANCE_FORMAT: &str = "pondcapsule.legacy.1-import-provenance.1";
const LEGACY_REPLAY_FILE: &str = "LEGACY_CAPSULE_REPLAY.json";
const LEGACY_REPLAY_FORMAT: &str = "pondcapsule.legacy.1-replay.1";
const LEGACY_REPLAY_PAYLOAD_DIR: &str = ".legacy-capsule-replay-payloads";

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
    let parent = validate_import_target(target)?;
    let format = read_capsule_format(capsule_dir)?;
    if format == LEGACY_CAPSULE_FORMAT {
        return import_legacy_capsule(capsule_dir, target, parent, birthplace.into()).await;
    }
    import_logical_capsule(capsule_dir, target, parent, birthplace.into()).await
}

fn validate_import_target(target: &Path) -> Result<&Path, StewardError> {
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
    Ok(parent)
}

#[derive(Deserialize)]
struct CapsuleFormatProbe {
    format: String,
}

fn read_capsule_format(capsule_dir: &Path) -> Result<String, StewardError> {
    let recovery = capsule_dir.join("recovery");
    let reference = std::fs::read_to_string(recovery.join("refs/latest"))
        .map_err(|error| StewardError::Content(format!("read recovery/refs/latest: {error}")))?;
    let root = ObjectHash::from_hex(reference.trim_end()).map_err(|error| {
        StewardError::Content(format!("decode capsule latest ref as BLAKE3 hash: {error}"))
    })?;
    let bytes = std::fs::read(
        recovery
            .join("manifests")
            .join(format!("{}.json", root.to_hex())),
    )
    .map_err(|error| StewardError::Content(format!("read capsule manifest {root}: {error}")))?;
    let probe: CapsuleFormatProbe = serde_json::from_slice(&bytes)
        .map_err(|error| StewardError::Content(format!("decode capsule format: {error}")))?;
    Ok(probe.format)
}

async fn import_logical_capsule(
    capsule_dir: &Path,
    target: &Path,
    parent: &Path,
    birthplace: String,
) -> Result<CapsuleImportReport, StewardError> {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LegacyReplayManifest {
    format: String,
    capsule_root: String,
    entries: Vec<LegacyReplayEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LegacyReplayEntry {
    path: String,
    entry_type: EntryType,
    target_child_hash: String,
    target_entry_metadata: Vec<LegacyReplayMetadata>,
    versions: Vec<LegacyReplayVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LegacyReplayVersion {
    source_version: u64,
    target_version: i64,
    source_objects: Vec<String>,
    target_blob_hash: String,
    target_physical_size: u64,
    logical_count: u64,
    logical_leaf_hash: Option<String>,
    schema_fingerprint: Option<String>,
    metadata: LegacyReplayMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LegacyReplayMetadata {
    timestamp: i64,
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    logical_attributes: Option<String>,
}

enum LegacyPreparedPayload {
    SourceObjects(Vec<LegacyCapsuleObject>),
    TableParquet(PathBuf),
}

struct LegacyPreparedVersion {
    replay: LegacyReplayVersion,
    payload: LegacyPreparedPayload,
}

struct LegacyPreparedImport {
    report: LegacyReplayManifest,
    physical: BTreeMap<String, Vec<LegacyPreparedVersion>>,
}

async fn import_legacy_capsule(
    capsule_dir: &Path,
    target: &Path,
    parent: &Path,
    birthplace: String,
) -> Result<CapsuleImportReport, StewardError> {
    let (manifest, capsule_root_hash) =
        read_legacy_capsule_manifest(capsule_dir).map_err(|error| {
            StewardError::Content(format!("read source legacy capsule manifest: {error}"))
        })?;
    let objects_dir = capsule_dir.join("recovery/objects");
    let verify_report = verify_legacy_capsule_payload_directory(&manifest, &objects_dir)
        .map_err(|error| StewardError::Content(format!("verify source legacy capsule: {error}")))?;

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
                "create staged legacy-import pond at {}: {error}",
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
                "disable post-commit dispatch for staged legacy-import pond at {}: {error}",
                staging.display()
            ))
        })?;
    write_legacy_provenance(&staging, &manifest, capsule_root_hash).map_err(|error| {
        StewardError::Content(format!(
            "legacy capsule import failed writing provenance at {} (left in place for \
             inspection): {error}",
            staging.display()
        ))
    })?;

    let replay_payloads = staging.join(LEGACY_REPLAY_PAYLOAD_DIR);
    std::fs::create_dir(&replay_payloads).map_err(|error| {
        StewardError::Content(format!(
            "legacy capsule import failed creating replay payload staging at {} (left in place \
             for inspection): {error}",
            replay_payloads.display()
        ))
    })?;
    let prepared =
        prepare_legacy_import(&manifest, capsule_root_hash, &objects_dir, &replay_payloads)
            .map_err(|error| {
                StewardError::Content(format!(
                    "legacy capsule import failed preparing target replay at {} (left in place for \
             inspection): {error}",
                    staging.display()
                ))
            })?;
    write_legacy_replay_report(&staging, &prepared.report).map_err(|error| {
        StewardError::Content(format!(
            "legacy capsule import failed writing deterministic replay report at {} (left in \
             place for inspection): {error}",
            staging.display()
        ))
    })?;

    if let Err(error) =
        write_legacy_entries(&mut ship, &manifest, &objects_dir, &prepared.physical).await
    {
        return Err(StewardError::Content(format!(
            "legacy capsule import failed while staging at {} (left in place for inspection): \
             {error}",
            staging.display()
        )));
    }
    if let Err(error) =
        verify_staged_legacy_replay(&ship, &manifest, &objects_dir, &prepared.report).await
    {
        return Err(StewardError::Content(format!(
            "legacy capsule import failed target replay validation at {} (left in place for \
             inspection): {error}",
            staging.display()
        )));
    }
    std::fs::remove_dir_all(&replay_payloads).map_err(|error| {
        StewardError::Content(format!(
            "legacy capsule import verified at {} but could not remove private replay payloads \
             before promotion (left in place for inspection): {error}",
            staging.display()
        ))
    })?;

    let target_pond_id = ship.control_table().pond_id_uuid().to_string();
    drop(ship);
    promote_staging(&staging, target, parent)?;

    let (directories, physical, symlinks, dynamic) = count_legacy_kinds(&manifest);
    let logical_count = prepared
        .report
        .entries
        .iter()
        .flat_map(|entry| entry.versions.iter())
        .try_fold(0u64, |sum, version| {
            sum.checked_add(version.logical_count)
                .ok_or_else(|| StewardError::Content("legacy logical count overflow".to_string()))
        })?;
    Ok(CapsuleImportReport {
        target: target.to_path_buf(),
        target_pond_id,
        source_pond_id: manifest.source.pond_id,
        source_tip: manifest.source.source_tip,
        capsule_root: capsule_root_hash,
        entries: verify_report.entries,
        directories,
        physical,
        symlinks,
        dynamic,
        logical_count,
    })
}

fn prepare_legacy_import(
    manifest: &LegacyCapsuleManifest,
    capsule_root_hash: ObjectHash,
    objects_dir: &Path,
    replay_payloads: &Path,
) -> Result<LegacyPreparedImport, StewardError> {
    let mut report_entries = Vec::new();
    let mut physical = BTreeMap::new();
    for entry in &manifest.entries {
        let LegacyCapsuleNode::Physical {
            payload_kind,
            versions,
            ..
        } = &entry.node
        else {
            continue;
        };
        if versions.is_empty() {
            return Err(StewardError::Content(format!(
                "legacy physical path {:?} has no source versions",
                entry.path
            )));
        }
        let mut prepared_versions = Vec::with_capacity(versions.len());
        for version in versions {
            prepared_versions.push(prepare_legacy_version(
                entry,
                *payload_kind,
                version,
                objects_dir,
                replay_payloads,
            )?);
        }
        validate_empty_legacy_series(entry, &prepared_versions)?;
        let (target_child_hash, target_entry_metadata) =
            replay_entry_identity(entry, &prepared_versions)?;
        report_entries.push(LegacyReplayEntry {
            path: entry.path.clone(),
            entry_type: entry.entry_type,
            target_child_hash: target_child_hash.to_hex(),
            target_entry_metadata,
            versions: prepared_versions
                .iter()
                .map(|version| version.replay.clone())
                .collect(),
        });
        let _ = physical.insert(entry.path.clone(), prepared_versions);
    }
    report_entries.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    Ok(LegacyPreparedImport {
        report: LegacyReplayManifest {
            format: LEGACY_REPLAY_FORMAT.to_string(),
            capsule_root: capsule_root_hash.to_hex(),
            entries: report_entries,
        },
        physical,
    })
}

fn prepare_legacy_version(
    entry: &LegacyCapsuleEntry,
    payload_kind: LegacyCapsulePayloadKind,
    version: &LegacyCapsuleVersion,
    objects_dir: &Path,
    replay_payloads: &Path,
) -> Result<LegacyPreparedVersion, StewardError> {
    let timestamp = version.source_timestamp.ok_or_else(|| {
        StewardError::Content(format!(
            "legacy path {:?} source version {} has no timestamp; target replay cannot invent \
             original metadata",
            entry.path, version.source_version
        ))
    })?;
    if version.min_event_time.is_some() != version.max_event_time.is_some() {
        return Err(StewardError::Content(format!(
            "legacy path {:?} source version {} carries only one event-time bound",
            entry.path, version.source_version
        )));
    }
    let logical_attributes = canonical_legacy_attributes(
        version.extended_attributes.as_deref(),
        &entry.path,
        version.source_version,
    )?;
    if version.min_event_time.is_some() && logical_attributes.is_none() {
        return Err(StewardError::Content(format!(
            "legacy path {:?} source version {} has event-time bounds but no logical attributes; \
             the target writer would have to invent a timestamp-column attribute",
            entry.path, version.source_version
        )));
    }
    let metadata = LegacyReplayMetadata {
        timestamp,
        min_event_time: version.min_event_time,
        max_event_time: version.max_event_time,
        logical_attributes,
    };
    if matches!(
        entry.entry_type,
        EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion
    ) && version.min_event_time.is_some()
    {
        return Err(StewardError::Content(format!(
            "legacy singleton {:?} carries event-time bounds that the target singleton row \
             format cannot preserve",
            entry.path
        )));
    }
    let source_objects = version
        .objects
        .iter()
        .map(|object| object.hash.to_hex())
        .collect();

    match payload_kind {
        LegacyCapsulePayloadKind::File => {
            let (blob_hash, physical_size, logical_leaf_hash) =
                prepare_legacy_file_version(entry, version, objects_dir, &metadata)?;
            Ok(LegacyPreparedVersion {
                replay: LegacyReplayVersion {
                    source_version: version.source_version,
                    target_version: legacy_target_version(version.source_version)?,
                    source_objects,
                    target_blob_hash: blob_hash.to_hex(),
                    target_physical_size: physical_size,
                    logical_count: physical_size,
                    logical_leaf_hash: logical_leaf_hash.map(|hash| hash.to_hex()),
                    schema_fingerprint: None,
                    metadata,
                },
                payload: LegacyPreparedPayload::SourceObjects(version.objects.clone()),
            })
        }
        LegacyCapsulePayloadKind::Table => {
            if entry.entry_type == EntryType::TablePhysicalSeries
                && version.min_event_time.is_none()
            {
                return Err(StewardError::Content(format!(
                    "legacy table series {:?} source version {} has no event-time bounds; the \
                     target table-series writer cannot preserve an unbounded nonempty append",
                    entry.path, version.source_version
                )));
            }
            let path_hash = ObjectHash::of_bytes(entry.path.as_bytes());
            let prepared_path = replay_payloads.join(format!(
                "{}-{:020}.parquet",
                path_hash.to_hex(),
                version.source_version
            ));
            let prepared = prepare_legacy_table_version(
                entry,
                version,
                objects_dir,
                &prepared_path,
                &metadata,
            )?;
            Ok(LegacyPreparedVersion {
                replay: LegacyReplayVersion {
                    source_version: version.source_version,
                    target_version: legacy_target_version(version.source_version)?,
                    source_objects,
                    target_blob_hash: prepared.blob_hash.to_hex(),
                    target_physical_size: prepared.physical_size,
                    logical_count: prepared.logical_count,
                    logical_leaf_hash: prepared.logical_leaf_hash.map(|hash| hash.to_hex()),
                    schema_fingerprint: Some(prepared.schema_fingerprint.to_hex()),
                    metadata,
                },
                payload: LegacyPreparedPayload::TableParquet(prepared_path),
            })
        }
    }
}

fn legacy_target_version(source_version: u64) -> Result<i64, StewardError> {
    i64::try_from(source_version)
        .ok()
        .and_then(|version| version.checked_add(1))
        .ok_or_else(|| {
            StewardError::Content(format!(
                "legacy source version {source_version} cannot map to a positive target version"
            ))
        })
}

fn canonical_legacy_attributes(
    attributes: Option<&str>,
    path: &str,
    source_version: u64,
) -> Result<Option<String>, StewardError> {
    attributes
        .map(|attributes| {
            let bytes = encode_canonical_attributes(attributes).map_err(|error| {
                StewardError::Content(format!(
                    "canonicalize legacy logical attributes for {path:?} source version \
                     {source_version}: {error}"
                ))
            })?;
            String::from_utf8(bytes).map_err(|error| {
                StewardError::Content(format!(
                    "canonical legacy logical attributes for {path:?} are not UTF-8: {error}"
                ))
            })
        })
        .transpose()
}

fn prepare_legacy_file_version(
    entry: &LegacyCapsuleEntry,
    version: &LegacyCapsuleVersion,
    objects_dir: &Path,
    metadata: &LegacyReplayMetadata,
) -> Result<(ObjectHash, u64, Option<ObjectHash>), StewardError> {
    let logical_count = version.objects.iter().try_fold(0u64, |sum, object| {
        sum.checked_add(object.size)
            .ok_or_else(|| StewardError::Content("legacy file size overflow".to_string()))
    })?;
    let is_series = entry.entry_type == EntryType::FilePhysicalSeries;
    let attributes = metadata.logical_attributes.as_deref().map(str::as_bytes);
    let mut leaf_hasher = if is_series && logical_count > 0 {
        Some(
            IncrementalFileLeafHasher::new(
                logical_count,
                metadata.min_event_time,
                metadata.max_event_time,
                attributes,
            )
            .map_err(|error| {
                StewardError::Content(format!(
                    "prepare target file leaf for {:?} source version {}: {error}",
                    entry.path, version.source_version
                ))
            })?,
        )
    } else {
        None
    };
    let mut blob_hasher = blake3::Hasher::new();
    for object in &version.objects {
        stream_verified_legacy_object(objects_dir, object, |chunk| {
            let _ = blob_hasher.update(chunk);
            if let Some(hasher) = leaf_hasher.as_mut() {
                hasher.write(chunk).map_err(|error| {
                    StewardError::Content(format!(
                        "hash target file leaf for {:?} source version {}: {error}",
                        entry.path, version.source_version
                    ))
                })?;
            }
            Ok(())
        })?;
    }
    let logical_leaf_hash = leaf_hasher
        .map(|hasher| {
            hasher.finish().map_err(|error| {
                StewardError::Content(format!(
                    "finish target file leaf for {:?} source version {}: {error}",
                    entry.path, version.source_version
                ))
            })
        })
        .transpose()?;
    Ok((
        ObjectHash::from_bytes(*blob_hasher.finalize().as_bytes()),
        logical_count,
        logical_leaf_hash,
    ))
}

struct PreparedLegacyTable {
    blob_hash: ObjectHash,
    physical_size: u64,
    logical_count: u64,
    schema_fingerprint: ObjectHash,
    logical_leaf_hash: Option<ObjectHash>,
}

fn prepare_legacy_table_version(
    entry: &LegacyCapsuleEntry,
    version: &LegacyCapsuleVersion,
    objects_dir: &Path,
    prepared_path: &Path,
    metadata: &LegacyReplayMetadata,
) -> Result<PreparedLegacyTable, StewardError> {
    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(prepared_path)
        .map_err(|error| {
            StewardError::Content(format!(
                "create prepared target Parquet for {:?} source version {}: {error}",
                entry.path, version.source_version
            ))
        })?;
    let mut target_schema: Option<Arc<Schema>> = None;
    let mut writer: Option<ArrowWriter<File>> = None;
    let mut logical_count = 0u64;
    let mut canonical_rows_len = 0u64;

    for object in &version.objects {
        verify_legacy_object_file(objects_dir, object)?;
        let path = legacy_object_path(objects_dir, object);
        let file = File::open(&path).map_err(|error| {
            StewardError::Content(format!(
                "open legacy table payload {} for {:?}: {error}",
                object.hash, entry.path
            ))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            StewardError::Content(format!(
                "open legacy Parquet payload {} for {:?}: {error}",
                object.hash, entry.path
            ))
        })?;
        let object_schema = legacy_target_schema(builder.schema().as_ref()).map_err(|error| {
            StewardError::Content(format!(
                "normalize legacy Parquet schema {} for {:?} source version {}: {error}",
                object.hash, entry.path, version.source_version
            ))
        })?;
        if let Some(schema) = &target_schema {
            if schema.as_ref() != object_schema.as_ref() {
                return Err(StewardError::Content(format!(
                    "legacy path {:?} source version {} maps multiple Parquet objects with \
                     different schemas",
                    entry.path, version.source_version
                )));
            }
        } else {
            target_schema = Some(object_schema.clone());
            writer = Some(
                ArrowWriter::try_new(
                    output.try_clone().map_err(|error| {
                        StewardError::Content(format!(
                            "clone prepared Parquet file for {:?}: {error}",
                            entry.path
                        ))
                    })?,
                    object_schema,
                    Some(WriterProperties::builder().build()),
                )
                .map_err(|error| {
                    StewardError::Content(format!(
                        "open target Parquet encoder for {:?} source version {}: {error}",
                        entry.path, version.source_version
                    ))
                })?,
            );
        }
        for batch in builder.build().map_err(|error| {
            StewardError::Content(format!(
                "build legacy Parquet reader {} for {:?}: {error}",
                object.hash, entry.path
            ))
        })? {
            let batch = batch.map_err(|error| {
                StewardError::Content(format!(
                    "read legacy Parquet rows {} for {:?}: {error}",
                    object.hash, entry.path
                ))
            })?;
            let batch = normalize_legacy_table_batch(
                batch,
                target_schema
                    .as_ref()
                    .expect("target schema established before reading batches"),
                &entry.path,
            )?;
            logical_count = logical_count
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| {
                    StewardError::Content("legacy table row count overflow".to_string())
                })?;
            let row_bytes = encode_canonical_batch_rows(
                target_schema
                    .as_ref()
                    .expect("target schema established before reading batches"),
                &batch,
            )
            .map_err(|error| {
                StewardError::Content(format!(
                    "encode target canonical rows for {:?} source version {}: {error}",
                    entry.path, version.source_version
                ))
            })?;
            canonical_rows_len = canonical_rows_len
                .checked_add(row_bytes.len() as u64)
                .ok_or_else(|| {
                    StewardError::Content("legacy canonical row bytes overflow".to_string())
                })?;
            writer
                .as_mut()
                .expect("target writer established with schema")
                .write(&batch)
                .map_err(|error| {
                    StewardError::Content(format!(
                        "write prepared target Parquet for {:?} source version {}: {error}",
                        entry.path, version.source_version
                    ))
                })?;
        }
    }
    let target_schema = target_schema.ok_or_else(|| {
        StewardError::Content(format!(
            "legacy table {:?} source version {} has no raw Parquet objects",
            entry.path, version.source_version
        ))
    })?;
    let _ = writer
        .take()
        .expect("writer established with target schema")
        .close()
        .map_err(|error| {
            StewardError::Content(format!(
                "close prepared target Parquet for {:?} source version {}: {error}",
                entry.path, version.source_version
            ))
        })?;
    output.sync_all().map_err(|error| {
        StewardError::Content(format!(
            "sync prepared target Parquet for {:?}: {error}",
            entry.path
        ))
    })?;
    let (blob_hash, physical_size) = hash_file(prepared_path)?;
    let fingerprint = schema_fingerprint(target_schema.as_ref()).map_err(|error| {
        StewardError::Content(format!(
            "fingerprint target schema for {:?} source version {}: {error}",
            entry.path, version.source_version
        ))
    })?;
    let logical_leaf_hash = if entry.entry_type == EntryType::TablePhysicalSeries {
        if logical_count == 0 {
            return Err(StewardError::Content(format!(
                "legacy table series {:?} source version {} has zero rows; the target's \
                 nonempty-leaf series model cannot preserve that physical version",
                entry.path, version.source_version
            )));
        }
        let mut hasher = IncrementalTableLeafHasher::new(
            target_schema.as_ref(),
            logical_count,
            canonical_rows_len,
            metadata.min_event_time,
            metadata.max_event_time,
            metadata.logical_attributes.as_deref().map(str::as_bytes),
        )
        .map_err(|error| {
            StewardError::Content(format!(
                "prepare target table leaf for {:?} source version {}: {error}",
                entry.path, version.source_version
            ))
        })?;
        let file = File::open(prepared_path).map_err(|error| {
            StewardError::Content(format!(
                "reopen prepared target Parquet for {:?}: {error}",
                entry.path
            ))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            StewardError::Content(format!(
                "reopen prepared target Parquet reader for {:?}: {error}",
                entry.path
            ))
        })?;
        for batch in builder.build().map_err(|error| {
            StewardError::Content(format!(
                "build prepared target Parquet reader for {:?}: {error}",
                entry.path
            ))
        })? {
            let batch = normalize_legacy_table_batch(
                batch.map_err(|error| {
                    StewardError::Content(format!(
                        "read prepared target Parquet rows for {:?}: {error}",
                        entry.path
                    ))
                })?,
                &target_schema,
                &entry.path,
            )?;
            hasher.write_batch(&batch).map_err(|error| {
                StewardError::Content(format!(
                    "hash prepared target rows for {:?}: {error}",
                    entry.path
                ))
            })?;
        }
        Some(hasher.finish().map_err(|error| {
            StewardError::Content(format!(
                "finish prepared target table leaf for {:?}: {error}",
                entry.path
            ))
        })?)
    } else {
        None
    };
    Ok(PreparedLegacyTable {
        blob_hash,
        physical_size,
        logical_count,
        schema_fingerprint: fingerprint,
        logical_leaf_hash,
    })
}

fn legacy_target_schema(schema: &Schema) -> Result<Arc<Schema>, String> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(Arc::new(
                Field::new(
                    field.name(),
                    legacy_target_data_type(field.data_type())?,
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    canonicalize_schema(&Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

fn legacy_target_data_type(data_type: &DataType) -> Result<DataType, String> {
    match data_type {
        DataType::Dictionary(_, value) => legacy_target_data_type(value),
        DataType::Utf8View => Ok(DataType::Utf8),
        DataType::BinaryView => Ok(DataType::Binary),
        other => Ok(other.clone()),
    }
}

fn normalize_legacy_table_batch(
    batch: RecordBatch,
    target_schema: &Arc<Schema>,
    path: &str,
) -> Result<RecordBatch, StewardError> {
    if batch.num_columns() != target_schema.fields().len() {
        return Err(StewardError::Content(format!(
            "legacy table {path:?} batch has {} columns, target schema has {}",
            batch.num_columns(),
            target_schema.fields().len()
        )));
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (index, target_field) in target_schema.fields().iter().enumerate() {
        let source = batch.column(index);
        let column = if source.data_type() == target_field.data_type() {
            source.clone()
        } else {
            cast_array(source, target_field.data_type()).map_err(|error| {
                StewardError::Content(format!(
                    "cast legacy table {path:?} column {:?} from {:?} to {:?}: {error}",
                    target_field.name(),
                    source.data_type(),
                    target_field.data_type()
                ))
            })?
        };
        columns.push(column);
    }
    RecordBatch::try_new(target_schema.clone(), columns).map_err(|error| {
        StewardError::Content(format!(
            "construct normalized legacy table batch for {path:?}: {error}"
        ))
    })
}

fn validate_empty_legacy_series(
    entry: &LegacyCapsuleEntry,
    versions: &[LegacyPreparedVersion],
) -> Result<(), StewardError> {
    if !matches!(
        entry.entry_type,
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
    ) {
        return Ok(());
    }
    let empty_versions = versions
        .iter()
        .filter(|version| version.replay.logical_count == 0)
        .count();
    if empty_versions > 0 && (versions.len() != 1 || empty_versions != 1) {
        return Err(StewardError::Content(format!(
            "legacy series {:?} contains an empty version among other versions; the target \
             nonempty-leaf model cannot preserve that version order",
            entry.path
        )));
    }
    Ok(())
}

fn replay_entry_identity(
    entry: &LegacyCapsuleEntry,
    versions: &[LegacyPreparedVersion],
) -> Result<(ObjectHash, Vec<LegacyReplayMetadata>), StewardError> {
    if !matches!(
        entry.entry_type,
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
    ) {
        let only = versions.first().ok_or_else(|| {
            StewardError::Content(format!("legacy singleton {:?} has no version", entry.path))
        })?;
        return Ok((
            ObjectHash::from_hex(&only.replay.target_blob_hash).map_err(StewardError::Content)?,
            vec![only.replay.metadata.clone()],
        ));
    }

    let leaf_versions: Vec<&LegacyReplayVersion> = versions
        .iter()
        .map(|version| &version.replay)
        .filter(|version| version.logical_leaf_hash.is_some())
        .collect();
    let leaf_hashes = leaf_versions
        .iter()
        .map(|version| {
            ObjectHash::from_hex(
                version
                    .logical_leaf_hash
                    .as_deref()
                    .expect("filtered to leaf-bearing versions"),
            )
            .map_err(StewardError::Content)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let logical_count = leaf_versions.iter().try_fold(0u64, |sum, version| {
        sum.checked_add(version.logical_count)
            .ok_or_else(|| StewardError::Content("legacy series count overflow".to_string()))
    })?;
    let min_event_time = leaf_versions
        .iter()
        .filter_map(|version| version.metadata.min_event_time)
        .min();
    let max_event_time = leaf_versions
        .iter()
        .filter_map(|version| version.metadata.max_event_time)
        .max();
    let latest_leaf_metadata = leaf_versions.last().map(|version| version.metadata.clone());
    let logical_attributes = latest_leaf_metadata
        .as_ref()
        .and_then(|metadata| metadata.logical_attributes.as_deref())
        .map(|attributes| attributes.as_bytes().to_vec());
    let payload_kind = match entry.entry_type {
        EntryType::FilePhysicalSeries => PayloadKind::File,
        EntryType::TablePhysicalSeries => PayloadKind::Table,
        _ => unreachable!("series entry type checked above"),
    };
    let series = SeriesManifest::new_v2(
        payload_kind,
        logical_count,
        leaf_hashes.len() as u64,
        min_event_time,
        max_event_time,
        logical_attributes,
        merkle_root(&leaf_hashes),
    )
    .map_err(|error| {
        StewardError::Content(format!(
            "construct deterministic target series identity for {:?}: {error}",
            entry.path
        ))
    })?;
    let entry_metadata = match latest_leaf_metadata {
        Some(mut metadata) => {
            metadata.min_event_time = min_event_time;
            metadata.max_event_time = max_event_time;
            vec![metadata]
        }
        None => {
            let latest = versions.last().ok_or_else(|| {
                StewardError::Content(format!("legacy series {:?} has no versions", entry.path))
            })?;
            vec![LegacyReplayMetadata {
                timestamp: latest.replay.metadata.timestamp,
                min_event_time: None,
                max_event_time: None,
                logical_attributes: None,
            }]
        }
    };
    Ok((series.hash(), entry_metadata))
}

fn write_legacy_replay_report(
    staging: &Path,
    report: &LegacyReplayManifest,
) -> Result<(), StewardError> {
    let bytes = serde_json::to_vec(report)
        .map_err(|error| StewardError::Content(format!("encode legacy replay report: {error}")))?;
    let path = staging.join(LEGACY_REPLAY_FILE);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| StewardError::Content(format!("create legacy replay report: {error}")))?;
    file.write_all(&bytes)
        .map_err(|error| StewardError::Content(format!("write legacy replay report: {error}")))?;
    file.sync_all()
        .map_err(|error| StewardError::Content(format!("sync legacy replay report: {error}")))
}

async fn write_legacy_entries(
    ship: &mut Ship,
    manifest: &LegacyCapsuleManifest,
    objects_dir: &Path,
    prepared: &BTreeMap<String, Vec<LegacyPreparedVersion>>,
) -> Result<(), StewardError> {
    let meta = PondUserMetadata::new(vec!["capsule".to_string(), "legacy-migration".to_string()]);
    let tx = ship.begin_write_suppressed(&meta).await?;
    let apply_result: Result<(), StewardError> = async {
        let root_wd = tx.root().await?;
        let mut dirs = std::collections::HashMap::new();
        let _ = dirs.insert("/".to_string(), root_wd);
        for entry in manifest.entries.iter().filter(|entry| entry.path != "/") {
            let (parent_path, name) = split_path(&entry.path)?;
            let parent_wd = dirs
                .get(&parent_path)
                .ok_or_else(|| {
                    StewardError::Content(format!(
                        "legacy capsule path {:?} has no recreated parent {parent_path:?}",
                        entry.path
                    ))
                })?
                .clone();
            match &entry.node {
                LegacyCapsuleNode::Directory => {
                    let wd = parent_wd.create_dir_path(name).await?;
                    let _ = dirs.insert(entry.path.clone(), wd);
                }
                LegacyCapsuleNode::Symlink { target } => {
                    let bytes = read_legacy_object(objects_dir, target)?;
                    let target = std::str::from_utf8(&bytes).map_err(|error| {
                        StewardError::Content(format!(
                            "legacy symlink target for {:?} is not UTF-8: {error}",
                            entry.path
                        ))
                    })?;
                    let _ = parent_wd.create_symlink_path(name, target).await?;
                }
                LegacyCapsuleNode::Dynamic { recipe } => {
                    let bytes = read_legacy_object(objects_dir, recipe)?;
                    let (factory, config) = decode_recipe(&bytes).map_err(|error| {
                        StewardError::Content(format!(
                            "decode legacy dynamic recipe for {:?}: {error}",
                            entry.path
                        ))
                    })?;
                    let _ = parent_wd
                        .create_dynamic_path(name, entry.entry_type, &factory, config)
                        .await?;
                }
                LegacyCapsuleNode::Physical { .. } => {
                    let versions = prepared.get(&entry.path).ok_or_else(|| {
                        StewardError::Content(format!(
                            "legacy path {:?} has no prepared replay versions",
                            entry.path
                        ))
                    })?;
                    for version in versions {
                        write_legacy_physical_version(
                            &parent_wd,
                            name,
                            entry.entry_type,
                            objects_dir,
                            version,
                            &entry.path,
                        )
                        .await?;
                    }
                }
            }
        }
        Ok(())
    }
    .await;
    match apply_result {
        Ok(()) => tx.commit().await.map(|_| ()),
        Err(error) => Err(tx.abort_preserving(error).await),
    }
}

async fn write_legacy_physical_version(
    parent_wd: &WD,
    name: &str,
    entry_type: EntryType,
    objects_dir: &Path,
    version: &LegacyPreparedVersion,
    path: &str,
) -> Result<(), StewardError> {
    let mut writer = parent_wd
        .async_writer_path_with_type(name, entry_type)
        .await?;
    match &version.payload {
        LegacyPreparedPayload::SourceObjects(objects) => {
            for object in objects {
                let object_path = legacy_object_path(objects_dir, object);
                let metadata = std::fs::symlink_metadata(&object_path).map_err(|error| {
                    StewardError::Content(format!(
                        "inspect legacy file payload {} for {path:?}: {error}",
                        object.hash
                    ))
                })?;
                if !metadata.file_type().is_file() {
                    return Err(StewardError::Content(format!(
                        "legacy file payload {} for {path:?} is not a regular file",
                        object.hash
                    )));
                }
                let mut file = File::open(&object_path).map_err(|error| {
                    StewardError::Content(format!(
                        "open legacy file payload {} for {path:?}: {error}",
                        object.hash
                    ))
                })?;
                let mut hasher = blake3::Hasher::new();
                let mut size = 0u64;
                let mut buffer = vec![0u8; 1024 * 1024];
                loop {
                    let count = std::io::Read::read(&mut file, &mut buffer).map_err(|error| {
                        StewardError::Content(format!(
                            "read legacy file payload {} for {path:?}: {error}",
                            object.hash
                        ))
                    })?;
                    if count == 0 {
                        break;
                    }
                    let chunk = &buffer[..count];
                    let _ = hasher.update(chunk);
                    size = size.checked_add(count as u64).ok_or_else(|| {
                        StewardError::Content("legacy file payload size overflow".to_string())
                    })?;
                    writer.write_all(chunk).await.map_err(|error| {
                        StewardError::Content(format!(
                            "write legacy file payload {} for {path:?}: {error}",
                            object.hash
                        ))
                    })?;
                }
                let computed = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
                if computed != object.hash || size != object.size {
                    return Err(StewardError::Content(format!(
                        "legacy file payload {} changed after preflight: hash {computed}, size \
                         {size}, expected size {}",
                        object.hash, object.size
                    )));
                }
            }
        }
        LegacyPreparedPayload::TableParquet(prepared_path) => {
            let mut file = File::open(prepared_path).map_err(|error| {
                StewardError::Content(format!(
                    "open prepared target Parquet for {path:?}: {error}"
                ))
            })?;
            let mut buffer = vec![0u8; 1024 * 1024];
            loop {
                let count = std::io::Read::read(&mut file, &mut buffer).map_err(|error| {
                    StewardError::Content(format!(
                        "read prepared target Parquet for {path:?}: {error}"
                    ))
                })?;
                if count == 0 {
                    break;
                }
                writer.write_all(&buffer[..count]).await.map_err(|error| {
                    StewardError::Content(format!(
                        "write prepared target Parquet for {path:?}: {error}"
                    ))
                })?;
            }
        }
    }
    apply_legacy_metadata(&mut writer, &version.replay.metadata, path)?;
    writer.shutdown().await.map_err(|error| {
        StewardError::Content(format!(
            "finalize legacy target version {} for {path:?}: {error}",
            version.replay.target_version
        ))
    })
}

fn apply_legacy_metadata(
    writer: &mut std::pin::Pin<Box<dyn tinyfs::FileMetadataWriter>>,
    metadata: &LegacyReplayMetadata,
    path: &str,
) -> Result<(), StewardError> {
    writer.set_mtime(metadata.timestamp);
    match (metadata.min_event_time, metadata.max_event_time) {
        (Some(minimum), Some(maximum)) => {
            writer.set_temporal_metadata(
                minimum,
                maximum,
                legacy_timestamp_column(metadata, path)?,
            );
        }
        (None, None) => {}
        _ => {
            return Err(StewardError::Content(format!(
                "legacy replay metadata for {path:?} carries only one event-time bound"
            )));
        }
    }
    if let Some(attributes) = &metadata.logical_attributes {
        writer.set_exact_logical_attributes(attributes.as_bytes().to_vec());
    }
    Ok(())
}

fn legacy_timestamp_column(
    metadata: &LegacyReplayMetadata,
    path: &str,
) -> Result<String, StewardError> {
    let Some(attributes) = &metadata.logical_attributes else {
        return Ok("Timestamp".to_string());
    };
    let value: serde_json::Value = serde_json::from_str(attributes).map_err(|error| {
        StewardError::Content(format!(
            "decode canonical legacy logical attributes for {path:?}: {error}"
        ))
    })?;
    match value.get(tlogfs::schema::watertown::TIMESTAMP_COLUMN) {
        None => Ok("Timestamp".to_string()),
        Some(serde_json::Value::String(column)) => Ok(column.clone()),
        Some(other) => Err(StewardError::Content(format!(
            "legacy logical attributes for {path:?} name a non-string '{}' value: {other}",
            tlogfs::schema::watertown::TIMESTAMP_COLUMN
        ))),
    }
}

async fn verify_staged_legacy_replay(
    ship: &Ship,
    source: &LegacyCapsuleManifest,
    objects_dir: &Path,
    replay: &LegacyReplayManifest,
) -> Result<(), StewardError> {
    if replay.format != LEGACY_REPLAY_FORMAT {
        return Err(StewardError::Content(format!(
            "unexpected legacy replay format {:?}",
            replay.format
        )));
    }
    if replay.capsule_root
        != legacy_capsule_root(source)
            .map_err(StewardError::Content)?
            .to_hex()
    {
        return Err(StewardError::Content(
            "legacy replay report capsule root changed before validation".to_string(),
        ));
    }

    let materialized = crate::content_tree::materialize_content_objects(ship).await?;
    let (_, manifest_bytes) = materialized.manifest.as_ref().ok_or_else(|| {
        StewardError::Content("staged legacy target has no node manifest".to_string())
    })?;
    let target_manifest = decode_manifest(manifest_bytes).map_err(StewardError::Content)?;
    let target_by_path = target_manifest_paths(&target_manifest)?;
    if target_by_path.len() != source.entries.len() {
        return Err(StewardError::Content(format!(
            "staged legacy target has {} manifest paths, source capsule has {}",
            target_by_path.len(),
            source.entries.len()
        )));
    }
    for source_entry in &source.entries {
        let target_entry = target_by_path.get(&source_entry.path).ok_or_else(|| {
            StewardError::Content(format!(
                "staged legacy target is missing path {:?}",
                source_entry.path
            ))
        })?;
        if target_entry.entry_type != source_entry.entry_type {
            return Err(StewardError::Content(format!(
                "staged legacy target path {:?} has type {:?}, expected {:?}",
                source_entry.path, target_entry.entry_type, source_entry.entry_type
            )));
        }
        match &source_entry.node {
            LegacyCapsuleNode::Symlink { target } if target_entry.child_hash != target.hash => {
                return Err(StewardError::Content(format!(
                    "staged legacy symlink {:?} hashes to {}, source target object is {}",
                    source_entry.path, target_entry.child_hash, target.hash
                )));
            }
            LegacyCapsuleNode::Dynamic { recipe } => {
                let bytes = read_legacy_object(objects_dir, recipe)?;
                let (factory, config) = decode_recipe(&bytes).map_err(|error| {
                    StewardError::Content(format!(
                        "decode source dynamic recipe for staged validation at {:?}: {error}",
                        source_entry.path
                    ))
                })?;
                let expected = recipe_hash(&factory, &config);
                if target_entry.child_hash != expected {
                    return Err(StewardError::Content(format!(
                        "staged legacy dynamic recipe {:?} hashes to {}, replay expected {}",
                        source_entry.path, target_entry.child_hash, expected
                    )));
                }
            }
            _ => {}
        }
    }

    let rows = scan_staged_physical_rows(ship).await?;
    let mut rows_by_node: BTreeMap<String, Vec<OplogEntry>> = BTreeMap::new();
    for row in rows {
        rows_by_node
            .entry(row.node_id.to_string())
            .or_default()
            .push(row);
    }
    for expected in &replay.entries {
        let target_entry = target_by_path.get(&expected.path).ok_or_else(|| {
            StewardError::Content(format!(
                "staged legacy target is missing replay path {:?}",
                expected.path
            ))
        })?;
        if target_entry.child_hash.to_hex() != expected.target_child_hash {
            return Err(StewardError::Content(format!(
                "staged legacy target path {:?} child identity is {}, replay expected {}",
                expected.path, target_entry.child_hash, expected.target_child_hash
            )));
        }
        let target_metadata: Vec<LegacyReplayMetadata> = target_entry
            .versions
            .iter()
            .map(replay_metadata_from_version_meta)
            .collect::<Result<_, _>>()?;
        if target_metadata != expected.target_entry_metadata {
            return Err(StewardError::Content(format!(
                "staged legacy target path {:?} tree metadata differs from replay report",
                expected.path
            )));
        }
        let actual_versions = rows_by_node.remove(&target_entry.node_id).ok_or_else(|| {
            StewardError::Content(format!(
                "staged legacy target path {:?} has no physical rows",
                expected.path
            ))
        })?;
        if actual_versions.len() != expected.versions.len() {
            return Err(StewardError::Content(format!(
                "staged legacy target path {:?} has {} physical versions, replay expected {}",
                expected.path,
                actual_versions.len(),
                expected.versions.len()
            )));
        }
        for (actual, expected_version) in actual_versions.iter().zip(&expected.versions) {
            verify_staged_legacy_version(expected, actual, expected_version)?;
        }
    }
    Ok(())
}

fn verify_staged_legacy_version(
    entry: &LegacyReplayEntry,
    actual: &OplogEntry,
    expected: &LegacyReplayVersion,
) -> Result<(), StewardError> {
    if actual.file_type != entry.entry_type
        || actual.version != expected.target_version
        || actual.timestamp != expected.metadata.timestamp
        || actual.min_event_time != expected.metadata.min_event_time
        || actual.max_event_time != expected.metadata.max_event_time
        || actual.extended_attributes != expected.metadata.logical_attributes
        || actual.size.and_then(|size| u64::try_from(size).ok())
            != Some(expected.target_physical_size)
    {
        return Err(StewardError::Content(format!(
            "staged legacy target path {:?} version {} metadata/size differs from replay report",
            entry.path, expected.target_version
        )));
    }
    let expected_leaf = expected.logical_leaf_hash.as_deref().map(str::to_string);
    let expected_count = (expected.logical_count > 0).then_some(
        i64::try_from(expected.logical_count).map_err(|_| {
            StewardError::Content(format!(
                "legacy replay logical count exceeds i64::MAX at {:?}",
                entry.path
            ))
        })?,
    );
    if matches!(
        entry.entry_type,
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
    ) && (actual.logical_leaf_hash != expected_leaf || actual.logical_count != expected_count)
    {
        return Err(StewardError::Content(format!(
            "staged legacy target path {:?} version {} logical leaf identity differs from replay \
             report",
            entry.path, expected.target_version
        )));
    }
    if entry.entry_type == EntryType::TablePhysicalSeries
        && actual.series_schema_fingerprint != expected.schema_fingerprint
    {
        return Err(StewardError::Content(format!(
            "staged legacy target path {:?} version {} schema fingerprint differs from replay \
             report",
            entry.path, expected.target_version
        )));
    }
    if !matches!(entry.entry_type, EntryType::FilePhysicalSeries)
        && actual.blake3.as_deref() != Some(expected.target_blob_hash.as_str())
    {
        return Err(StewardError::Content(format!(
            "staged legacy target path {:?} version {} blob identity differs from replay report",
            entry.path, expected.target_version
        )));
    }
    Ok(())
}

fn replay_metadata_from_version_meta(
    meta: &VersionMeta,
) -> Result<LegacyReplayMetadata, StewardError> {
    Ok(LegacyReplayMetadata {
        timestamp: meta.timestamp.ok_or_else(|| {
            StewardError::Content(
                "staged legacy target manifest metadata is missing its timestamp".to_string(),
            )
        })?,
        min_event_time: meta.min_event_time,
        max_event_time: meta.max_event_time,
        logical_attributes: meta.extended_attributes.clone(),
    })
}

fn target_manifest_paths(
    entries: &[ManifestEntry],
) -> Result<BTreeMap<String, ManifestEntry>, StewardError> {
    let by_id: BTreeMap<&str, &ManifestEntry> = entries
        .iter()
        .map(|entry| (entry.node_id.as_str(), entry))
        .collect();
    if by_id.len() != entries.len() {
        return Err(StewardError::Content(
            "staged target manifest contains duplicate node IDs".to_string(),
        ));
    }
    let roots: Vec<&ManifestEntry> = entries
        .iter()
        .filter(|entry| entry.parent_node_id.is_empty() && entry.name.is_empty())
        .collect();
    if roots.len() != 1 {
        return Err(StewardError::Content(
            "staged target manifest must have exactly one root".to_string(),
        ));
    }
    let root_id = roots[0].node_id.clone();
    let mut paths = BTreeMap::new();
    let _ = paths.insert(root_id.clone(), "/".to_string());
    fn resolve(
        node_id: &str,
        root_id: &str,
        by_id: &BTreeMap<&str, &ManifestEntry>,
        paths: &mut BTreeMap<String, String>,
        visiting: &mut Vec<String>,
    ) -> Result<String, StewardError> {
        if let Some(path) = paths.get(node_id) {
            return Ok(path.clone());
        }
        if visiting.iter().any(|value| value == node_id) {
            return Err(StewardError::Content(
                "staged target manifest contains a parent cycle".to_string(),
            ));
        }
        let entry = by_id.get(node_id).ok_or_else(|| {
            StewardError::Content(format!(
                "staged target manifest is missing node {node_id:?}"
            ))
        })?;
        if entry.parent_node_id.is_empty() || entry.name.is_empty() {
            return Err(StewardError::Content(format!(
                "staged target manifest node {node_id:?} is disconnected"
            )));
        }
        if entry.name.contains('/') || matches!(entry.name.as_str(), "." | "..") {
            return Err(StewardError::Content(format!(
                "staged target manifest node {node_id:?} has unsafe name {:?}",
                entry.name
            )));
        }
        visiting.push(node_id.to_string());
        let parent = resolve(&entry.parent_node_id, root_id, by_id, paths, visiting)?;
        let _ = visiting.pop();
        let path = if entry.parent_node_id == root_id {
            format!("/{}", entry.name)
        } else {
            format!("{parent}/{}", entry.name)
        };
        let _ = paths.insert(node_id.to_string(), path.clone());
        Ok(path)
    }
    for entry in entries {
        let _ = resolve(
            &entry.node_id,
            &root_id,
            &by_id,
            &mut paths,
            &mut Vec::new(),
        )?;
    }
    let mut by_path = BTreeMap::new();
    for entry in entries {
        let path = paths
            .get(&entry.node_id)
            .expect("every target manifest node resolved")
            .clone();
        if by_path.insert(path.clone(), entry.clone()).is_some() {
            return Err(StewardError::Content(format!(
                "staged target manifest resolves multiple nodes to {path:?}"
            )));
        }
    }
    Ok(by_path)
}

async fn scan_staged_physical_rows(ship: &Ship) -> Result<Vec<OplogEntry>, StewardError> {
    let context = SessionContext::new();
    let _ = context
        .register_table(
            "legacy_replay_rows",
            Arc::new(ship.data_persistence().table().clone()),
        )
        .map_err(|error| StewardError::DeltaLake(error.to_string()))?;
    let pond_id = ship.control_table().pond_id_uuid().to_string();
    let sql = format!(
        "SELECT part_id, node_id, file_type, timestamp, version, \
         arrow_cast(NULL, 'Binary') AS content, blake3, size, min_event_time, max_event_time, \
         extended_attributes, factory, format, txn_seq, pond_id, \
         arrow_cast(NULL, 'Binary') AS bao_outboard, collapsed_through, collapsed_from, \
         logical_leaf_hash, logical_count, series_schema_fingerprint \
         FROM legacy_replay_rows WHERE pond_id = '{}' \
         ORDER BY node_id, version",
        pond_id.replace('\'', "''")
    );
    let batches = context
        .sql(&sql)
        .await
        .map_err(|error| StewardError::DeltaLake(error.to_string()))?
        .collect()
        .await
        .map_err(|error| StewardError::DeltaLake(error.to_string()))?;
    let mut rows = Vec::new();
    for batch in &batches {
        let decoded: Vec<OplogEntry> = serde_arrow::from_record_batch(batch)
            .map_err(|error| StewardError::DeltaLake(error.to_string()))?;
        rows.extend(decoded.into_iter().filter(|row| {
            matches!(
                row.file_type,
                EntryType::FilePhysicalVersion
                    | EntryType::FilePhysicalSeries
                    | EntryType::TablePhysicalVersion
                    | EntryType::TablePhysicalSeries
            )
        }));
    }
    Ok(rows)
}

fn legacy_object_path(objects_dir: &Path, object: &LegacyCapsuleObject) -> PathBuf {
    objects_dir.join(format!("blake3={}", object.hash.to_hex()))
}

fn verify_legacy_object_file(
    objects_dir: &Path,
    object: &LegacyCapsuleObject,
) -> Result<(), StewardError> {
    stream_verified_legacy_object(objects_dir, object, |_| Ok(()))
}

fn read_legacy_object(
    objects_dir: &Path,
    object: &LegacyCapsuleObject,
) -> Result<Vec<u8>, StewardError> {
    let path = legacy_object_path(objects_dir, object);
    let bytes = std::fs::read(&path).map_err(|error| {
        StewardError::Content(format!(
            "read legacy capsule object {}: {error}",
            object.hash
        ))
    })?;
    if ObjectHash::of_bytes(&bytes) != object.hash || bytes.len() as u64 != object.size {
        return Err(StewardError::Content(format!(
            "legacy capsule object {} changed after preflight verification",
            object.hash
        )));
    }
    Ok(bytes)
}

fn stream_verified_legacy_object<F>(
    objects_dir: &Path,
    object: &LegacyCapsuleObject,
    mut consume: F,
) -> Result<(), StewardError>
where
    F: FnMut(&[u8]) -> Result<(), StewardError>,
{
    let path = legacy_object_path(objects_dir, object);
    let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
        StewardError::Content(format!(
            "inspect legacy capsule object {}: {error}",
            object.hash
        ))
    })?;
    if !metadata.file_type().is_file() {
        return Err(StewardError::Content(format!(
            "legacy capsule object {} is not a regular file",
            object.hash
        )));
    }
    let mut file = File::open(&path).map_err(|error| {
        StewardError::Content(format!(
            "open legacy capsule object {}: {error}",
            object.hash
        ))
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let count = std::io::Read::read(&mut file, &mut buffer).map_err(|error| {
            StewardError::Content(format!(
                "read legacy capsule object {}: {error}",
                object.hash
            ))
        })?;
        if count == 0 {
            break;
        }
        let chunk = &buffer[..count];
        let _ = hasher.update(chunk);
        size = size
            .checked_add(count as u64)
            .ok_or_else(|| StewardError::Content("legacy object size overflow".to_string()))?;
        consume(chunk)?;
    }
    let computed = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
    if computed != object.hash || size != object.size {
        return Err(StewardError::Content(format!(
            "legacy capsule object {} has hash {computed} and size {size}, expected size {}",
            object.hash, object.size
        )));
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<(ObjectHash, u64), StewardError> {
    let mut file = File::open(path).map_err(|error| {
        StewardError::Content(format!(
            "open prepared replay payload {}: {error}",
            path.display()
        ))
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let count = std::io::Read::read(&mut file, &mut buffer).map_err(|error| {
            StewardError::Content(format!(
                "read prepared replay payload {}: {error}",
                path.display()
            ))
        })?;
        if count == 0 {
            break;
        }
        let _ = hasher.update(&buffer[..count]);
        size = size
            .checked_add(count as u64)
            .ok_or_else(|| StewardError::Content("prepared payload size overflow".to_string()))?;
    }
    Ok((ObjectHash::from_bytes(*hasher.finalize().as_bytes()), size))
}

fn count_legacy_kinds(manifest: &LegacyCapsuleManifest) -> (usize, usize, usize, usize) {
    let mut directories = 0;
    let mut physical = 0;
    let mut symlinks = 0;
    let mut dynamic = 0;
    for entry in &manifest.entries {
        match entry.node {
            LegacyCapsuleNode::Directory => directories += 1,
            LegacyCapsuleNode::Physical { .. } => physical += 1,
            LegacyCapsuleNode::Symlink { .. } => symlinks += 1,
            LegacyCapsuleNode::Dynamic { .. } => dynamic += 1,
        }
    }
    (directories, physical, symlinks, dynamic)
}

fn promote_staging(staging: &Path, target: &Path, parent: &Path) -> Result<(), StewardError> {
    sync_tree(staging).map_err(|error| {
        StewardError::Content(format!(
            "sync verified staged pond at {} before promotion (left in place for inspection): \
             {error}",
            staging.display()
        ))
    })?;
    rename_no_replace(staging, target).map_err(|error| {
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
    use super::{
        canonicalize_table_batch, legacy_target_schema, normalize_legacy_table_batch,
        rename_no_replace,
    };
    use arrow_array::{
        DictionaryArray, RecordBatch, StringArray, StringViewArray, types::UInt16Type,
    };
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

    #[test]
    fn legacy_target_normalizes_utf8_view_without_touching_source_protocols() {
        let view_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8View,
            false,
        )]));
        let batch = RecordBatch::try_new(
            view_schema.clone(),
            vec![Arc::new(StringViewArray::from(vec!["view"]))],
        )
        .unwrap();
        let target_schema = legacy_target_schema(view_schema.as_ref()).unwrap();
        assert_eq!(target_schema.field(0).data_type(), &DataType::Utf8);
        let normalized =
            normalize_legacy_table_batch(batch, &target_schema, "/legacy-table").unwrap();
        assert_eq!(normalized.schema(), target_schema);
        assert_eq!(
            normalized
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "view"
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
    write_provenance_fields(
        staging,
        IMPORT_PROVENANCE_FORMAT,
        &manifest.source.pond_id,
        &manifest.source.birthplace,
        manifest.source.source_tip,
        capsule_root_hash,
    )
}

fn write_legacy_provenance(
    staging: &Path,
    manifest: &LegacyCapsuleManifest,
    capsule_root_hash: ObjectHash,
) -> Result<(), StewardError> {
    write_provenance_fields(
        staging,
        LEGACY_IMPORT_PROVENANCE_FORMAT,
        &manifest.source.pond_id,
        &manifest.source.birthplace,
        manifest.source.source_tip,
        capsule_root_hash,
    )
}

fn write_provenance_fields(
    staging: &Path,
    format: &str,
    source_pond_id: &str,
    source_birthplace: &str,
    source_tip: ObjectHash,
    capsule_root_hash: ObjectHash,
) -> Result<(), StewardError> {
    let provenance = CapsuleImportProvenance {
        format: format.to_string(),
        source_pond_id: source_pond_id.to_string(),
        source_birthplace: source_birthplace.to_string(),
        source_tip: source_tip.to_hex(),
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
        return finalize_writer(parent_wd, name, writer, entry_type, &VersionMeta::default()).await;
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
        finalize_writer(parent_wd, name, writer, entry_type, &VersionMeta::default()).await?;
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
                    finalize_writer(
                        parent_wd,
                        name,
                        writer,
                        entry_type,
                        &leaf_meta(&leaves[leaf_index]),
                    )
                    .await?;
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
    finalize_writer(
        parent_wd,
        name,
        writer,
        entry_type,
        &leaf_meta(&leaves[leaf_index]),
    )
    .await
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
    let mut pending: Option<ArrowWriter<File>> = None;
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
                    if pending.is_none() {
                        let file = tempfile::tempfile().map_err(|error| {
                            StewardError::Content(format!(
                                "create temporary Parquet leaf for {name:?}: {error}"
                            ))
                        })?;
                        let properties = WriterProperties::builder().build();
                        pending = Some(
                            ArrowWriter::try_new(
                                file,
                                schema
                                    .as_ref()
                                    .expect("schema established by first object")
                                    .clone(),
                                Some(properties),
                            )
                            .map_err(|error| {
                                StewardError::Content(format!(
                                    "open Parquet encoder for {name:?} leaf: {error}"
                                ))
                            })?,
                        );
                    }
                    pending
                        .as_mut()
                        .expect("leaf writer initialized")
                        .write(&batch.slice(offset, take))
                        .map_err(|error| {
                            StewardError::Content(format!(
                                "encode Parquet rows for {name:?} leaf: {error}"
                            ))
                        })?;
                }
                collected += take as u64;
                offset += take;
                if collected == leaf.logical_count {
                    write_table_leaf(
                        parent_wd,
                        name,
                        entry_type,
                        pending.take().expect("non-empty leaf has a writer"),
                        leaf,
                    )
                    .await?;
                    collected = 0;
                    leaf_index += 1;
                }
            }
        }
    }
    if leaf_index != leaves.len() || collected != 0 || pending.is_some() {
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
    arrow_writer: ArrowWriter<File>,
    leaf: &CapsuleLeaf,
) -> Result<(), StewardError> {
    let mut file = arrow_writer.into_inner().map_err(|error| {
        StewardError::Content(format!("finish Parquet encoder for {name:?} leaf: {error}"))
    })?;
    let _ = file.seek(SeekFrom::Start(0)).map_err(|error| {
        StewardError::Content(format!(
            "rewind temporary Parquet leaf for {name:?}: {error}"
        ))
    })?;
    let mut file = tokio::fs::File::from_std(file);
    let mut writer = parent_wd
        .async_writer_path_with_type(name, entry_type)
        .await?;
    let _ = tokio::io::copy(&mut file, &mut writer)
        .await
        .map_err(|error| {
            StewardError::Content(format!("write Parquet bytes for {name:?} leaf: {error}"))
        })?;
    finalize_writer(parent_wd, name, writer, entry_type, &leaf_meta(leaf)).await
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
                if source.format == rebuilt.format && want != got {
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
                let compatible_logical_match = if source.format == rebuilt.format {
                    want_kind == got_kind
                        && want_schema == got_schema
                        && want_root == got_root
                        && want_leaves == got_leaves
                } else {
                    want_kind == got_kind
                        && want_leaves.len() == got_leaves.len()
                        && want_leaves.iter().zip(got_leaves).all(|(want, got)| {
                            want.logical_count == got.logical_count
                                && want.source_timestamp == got.source_timestamp
                                && want.min_event_time == got.min_event_time
                                && want.max_event_time == got.max_event_time
                                && want.logical_attributes == got.logical_attributes
                        })
                };
                if !compatible_logical_match {
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
