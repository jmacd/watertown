// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Generic staged import: materialize a downloaded `pondcapsule.4` into a
//! brand-new pond (`docs/recovery-capsule-design.md`, "Generic staged import").
//!
//! [`import_capsule`] implements the staged, resumable import:
//!
//! 1. the target must be UTF-8 and must not exist; an exclusive ownership
//!    intent is persisted beside a private initialization container;
//! 2. a fresh pond identity is minted in that container's `pond/` child and
//!    published to the deterministic staging path only after its first durable
//!    checkpoint;
//! 3. immutable provenance (source pond, source tip, capsule root, importer
//!    version) is written alongside the staged pond's `data/`/`control/`, not
//!    inside its tinyfs namespace;
//! 4. every write transaction used while staging suppresses post-commit
//!    factory execution and remote auto-push
//!    ([`crate::Ship::begin_write_suppressed`]), so a capsule that carries
//!    `/system/run/*` configs or `/sys/remotes/*` attachments cannot dispatch
//!    before the operator seals and unsuppresses the target;
//! 5. entries are recreated in the capsule's canonical path order, which is
//!    already parent-before-child;
//! 6. deterministic batches contain at most
//!    [`DEFAULT_IMPORT_BATCH_UNITS`] entry/leaf units and
//!    [`DEFAULT_IMPORT_BATCH_LOGICAL_COUNT`] logical bytes/rows (except that
//!    one indivisible logical leaf is always allowed);
//! 7. a durable journal beside `data/` and `control/` records the capsule
//!    root, import parameters, exact entry/leaf cursor, and an in-flight
//!    transaction sequence. A retry locates the root-addressed staging
//!    directory and reconciles a commit that landed before its journal
//!    checkpoint, so a series leaf is never appended twice;
//! 8. file bytes and decoded table rows are streamed leaf-by-leaf rather than
//!    buffered whole;
//! 9. series leaves are recreated in original order carrying their
//!    `source_timestamp`/event-time bounds/logical attributes as
//!    [`sync_store::VersionMeta`], reusing the same version-finalization
//!    helper the content-addressed puller uses
//!    ([`crate::content_pull::finalize_writer`]);
//! 10. the staged pond is re-read from scratch with the existing capsule
//!     builder ([`crate::build_recovery_capsule`]) and compared against the
//!     source manifest by logical projection (paths, entry types, payload
//!     kind, schema fingerprint, and ordered leaves -- deliberately ignoring
//!     `source_node_id` and physical object boundaries, which the design
//!     explicitly allows to change); and
//!
//! The staging directory is renamed atomically onto the target only after
//! that comparison succeeds. The promoted pond remains persistently inert.
//! [`activate_capsule_import`] separately inspects restored
//! `/sys/remotes/*` attachments and `/system/run/*` dynamic configs without
//! executing factories, validates their modes and configuration, checks
//! remote identities, and enables dispatch only after every check succeeds.
//!
//! # Errors
//!
//! On any failure after the staging directory is created, the staging
//! directory is deliberately left in place (never silently removed) so the
//! operator can inspect it, resume by hand, or delete it once the capsule and
//! source have been re-checked. The error message always names the staging
//! path.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use sync_store::{
    CapsuleEntry, CapsuleLeaf, CapsuleManifest, CapsuleNode, CapsuleObject, CapsulePayloadKind,
    ObjectHash, VersionMeta, canonicalize_schema, decode_recipe, read_capsule_manifest,
    schema_fingerprint, verify_capsule_payload_directory,
};
use tinyfs::{EntryType, WD};
use tokio::io::AsyncWriteExt;

use crate::content_pull::finalize_writer;
use crate::control_table::{POST_COMMIT_DISPATCH_SETTING, POST_COMMIT_DISPATCH_SUPPRESSED};
use crate::{
    PondUserMetadata, REMOTE_MODE_PREFIX, REMOTE_MOUNT_PATH_PREFIX, RemoteAttachment, RemoteMode,
    SYS_REMOTES_DIR, Ship, StewardError,
};

/// Provenance filename written at the pond directory's top level (a sibling
/// of `data/` and `control/`), so it survives the atomic rename to the
/// target without becoming a tinyfs namespace entry that the post-write
/// logical comparison would have to special-case.
const IMPORT_PROVENANCE_FILE: &str = "CAPSULE_IMPORT_PROVENANCE.json";
const IMPORT_PROVENANCE_FORMAT: &str = "pondcapsule.4-import-provenance.1";
const IMPORT_JOURNAL_PREFIX: &str = "CAPSULE_IMPORT_JOURNAL.";
const IMPORT_JOURNAL_STAGING_PREFIX: &str = ".CAPSULE_IMPORT_CHECKPOINT_STAGING.";
const IMPORT_JOURNAL_FORMAT: &str = "pondcapsule.4-import-journal.1";
const IMPORT_INIT_INTENT_FORMAT: &str = "pondcapsule.4-import-init.1";
const IMPORT_INIT_INTENT_SUFFIX: &str = ".json";
const IMPORT_INIT_CONTAINER_SUFFIX: &str = ".work";
const IMPORT_INIT_CHILD: &str = "pond";
const POST_COMMIT_DISPATCH_ENABLED: &str = "enabled";
const SYSTEM_RUN_DIR: &str = "/system/run";

/// Default maximum number of manifest-entry or logical-leaf units committed
/// by one staged import transaction.
pub const DEFAULT_IMPORT_BATCH_UNITS: usize = 64;
/// Default maximum sum of logical byte/row counts in one staged import
/// transaction. One indivisible leaf larger than this limit is committed
/// alone.
pub const DEFAULT_IMPORT_BATCH_LOGICAL_COUNT: u64 = 64 * 1024 * 1024;

/// Deterministic transaction limits for a staged capsule import.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapsuleImportLimits {
    pub max_units: usize,
    pub max_logical_count: u64,
}

impl Default for CapsuleImportLimits {
    fn default() -> Self {
        Self {
            max_units: DEFAULT_IMPORT_BATCH_UNITS,
            max_logical_count: DEFAULT_IMPORT_BATCH_LOGICAL_COUNT,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct ImportCursor {
    entry_index: usize,
    part_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PendingBatch {
    from: ImportCursor,
    to: ImportCursor,
    txn_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CapsuleImportJournal {
    format: String,
    capsule_root: String,
    target: String,
    birthplace: String,
    limits: CapsuleImportLimits,
    cursor: ImportCursor,
    completed_batches: usize,
    last_committed_txn_seq: i64,
    pending: Option<PendingBatch>,
    verified: bool,
    checkpoint_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CapsuleInitializationIntent {
    format: String,
    token: String,
    capsule_root: String,
    target: String,
    birthplace: String,
}

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
    /// Number of bounded write transactions committed during staging.
    pub batches: usize,
    /// Physical file payload opens performed while importing logical leaves.
    pub file_payload_opens: usize,
    /// Indexed physical file-object ranges examined while selecting payloads.
    ///
    /// Binary-searching the first overlap keeps this proportional to logical
    /// leaves plus actual overlaps rather than leaves times objects.
    pub file_object_range_checks: usize,
    /// Parquet payload opens performed by table import planning and execution.
    ///
    /// A complete pass is bounded by one metadata open per physical object
    /// plus one data open per object that overlaps an imported logical leaf.
    pub table_payload_opens: usize,
    /// Indexed physical-object ranges examined while selecting table payloads.
    ///
    /// Binary-searching the first overlapping object keeps this proportional
    /// to logical leaves plus actual overlaps rather than leaves times objects.
    pub table_object_range_checks: usize,
}

/// Result of a safe activation preflight.
#[derive(Debug, Clone)]
pub struct CapsuleActivationReport {
    /// Activated pond path.
    pub target: PathBuf,
    /// Number of restored remote attachments validated.
    pub remotes: usize,
    /// Number of restored automatic factory configs validated.
    pub run_configs: usize,
}

/// Materialize a downloaded `pondcapsule.4` capsule at `capsule_dir` into a
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
    import_capsule_with_limits(
        capsule_dir,
        target,
        birthplace,
        CapsuleImportLimits::default(),
        None,
    )
    .await?
    .ok_or_else(|| {
        StewardError::Content("capsule import unexpectedly stopped before completion".to_string())
    })
}

/// Run a resumable capsule import with explicit batch limits.
///
/// `batch_budget` limits the number of batches committed by this invocation.
/// `None` runs through verification and promotion; `Some(n)` returns
/// `Ok(None)` after at most `n` new transactions and deliberately stops after
/// the final commit but before advancing its journal checkpoint. A later call
/// with the same capsule, target, birthplace, and limits reconciles that
/// in-flight marker. This primarily supports deterministic crash-window tests.
pub async fn import_capsule_with_limits(
    capsule_dir: &Path,
    target: &Path,
    birthplace: impl Into<String>,
    limits: CapsuleImportLimits,
    batch_budget: Option<usize>,
) -> Result<Option<CapsuleImportReport>, StewardError> {
    if limits.max_units == 0 || limits.max_logical_count == 0 {
        return Err(StewardError::Content(
            "capsule import batch limits must both be greater than zero".to_string(),
        ));
    }
    let parent = validate_import_target(target)?;
    import_logical_capsule(
        capsule_dir,
        target,
        parent,
        birthplace.into(),
        limits,
        batch_budget,
    )
    .await
}

fn validate_import_target(target: &Path) -> Result<&Path, StewardError> {
    if target.to_str().is_none()
        || target
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .is_none()
    {
        return Err(StewardError::Content(format!(
            "capsule import target {} is not valid UTF-8",
            target.display()
        )));
    }
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
    let parent_metadata = parent.symlink_metadata().map_err(|error| {
        StewardError::Content(format!(
            "inspect capsule import target parent {}: {error}",
            parent.display()
        ))
    })?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(StewardError::Content(format!(
            "capsule import target {}'s parent {} is not a regular directory",
            target.display(),
            parent.display()
        )));
    }
    Ok(parent)
}

async fn import_logical_capsule(
    capsule_dir: &Path,
    target: &Path,
    parent: &Path,
    birthplace: String,
    limits: CapsuleImportLimits,
    batch_budget: Option<usize>,
) -> Result<Option<CapsuleImportReport>, StewardError> {
    // Everything below only opens files under `capsule_dir` for reading: the
    // source capsule is never written to.
    let (manifest, capsule_root_hash) = read_capsule_manifest(capsule_dir)
        .map_err(|error| StewardError::Content(format!("read source capsule manifest: {error}")))?;
    let objects_dir = capsule_dir.join("recovery").join("objects");
    let verify_report = verify_capsule_payload_directory(&manifest, &objects_dir)
        .map_err(|error| StewardError::Content(format!("verify source capsule: {error}")))?;
    let mut file_payloads = FilePayloadIndex::build(&manifest)?;
    let mut table_payloads = TablePayloadIndex::build(&manifest, &objects_dir)?;

    let staging = staging_path(target, parent, capsule_root_hash)?;
    reject_conflicting_staging(parent, target, &staging)?;

    let (mut ship, mut journal) = if staging.symlink_metadata().is_ok() {
        open_resumable_staging(
            &staging,
            target,
            &birthplace,
            &manifest,
            capsule_root_hash,
            limits,
        )
        .await?
    } else {
        initialize_and_publish_staging(
            parent,
            target,
            &staging,
            &birthplace,
            &manifest,
            capsule_root_hash,
            limits,
        )
        .await?
    };

    if !ship.control_table().post_commit_dispatch_suppressed() {
        return Err(staging_error(
            &staging,
            "resume refused because persistent post-commit suppression is not set",
        ));
    }
    reconcile_pending_batch(&staging, &ship, &mut journal)?;

    let mut batches_this_run = 0usize;
    while !cursor_complete(&manifest, journal.cursor) {
        if batch_budget == Some(0) {
            return Ok(None);
        }
        let batch = plan_batch(&manifest, journal.cursor, limits)?;
        let meta = PondUserMetadata::new(vec![
            "capsule".to_string(),
            "import".to_string(),
            format!("batch-{}", journal.completed_batches + 1),
        ]);
        let tx = ship.begin_write_suppressed(&meta).await?;
        let txn_seq = tx.txn_meta().txn_seq;
        journal.pending = Some(PendingBatch {
            from: journal.cursor,
            to: batch.next,
            txn_seq,
        });
        if let Err(error) = write_journal(&staging, &mut journal) {
            return Err(tx.abort_preserving(error).await);
        }

        let root = tx.root().await?;
        let apply_result = apply_batch(
            &root,
            &manifest,
            &batch,
            &objects_dir,
            &staging,
            &mut file_payloads,
            &mut table_payloads,
        )
        .await;
        if let Err(error) = apply_result {
            return Err(tx
                .abort_preserving(staging_error(
                    &staging,
                    &format!("batch {} failed: {error}", journal.completed_batches + 1),
                ))
                .await);
        }
        _ = tx.commit().await?;
        batches_this_run += 1;
        if batch_budget.is_some_and(|budget| batches_this_run >= budget) {
            return Ok(None);
        }

        journal.cursor = batch.next;
        journal.completed_batches += 1;
        journal.last_committed_txn_seq = txn_seq;
        journal.pending = None;
        write_journal(&staging, &mut journal)?;
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
    journal.verified = true;
    write_journal(&staging, &mut journal)?;

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
    Ok(Some(CapsuleImportReport {
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
        batches: journal.completed_batches,
        file_payload_opens: file_payloads.opens,
        file_object_range_checks: file_payloads.range_checks,
        table_payload_opens: table_payloads.opens,
        table_object_range_checks: table_payloads.range_checks,
    }))
}

#[derive(Debug)]
struct RemotePreflight {
    name: String,
    attachment: RemoteAttachment,
    mode: RemoteMode,
    mount_path: Option<String>,
}

/// Validate all restored automatic dispatch configuration and enable it.
///
/// The pond must be a completed capsule import whose persistent dispatch
/// setting is still `suppressed`. The preflight reads dynamic-node recipes
/// directly from transaction state, never instantiates a factory, and opens
/// remotes read-only to verify that their identities agree with the restored
/// mode and mount semantics.
pub async fn activate_capsule_import(
    target: &Path,
) -> Result<CapsuleActivationReport, StewardError> {
    let journal = read_journal(target)?;
    if !journal.verified || journal.pending.is_some() {
        return Err(StewardError::Content(format!(
            "capsule activation refused for {}: import journal is not verified and complete",
            target.display()
        )));
    }
    let provenance_path = target.join(IMPORT_PROVENANCE_FILE);
    let provenance: CapsuleImportProvenance =
        serde_json::from_slice(&std::fs::read(&provenance_path).map_err(|error| {
            StewardError::Content(format!(
                "capsule activation cannot read provenance {}: {error}",
                provenance_path.display()
            ))
        })?)
        .map_err(|error| {
            StewardError::Content(format!(
                "capsule activation cannot decode provenance {}: {error}",
                provenance_path.display()
            ))
        })?;
    if provenance.format != IMPORT_PROVENANCE_FORMAT
        || provenance.capsule_root != journal.capsule_root
    {
        return Err(StewardError::Content(format!(
            "capsule activation refused for {}: provenance and import journal disagree",
            target.display()
        )));
    }

    let mut ship = Ship::open_pond(target).await?;
    if !ship.control_table().post_commit_dispatch_suppressed() {
        return Err(StewardError::Content(format!(
            "capsule activation refused for {}: post-commit dispatch is not suppressed; \
             refusing to certify an already-active pond",
            target.display()
        )));
    }

    let (remotes, run_configs) = inspect_automatic_configs(&mut ship).await?;
    validate_remote_preflight(&mut ship, &remotes).await?;

    ship.control_table_mut()
        .set_setting(POST_COMMIT_DISPATCH_SETTING, POST_COMMIT_DISPATCH_ENABLED)
        .await
        .map_err(|error| {
            StewardError::Content(format!(
                "capsule preflight succeeded but enabling post-commit dispatch at {} failed: \
                 {error}",
                target.display()
            ))
        })?;

    Ok(CapsuleActivationReport {
        target: target.to_path_buf(),
        remotes: remotes.len(),
        run_configs,
    })
}

async fn inspect_automatic_configs(
    ship: &mut Ship,
) -> Result<(Vec<RemotePreflight>, usize), StewardError> {
    let meta = PondUserMetadata::new(vec![
        "capsule".to_string(),
        "activation-preflight".to_string(),
    ]);
    let tx = ship.begin_read(&meta).await?;
    let result = async {
        let root = tx.root().await?;
        let mut remotes = Vec::new();
        if root.resolve_path(SYS_REMOTES_DIR).await.is_ok() {
            let mut matches = root
                .collect_matches(&format!("{SYS_REMOTES_DIR}/*"))
                .await?;
            matches.sort_by_key(|entry| entry.0.path());
            for (node_path, _) in matches {
                let path = node_path.path();
                let name = path
                    .file_name()
                    .and_then(|part| part.to_str())
                    .ok_or_else(|| {
                        StewardError::Content(format!(
                            "capsule activation found a non-UTF-8 remote attachment name at {}",
                            path.display()
                        ))
                    })?
                    .to_string();
                validate_remote_name(&name)?;
                if node_path.entry_type() != EntryType::FilePhysicalVersion {
                    return Err(StewardError::Content(format!(
                        "capsule activation refuses remote attachment {} with entry type {}; \
                         expected file:physical:version",
                        path.display(),
                        node_path.entry_type()
                    )));
                }
                let bytes = root.read_file_path_to_vec(&path).await?;
                let attachment = RemoteAttachment::from_yaml_bytes(&bytes).map_err(|error| {
                    StewardError::Content(format!(
                        "capsule activation cannot parse remote attachment {}: {error}",
                        path.display()
                    ))
                })?;
                let mode_value = tx
                    .control_table()
                    .raw_config_get(&format!("{REMOTE_MODE_PREFIX}{name}"))
                    .await?
                    .unwrap_or_else(|| RemoteMode::Push.as_str().to_string());
                let mode = RemoteMode::parse(&mode_value).map_err(|error| {
                    StewardError::Content(format!(
                        "capsule activation found invalid mode for remote `{name}`: {error}"
                    ))
                })?;
                let mount_path = tx
                    .control_table()
                    .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}{name}"))
                    .await?
                    .filter(|value| !value.is_empty());
                remotes.push(RemotePreflight {
                    name,
                    attachment,
                    mode,
                    mount_path,
                });
            }
        }

        let mut run_configs = 0usize;
        if root.resolve_path(SYSTEM_RUN_DIR).await.is_ok() {
            let mut matches = root.collect_matches("/system/run/*").await?;
            matches.sort_by_key(|entry| entry.0.path());
            for (node_path, _) in matches {
                let path = node_path.path();
                if !node_path.entry_type().is_dynamic() {
                    return Err(StewardError::Content(format!(
                        "capsule activation refuses non-dynamic automatic config {} (type {})",
                        path.display(),
                        node_path.entry_type()
                    )));
                }
                let (factory, config) = tx
                    .get_dynamic_node_config(node_path.id())
                    .await
                    .map_err(StewardError::DataInit)?
                    .ok_or_else(|| {
                        StewardError::Content(format!(
                            "capsule activation cannot read dynamic config {}",
                            path.display()
                        ))
                    })?;
                validate_run_factory(&path, node_path.entry_type(), &factory, &config)?;
                let mode = tx
                    .control_table()
                    .get_factory_mode(&factory)
                    .unwrap_or_else(|| "push".to_string());
                if mode != "push" && mode != "pull" {
                    return Err(StewardError::Content(format!(
                        "capsule activation found invalid mode {mode:?} for factory `{factory}` \
                         at {}; expected `push` or `pull`",
                        path.display()
                    )));
                }
                run_configs += 1;
            }
        }
        Ok((remotes, run_configs))
    }
    .await;
    let commit_result = tx.commit().await;
    match result {
        Ok(value) => {
            _ = commit_result?;
            Ok(value)
        }
        Err(error) => {
            let _ = commit_result;
            Err(error)
        }
    }
}

fn validate_remote_name(name: &str) -> Result<(), StewardError> {
    if name.is_empty()
        || name.contains('/')
        || name.starts_with('.')
        || !name.chars().all(|character| character.is_ascii_graphic())
    {
        return Err(StewardError::Content(format!(
            "capsule activation found invalid remote name {name:?}; names must be one visible \
             ASCII path segment and must not start with `.`"
        )));
    }
    Ok(())
}

fn validate_run_factory(
    path: &Path,
    entry_type: EntryType,
    factory: &str,
    config: &[u8],
) -> Result<(), StewardError> {
    provider::FactoryRegistry::validate_raw_config(factory, config).map_err(|error| {
        StewardError::Content(format!(
            "capsule activation rejected automatic config {} for factory `{factory}`: {error}",
            path.display()
        ))
    })?;
    let config_text = std::str::from_utf8(config).map_err(|error| {
        StewardError::Content(format!(
            "capsule activation rejected automatic config {}: not UTF-8: {error}",
            path.display()
        ))
    })?;
    let expanded =
        utilities::env_substitution::substitute_env_vars(config_text).map_err(|error| {
            StewardError::Content(format!(
                "capsule activation rejected automatic config {}: environment expansion failed: \
             {error}",
                path.display()
            ))
        })?;
    let _ = provider::FactoryRegistry::validate_config(factory, expanded.as_bytes()).map_err(
        |error| {
            StewardError::Content(format!(
                "capsule activation rejected automatic config {} for factory `{factory}`: {error}",
                path.display()
            ))
        },
    )?;
    let creates_directory =
        provider::FactoryRegistry::factory_creates_directory(factory).map_err(|error| {
            StewardError::Content(format!(
                "capsule activation rejected automatic config {} for factory `{factory}`: {error}",
                path.display()
            ))
        })?;
    if creates_directory != entry_type.is_directory() {
        return Err(StewardError::Content(format!(
            "capsule activation rejected automatic config {}: factory `{factory}` creates {}, \
             but the restored node type is {}",
            path.display(),
            if creates_directory {
                "a directory"
            } else {
                "a file"
            },
            entry_type
        )));
    }
    Ok(())
}

async fn validate_remote_preflight(
    ship: &mut Ship,
    remotes: &[RemotePreflight],
) -> Result<(), StewardError> {
    let local_pond_id = ship.control_table().pond_id_uuid();
    let mut pull_mounts = std::collections::HashMap::<String, String>::new();
    for remote in remotes {
        let url = remote.attachment.url.as_str();
        let _ = url::Url::parse(url).map_err(|error| {
            StewardError::Content(format!(
                "capsule activation found invalid URL for remote `{}`: {url:?}: {error}",
                remote.name
            ))
        })?;
        if !remote.attachment.secret_access_key.is_empty()
            && !utilities::env_substitution::has_env_refs(&remote.attachment.secret_access_key)
        {
            return Err(StewardError::Content(format!(
                "capsule activation refuses remote `{}` because secret_access_key is literal; \
                 reattach it with an `${{env:VAR}}` reference",
                remote.name
            )));
        }
        let mount = match remote.mode {
            RemoteMode::Pull => {
                let mount = remote.mount_path.as_deref().ok_or_else(|| {
                    StewardError::Content(format!(
                        "capsule activation refuses pull remote `{}` without a mount path; \
                         reattach it with `pond remote add ... <path> --overwrite`",
                        remote.name
                    ))
                })?;
                if !mount.starts_with('/') {
                    return Err(StewardError::Content(format!(
                        "capsule activation refuses pull remote `{}` with non-absolute mount \
                         path {mount:?}",
                        remote.name
                    )));
                }
                if let Some(other) = pull_mounts.insert(mount.to_string(), remote.name.clone()) {
                    return Err(StewardError::Content(format!(
                        "capsule activation refuses pull remotes `{other}` and `{}` because both \
                         use mount path {mount:?}",
                        remote.name
                    )));
                }
                Some(mount)
            }
            RemoteMode::Push | RemoteMode::Both => {
                if remote.mount_path.is_some() {
                    return Err(StewardError::Content(format!(
                        "capsule activation refuses {} remote `{}` with a pull mount path; \
                         reattach it with `pond backup add ... --overwrite`",
                        remote.mode.as_str(),
                        remote.name
                    )));
                }
                None
            }
        };

        let storage_options = crate::storage_profile::prepare_storage(ship, &remote.attachment)
            .await
            .map_err(|error| {
                StewardError::Content(format!(
                    "capsule activation cannot prepare storage for remote `{}`: {error}",
                    remote.name
                ))
            })?;
        let limit_spec = remote.attachment.resolved_limits().map_err(|error| {
            StewardError::Content(format!(
                "capsule activation rejected limits for remote `{}`: {error}",
                remote.name
            ))
        })?;
        let _limits = crate::LimiterSet::open(ship, &limit_spec)
            .await
            .map_err(|error| {
                StewardError::Content(format!(
                    "capsule activation cannot bind limiters for remote `{}`: {error}",
                    remote.name
                ))
            })?;

        let remote_pond_id = if let Some(path) = url.strip_prefix("pond://") {
            use crate::ContentSource;
            crate::LocalPondSource::open(path)
                .await
                .map_err(|error| {
                    StewardError::Content(format!(
                        "capsule activation cannot open remote `{}` at {url}: {error}",
                        remote.name
                    ))
                })?
                .pond_id()
        } else {
            sync_store::ContentRemote::open_at_url(url, storage_options)
                .await
                .map_err(|error| {
                    StewardError::Content(format!(
                        "capsule activation cannot open remote `{}` at {url}: {error}",
                        remote.name
                    ))
                })?
                .pond_id()
        };
        let expects_same_identity = remote.mode != RemoteMode::Pull || matches!(mount, Some("/"));
        if expects_same_identity && remote_pond_id != local_pond_id {
            return Err(StewardError::Content(format!(
                "capsule activation refuses {} remote `{}`: destination pond_id {} does not \
                 match restored pond_id {}; reattach the remote to a destination initialized \
                 for this fresh pond identity",
                remote.mode.as_str(),
                remote.name,
                remote_pond_id,
                local_pond_id
            )));
        }
        if !expects_same_identity && remote_pond_id == local_pond_id {
            return Err(StewardError::Content(format!(
                "capsule activation refuses pull remote `{}` at mount {:?}: a cross-pond import \
                 must use a different pond identity",
                remote.name, mount
            )));
        }
    }
    Ok(())
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

/// Choose the capsule-root-addressed private sibling staging path.
fn staging_path(
    target: &Path,
    parent: &Path,
    capsule_root: ObjectHash,
) -> Result<PathBuf, StewardError> {
    let name = target.file_name().ok_or_else(|| {
        StewardError::Content(format!(
            "capsule import target {} has no file name component",
            target.display()
        ))
    })?;
    Ok(parent.join(format!(
        ".{}.capsule-import-{}",
        name.to_str().expect("target UTF-8 validated"),
        capsule_root.to_hex()
    )))
}

fn staging_prefix(target: &Path) -> Result<String, StewardError> {
    let name = target.file_name().ok_or_else(|| {
        StewardError::Content(format!(
            "capsule import target {} has no file name component",
            target.display()
        ))
    })?;
    Ok(format!(
        ".{}.capsule-import-",
        name.to_str().expect("target UTF-8 validated")
    ))
}

fn initialization_prefix(target: &Path) -> Result<String, StewardError> {
    let name = target.file_name().ok_or_else(|| {
        StewardError::Content(format!(
            "capsule import target {} has no file name component",
            target.display()
        ))
    })?;
    Ok(format!(
        ".{}.capsule-init-",
        name.to_str().expect("target UTF-8 validated")
    ))
}

fn find_initialization_intent(
    parent: &Path,
    target: &Path,
    capsule_root: ObjectHash,
) -> Result<Option<(PathBuf, CapsuleInitializationIntent)>, StewardError> {
    let prefix = initialization_prefix(target)?;
    let expected_prefix = format!("{prefix}{}-", capsule_root.to_hex());
    let mut matching = Vec::new();
    let mut containers = Vec::new();
    for entry in std::fs::read_dir(parent).map_err(|error| {
        StewardError::Content(format!(
            "scan {} for capsule initialization directories: {error}",
            parent.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            StewardError::Content(format!(
                "scan {} for capsule initialization directories: {error}",
                parent.display()
            ))
        })?;
        let name = entry.file_name();
        let name_bytes = name.as_encoded_bytes();
        if !name_bytes.starts_with(prefix.as_bytes()) {
            continue;
        }
        let Some(name) = name.to_str() else {
            return Err(StewardError::Content(format!(
                "non-UTF-8 capsule initialization entry {} conflicts with target {}",
                entry.path().display(),
                target.display()
            )));
        };
        if !name.starts_with(&expected_prefix) {
            return Err(StewardError::Content(format!(
                "conflicting capsule initialization entry {} already exists for target {}; \
                 inspect or remove it before importing a different capsule",
                entry.path().display(),
                target.display()
            )));
        }
        if name.ends_with(IMPORT_INIT_CONTAINER_SUFFIX) {
            require_regular_directory(&entry.path(), "capsule initialization container")?;
            containers.push(entry.path());
            continue;
        }
        if !name.ends_with(IMPORT_INIT_INTENT_SUFFIX) {
            return Err(StewardError::Content(format!(
                "unrecognized capsule initialization entry {} exists for target {}",
                entry.path().display(),
                target.display()
            )));
        }
        require_regular_file(&entry.path(), "capsule initialization intent")?;
        let bytes = std::fs::read(entry.path()).map_err(|error| {
            StewardError::Content(format!(
                "read capsule initialization intent {}: {error}",
                entry.path().display()
            ))
        })?;
        let intent: CapsuleInitializationIntent =
            serde_json::from_slice(&bytes).map_err(|error| {
                StewardError::Content(format!(
                    "decode capsule initialization intent {}: {error}",
                    entry.path().display()
                ))
            })?;
        validate_initialization_intent(&intent, target, capsule_root)?;
        matching.push((entry.path(), intent));
    }
    matching.sort_by(|left, right| left.0.cmp(&right.0));
    if matching.len() > 1 {
        return Err(StewardError::Content(format!(
            "multiple capsule initialization intents exist for target {} and capsule {}; \
             inspect them before retrying",
            target.display(),
            capsule_root
        )));
    }
    for container in containers {
        if matching
            .iter()
            .all(|(intent_path, _)| initialization_container(intent_path) != container)
        {
            return Err(StewardError::Content(format!(
                "capsule initialization container {} has no matching importer ownership intent",
                container.display()
            )));
        }
    }
    Ok(matching.pop())
}

fn require_regular_file(path: &Path, what: &str) -> Result<(), StewardError> {
    let metadata = path.symlink_metadata().map_err(|error| {
        StewardError::Content(format!("inspect {what} {}: {error}", path.display()))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(StewardError::Content(format!(
            "{what} {} is not a regular file",
            path.display()
        )));
    }
    Ok(())
}

fn require_regular_directory(path: &Path, what: &str) -> Result<(), StewardError> {
    let metadata = path.symlink_metadata().map_err(|error| {
        StewardError::Content(format!("inspect {what} {}: {error}", path.display()))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(StewardError::Content(format!(
            "{what} {} is not a regular directory",
            path.display()
        )));
    }
    Ok(())
}

fn validate_initialization_intent(
    intent: &CapsuleInitializationIntent,
    target: &Path,
    capsule_root: ObjectHash,
) -> Result<(), StewardError> {
    let expected_target = target.to_str().expect("target UTF-8 validated");
    if intent.format != IMPORT_INIT_INTENT_FORMAT
        || intent.token.is_empty()
        || intent.capsule_root != capsule_root.to_hex()
        || intent.target != expected_target
    {
        return Err(StewardError::Content(format!(
            "capsule initialization intent does not belong to target {} and capsule {}",
            target.display(),
            capsule_root
        )));
    }
    Ok(())
}

fn write_initialization_intent(
    path: &Path,
    intent: &CapsuleInitializationIntent,
) -> Result<(), StewardError> {
    let mut bytes = serde_json::to_vec_pretty(intent).map_err(|error| {
        StewardError::Content(format!("encode capsule initialization intent: {error}"))
    })?;
    bytes.push(b'\n');
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            StewardError::Content(format!(
                "create capsule initialization intent {}: {error}",
                path.display()
            ))
        })?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| {
            StewardError::Content(format!(
                "write capsule initialization intent {}: {error}",
                path.display()
            ))
        })
}

fn initialization_container(intent_path: &Path) -> PathBuf {
    let name = intent_path
        .file_name()
        .expect("intent has file name")
        .to_str()
        .expect("initialization intent name is UTF-8")
        .strip_suffix(IMPORT_INIT_INTENT_SUFFIX)
        .expect("intent suffix");
    intent_path.with_file_name(format!("{name}{IMPORT_INIT_CONTAINER_SUFFIX}"))
}

fn initial_journal(
    ship: &Ship,
    manifest: &CapsuleManifest,
    target: &Path,
    birthplace: &str,
    capsule_root: ObjectHash,
    limits: CapsuleImportLimits,
) -> CapsuleImportJournal {
    CapsuleImportJournal {
        format: IMPORT_JOURNAL_FORMAT.to_string(),
        capsule_root: capsule_root.to_hex(),
        target: target.to_str().expect("target UTF-8 validated").to_string(),
        birthplace: birthplace.to_string(),
        limits,
        cursor: initial_cursor(manifest),
        completed_batches: 0,
        last_committed_txn_seq: ship.data_persistence().last_txn_seq(),
        pending: None,
        verified: false,
        checkpoint_seq: 0,
    }
}

async fn open_resumable_staging(
    path: &Path,
    target: &Path,
    birthplace: &str,
    manifest: &CapsuleManifest,
    capsule_root: ObjectHash,
    limits: CapsuleImportLimits,
) -> Result<(Ship, CapsuleImportJournal), StewardError> {
    require_regular_directory(path, "capsule import staging directory")?;
    let ship = Ship::open_pond(path).await.map_err(|error| {
        StewardError::Content(format!(
            "open staged pond at {} for resume: {error}",
            path.display()
        ))
    })?;
    if !has_durable_journal(path)? {
        return Err(staging_error(
            path,
            "no importer-owned durable journal exists",
        ));
    }
    let journal = read_journal(path)?;
    validate_journal(&journal, target, birthplace, capsule_root, limits)?;
    validate_initialization_provenance(path, manifest, capsule_root)?;
    if !ship.control_table().post_commit_dispatch_suppressed() {
        return Err(staging_error(
            path,
            "resume refused because persistent post-commit suppression is not set",
        ));
    }
    Ok((ship, journal))
}

async fn initialize_and_publish_staging(
    parent: &Path,
    target: &Path,
    staging: &Path,
    birthplace: &str,
    manifest: &CapsuleManifest,
    capsule_root: ObjectHash,
    limits: CapsuleImportLimits,
) -> Result<(Ship, CapsuleImportJournal), StewardError> {
    let init_prefix = initialization_prefix(target)?;
    let (intent_path, intent) =
        if let Some((path, intent)) = find_initialization_intent(parent, target, capsule_root)? {
            if intent.birthplace != birthplace {
                return Err(StewardError::Content(format!(
                    "capsule initialization intent {} has birthplace {:?}, requested {:?}",
                    path.display(),
                    intent.birthplace,
                    birthplace
                )));
            }
            (path, intent)
        } else {
            let token = uuid::Uuid::new_v4().to_string();
            let path = parent.join(format!(
                "{}{}-{token}{IMPORT_INIT_INTENT_SUFFIX}",
                init_prefix,
                capsule_root.to_hex()
            ));
            let intent = CapsuleInitializationIntent {
                format: IMPORT_INIT_INTENT_FORMAT.to_string(),
                token,
                capsule_root: capsule_root.to_hex(),
                target: target.to_str().expect("target UTF-8 validated").to_string(),
                birthplace: birthplace.to_string(),
            };
            write_initialization_intent(&path, &intent)?;
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    StewardError::Content(format!(
                        "sync capsule initialization intent parent {}: {error}",
                        parent.display()
                    ))
                })?;
            (path, intent)
        };
    validate_initialization_intent(&intent, target, capsule_root)?;
    let container = initialization_container(&intent_path);
    if container.symlink_metadata().is_ok() {
        require_regular_directory(&container, "capsule initialization container")?;
    } else {
        std::fs::create_dir(&container).map_err(|error| {
            StewardError::Content(format!(
                "create capsule initialization container {}: {error}",
                container.display()
            ))
        })?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                StewardError::Content(format!(
                    "sync capsule initialization container parent {}: {error}",
                    parent.display()
                ))
            })?;
    }
    let initialization = container.join(IMPORT_INIT_CHILD);
    let (ship, journal) = if initialization.symlink_metadata().is_ok() {
        require_regular_directory(&initialization, "capsule initialization pond")?;
        match Ship::open_pond(&initialization).await {
            Ok(ship) if has_durable_journal(&initialization)? => {
                drop(ship);
                open_resumable_staging(
                    &initialization,
                    target,
                    birthplace,
                    manifest,
                    capsule_root,
                    limits,
                )
                .await?
            }
            Ok(ship) => {
                finish_owned_initialization(
                    ship,
                    &initialization,
                    target,
                    birthplace,
                    manifest,
                    capsule_root,
                    limits,
                )
                .await?
            }
            Err(_) => {
                std::fs::remove_dir_all(&initialization).map_err(|error| {
                    StewardError::Content(format!(
                        "remove importer-owned partial pond {}: {error}",
                        initialization.display()
                    ))
                })?;
                create_initialized_pond(
                    &initialization,
                    target,
                    birthplace,
                    manifest,
                    capsule_root,
                    limits,
                )
                .await?
            }
        }
    } else {
        create_initialized_pond(
            &initialization,
            target,
            birthplace,
            manifest,
            capsule_root,
            limits,
        )
        .await?
    };
    sync_tree(&initialization).map_err(|error| {
        staging_error(
            &initialization,
            &format!("sync initialized pond before staging publication: {error}"),
        )
    })?;
    drop(ship);

    match rename_no_replace(&initialization, staging) {
        Ok(()) => {
            std::fs::remove_dir(&container).map_err(|error| {
                staging_error(
                    staging,
                    &format!("remove empty initialization container: {error}"),
                )
            })?;
            std::fs::remove_file(&intent_path).map_err(|error| {
                staging_error(staging, &format!("remove initialization intent: {error}"))
            })?;
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    staging_error(
                        staging,
                        &format!("sync parent after publishing initialized staging pond: {error}"),
                    )
                })?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let (ship, journal) =
                open_resumable_staging(staging, target, birthplace, manifest, capsule_root, limits)
                    .await?;
            std::fs::remove_dir_all(&initialization).map_err(|cleanup| {
                staging_error(
                    &initialization,
                    &format!("remove redundant importer-owned pond: {cleanup}"),
                )
            })?;
            std::fs::remove_dir(&container).map_err(|cleanup| {
                staging_error(
                    &container,
                    &format!("remove redundant initialization container: {cleanup}"),
                )
            })?;
            std::fs::remove_file(&intent_path).map_err(|cleanup| {
                staging_error(
                    &intent_path,
                    &format!("remove redundant initialization intent: {cleanup}"),
                )
            })?;
            return Ok((ship, journal));
        }
        Err(error) => {
            return Err(staging_error(
                &initialization,
                &format!(
                    "publish initialized staging pond at {} atomically: {error}",
                    staging.display()
                ),
            ));
        }
    }
    let ship = Ship::open_pond(staging).await.map_err(|error| {
        staging_error(
            staging,
            &format!("open atomically published staging pond: {error}"),
        )
    })?;
    Ok((ship, journal))
}

async fn create_initialized_pond(
    initialization: &Path,
    target: &Path,
    birthplace: &str,
    manifest: &CapsuleManifest,
    capsule_root: ObjectHash,
    limits: CapsuleImportLimits,
) -> Result<(Ship, CapsuleImportJournal), StewardError> {
    let ship = Ship::create_pond(initialization, birthplace.to_string())
        .await
        .map_err(|error| {
            StewardError::Content(format!(
                "create capsule initialization pond at {}: {error}",
                initialization.display()
            ))
        })?;
    finish_owned_initialization(
        ship,
        initialization,
        target,
        birthplace,
        manifest,
        capsule_root,
        limits,
    )
    .await
}

async fn finish_owned_initialization(
    mut ship: Ship,
    initialization: &Path,
    target: &Path,
    birthplace: &str,
    manifest: &CapsuleManifest,
    capsule_root: ObjectHash,
    limits: CapsuleImportLimits,
) -> Result<(Ship, CapsuleImportJournal), StewardError> {
    if ship.control_table().pond_metadata().birthplace != birthplace
        || ship.data_persistence().last_txn_seq() != 1
    {
        return Err(staging_error(
            initialization,
            "owned unjournaled pond has unexpected identity or data transactions",
        ));
    }
    ship.control_table_mut()
        .set_setting(
            POST_COMMIT_DISPATCH_SETTING,
            POST_COMMIT_DISPATCH_SUPPRESSED,
        )
        .await
        .map_err(|error| {
            StewardError::Content(format!(
                "disable post-commit dispatch for capsule initialization at {}: {error}",
                initialization.display()
            ))
        })?;
    if initialization
        .join(IMPORT_PROVENANCE_FILE)
        .symlink_metadata()
        .is_ok()
    {
        validate_initialization_provenance(initialization, manifest, capsule_root)?;
    } else {
        write_provenance(initialization, manifest, capsule_root).map_err(|error| {
            StewardError::Content(format!(
                "capsule import failed writing provenance at {} (left in place for inspection): \
                 {error}",
                initialization.display()
            ))
        })?;
    }
    let mut journal = initial_journal(&ship, manifest, target, birthplace, capsule_root, limits);
    write_journal(initialization, &mut journal)?;
    Ok((ship, journal))
}

fn reject_conflicting_staging(
    parent: &Path,
    target: &Path,
    expected: &Path,
) -> Result<(), StewardError> {
    let prefix = staging_prefix(target)?;
    for entry in std::fs::read_dir(parent).map_err(|error| {
        StewardError::Content(format!(
            "scan {} for resumable capsule imports: {error}",
            parent.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            StewardError::Content(format!(
                "scan {} for resumable capsule imports: {error}",
                parent.display()
            ))
        })?;
        if entry
            .file_name()
            .as_encoded_bytes()
            .starts_with(prefix.as_bytes())
            && entry.path() != expected
        {
            return Err(StewardError::Content(format!(
                "conflicting capsule import staging directory {} already exists for target {}; \
                 inspect or remove it before importing a different capsule",
                entry.path().display(),
                target.display()
            )));
        }
    }
    Ok(())
}

fn has_durable_journal(staging: &Path) -> Result<bool, StewardError> {
    for entry in std::fs::read_dir(staging)
        .map_err(|error| staging_error(staging, &format!("scan journal checkpoints: {error}")))?
    {
        let entry = entry.map_err(|error| {
            staging_error(staging, &format!("scan journal checkpoint entry: {error}"))
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(IMPORT_JOURNAL_PREFIX) && name.ends_with(".json") {
            return Ok(true);
        }
    }
    Ok(false)
}

fn staging_error(staging: &Path, message: &str) -> StewardError {
    StewardError::Content(format!(
        "capsule import staging at {} is not resumable: {message}; left in place for inspection",
        staging.display()
    ))
}

fn read_journal(staging: &Path) -> Result<CapsuleImportJournal, StewardError> {
    let mut checkpoints = Vec::new();
    for entry in std::fs::read_dir(staging)
        .map_err(|error| staging_error(staging, &format!("scan journal checkpoints: {error}")))?
    {
        let entry = entry.map_err(|error| {
            staging_error(staging, &format!("scan journal checkpoint entry: {error}"))
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(IMPORT_JOURNAL_STAGING_PREFIX) {
            std::fs::remove_file(entry.path()).map_err(|error| {
                staging_error(
                    staging,
                    &format!("remove abandoned journal staging file {name}: {error}"),
                )
            })?;
        } else if name.starts_with(IMPORT_JOURNAL_PREFIX) && name.ends_with(".json") {
            checkpoints.push(entry.path());
        }
    }
    let path = checkpoints
        .into_iter()
        .max()
        .ok_or_else(|| staging_error(staging, "no durable journal checkpoint exists"))?;
    let bytes = std::fs::read(&path).map_err(|error| {
        staging_error(
            staging,
            &format!("read durable journal {}: {error}", path.display()),
        )
    })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        staging_error(
            staging,
            &format!("decode durable journal {}: {error}", path.display()),
        )
    })
}

fn validate_journal(
    journal: &CapsuleImportJournal,
    target: &Path,
    birthplace: &str,
    capsule_root: ObjectHash,
    limits: CapsuleImportLimits,
) -> Result<(), StewardError> {
    if journal.format != IMPORT_JOURNAL_FORMAT {
        return Err(StewardError::Content(format!(
            "capsule import journal format {:?} is unsupported (expected {IMPORT_JOURNAL_FORMAT})",
            journal.format
        )));
    }
    let expected_target = target.to_str().expect("target UTF-8 validated");
    if journal.capsule_root != capsule_root.to_hex()
        || journal.target != expected_target
        || journal.birthplace != birthplace
        || journal.limits != limits
    {
        return Err(StewardError::Content(format!(
            "capsule import resume parameters conflict with staged journal: \
             capsule_root={} target={:?} birthplace={:?} limits={:?}; requested \
             capsule_root={} target={:?} birthplace={:?} limits={:?}",
            journal.capsule_root,
            journal.target,
            journal.birthplace,
            journal.limits,
            capsule_root,
            expected_target,
            birthplace,
            limits
        )));
    }
    Ok(())
}

fn write_journal(staging: &Path, journal: &mut CapsuleImportJournal) -> Result<(), StewardError> {
    journal.checkpoint_seq = journal
        .checkpoint_seq
        .checked_add(1)
        .ok_or_else(|| staging_error(staging, "journal checkpoint sequence overflow"))?;
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|error| staging_error(staging, &format!("encode journal: {error}")))?;
    let final_path = staging.join(format!(
        "{IMPORT_JOURNAL_PREFIX}{:020}.json",
        journal.checkpoint_seq
    ));
    let staging_path = staging.join(format!(
        "{IMPORT_JOURNAL_STAGING_PREFIX}{:020}.{}",
        journal.checkpoint_seq,
        uuid::Uuid::new_v4()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .map_err(|error| {
            staging_error(
                staging,
                &format!(
                    "create journal checkpoint staging file {}: {error}",
                    staging_path.display()
                ),
            )
        })?;
    if let Err(error) = file.write_all(&bytes).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = std::fs::remove_file(&staging_path);
        return Err(staging_error(
            staging,
            &format!("write and sync journal checkpoint staging file: {error}"),
        ));
    }
    drop(file);
    if let Err(error) = rename_no_replace(&staging_path, &final_path) {
        let _ = std::fs::remove_file(&staging_path);
        return Err(staging_error(
            staging,
            &format!(
                "publish journal checkpoint {} atomically: {error}",
                final_path.display()
            ),
        ));
    }
    File::open(staging)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| staging_error(staging, &format!("sync journal directory: {error}")))
}

fn reconcile_pending_batch(
    staging: &Path,
    ship: &Ship,
    journal: &mut CapsuleImportJournal,
) -> Result<(), StewardError> {
    let actual_seq = ship.data_persistence().last_txn_seq();
    let Some(pending) = journal.pending.clone() else {
        if actual_seq != journal.last_committed_txn_seq {
            return Err(staging_error(
                staging,
                &format!(
                    "data transaction sequence is {actual_seq}, but journal checkpoint records {}; \
                     the private staging pond was modified outside the importer",
                    journal.last_committed_txn_seq
                ),
            ));
        }
        return Ok(());
    };

    if pending.from != journal.cursor {
        return Err(staging_error(
            staging,
            "pending batch does not start at the journal cursor",
        ));
    }
    if actual_seq == pending.txn_seq {
        journal.cursor = pending.to;
        journal.completed_batches += 1;
        journal.last_committed_txn_seq = pending.txn_seq;
        journal.pending = None;
        write_journal(staging, journal)?;
        return Ok(());
    }
    if actual_seq == journal.last_committed_txn_seq {
        journal.pending = None;
        write_journal(staging, journal)?;
        return Ok(());
    }
    Err(staging_error(
        staging,
        &format!(
            "cannot reconcile pending transaction {} against staged data sequence {actual_seq} \
             (last checkpoint {})",
            pending.txn_seq, journal.last_committed_txn_seq
        ),
    ))
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

fn validate_initialization_provenance(
    staging: &Path,
    manifest: &CapsuleManifest,
    capsule_root_hash: ObjectHash,
) -> Result<(), StewardError> {
    let path = staging.join(IMPORT_PROVENANCE_FILE);
    let bytes = std::fs::read(&path).map_err(|error| {
        staging_error(
            staging,
            &format!("read unjournaled initialization provenance: {error}"),
        )
    })?;
    let provenance: CapsuleImportProvenance = serde_json::from_slice(&bytes).map_err(|error| {
        staging_error(
            staging,
            &format!("decode unjournaled initialization provenance: {error}"),
        )
    })?;
    if provenance.format != IMPORT_PROVENANCE_FORMAT
        || provenance.source_pond_id != manifest.source.pond_id
        || provenance.source_birthplace != manifest.source.birthplace
        || provenance.source_tip != manifest.source.source_tip.to_hex()
        || provenance.capsule_root != capsule_root_hash.to_hex()
    {
        return Err(staging_error(
            staging,
            "unjournaled initialization provenance does not match the requested capsule",
        ));
    }
    Ok(())
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

#[derive(Debug)]
struct ImportBatch {
    units: Vec<ImportCursor>,
    next: ImportCursor,
}

fn part_count(entry: &CapsuleEntry) -> usize {
    match &entry.node {
        CapsuleNode::Physical {
            objects, leaves, ..
        } if leaves.is_empty() => objects.len().max(1),
        CapsuleNode::Physical { leaves, .. } => leaves.len(),
        _ => 1,
    }
}

fn normalize_cursor(manifest: &CapsuleManifest, mut cursor: ImportCursor) -> ImportCursor {
    while cursor.entry_index < manifest.entries.len() {
        let entry = &manifest.entries[cursor.entry_index];
        if entry.path == "/" || cursor.part_index >= part_count(entry) {
            cursor.entry_index += 1;
            cursor.part_index = 0;
        } else {
            break;
        }
    }
    cursor
}

fn initial_cursor(manifest: &CapsuleManifest) -> ImportCursor {
    normalize_cursor(
        manifest,
        ImportCursor {
            entry_index: 0,
            part_index: 0,
        },
    )
}

fn cursor_complete(manifest: &CapsuleManifest, cursor: ImportCursor) -> bool {
    normalize_cursor(manifest, cursor).entry_index == manifest.entries.len()
}

fn unit_logical_count(entry: &CapsuleEntry, part_index: usize) -> u64 {
    match &entry.node {
        CapsuleNode::Symlink { target } => target.size,
        CapsuleNode::Dynamic { recipe, .. } => recipe.size,
        CapsuleNode::Physical {
            objects, leaves, ..
        } if leaves.is_empty() => objects.get(part_index).map_or(0, |object| object.size),
        CapsuleNode::Physical { leaves, .. } => leaves[part_index].logical_count,
        CapsuleNode::Directory => 0,
    }
}

fn advance_cursor(manifest: &CapsuleManifest, cursor: ImportCursor) -> ImportCursor {
    normalize_cursor(
        manifest,
        ImportCursor {
            entry_index: cursor.entry_index,
            part_index: cursor.part_index + 1,
        },
    )
}

fn plan_batch(
    manifest: &CapsuleManifest,
    cursor: ImportCursor,
    limits: CapsuleImportLimits,
) -> Result<ImportBatch, StewardError> {
    let mut next = normalize_cursor(manifest, cursor);
    let mut units = Vec::new();
    let mut logical_count = 0u64;
    while next.entry_index < manifest.entries.len() && units.len() < limits.max_units {
        let entry = &manifest.entries[next.entry_index];
        let cost = unit_logical_count(entry, next.part_index);
        if !units.is_empty() && logical_count.saturating_add(cost) > limits.max_logical_count {
            break;
        }
        units.push(next);
        logical_count = logical_count.saturating_add(cost);
        next = advance_cursor(manifest, next);
        if logical_count >= limits.max_logical_count {
            break;
        }
    }
    if units.is_empty() {
        return Err(StewardError::Content(format!(
            "capsule import could not plan a batch at entry {} part {}",
            cursor.entry_index, cursor.part_index
        )));
    }
    Ok(ImportBatch { units, next })
}

async fn apply_batch(
    root: &WD,
    manifest: &CapsuleManifest,
    batch: &ImportBatch,
    objects_dir: &Path,
    staging: &Path,
    file_payloads: &mut FilePayloadIndex,
    table_payloads: &mut TablePayloadIndex,
) -> Result<(), StewardError> {
    for unit in &batch.units {
        let entry = &manifest.entries[unit.entry_index];
        write_unit(
            root,
            entry,
            unit.entry_index,
            unit.part_index,
            objects_dir,
            staging,
            file_payloads,
            table_payloads,
        )
        .await?;
    }
    Ok(())
}

async fn write_unit(
    root: &WD,
    entry: &CapsuleEntry,
    entry_index: usize,
    part_index: usize,
    objects_dir: &Path,
    staging: &Path,
    file_payloads: &mut FilePayloadIndex,
    table_payloads: &mut TablePayloadIndex,
) -> Result<(), StewardError> {
    let path = entry.path.as_str();
    match &entry.node {
        CapsuleNode::Directory => {
            let _ = root.create_dir_path(path).await?;
        }
        CapsuleNode::Symlink { target } => {
            let bytes = read_object(objects_dir, target)?;
            let target_str = std::str::from_utf8(&bytes).map_err(|error| {
                StewardError::Content(format!("symlink target for {path:?} is not utf-8: {error}"))
            })?;
            let _ = root.create_symlink_path(path, target_str).await?;
        }
        CapsuleNode::Dynamic { recipe, metadata } => {
            let bytes = read_object(objects_dir, recipe)?;
            let (factory, config) = decode_recipe(&bytes).map_err(|error| {
                StewardError::Content(format!("decode dynamic recipe for {path:?}: {error}"))
            })?;
            let _ = root
                .create_dynamic_path_with_mtime(
                    path,
                    entry.entry_type,
                    &factory,
                    config,
                    metadata.as_ref().map(|metadata| metadata.timestamp),
                )
                .await?;
        }
        CapsuleNode::Physical {
            payload_kind,
            schema_fingerprint,
            objects,
            leaves,
            ..
        } => {
            if leaves.is_empty() {
                write_empty_version(
                    root,
                    path,
                    entry.entry_type,
                    objects.get(part_index),
                    objects_dir,
                )
                .await?;
            } else {
                match payload_kind {
                    CapsulePayloadKind::File => {
                        let (entries, payload_opens, range_checks) = (
                            &file_payloads.entries,
                            &mut file_payloads.opens,
                            &mut file_payloads.range_checks,
                        );
                        let file_objects = entries
                            .get(entry_index)
                            .and_then(Option::as_ref)
                            .ok_or_else(|| {
                                StewardError::Content(format!(
                                    "file {path:?} has no validated payload offset index"
                                ))
                            })?;
                        write_file_leaf(
                            root,
                            path,
                            entry.entry_type,
                            leaves,
                            part_index,
                            objects_dir,
                            file_objects,
                            payload_opens,
                            range_checks,
                        )
                        .await?;
                    }
                    CapsulePayloadKind::Table => {
                        let (entries, payload_opens, range_checks) = (
                            &table_payloads.entries,
                            &mut table_payloads.opens,
                            &mut table_payloads.range_checks,
                        );
                        let table_objects = entries
                            .get(entry_index)
                            .and_then(Option::as_ref)
                            .ok_or_else(|| {
                                StewardError::Content(format!(
                                    "table {path:?} has no validated payload offset index"
                                ))
                            })?;
                        write_table_leaf_from_capsule(
                            root,
                            path,
                            entry.entry_type,
                            leaves,
                            part_index,
                            objects_dir,
                            *schema_fingerprint,
                            staging,
                            table_objects,
                            payload_opens,
                            range_checks,
                        )
                        .await?;
                    }
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct FilePayloadObject {
    object: CapsuleObject,
    byte_start: u64,
    byte_end: u64,
}

#[derive(Debug)]
struct FilePayloadEntry {
    objects: Vec<FilePayloadObject>,
    leaf_ranges: Vec<(u64, u64)>,
}

#[derive(Debug)]
struct FilePayloadIndex {
    entries: Vec<Option<FilePayloadEntry>>,
    opens: usize,
    range_checks: usize,
}

impl FilePayloadIndex {
    fn build(manifest: &CapsuleManifest) -> Result<Self, StewardError> {
        let mut entries = Vec::with_capacity(manifest.entries.len());
        for entry in &manifest.entries {
            let CapsuleNode::Physical {
                payload_kind: CapsulePayloadKind::File,
                objects,
                leaves,
                ..
            } = &entry.node
            else {
                entries.push(None);
                continue;
            };
            if leaves.is_empty() {
                entries.push(None);
                continue;
            }
            let mut indexed = Vec::with_capacity(objects.len());
            let mut byte_start = 0u64;
            for object in objects {
                let byte_end = byte_start.checked_add(object.size).ok_or_else(|| {
                    StewardError::Content(format!(
                        "file {:?} payload byte offset overflow",
                        entry.path
                    ))
                })?;
                indexed.push(FilePayloadObject {
                    object: object.clone(),
                    byte_start,
                    byte_end,
                });
                byte_start = byte_end;
            }
            let mut leaf_ranges = Vec::with_capacity(leaves.len());
            let mut declared_bytes = 0u64;
            for leaf in leaves {
                let leaf_end = declared_bytes
                    .checked_add(leaf.logical_count)
                    .ok_or_else(|| {
                        StewardError::Content(format!(
                            "file {:?} logical byte count overflow",
                            entry.path
                        ))
                    })?;
                leaf_ranges.push((declared_bytes, leaf_end));
                declared_bytes = leaf_end;
            }
            if byte_start != declared_bytes {
                return Err(StewardError::Content(format!(
                    "file {:?} physical payloads contain {byte_start} bytes, logical leaves \
                     declare {declared_bytes}",
                    entry.path
                )));
            }
            entries.push(Some(FilePayloadEntry {
                objects: indexed,
                leaf_ranges,
            }));
        }
        Ok(Self {
            entries,
            opens: 0,
            range_checks: 0,
        })
    }
}

#[derive(Debug)]
struct TablePayloadObject {
    object: CapsuleObject,
    row_start: u64,
    row_end: u64,
    schema: Arc<Schema>,
}

#[derive(Debug)]
struct TablePayloadEntry {
    objects: Vec<TablePayloadObject>,
    leaf_ranges: Vec<(u64, u64)>,
}

#[derive(Debug)]
struct TablePayloadIndex {
    entries: Vec<Option<TablePayloadEntry>>,
    opens: usize,
    range_checks: usize,
}

impl TablePayloadIndex {
    fn build(manifest: &CapsuleManifest, objects_dir: &Path) -> Result<Self, StewardError> {
        let mut entries = Vec::with_capacity(manifest.entries.len());
        let mut opens = 0usize;
        for entry in &manifest.entries {
            let CapsuleNode::Physical {
                payload_kind: CapsulePayloadKind::Table,
                objects,
                leaves,
                ..
            } = &entry.node
            else {
                entries.push(None);
                continue;
            };
            if leaves.is_empty() {
                entries.push(None);
                continue;
            }
            let mut indexed = Vec::with_capacity(objects.len());
            let mut row_start = 0u64;
            for object in objects {
                let object_path = objects_dir.join(format!("blake3={}", object.hash.to_hex()));
                let file = File::open(&object_path).map_err(|error| {
                    StewardError::Content(format!(
                        "open capsule payload {} for {:?}: {error}",
                        object.hash, entry.path
                    ))
                })?;
                opens += 1;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
                    StewardError::Content(format!(
                        "open Parquet payload {} for {:?}: {error}",
                        object.hash, entry.path
                    ))
                })?;
                let rows =
                    u64::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
                        StewardError::Content(format!(
                            "Parquet payload {} for {:?} declares a negative row count",
                            object.hash, entry.path
                        ))
                    })?;
                let row_end = row_start.checked_add(rows).ok_or_else(|| {
                    StewardError::Content(format!(
                        "table {:?} payload row offset overflow",
                        entry.path
                    ))
                })?;
                let schema = canonicalize_schema(builder.schema().as_ref()).map_err(|error| {
                    StewardError::Content(format!(
                        "canonicalize Parquet schema for payload {} in {:?}: {error}",
                        object.hash, entry.path
                    ))
                })?;
                indexed.push(TablePayloadObject {
                    object: object.clone(),
                    row_start,
                    row_end,
                    schema,
                });
                row_start = row_end;
            }
            let mut leaf_ranges = Vec::with_capacity(leaves.len());
            let mut declared_rows = 0u64;
            for leaf in leaves {
                let leaf_end = declared_rows
                    .checked_add(leaf.logical_count)
                    .ok_or_else(|| {
                        StewardError::Content(format!(
                            "table {:?} logical row count overflow",
                            entry.path
                        ))
                    })?;
                leaf_ranges.push((declared_rows, leaf_end));
                declared_rows = leaf_end;
            }
            if row_start != declared_rows {
                return Err(StewardError::Content(format!(
                    "table {:?} physical payloads contain {row_start} rows, logical leaves declare \
                     {declared_rows}",
                    entry.path
                )));
            }
            entries.push(Some(TablePayloadEntry {
                objects: indexed,
                leaf_ranges,
            }));
        }
        Ok(Self {
            entries,
            opens,
            range_checks: 0,
        })
    }
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
/// or zero rows) -- a state `pondcapsule.4` cannot represent as a logical
/// leaf (`capsule_leaf_hash` rejects an empty leaf; see
/// `docs/recovery-capsule-design.md`). Any schema-carrying Parquet objects
/// the source still declared are copied verbatim, one per version, so at
/// least the physical schema survives; a node with no objects at all (a
/// plain empty file) gets a single empty version so its path and entry type
/// still exist.
async fn write_empty_version(
    root: &WD,
    path: &str,
    entry_type: EntryType,
    object: Option<&CapsuleObject>,
    objects_dir: &Path,
) -> Result<(), StewardError> {
    let mut writer = root.async_writer_path_with_type(path, entry_type).await?;
    if let Some(object) = object {
        let bytes = read_object(objects_dir, object)?;
        writer.write_all(&bytes).await.map_err(|error| {
            StewardError::Content(format!(
                "write empty-leaf payload {} for {path:?}: {error}",
                object.hash
            ))
        })?;
    }
    finalize_writer(root, path, writer, entry_type, &VersionMeta::default()).await
}

/// Recreate exactly one file-series logical leaf from the prevalidated
/// object/byte index, opening only physical objects that overlap this leaf.
async fn write_file_leaf(
    root: &WD,
    path: &str,
    entry_type: EntryType,
    leaves: &[CapsuleLeaf],
    leaf_index: usize,
    objects_dir: &Path,
    file: &FilePayloadEntry,
    payload_opens: &mut usize,
    range_checks: &mut usize,
) -> Result<(), StewardError> {
    let leaf = leaves.get(leaf_index).ok_or_else(|| {
        StewardError::Content(format!("file {path:?} has no logical leaf {leaf_index}"))
    })?;
    let (start, end) = file.leaf_ranges.get(leaf_index).copied().ok_or_else(|| {
        StewardError::Content(format!(
            "file {path:?} has no indexed logical byte range for leaf {leaf_index}"
        ))
    })?;
    let mut writer = root.async_writer_path_with_type(path, entry_type).await?;
    let mut copied = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    let first_object = file
        .objects
        .partition_point(|object| object.byte_end <= start);
    for object in &file.objects[first_object..] {
        *range_checks += 1;
        if object.byte_start >= end {
            break;
        }
        let overlap_start = start.max(object.byte_start);
        let overlap_end = end.min(object.byte_end);
        if overlap_start < overlap_end {
            let object_path = objects_dir.join(format!("blake3={}", object.object.hash.to_hex()));
            let mut file = File::open(&object_path).map_err(|error| {
                StewardError::Content(format!(
                    "open capsule payload {} for {path:?}: {error}",
                    object.object.hash
                ))
            })?;
            *payload_opens += 1;
            let offset = overlap_start - object.byte_start;
            let _ = file.seek(SeekFrom::Start(offset)).map_err(|error| {
                StewardError::Content(format!(
                    "seek capsule payload {} for {path:?}: {error}",
                    object.object.hash
                ))
            })?;
            let mut remaining = overlap_end - overlap_start;
            while remaining > 0 {
                let take = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(buffer.len());
                let count =
                    std::io::Read::read(&mut file, &mut buffer[..take]).map_err(|error| {
                        StewardError::Content(format!(
                            "read capsule payload {} for {path:?}: {error}",
                            object.object.hash
                        ))
                    })?;
                if count == 0 {
                    return Err(StewardError::Content(format!(
                        "capsule payload {} ended while reading leaf {leaf_index} of {path:?}",
                        object.object.hash
                    )));
                }
                writer.write_all(&buffer[..count]).await.map_err(|error| {
                    StewardError::Content(format!("write file {path:?} leaf {leaf_index}: {error}"))
                })?;
                copied += count as u64;
                remaining -= count as u64;
            }
        }
        if object.byte_end >= end {
            break;
        }
    }
    if copied != leaf.logical_count {
        return Err(StewardError::Content(format!(
            "file {path:?} leaf {leaf_index} copied {copied} bytes, expected {}",
            leaf.logical_count
        )));
    }
    finalize_writer(root, path, writer, entry_type, &leaf_meta(leaf)).await
}

/// Recreate exactly one table-series logical leaf from the prevalidated
/// object/row index, opening only physical objects that overlap this leaf.
async fn write_table_leaf_from_capsule(
    root: &WD,
    path: &str,
    entry_type: EntryType,
    leaves: &[CapsuleLeaf],
    leaf_index: usize,
    objects_dir: &Path,
    _node_schema_fingerprint: Option<ObjectHash>,
    staging: &Path,
    table: &TablePayloadEntry,
    payload_opens: &mut usize,
    range_checks: &mut usize,
) -> Result<(), StewardError> {
    let leaf = leaves.get(leaf_index).ok_or_else(|| {
        StewardError::Content(format!("table {path:?} has no logical leaf {leaf_index}"))
    })?;
    let (start, end) = table.leaf_ranges.get(leaf_index).copied().ok_or_else(|| {
        StewardError::Content(format!(
            "table {path:?} has no indexed logical row range for leaf {leaf_index}"
        ))
    })?;
    let expected = leaf.schema_fingerprint.ok_or_else(|| {
        StewardError::Content(format!(
            "table {path:?} leaf {leaf_index} has no schema fingerprint"
        ))
    })?;
    let mut collected = 0u64;
    let mut pending: Option<ArrowWriter<tempfile::NamedTempFile>> = None;

    let first_object = table
        .objects
        .partition_point(|object| object.row_end <= start);
    for object in &table.objects[first_object..] {
        *range_checks += 1;
        let object_start = object.row_start;
        let object_end = object.row_end;
        if object_start >= end {
            break;
        }
        let overlap_start = start.max(object_start);
        let overlap_end = end.min(object_end);
        if overlap_start >= overlap_end {
            continue;
        }
        let actual = schema_fingerprint(object.schema.as_ref()).map_err(|error| {
            StewardError::Content(format!(
                "fingerprint Parquet schema for payload {} in {path:?}: {error}",
                object.object.hash
            ))
        })?;
        if actual != expected {
            return Err(StewardError::Content(format!(
                "Parquet payload {} for {path:?} overlaps leaf {leaf_index} with schema \
                 {actual}, but that leaf requires {expected}",
                object.object.hash
            )));
        }
        let payload_path = objects_dir.join(format!("blake3={}", object.object.hash.to_hex()));
        let file = File::open(&payload_path).map_err(|error| {
            StewardError::Content(format!(
                "open capsule payload {} for {path:?}: {error}",
                object.object.hash
            ))
        })?;
        *payload_opens += 1;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            StewardError::Content(format!(
                "open Parquet payload {} for {path:?}: {error}",
                object.object.hash
            ))
        })?;
        let offset = usize::try_from(overlap_start - object_start).map_err(|_| {
            StewardError::Content(format!("table {path:?} object offset does not fit usize"))
        })?;
        let limit = usize::try_from(overlap_end - overlap_start).map_err(|_| {
            StewardError::Content(format!("table {path:?} object limit does not fit usize"))
        })?;
        let reader = builder
            .with_offset(offset)
            .with_limit(limit)
            .build()
            .map_err(|error| {
                StewardError::Content(format!("build Parquet reader for {path:?}: {error}"))
            })?;
        for batch in reader {
            let batch = batch.map_err(|error| {
                StewardError::Content(format!("read Parquet rows for {path:?}: {error}"))
            })?;
            let batch = canonicalize_table_batch(batch, &object.schema, path)?;
            if pending.is_none() {
                let file = tempfile::NamedTempFile::new_in(staging).map_err(|error| {
                    StewardError::Content(format!(
                        "create staged Parquet leaf for {path:?}: {error}"
                    ))
                })?;
                pending = Some(
                    ArrowWriter::try_new(
                        file,
                        object.schema.clone(),
                        Some(WriterProperties::builder().build()),
                    )
                    .map_err(|error| {
                        StewardError::Content(format!(
                            "open Parquet encoder for {path:?} leaf {leaf_index}: {error}"
                        ))
                    })?,
                );
            }
            let take = batch.num_rows();
            pending
                .as_mut()
                .expect("leaf writer initialized")
                .write(&batch)
                .map_err(|error| {
                    StewardError::Content(format!(
                        "encode Parquet rows for {path:?} leaf {leaf_index}: {error}"
                    ))
                })?;
            collected += take as u64;
        }
        if object_end >= end {
            break;
        }
    }
    if collected != leaf.logical_count {
        return Err(StewardError::Content(format!(
            "table {path:?} leaf {leaf_index} collected {collected} rows, expected {}",
            leaf.logical_count
        )));
    }
    write_table_leaf(
        root,
        path,
        entry_type,
        pending.ok_or_else(|| {
            StewardError::Content(format!(
                "table {path:?} leaf {leaf_index} produced no Parquet writer"
            ))
        })?,
        leaf,
    )
    .await
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
    arrow_writer: ArrowWriter<tempfile::NamedTempFile>,
    leaf: &CapsuleLeaf,
) -> Result<(), StewardError> {
    let mut scratch = arrow_writer.into_inner().map_err(|error| {
        StewardError::Content(format!("finish Parquet encoder for {name:?} leaf: {error}"))
    })?;
    let _ = scratch.seek(SeekFrom::Start(0)).map_err(|error| {
        StewardError::Content(format!(
            "rewind temporary Parquet leaf for {name:?}: {error}"
        ))
    })?;
    let file = scratch.reopen().map_err(|error| {
        StewardError::Content(format!("reopen staged Parquet leaf for {name:?}: {error}"))
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
            (
                CapsuleNode::Dynamic {
                    recipe: want_recipe,
                    metadata: want_metadata,
                },
                CapsuleNode::Dynamic {
                    recipe: got_recipe,
                    metadata: got_metadata,
                },
            ) => {
                if source.format == rebuilt.format
                    && (want_recipe != got_recipe || want_metadata != got_metadata)
                {
                    return Err(format!("{:?} dynamic node changed", expected.path));
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
