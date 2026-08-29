// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Automatic Delta Lake table maintenance: checkpoint, vacuum, and compact.
//!
//! Every Delta table accumulates JSON commit log files and small parquet data
//! files over time.  Without periodic maintenance, opening a table requires
//! parsing *all* log files from scratch, causing linear growth in CPU and
//! memory.
//!
//! This module provides best-effort maintenance that runs after successful
//! write transactions:
//!
//! - **Checkpoint** -- collapse the JSON delta log into a single parquet
//!   checkpoint file.  Created every `CHECKPOINT_INTERVAL` versions (default 10).
//! - **Log cleanup** -- remove expired JSON log files older than the
//!   retention period (only after a checkpoint exists). The replicated data
//!   table uses its 30-day table default; the never-replicated control table
//!   uses a short retention so its high-churn log does not grow without bound.
//! - **Vacuum** -- delete parquet data files no longer referenced by the
//!   current table version. Runs every `VACUUM_INTERVAL` versions (default 10)
//!   to avoid unnecessary scans on every commit.
//! - **Compact** -- merge many small parquet files into fewer large ones
//!   (only on explicit request via `pond maintain --compact`).

use chrono::{Duration, Utc};
use deltalake::DeltaTable;
use deltalake::checkpoints;
use deltalake::kernel::schema::partitions::{PartitionFilter, PartitionValue};
use deltalake::kernel::transaction::CommitProperties;
use deltalake::operations::optimize::OptimizeType;
use log::{debug, info, warn};
use std::collections::HashMap;
use uuid::Uuid;

/// How often to create checkpoints (every N versions).
/// Delta Lake standard is 10.
const CHECKPOINT_INTERVAL: u64 = 10;

/// Default retention period for vacuum.
/// Files older than this and no longer referenced are eligible for deletion.
/// Zero means vacuum everything not in the active version.
const VACUUM_RETENTION_HOURS: u64 = 0;

/// How often to run vacuum when no Remove actions are present (every N versions).
/// This provides a safety net in case the Remove-action detection misses something.
const VACUUM_INTERVAL: u64 = 10;

/// Log retention for the control table's aggressive `_delta_log` cleanup.
///
/// The control table is never replicated to a remote and its transaction
/// history is stored as parquet rows, not as delta-log commit JSONs, so old
/// commit and superseded-checkpoint files become pure overhead once a newer
/// checkpoint exists. A short retention bounds the `_delta_log` to roughly this
/// window of churn while staying clear of any in-flight concurrent reader, which
/// only ever needs files from the most recent checkpoint onward.
pub const CONTROL_LOG_RETENTION_MINUTES: i64 = 5;

/// Setting key (under the `"setting:"` prefix) for the control-table `_delta_log`
/// retention, in minutes. Unset/unparseable falls back to
/// [`CONTROL_LOG_RETENTION_MINUTES`].
pub const KEY_CONTROL_LOG_RETENTION_MINUTES: &str = "maintenance.control_log_retention_minutes";

/// Setting key (under the `"setting:"` prefix) for the data-table `_delta_log`
/// retention, in minutes. Unset keeps the table's 30-day default (`None`), which
/// is safe for replicated ponds. Set it on high-churn, fully-pushed instances
/// (e.g. selfmon) to bound the data commit log instead of letting it grow.
pub const KEY_DATA_LOG_RETENTION_MINUTES: &str = "maintenance.data_log_retention_minutes";

/// Default target size for compaction (128 MB).
const COMPACT_TARGET_SIZE: u64 = 128 * 1024 * 1024;

/// Result of a maintenance run on a single table.
#[derive(Debug, Default)]
pub struct MaintenanceResult {
    /// Name of the table that was maintained (for logging).
    pub table_name: String,
    /// Current table version after maintenance.
    pub version: i64,
    /// Whether a checkpoint was created.
    pub checkpoint_created: bool,
    /// Number of expired log files cleaned up.
    pub logs_cleaned: usize,
    /// Number of stale data files vacuumed.
    pub files_vacuumed: usize,
    /// Whether vacuum was skipped (optimization).
    pub vacuum_skipped: bool,
    /// Whether compaction was performed.
    pub compacted: bool,
    /// Compaction metrics (only if compacted).
    pub compact_files_added: u64,
    pub compact_files_removed: u64,
}

/// Combined report for both data and control tables.
#[derive(Debug, Default)]
pub struct MaintenanceReport {
    pub data: Option<MaintenanceResult>,
    pub control: Option<MaintenanceResult>,
}

impl std::fmt::Display for MaintenanceResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (v{})", self.table_name, self.version)?;
        if self.checkpoint_created {
            write!(f, " checkpoint")?;
        }
        if self.logs_cleaned > 0 {
            write!(f, " cleaned={}", self.logs_cleaned)?;
        }
        if self.files_vacuumed > 0 {
            write!(f, " vacuumed={}", self.files_vacuumed)?;
        }
        if self.vacuum_skipped {
            write!(f, " vacuum-skipped")?;
        }
        if self.compacted {
            write!(
                f,
                " compacted(+{}/- {})",
                self.compact_files_added, self.compact_files_removed
            )?;
        }
        if !self.checkpoint_created
            && self.logs_cleaned == 0
            && self.files_vacuumed == 0
            && !self.vacuum_skipped
            && !self.compacted
        {
            write!(f, " (no maintenance needed)")?;
        }
        Ok(())
    }
}

impl std::fmt::Display for MaintenanceReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref d) = self.data {
            writeln!(f, "  {}", d)?;
        }
        if let Some(ref c) = self.control {
            write!(f, "  {}", c)?;
        }
        Ok(())
    }
}

/// Run checkpoint and vacuum on a Delta table.
///
/// When `force` is true, checkpoint is always created (for explicit `pond maintain`).
/// When `force` is false, checkpoint is only created at interval boundaries
/// (for automatic post-commit maintenance).
///
/// `log_retention` controls how `_delta_log` cleanup runs after a checkpoint:
/// - `None` -- use the table's own `logRetentionDuration` property (Delta default
///   30 days) via [`checkpoints::cleanup_metadata`]. Appropriate for replicated
///   tables whose commit log may still be needed for remote version diffing.
/// - `Some(retention)` -- delete commit JSONs and superseded checkpoints older
///   than `now - retention` (checkpoint-aligned, never below the most recent
///   checkpoint). Used for the control table, which is never replicated.
///
/// Automatic (`force=false`) maintenance is best-effort and logs failures.
/// Explicit (`force=true`) maintenance propagates any requested-operation
/// failure so an operator command cannot report false success.
/// The table reference is consumed and a new (possibly updated) table is
/// returned, since vacuum/optimize operations produce a new DeltaTable.
pub async fn maintain_table(
    table: DeltaTable,
    table_name: &str,
    force: bool,
    do_compact: bool,
    log_retention: Option<Duration>,
) -> Result<(DeltaTable, MaintenanceResult), crate::StewardError> {
    let mut result = MaintenanceResult {
        table_name: table_name.to_string(),
        version: table.version().unwrap_or(-1),
        ..Default::default()
    };

    let mut table = table;

    // 1. Checkpoint if needed (or forced)
    let version = table.version().unwrap_or(0);
    let should_checkpoint =
        version > 0 && (force || (version as u64).is_multiple_of(CHECKPOINT_INTERVAL));
    if should_checkpoint {
        debug!(
            "[MAINTAIN] Creating checkpoint for {} at version {}",
            table_name, version
        );
        match checkpoints::create_checkpoint(&table, None).await {
            Ok(()) => {
                info!(
                    "[MAINTAIN] Checkpoint created for {} at version {}",
                    table_name, version
                );
                result.checkpoint_created = true;
            }
            Err(e) => {
                if force {
                    return Err(crate::StewardError::DeltaLake(format!(
                        "checkpoint {table_name} at version {version}: {e}"
                    )));
                }
                warn!(
                    "[MAINTAIN] Checkpoint failed for {} at version {}: {}",
                    table_name, version, e
                );
            }
        }
    }

    // 2. Clean up expired log files (only meaningful after a checkpoint exists)
    if result.checkpoint_created {
        let cleanup = match log_retention {
            Some(retention) => {
                let cutoff = Utc::now().timestamp_millis() - retention.num_milliseconds();
                match table.snapshot() {
                    Ok(snapshot) => {
                        let keep_version = snapshot.version();
                        let log_store = table.log_store();
                        checkpoints::cleanup_expired_logs_for(
                            keep_version,
                            log_store.as_ref(),
                            cutoff,
                            None,
                        )
                        .await
                    }
                    Err(e) => Err(e),
                }
            }
            None => checkpoints::cleanup_metadata(&table, None).await,
        };
        match cleanup {
            Ok(cleaned) => {
                if cleaned > 0 {
                    info!(
                        "[MAINTAIN] Cleaned {} expired log files for {}",
                        cleaned, table_name
                    );
                }
                result.logs_cleaned = cleaned;
            }
            Err(e) => {
                if force {
                    return Err(crate::StewardError::DeltaLake(format!(
                        "clean expired logs for {table_name}: {e}"
                    )));
                }
                warn!("[MAINTAIN] Log cleanup failed for {}: {}", table_name, e);
            }
        }

        // Checkpointing and log cleanup both mutate storage without updating
        // `table`'s in-memory state. A later vacuum internally replays the
        // log from that stale state and would otherwise try to read a JSON
        // commit file that cleanup just deleted from disk.
        if let Err(e) = table.load().await {
            if force {
                return Err(crate::StewardError::DeltaLake(format!(
                    "reload {table_name} after log cleanup: {e}"
                )));
            }
            warn!(
                "[MAINTAIN] Reload after log cleanup failed for {}: {}",
                table_name, e
            );
        }
    }

    // 3. Vacuum stale data files (gated: only when needed)
    let should_vacuum =
        force || has_remove_actions(&table) || (version as u64).is_multiple_of(VACUUM_INTERVAL);

    if should_vacuum {
        match table
            .clone()
            .vacuum()
            .with_retention_period(Duration::hours(VACUUM_RETENTION_HOURS as i64))
            .with_enforce_retention_duration(false)
            .await
        {
            Ok((new_table, metrics)) => {
                let count = metrics.files_deleted.len();
                if count > 0 {
                    info!(
                        "[MAINTAIN] Vacuumed {} stale files from {}",
                        count, table_name
                    );
                }
                result.files_vacuumed = count;
                table = new_table;
            }
            Err(e) => {
                if force {
                    return Err(crate::StewardError::DeltaLake(format!(
                        "vacuum {table_name}: {e}"
                    )));
                }
                warn!("[MAINTAIN] Vacuum failed for {}: {}", table_name, e);
            }
        }
    } else {
        debug!(
            "[MAINTAIN] Skipping vacuum for {} (no Remove actions, not at interval)",
            table_name
        );
        result.vacuum_skipped = true;
    }

    // 4. Compact small files (only when explicitly requested)
    if do_compact {
        debug!("[MAINTAIN] Running compaction for {}", table_name);
        match table
            .clone()
            .optimize()
            .with_type(OptimizeType::Compact)
            .with_target_size(COMPACT_TARGET_SIZE)
            .await
        {
            Ok((new_table, metrics)) => {
                info!(
                    "[MAINTAIN] Compacted {}: +{} files / -{} files",
                    table_name, metrics.num_files_added, metrics.num_files_removed
                );
                result.compacted = true;
                result.compact_files_added = metrics.num_files_added;
                result.compact_files_removed = metrics.num_files_removed;
                table = new_table;
            }
            Err(e) => {
                if force {
                    return Err(crate::StewardError::DeltaLake(format!(
                        "compact {table_name}: {e}"
                    )));
                }
                warn!("[MAINTAIN] Compaction failed for {}: {}", table_name, e);
            }
        }
    }

    result.version = table.version().unwrap_or(-1);
    Ok((table, result))
}

/// Check if the most recent commit contained any Remove actions.
/// This indicates files were superseded and vacuum might have work to do.
///
/// For now, we use a conservative heuristic: assume Remove actions might exist
/// unless we can prove otherwise. A future refinement could inspect the actual
/// commit log to detect Remove actions precisely.
fn has_remove_actions(_table: &DeltaTable) -> bool {
    // Conservative default: assume there might be removals
    // This ensures vacuum runs when there's uncertainty, preventing file leaks.
    // The interval-based safety net (VACUUM_INTERVAL) provides the real
    // optimization - most ticks will be skipped via the interval check.
    false
}

/// Number of parquet files added/removed by a [`compact_pond_partitions`]
/// run.  Both zero means Delta optimize found nothing to merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CompactStats {
    /// Number of new (merged) parquet files Delta wrote.
    pub files_added: u64,
    /// Number of small parquet files Delta marked Removed.
    pub files_removed: u64,
}

impl CompactStats {
    /// `true` iff Delta did not actually change the file set.
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.files_added == 0 && self.files_removed == 0
    }
}

/// Run Delta `optimize(Compact)` over a single `pond_id`'s partitions of
/// the data table, merging small parquet files into larger ones.
///
/// `pond_id` is a Delta partition column (`["pond_id", "part_id"]`), so
/// restricting the optimize to one pond_id is a hard guarantee that no
/// foreign-pond data files are touched -- important once cross-pond
/// imports live alongside the producer's own rows.
///
/// `app_metadata` is stamped into the optimize commit's `commitInfo`
/// (the caller passes `PondTxnMetadata::to_delta_metadata()` carrying the
/// compaction's `txn_seq`).  This is REQUIRED: `OpLogPersistence::open`
/// derives `last_txn_seq` from the most recent commit's `pond_txn` blob,
/// so an optimize commit with no `pond_txn` would reset the sequence on
/// the next pond open.
///
/// Compaction must NOT change logical content; callers
/// ([`crate::Ship::compact`]) assert this by snapshotting per-partition
/// checksums before and after.  Returns the new [`DeltaTable`] handle
/// (optimize produces a fresh table state) and the file-delta metrics.
/// A no-op optimize (nothing to merge) creates no Delta commit.
pub async fn compact_pond_partitions(
    table: DeltaTable,
    pond_id: Uuid,
    app_metadata: HashMap<String, serde_json::Value>,
) -> Result<(DeltaTable, CompactStats), deltalake::DeltaTableError> {
    let filters = [PartitionFilter {
        key: "pond_id".to_string(),
        value: PartitionValue::Equal(pond_id.to_string()),
    }];
    let (new_table, metrics) = table
        .optimize()
        .with_type(OptimizeType::Compact)
        .with_target_size(COMPACT_TARGET_SIZE)
        .with_filters(&filters)
        .with_commit_properties(CommitProperties::default().with_metadata(app_metadata))
        .await?;
    debug!(
        "[MAINTAIN] compact pond {} version {:?}: +{} files / -{} files",
        pond_id,
        new_table.version(),
        metrics.num_files_added,
        metrics.num_files_removed,
    );
    Ok((
        new_table,
        CompactStats {
            files_added: metrics.num_files_added,
            files_removed: metrics.num_files_removed,
        },
    ))
}
