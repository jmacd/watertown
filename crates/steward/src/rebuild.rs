// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond rebuild-control` (D6.3): reconstruct the control table from the
//! data Delta table alone.
//!
//! This is a disaster-recovery operation for the case where the data
//! Delta table survives but the control table is lost or corrupt (so the
//! pond can no longer be opened).  It recovers:
//!
//! * **Pond identity** -- the canonical `pond_id` from the data table's
//!   bootstrap row (via [`OpLogPersistence::peek_pond_id`]), plus a
//!   birth timestamp taken from the bootstrap commit.  The birth
//!   hostname/username are NOT carried in the data table, so they are
//!   recorded as `"unknown"`.
//! * **Transaction-log skeleton** -- one `Begin` + `DataCommitted`
//!   (plus a trailing `Completed` for the bootstrap) per write
//!   transaction found in the data Delta commit history, so `pond log`
//!   and `last_committed_seq` work again.  The original `txn_seq`,
//!   `txn_id`, CLI args, Delta version, and commit timestamp are all
//!   recovered from the `pond_txn` commit metadata.
//!
//! It does NOT recover:
//!
//! * **Operator settings** -- remote modes and `last_pushed_seq` /
//!   `last_pulled_seq` watermarks live only in the control table.  After
//!   a rebuild the operator must re-attach remotes (`pond remote add` /
//!   `pond backup add`).
//!
//! The commit spine (`commit_hash` / `root_tree_hash` / `commit_object`)
//! IS recovered: it is replayed from the authoritative, pond-resident log
//! node, so `pond log`, tip lookups, and content-addressed push work after
//! a rebuild.  Per-transaction partition checksums are no longer recorded
//! at all (retired in favour of the content tree, Decision D9 step 5b), so
//! the reconstructed `DataCommitted` records carry empty checksums -- the
//! same as a normal commit.

use std::path::{Path, PathBuf};

use tlogfs::OpLogPersistence;
use url::Url;
use uuid::Uuid as StdUuid;

use crate::{ControlTable, PondMetadata, StewardError, get_control_path, get_data_path};

/// Summary of a [`rebuild_control_table`] run.
#[derive(Debug, Clone)]
pub struct RebuildReport {
    /// The pond identity recovered from the data table.
    pub pond_id: StdUuid,
    /// Number of write transactions reconstructed into the control table.
    pub txns_reconstructed: usize,
    /// Highest reconstructed `txn_seq` (0 if none).
    pub last_txn_seq: i64,
    /// Where the previous control directory was moved aside, if one
    /// existed and `force` was set.
    pub backup_path: Option<PathBuf>,
}

/// Rebuild the control table for the pond at `pond_path` from its data
/// Delta table.
///
/// If a control directory already exists, it is moved aside to a
/// `control.bak.<unix_ts>` sibling first -- but only when `force` is
/// `true`; otherwise this returns an error so the operator does not
/// accidentally discard a recoverable control table.
pub async fn rebuild_control_table(
    pond_path: &Path,
    force: bool,
) -> Result<RebuildReport, StewardError> {
    let data_path = get_data_path(pond_path);
    let control_path = get_control_path(pond_path);

    if !data_path.exists() {
        return Err(StewardError::Aborted(format!(
            "no data table at {}; nothing to rebuild from",
            data_path.display()
        )));
    }

    // Serialize against writers and refuse an active freeze rather than
    // silently making the replacement writable.
    let has_delta_log = control_path.join("_delta_log").exists();
    let rebuild_meta = tlogfs::PondTxnMetadata::new(
        0,
        tlogfs::PondUserMetadata::new(vec!["pond".to_string(), "rebuild-control".to_string()]),
    );
    std::fs::create_dir_all(&control_path)?;
    let _write_lock = crate::write_lock::WriteLockGuard::try_acquire(&control_path, &rebuild_meta)?;
    if has_delta_log && !force {
        return Err(StewardError::Aborted(format!(
            "control table already exists at {}; pass --force to move it \
             aside (to control.bak.<ts>) and rebuild",
            control_path.display()
        )));
    }
    if let Some(freeze) = crate::write_lock::read_write_freeze(&control_path)? {
        return Err(StewardError::PondWriteFrozen {
            path: control_path.join("write.freeze"),
            details: format!(
                "frozen_at={}, source_tip={}, reason={}; disable the freeze explicitly before rebuilding control state",
                freeze.frozen_at.to_rfc3339(),
                freeze.source_tip.as_deref().unwrap_or("<none>"),
                freeze.reason
            ),
        });
    }

    // 1) Recover the canonical pond identity from the data table.
    let pond_id_str = OpLogPersistence::peek_pond_id(&data_path)
        .await
        .map_err(StewardError::DataInit)?
        .ok_or_else(|| {
            StewardError::Aborted(
                "data table has no committed local rows; cannot recover pond identity \
                 (a freshly restored replica should be re-bootstrapped via \
                 `pond remote add` + `pond pull`, not rebuilt)"
                    .to_string(),
            )
        })?;
    let pond_id_uuid7 = pond_id_str
        .parse::<uuid7::Uuid>()
        .map_err(|e| StewardError::ControlTable(format!("data pond_id is not a UUID: {}", e)))?;
    let pond_id = StdUuid::from_bytes(*pond_id_uuid7.as_bytes());

    // 2) Reconstruct the write-transaction history from the data table's
    //    commit log.
    let txns = OpLogPersistence::reconstruct_txn_history(&data_path)
        .await
        .map_err(StewardError::DataInit)?;
    if txns.is_empty() {
        return Err(StewardError::Aborted(
            "data table carries no `pond_txn` commits; cannot reconstruct a \
             transaction history (pre-D5 layout or empty table)"
                .to_string(),
        ));
    }

    // The bootstrap txn is the one created by `pond init` (args
    // `["pond", "init"]`); `create_pond` records its data_delta_version
    // as 0 and adds a trailing Completed record.  Identify it so the
    // reconstruction reproduces that exact shape.
    let bootstrap_seq = txns
        .iter()
        .find(|t| t.meta.user.args == ["pond", "init"])
        .map(|t| t.meta.txn_seq)
        .unwrap_or_else(|| txns.iter().map(|t| t.meta.txn_seq).min().unwrap_or(1));

    let birth_timestamp = txns
        .iter()
        .find(|t| t.meta.txn_seq == bootstrap_seq)
        .map(|t| t.timestamp_micros)
        .unwrap_or_else(|| {
            txns.iter()
                .map(|t| t.timestamp_micros)
                .min()
                .unwrap_or_default()
        });

    // 2b) Recover the commit spine from the authoritative, pond-resident log
    //     node so the rebuilt control table's spine cache is not left empty.
    //     Keyed by transaction seq; only the local pond stamps log leaves, so
    //     foreign cross-pond txns get no spine (matching the live commit path).
    let data_url = Url::from_directory_path(&data_path)
        .or_else(|_| Url::from_file_path(&data_path))
        .map_err(|_| {
            StewardError::Aborted(format!(
                "cannot form a URL from data path {}",
                data_path.display()
            ))
        })?;
    let data_table = deltalake::open_table(data_url)
        .await
        .map_err(|e| StewardError::Aborted(format!("open data table for spine replay: {e}")))?;
    let spines = crate::content_tree::read_log_spines(data_table, &pond_id_str).await?;

    let metadata = PondMetadata {
        pond_id: pond_id_uuid7,
        birth_timestamp,
        birthplace: String::new(),
        birth_username: "unknown".to_string(),
    };

    // 3) Move an existing control directory aside (only with --force).
    //    A failed `open_pond` leaves an EMPTY `control/` directory behind
    //    (it calls `create_dir_all` before `ControlTable::open` fails),
    //    so "exists" must mean "is a real Delta table" -- i.e. it has a
    //    `_delta_log/`.  A stale empty directory is silently removed and
    //    does not require `--force` (there is nothing to preserve).
    let backup_path = if has_delta_log {
        let ts = chrono::Utc::now().timestamp();
        let backup = control_path.with_file_name(format!("control.bak.{}", ts));
        move_control_contents_preserving_lock(&control_path, &backup)?;
        Some(backup)
    } else {
        // Preserve the write-lock inode while clearing a stale scaffold.
        remove_control_contents_preserving_lock(&control_path)?;
        None
    };

    // 4) Create a fresh control table seeded with the recovered identity.
    let control_path_str = control_path.to_string_lossy().to_string();
    let mut control = ControlTable::create(&control_path_str, &metadata).await?;

    // 5) Replay the reconstructed lifecycle records in txn_seq order.
    let mut last_txn_seq = 0;
    for txn in &txns {
        let data_delta_version = if txn.meta.txn_seq == bootstrap_seq {
            0
        } else {
            txn.delta_version
        };
        let include_completed = txn.meta.txn_seq == bootstrap_seq;
        // The spine is stamped only by the local pond's content-changing
        // commits, so look it up only for local txns; foreign txns and
        // no-op/compaction commits legitimately have no leaf.
        let spine = if txn.meta.pond_id == pond_id_str {
            spines.get(&txn.meta.txn_seq)
        } else {
            None
        };
        control
            .reconstruct_write_txn(
                &txn.meta,
                txn.timestamp_micros,
                data_delta_version,
                include_completed,
                spine,
            )
            .await?;
        // Only the LOCAL pond's txns advance the reported `last_txn_seq`
        // (which must match the rebuilt Ship's local `last_write_seq`).
        // Foreign cross-pond txns live in their own per-pond_id seq space.
        if txn.meta.pond_id == pond_id_str {
            last_txn_seq = last_txn_seq.max(txn.meta.txn_seq);
        }
    }

    Ok(RebuildReport {
        pond_id,
        txns_reconstructed: txns.len(),
        last_txn_seq,
        backup_path,
    })
}

/// Move replaceable control-table contents while leaving `write.lock` at its
/// stable pathname. Renaming the directory itself would move the locked inode
/// away and let a concurrent writer create and acquire a different lock.
fn move_control_contents_preserving_lock(
    control_path: &Path,
    backup: &Path,
) -> Result<(), StewardError> {
    std::fs::create_dir(backup)?;
    let mut moved = Vec::new();
    let result = (|| -> Result<(), StewardError> {
        for entry in std::fs::read_dir(control_path)? {
            let entry = entry?;
            let name = entry.file_name();
            if name == "write.lock" || name == "write.freeze" {
                continue;
            }
            std::fs::rename(entry.path(), backup.join(&name))?;
            moved.push(name);
        }
        Ok(())
    })();

    if let Err(error) = result {
        for name in moved.iter().rev() {
            if let Err(rollback_error) = std::fs::rename(backup.join(name), control_path.join(name))
            {
                return Err(StewardError::Aborted(format!(
                    "move control table to {} failed: {error}; rollback of {} also failed: {rollback_error}",
                    backup.display(),
                    name.to_string_lossy()
                )));
            }
        }
        std::fs::remove_dir(backup)?;
        return Err(error);
    }
    Ok(())
}

fn remove_control_contents_preserving_lock(control_path: &Path) -> Result<(), StewardError> {
    for entry in std::fs::read_dir(control_path)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == "write.lock" || name == "write.freeze" {
            continue;
        }
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}
