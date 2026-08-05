// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Steward - Transaction coordination and audit logging
//!
//! Steward manages transaction lifecycle for the data filesystem (TLogFS),
//! recording transaction metadata in a separate control table (Delta Lake).
//! It sequences post-commit actions and enables recovery from incomplete transactions.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

mod content_diff;
mod content_objects;
mod content_pull;
mod content_push;
mod content_source;
mod content_tree;
mod content_verify;
mod control_table;
mod dispatch;
pub mod fsck;
mod graft;
mod guard;
mod host;
mod inner_control;
pub mod governed_source;
pub mod limiter;
pub mod limiter_usage;
pub mod maintenance;
mod rebuild;
pub mod reclaim;
mod remote_config;
mod ship;
pub mod storage_profile;
mod write_lock;

pub use content_diff::{ContentComparison, ContentDiff, DiffKind, compare_content_trees};
pub use content_objects::{ObjectInventory, ObjectKind, inventory_content_objects};
pub use content_pull::{
    FetchedGraph, FetchedObject, RebuildOutcome, fetch_object_graph, import_pond, rebuild_pond,
};
pub use content_push::{
    ContentPushOutcome, push_content_to_remote, push_content_to_remote_limited,
};
pub use content_source::{BlobReader, ContentSource, LocalPondSource};
pub use governed_source::GovernedSource;
pub use content_tree::{
    ContentTreeReport, MaterializedObjects, compute_content_tree, compute_content_tree_for_table,
    materialize_content_objects,
};
pub use content_verify::{ContentVerifyReport, ContentVerifyState, verify_content_against_remote};
pub use control_table::{CommitSpine, ControlTable};
pub use dispatch::{Steward, Transaction};
pub use fsck::{FsckError, FsckOptions, FsckReport, PartitionDigest, fsck};
pub use graft::{GraftPin, SYS_GRAFTS_DIR};
pub use guard::StewardTransactionGuard;
pub use host::{HostSteward, HostTransaction};
pub use limiter::{
    LIMITER_KEY_PREFIX, Limiter, LimiterError, LimiterSet, LimiterState, limiter_key,
};
pub use limiter_usage::{
    LIMITER_USAGE_PENDING_KEY, LIMITER_USAGE_SERIES, LimiterUsageRow, UsageSample,
};
pub use rebuild::{RebuildReport, rebuild_control_table};
pub use remote_config::{RemoteAttachment, RemoteConfigError, RemoteMode};
pub use ship::{CollapseReport, CompactOutcome, Ship};
pub use storage_profile::{ResolvedStorage, StorageProfileError};
pub use tlogfs::{PondMetadata, PondTxnMetadata, PondUserMetadata};

/// Recovery command result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryResult {
    /// Number of transactions recovered
    pub recovered_count: u64,
    /// Whether recovery was needed
    pub was_needed: bool,
}

#[derive(Debug, Error)]
pub enum StewardError {
    #[error("TinyFS error: {0}")]
    DataInit(#[from] tlogfs::TLogFSError),

    #[error("Control table error: {0}")]
    ControlTable(String),

    #[error("Transaction aborted: {0}")]
    Aborted(String),

    /// A rate limiter refused the action.  Distinct from [`Self::Aborted`] so
    /// callers can tell a governed throttle -- expected, transient, and safe to
    /// retry once the window slides -- from a genuine failure.
    #[error("{0}")]
    RateLimited(LimiterError),

    #[error("Transaction sequence mismatch: expected {expected}, found {actual}")]
    TransactionSequenceMismatch { expected: i64, actual: i64 },

    #[error("Recovery needed: incomplete transaction {txn_meta:?}. Run 'recover' command.")]
    RecoveryNeeded { txn_meta: PondTxnMetadata },

    #[error(
        "Transaction mode violation: write transaction made no changes (should have been read transaction)"
    )]
    WriteTransactionNoChanges,

    #[error("Transaction mode violation: read transaction attempted to write data")]
    ReadTransactionAttemptedWrite,

    #[error(
        "pond at {path} is locked by another process{}{}{}",
        format_pid(*holder_pid),
        format_since(holder_since.as_ref()),
        format_txn_id(holder_txn_id.as_ref()),
    )]
    PondLocked {
        path: PathBuf,
        holder_pid: Option<u32>,
        holder_since: Option<chrono::DateTime<chrono::Utc>>,
        holder_txn_id: Option<String>,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("uuid7 parse error: {0}")]
    Uuid(#[from] uuid7::ParseError),

    #[error("Delta Lake error: {0}")]
    DeltaLake(String),

    #[error("Content-graph error: {0}")]
    Content(String),

    #[error("Dynamic error: {0}")]
    Dyn(Box<dyn std::error::Error + Send + Sync>),
}

// Bridge the two-hop conversion tinyfs::Error -> TLogFSError -> StewardError
// so `?` works directly on tinyfs operations.
impl From<tinyfs::Error> for StewardError {
    fn from(e: tinyfs::Error) -> Self {
        StewardError::DataInit(tlogfs::TLogFSError::from(e))
    }
}

// ---------------------------------------------------------------------------
// PondLocked display helpers
// ---------------------------------------------------------------------------

fn format_pid(pid: Option<u32>) -> String {
    pid.map(|p| format!("\n  holder PID {p}"))
        .unwrap_or_default()
}

fn format_since(since: Option<&chrono::DateTime<chrono::Utc>>) -> String {
    since
        .map(|t| {
            format!(
                " since {}",
                t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            )
        })
        .unwrap_or_default()
}

fn format_txn_id(txn_id: Option<&String>) -> String {
    txn_id
        .map(|t| format!("\n  transaction {t}"))
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------------

/// Get the data filesystem path under the pond
#[must_use]
pub fn get_data_path(pond_path: &Path) -> PathBuf {
    pond_path.join("data")
}

/// Get the control table path under the pond
#[must_use]
pub fn get_control_path(pond_path: &Path) -> PathBuf {
    pond_path.join("control")
}

/// Get the git bare repo cache path under the pond
#[must_use]
pub fn get_git_path(pond_path: &Path) -> PathBuf {
    pond_path.join("git")
}

/// Get the transparency-log tile directory under the pond.
///
/// Holds the materialized C2SP `tlog-tiles` and the `tlog-checkpoint` note body
/// over the pond's linear commit spine (design Decision D5).
#[must_use]
pub fn get_tlog_path(pond_path: &Path) -> PathBuf {
    pond_path.join("tlog")
}

// ---------------------------------------------------------------------------
// In-pond filesystem conventions used by D4 (`pond remote add/push/pull`).
//
// Remote attachments live as small YAML files under `/sys/remotes/<name>`.
// The directory `/sys/` is reserved for system metadata; D4 only writes
// `/sys/remotes/*` but future D-phases may add more siblings (e.g. `/sys/keys/`).
//
// Per-remote runtime state — `last_pushed_seq:<url>`, `last_pulled_seq:<url>`,
// `remote_mode:<name>` — is stored in the control table's settings map via
// `ControlTable::raw_config_{get,set}`, NOT in the YAML config (so that
// pushing the config to a backup never accidentally ships local watermarks).

/// Filesystem directory holding `/sys/` metadata files (D4+).
pub const SYS_DIR: &str = "/sys";

/// Filesystem directory holding remote attachment configs.  Each child is a
/// YAML file named `<remote-name>` containing a [`RemoteAttachment`] (see
/// `crates/cmd/src/commands/remote.rs`).
pub const SYS_REMOTES_DIR: &str = "/sys/remotes";

/// Control-table raw_config key prefix for "what mode does remote `<name>`
/// operate in" (`push`, `pull`, or `both`).  Format the full key as
/// `format!("{REMOTE_MODE_PREFIX}{name}")`.
pub const REMOTE_MODE_PREFIX: &str = "remote_mode:";

/// Control-table raw_config key prefix for the in-pond mount path of a
/// pull-mode remote (D5.7b).  Format the full key as
/// `format!("{REMOTE_MOUNT_PATH_PREFIX}{name}")`.
///
/// Pull-mode remotes always set this key (default `/imports/<name>` or
/// `/` for a mirror restart).  Push-mode and `both`-mode remotes leave it
/// unset.
pub const REMOTE_MOUNT_PATH_PREFIX: &str = "remote_mount_path:";
