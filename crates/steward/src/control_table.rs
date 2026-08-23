// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Control Table - thin wrapper around [`crate::inner_control::ControlTable`].
//!
//! Built during the remote-redesign D2 substantive phase.  The rich
//! append-only `TransactionRecord` schema previously living here has been
//! replaced by the lean `sync_steward` schema (`pond_id`, `record_kind`,
//! `txn_seq`, `txn_id`, `commit_kind`, `parent_seq`, `duration_ms`,
//! `ts_micros`, `metadata_json`).  This wrapper:
//!
//! - Preserves the public API surface (`record_*`, cached config getters,
//!   `session_context`, etc.) that `Ship`, `StewardTransactionGuard` and
//!   `cmd/*` rely on -- callers do not need to change shape.
//! - Stores pond identity under [`BOOTSTRAP_POND_ID`] (`Uuid::nil()`) as
//!   `sync_steward` config rows, per the D2 plan; D5 will migrate
//!   identity to the bootstrap row of the data Delta table.
//! - Stores factory modes and per-instance settings under the local
//!   `pond_id` namespace with the `"factory_mode:"` and `"setting:"`
//!   key prefixes respectively.
//! - Drops the `record_import_*` / `update_import_watermark` /
//!   `query_import_partitions` methods entirely; D5 brings cross-pond
//!   import back via the row-level `pond_id` partitioning of tlogfs.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use crate::inner_control::{
    CommitKind, ControlRecord, ControlTable as InnerControlTable, DataCommittedMetadata,
    RecordKind, new_txn_id,
};
use chrono::{DateTime, Utc};
use datafusion::execution::context::SessionContext;
use deltalake::DeltaTable;
use serde::{Deserialize, Serialize};
use uuid::Uuid as StdUuid;

use crate::StewardError;

// Re-export the pond metadata types so callers can `use steward::ControlTable`
// alongside `use steward::PondMetadata` without a separate tlogfs import.
pub use tlogfs::{PondMetadata, PondTxnMetadata};

/// Bootstrap pond_id used as the namespace for instance-wide config that
/// is not bound to any particular pond (today: pond identity).  Matches
/// the sandbox prototype convention adopted by `sync_steward`.
const BOOTSTRAP_POND_ID: StdUuid = StdUuid::nil();

const KEY_STORE_ID: &str = "store_id";
const KEY_BIRTH_TIMESTAMP: &str = "birth_timestamp";
const KEY_BIRTHPLACE: &str = "birthplace";
const KEY_BIRTH_USERNAME: &str = "birth_username";

const FACTORY_MODE_PREFIX: &str = "factory_mode:";
const SETTING_PREFIX: &str = "setting:";

/// Per-instance setting that disables automatic post-commit factories and
/// remote pushes when set to [`POST_COMMIT_DISPATCH_SUPPRESSED`].
pub const POST_COMMIT_DISPATCH_SETTING: &str = "post_commit_dispatch";
/// Persisted value used by staged recovery imports until an operator
/// explicitly activates the recovered pond.
pub const POST_COMMIT_DISPATCH_SUPPRESSED: &str = "suppressed";

/// Lifecycle classification preserved for source-level compatibility with
/// pre-D2 callers.  Each variant maps onto the lean schema's
/// [`RecordKind`] (+ optional [`CommitKind`]) at write time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionType {
    Read,
    Write,
    PostCommit,
}

/// JSON payload stored in `metadata_json` for `PostPush*` records,
/// carrying the post-commit task attributes that the lean schema no
/// longer has dedicated columns for.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PostCommitMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    execution_seq: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    factory_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    config_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error_message: Option<String>,
}

/// Thin wrapper over [`crate::inner_control::ControlTable`] exposing the
/// watertown-flavored `record_*` API and caching pond identity, factory
/// modes and settings on top of the lean `config_*` primitives.
/// The content-graph spine recorded alongside a `DataCommitted` control
/// record: the root tree hash this commit produced, the previous commit on
/// the pond's linear chain (`None` at genesis), and this commit object's own
/// hash.  Computed by [`crate::compute_content_tree_for_table`] plus a
/// [`sync_store::content::Commit`] fold in the commit path.  See
/// `docs/content-addressed-pond-design.md` Section 5.3.
#[derive(Debug, Clone)]
pub struct CommitSpine {
    /// Hex BLAKE3 of the commit's root directory tree.
    pub root_tree_hash: String,
    /// Hex BLAKE3 of the parent commit, or `None` for the genesis commit.
    pub parent_commit_hash: Option<String>,
    /// Hex BLAKE3 of this commit object (the TIME-log leaf identity).
    pub commit_hash: String,
    /// Hex of the encoded commit object bytes (`Commit::encode`), retained so
    /// the commit object can be reproduced verbatim for content-addressed push.
    pub commit_object: String,
}

pub struct ControlTable {
    inner: InnerControlTable,
    pond_metadata: PondMetadata,
    factory_modes: HashMap<String, String>,
    settings: HashMap<String, String>,
}

impl ControlTable {
    /// Create a new control table at `path` and persist `pond_metadata`
    /// under the bootstrap namespace.  Errors if the table already exists.
    pub async fn create<P: AsRef<Path>>(
        path: P,
        pond_metadata: &PondMetadata,
    ) -> Result<Self, StewardError> {
        let mut inner = InnerControlTable::create(path.as_ref())
            .await
            .map_err(map_err)?;
        seed_pond_metadata(&mut inner, pond_metadata).await?;
        Ok(Self {
            inner,
            pond_metadata: pond_metadata.clone(),
            factory_modes: HashMap::new(),
            settings: HashMap::new(),
        })
    }

    /// Open an existing control table at `path`.  Reads back pond
    /// identity, factory modes and settings into the in-memory caches.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, StewardError> {
        let inner = InnerControlTable::open(path.as_ref())
            .await
            .map_err(map_err)?;
        let pond_metadata = load_pond_metadata(&inner).await?;
        let pond_id_uuid = pond_id_to_std(&pond_metadata.pond_id);
        let local_config = inner.config_list(pond_id_uuid).await.map_err(map_err)?;
        let (factory_modes, settings) = split_config(local_config);
        Ok(Self {
            inner,
            pond_metadata,
            factory_modes,
            settings,
        })
    }

    /// Convenience: create or open based on `create_new`.
    pub async fn open_or_create<P: AsRef<Path>>(
        path: P,
        create_new: bool,
        pond_metadata: Option<&PondMetadata>,
    ) -> Result<Self, StewardError> {
        if create_new {
            let metadata = pond_metadata.ok_or_else(|| {
                StewardError::ControlTable(
                    "pond_metadata is required when creating a new control table".to_string(),
                )
            })?;
            Self::create(path, metadata).await
        } else {
            Self::open(path).await
        }
    }

    // -------- Accessors --------

    /// Underlying [`DeltaTable`] handle for maintenance operations.
    #[must_use]
    pub fn table(&self) -> &DeltaTable {
        self.inner.delta_table()
    }

    /// Replace the underlying [`DeltaTable`] handle.  Called by
    /// [`crate::maintenance::maintain_table`] after vacuum/optimize
    /// produces a new table reference.  Best-effort: a failure to
    /// re-register only affects subsequent SQL queries on the cached
    /// session context.
    pub fn set_table(&mut self, table: DeltaTable) {
        if let Err(e) = self.inner.set_delta_table(table) {
            log::warn!("control_table set_table: re-register failed: {}", e);
        }
    }

    /// Shared DataFusion session context with the control table
    /// registered under [`crate::inner_control::TABLE_NAME`] (`"control"`).
    /// JSON helper functions are registered so callers can query
    /// `metadata_json` via `json_get_str` and friends.  A fresh
    /// context is built on every call so callers always see the
    /// latest Delta version of the table.
    #[must_use]
    pub fn session_context(&self) -> Arc<SessionContext> {
        let mut ctx = SessionContext::new();
        if let Err(e) = datafusion_functions_json::register_all(&mut ctx) {
            log::warn!(
                "control_table session_context: register JSON functions failed: {}",
                e
            );
        }
        if let Err(e) = ctx.register_table(
            crate::inner_control::TABLE_NAME,
            Arc::new(self.inner.delta_table().clone()),
        ) {
            log::warn!(
                "control_table session_context: register table failed: {}",
                e
            );
        }
        Arc::new(ctx)
    }

    /// Cached pond identity (immutable for the lifetime of the pond).
    #[must_use]
    pub fn pond_metadata(&self) -> &PondMetadata {
        &self.pond_metadata
    }

    /// Alias for [`Self::pond_metadata`].  Preserved for caller
    /// compatibility with the pre-D2 API.
    #[must_use]
    pub fn get_pond_metadata(&self) -> &PondMetadata {
        &self.pond_metadata
    }

    /// Replace cached pond metadata and persist it under the bootstrap
    /// namespace.  Used by restoration flows where the source pond's
    /// identity must be preserved on a fresh replica.
    pub async fn set_pond_metadata(&mut self, metadata: &PondMetadata) -> Result<(), StewardError> {
        seed_pond_metadata(&mut self.inner, metadata).await?;
        self.pond_metadata = metadata.clone();
        Ok(())
    }

    /// Cached factory execution modes (name -> mode string).
    #[must_use]
    pub fn factory_modes(&self) -> &HashMap<String, String> {
        &self.factory_modes
    }

    /// Cached factory execution mode for `name`, if set.
    #[must_use]
    pub fn get_factory_mode(&self, name: &str) -> Option<String> {
        self.factory_modes.get(name).cloned()
    }

    /// Set factory execution mode and persist under the local pond_id
    /// with the `"factory_mode:"` prefix.
    pub async fn set_factory_mode(&mut self, name: &str, mode: &str) -> Result<(), StewardError> {
        let key = format!("{}{}", FACTORY_MODE_PREFIX, name);
        let pond_id = pond_id_to_std(&self.pond_metadata.pond_id);
        self.inner
            .config_set(pond_id, &key, mode)
            .await
            .map_err(map_err)?;
        let _previous = self
            .factory_modes
            .insert(name.to_string(), mode.to_string());
        Ok(())
    }

    /// Cached per-instance settings.
    #[must_use]
    pub fn settings(&self) -> &HashMap<String, String> {
        &self.settings
    }

    /// Cached value of setting `key`, if set.
    #[must_use]
    pub fn get_setting(&self, key: &str) -> Option<String> {
        self.settings.get(key).cloned()
    }

    /// Whether automatic post-commit factories and remote pushes are
    /// persistently disabled for this pond instance.
    #[must_use]
    pub fn post_commit_dispatch_suppressed(&self) -> bool {
        self.get_setting(POST_COMMIT_DISPATCH_SETTING).as_deref()
            == Some(POST_COMMIT_DISPATCH_SUPPRESSED)
    }

    /// Set per-instance setting and persist under the local pond_id with
    /// the `"setting:"` prefix.
    pub async fn set_setting(&mut self, key: &str, value: &str) -> Result<(), StewardError> {
        let inner_key = format!("{}{}", SETTING_PREFIX, key);
        let pond_id = pond_id_to_std(&self.pond_metadata.pond_id);
        self.inner
            .config_set(pond_id, &inner_key, value)
            .await
            .map_err(map_err)?;
        let _previous = self.settings.insert(key.to_string(), value.to_string());
        Ok(())
    }

    // -------- Lifecycle records --------

    /// Record transaction `Begin`.  `_based_on_seq` and
    /// `_transaction_type` are accepted for source-level API
    /// compatibility but no longer persisted (the lean schema treats
    /// every begin uniformly).
    pub async fn record_begin(
        &mut self,
        txn_meta: &PondTxnMetadata,
        _based_on_seq: Option<i64>,
        _transaction_type: TransactionType,
    ) -> Result<(), StewardError> {
        let record = self.base_record(RecordKind::Begin, txn_meta);
        self.inner.write_record(record).await.map_err(map_err)
    }

    /// Record successful data filesystem commit.
    pub async fn record_data_committed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        _transaction_type: TransactionType,
        data_fs_version: i64,
        duration_ms: i64,
        commit_spine: Option<CommitSpine>,
    ) -> Result<(), StewardError> {
        self.record_committed_inner(
            txn_meta,
            CommitKind::Write,
            data_fs_version,
            duration_ms,
            commit_spine,
        )
        .await
    }

    /// Record a successful producer-side compaction commit (see
    /// [`crate::Ship::compact`]).  Identical to [`Self::record_data_committed`]
    /// except the `commit_kind` is [`CommitKind::Compact`].
    pub async fn record_compact_committed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        data_fs_version: i64,
        duration_ms: i64,
        commit_spine: Option<CommitSpine>,
    ) -> Result<(), StewardError> {
        self.record_committed_inner(
            txn_meta,
            CommitKind::Compact,
            data_fs_version,
            duration_ms,
            commit_spine,
        )
        .await
    }

    async fn record_committed_inner(
        &mut self,
        txn_meta: &PondTxnMetadata,
        commit_kind: CommitKind,
        data_fs_version: i64,
        duration_ms: i64,
        commit_spine: Option<CommitSpine>,
    ) -> Result<(), StewardError> {
        let record = self.data_committed_record(
            txn_meta,
            commit_kind,
            data_fs_version,
            duration_ms,
            commit_spine,
        );
        self.inner.write_record(record).await.map_err(map_err)
    }

    /// Build (without writing) a `DataCommitted` record.  Shared by the
    /// singular [`Self::record_committed_inner`] and the batched
    /// post-commit-factory terminal path so both serialize identical rows.
    fn data_committed_record(
        &self,
        txn_meta: &PondTxnMetadata,
        commit_kind: CommitKind,
        data_fs_version: i64,
        duration_ms: i64,
        commit_spine: Option<CommitSpine>,
    ) -> ControlRecord {
        let (root_tree_hash, parent_commit_hash, commit_hash, commit_object) = match commit_spine {
            Some(spine) => (
                Some(spine.root_tree_hash),
                spine.parent_commit_hash,
                Some(spine.commit_hash),
                Some(spine.commit_object),
            ),
            None => (None, None, None, None),
        };
        let metadata = DataCommittedMetadata {
            data_delta_version: data_fs_version,
            root_tree_hash,
            parent_commit_hash,
            commit_hash,
            commit_object,
        };
        let metadata_json = serde_json::to_string(&metadata).unwrap_or_else(|_| "{}".into());
        let mut record = self.base_record(RecordKind::DataCommitted, txn_meta);
        record.commit_kind = Some(commit_kind);
        record.duration_ms = Some(duration_ms);
        record.metadata_json = metadata_json;
        record
    }

    /// Record transaction failure.
    pub async fn record_failed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        _transaction_type: TransactionType,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let metadata_json = reason_json(&error_message);
        let mut record = self.base_record(RecordKind::Failed, txn_meta);
        record.duration_ms = Some(duration_ms);
        record.metadata_json = metadata_json;
        self.inner.write_record(record).await.map_err(map_err)
    }

    /// Record completed read transaction (or no-op write).
    pub async fn record_completed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        _transaction_type: TransactionType,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let mut record = self.base_record(RecordKind::Completed, txn_meta);
        record.duration_ms = Some(duration_ms);
        self.inner.write_record(record).await.map_err(map_err)
    }

    /// Reconstruct the lifecycle records for one historical write
    /// transaction at a known commit timestamp.  Used by
    /// `pond rebuild-control` to rebuild the control table from the data
    /// Delta table's commit history when the control table is lost or
    /// corrupt.
    ///
    /// Writes a `Begin` + `DataCommitted` pair (plus a trailing
    /// `Completed` when `include_completed` is set, matching the
    /// bootstrap shape produced by [`crate::Ship::create_pond`]).  All
    /// records carry the supplied `ts_micros` so `pond log` shows the
    /// original commit time rather than the rebuild time.
    ///
    /// The `DataCommitted` record carries EMPTY partition checksums:
    /// historical per-transaction checksums are not recoverable from the
    /// data table alone, so `pond verify` must be re-baselined (and
    /// remotes re-attached) after a rebuild.
    pub async fn reconstruct_write_txn(
        &mut self,
        txn_meta: &PondTxnMetadata,
        ts_micros: i64,
        data_delta_version: i64,
        include_completed: bool,
        spine: Option<&CommitSpine>,
    ) -> Result<(), StewardError> {
        let mut begin = self.base_record(RecordKind::Begin, txn_meta);
        begin.ts_micros = ts_micros;
        self.inner.write_record(begin).await.map_err(map_err)?;

        let metadata = DataCommittedMetadata {
            data_delta_version,
            root_tree_hash: spine.map(|s| s.root_tree_hash.clone()),
            parent_commit_hash: spine.and_then(|s| s.parent_commit_hash.clone()),
            commit_hash: spine.map(|s| s.commit_hash.clone()),
            commit_object: spine.map(|s| s.commit_object.clone()),
        };
        let metadata_json = serde_json::to_string(&metadata).unwrap_or_else(|_| "{}".into());
        let mut committed = self.base_record(RecordKind::DataCommitted, txn_meta);
        committed.commit_kind = Some(CommitKind::Write);
        committed.duration_ms = Some(0);
        committed.ts_micros = ts_micros;
        committed.metadata_json = metadata_json;
        self.inner.write_record(committed).await.map_err(map_err)?;

        if include_completed {
            let mut completed = self.base_record(RecordKind::Completed, txn_meta);
            completed.duration_ms = Some(0);
            completed.ts_micros = ts_micros;
            self.inner.write_record(completed).await.map_err(map_err)?;
        }

        Ok(())
    }

    // -------- Post-commit records --------

    /// Build (without writing) a `PostPush*` lifecycle record for the
    /// parent transaction.  Each gets its own UUID so that multiple
    /// factories sharing one parent transaction do not collide on
    /// `(pond_id, txn_seq, txn_id)` when read back.
    fn post_commit_record(
        &self,
        kind: RecordKind,
        txn_meta: &PondTxnMetadata,
        metadata: &PostCommitMetadata,
        duration_ms: Option<i64>,
    ) -> ControlRecord {
        let metadata_json = serde_json::to_string(metadata).unwrap_or_else(|_| "{}".into());
        ControlRecord {
            pond_id: pond_id_to_std(&self.pond_metadata.pond_id),
            record_kind: kind,
            txn_seq: txn_meta.txn_seq,
            txn_id: new_txn_id(),
            commit_kind: None,
            parent_seq: Some(txn_meta.txn_seq),
            duration_ms,
            ts_micros: Utc::now().timestamp_micros(),
            metadata_json,
        }
    }

    /// Record the full queue of post-commit factory tasks in a SINGLE
    /// control-table commit.  Replaces a per-factory `PostPushPending`
    /// write loop; the rows are identical, just batched into one Delta
    /// commit (and one add-file).  Each entry is `(execution_seq,
    /// factory_name, config_path)`.  An empty slice is a no-op.
    pub async fn record_post_commit_pending_batch(
        &mut self,
        txn_meta: &PondTxnMetadata,
        entries: &[(i64, String, String)],
    ) -> Result<(), StewardError> {
        let records: Vec<ControlRecord> = entries
            .iter()
            .map(|(execution_seq, factory_name, config_path)| {
                let metadata = PostCommitMetadata {
                    execution_seq: Some(*execution_seq),
                    factory_name: Some(factory_name.clone()),
                    config_path: Some(config_path.clone()),
                    error_message: None,
                };
                self.post_commit_record(RecordKind::PostPushPending, txn_meta, &metadata, None)
            })
            .collect();
        self.inner.write_records(records).await.map_err(map_err)
    }

    /// Record the terminal lifecycle of a post-commit factory execution in
    /// ONE control-table commit: the factory transaction's `DataCommitted`
    /// (omitted when `data_fs_version` is `None`, i.e. a write no-op) and
    /// `Completed`, plus the parent's `PostPushCompleted` (when `outcome`
    /// is `Ok`) or `PostPushFailed` (when `outcome` is `Err(reason)`).
    ///
    /// The factory transaction's own `Begin` is written separately, before
    /// the data commit, because replication keys its push bundle on that
    /// row landing first; only the post-commit rows are batched here.
    pub async fn record_factory_terminal_batch(
        &mut self,
        factory_meta: &PondTxnMetadata,
        parent_meta: &PondTxnMetadata,
        execution_seq: i64,
        data_fs_version: Option<i64>,
        duration_ms: i64,
        commit_spine: Option<CommitSpine>,
        outcome: Result<(), String>,
    ) -> Result<(), StewardError> {
        let mut records = Vec::with_capacity(3);
        if let Some(version) = data_fs_version {
            records.push(self.data_committed_record(
                factory_meta,
                CommitKind::Write,
                version,
                duration_ms,
                commit_spine,
            ));
        }
        let mut completed = self.base_record(RecordKind::Completed, factory_meta);
        completed.duration_ms = Some(duration_ms);
        records.push(completed);

        let post = match outcome {
            Ok(()) => {
                let metadata = PostCommitMetadata {
                    execution_seq: Some(execution_seq),
                    factory_name: None,
                    config_path: None,
                    error_message: None,
                };
                self.post_commit_record(
                    RecordKind::PostPushCompleted,
                    parent_meta,
                    &metadata,
                    Some(duration_ms),
                )
            }
            Err(error_message) => {
                let metadata = PostCommitMetadata {
                    execution_seq: Some(execution_seq),
                    factory_name: None,
                    config_path: None,
                    error_message: Some(error_message),
                };
                self.post_commit_record(
                    RecordKind::PostPushFailed,
                    parent_meta,
                    &metadata,
                    Some(duration_ms),
                )
            }
        };
        records.push(post);
        self.inner.write_records(records).await.map_err(map_err)
    }

    /// Record a post-commit factory whose data commit itself failed, in ONE
    /// control-table commit: the factory transaction's `Failed` plus the
    /// parent's `PostPushFailed`.
    pub async fn record_factory_aborted_batch(
        &mut self,
        factory_meta: &PondTxnMetadata,
        parent_meta: &PondTxnMetadata,
        execution_seq: i64,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let mut failed = self.base_record(RecordKind::Failed, factory_meta);
        failed.duration_ms = Some(duration_ms);
        failed.metadata_json = reason_json(&error_message);

        let metadata = PostCommitMetadata {
            execution_seq: Some(execution_seq),
            factory_name: None,
            config_path: None,
            error_message: Some(error_message),
        };
        let post = self.post_commit_record(
            RecordKind::PostPushFailed,
            parent_meta,
            &metadata,
            Some(duration_ms),
        );
        self.inner
            .write_records(vec![failed, post])
            .await
            .map_err(map_err)
    }

    // -------- Sync-remote integration surface --------
    //
    // The methods below are used by the D4 sync-remote adapter
    // (`crate::remote_adapter::ShipRemoteSteward`) and mirror the
    // native crate::inner_control::Steward API exactly (raw txn_ids, no
    // factory_name/execution_seq attached, no key-prefix munging).
    // They are also useful for any other consumer that needs the
    // unprefixed config namespace and direct PostPush lifecycle.

    /// Local pond_id as a [`uuid::Uuid`] (the format `sync_steward`
    /// uses).  Convenience for callers that want to interoperate with
    /// sync_steward APIs without re-deriving the conversion.
    #[must_use]
    pub fn pond_id_uuid(&self) -> StdUuid {
        pond_id_to_std(&self.pond_metadata.pond_id)
    }

    /// Borrow the underlying `crate::inner_control::ControlTable` for
    /// adapters that need to call its native API directly (e.g.
    /// `data_committed_record(pond_id, txn_seq)` from the
    /// sync-remote adapter).
    #[must_use]
    pub fn inner(&self) -> &crate::inner_control::ControlTable {
        &self.inner
    }

    /// Resolve the content-graph commit hash recorded at the local pond's
    /// `txn_seq`.  Returns `None` if no `DataCommitted` record exists at that
    /// seq or it predates / skipped the content-graph spine.
    pub async fn commit_hash_at(&self, txn_seq: i64) -> Result<Option<String>, StewardError> {
        self.inner
            .commit_hash_at(self.pond_id_uuid(), txn_seq)
            .await
            .map_err(map_err)
    }

    /// Resolve the content-graph root tree hash recorded at the local pond's
    /// `txn_seq`.  Returns `None` if no `DataCommitted` record exists at that
    /// seq or it predates / skipped the content-graph spine.
    pub async fn root_tree_hash_at(&self, txn_seq: i64) -> Result<Option<String>, StewardError> {
        self.inner
            .root_tree_hash_at(self.pond_id_uuid(), txn_seq)
            .await
            .map_err(map_err)
    }

    /// Resolve the content-graph parent commit hash recorded at the local
    /// pond's `txn_seq`.  Returns `None` for the genesis commit, an unstamped
    /// record, or when no `DataCommitted` record exists at that seq.
    pub async fn parent_commit_hash_at(
        &self,
        txn_seq: i64,
    ) -> Result<Option<String>, StewardError> {
        self.inner
            .parent_commit_hash_at(self.pond_id_uuid(), txn_seq)
            .await
            .map_err(map_err)
    }

    /// Resolve the encoded commit object bytes recorded at the local pond's
    /// `txn_seq`.  Returns `None` if no `DataCommitted` record exists at that
    /// seq or it predates / skipped the content-graph spine.
    pub async fn commit_object_at(&self, txn_seq: i64) -> Result<Option<String>, StewardError> {
        self.inner
            .commit_object_at(self.pond_id_uuid(), txn_seq)
            .await
            .map_err(map_err)
    }

    /// The authoritative transparency-log leaf sequence for the local pond: the
    /// encoded commit-object bytes (hex) of every spine-bearing `DataCommitted`
    /// record, in commit order.  The tile export is reconciled against this
    /// sequence (design Decision D5); see [`crate::StewardTransactionGuard`]'s
    /// `materialize_tlog`.
    pub async fn commit_objects_in_order(&self) -> Result<Vec<String>, StewardError> {
        self.inner
            .commit_objects_in_order(self.pond_id_uuid())
            .await
            .map_err(map_err)
    }

    /// The highest local `txn_seq` that stamped a content-graph commit spine:
    /// the pond's content tip.  Content-preserving transactions (compaction)
    /// record no spine, so their seq is skipped; push and verify resolve the
    /// tip through this method so a compaction seq never mis-resolves as the
    /// tip.  Returns `None` if the pond has no spine-bearing commit.
    pub async fn latest_spine_seq(&self) -> Result<Option<i64>, StewardError> {
        self.inner
            .latest_spine_seq(self.pond_id_uuid())
            .await
            .map_err(map_err)
    }

    /// Mutable view of the underlying `crate::inner_control::ControlTable`.
    pub fn inner_mut(&mut self) -> &mut crate::inner_control::ControlTable {
        &mut self.inner
    }

    /// Write a `PostPushPending` lifecycle record for the local
    /// pond_id and return the assigned `txn_id` so subsequent
    /// `PostPushCompleted`/`PostPushFailed` calls can pair against
    /// it.  Records have no `factory_name`/`execution_seq`/etc.
    /// metadata (cf. the `record_post_commit_*` family which packs
    /// that into `metadata_json`).
    pub async fn record_post_push_pending(&mut self, txn_seq: i64) -> Result<String, StewardError> {
        let txn_id = new_txn_id();
        let record = self.post_push_record(
            RecordKind::PostPushPending,
            txn_seq,
            txn_id.clone(),
            None,
            "{}".to_string(),
        );
        self.inner.write_record(record).await.map_err(map_err)?;
        Ok(txn_id)
    }

    /// Write a `PostPushCompleted` record paired with a prior
    /// `PostPushPending` (same `txn_id`).  Duration is computed from
    /// `pending_started_micros`.
    pub async fn record_post_push_completed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        pending_started_micros: i64,
    ) -> Result<(), StewardError> {
        let duration_ms = elapsed_ms_since(pending_started_micros);
        let record = self.post_push_record(
            RecordKind::PostPushCompleted,
            txn_seq,
            txn_id,
            Some(duration_ms),
            "{}".to_string(),
        );
        self.inner.write_record(record).await.map_err(map_err)
    }

    /// Write a `PostPushFailed` record paired with a prior
    /// `PostPushPending` (same `txn_id`).  `reason` is captured under
    /// `metadata_json.reason`.
    pub async fn record_post_push_failed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        pending_started_micros: i64,
        reason: String,
    ) -> Result<(), StewardError> {
        let duration_ms = elapsed_ms_since(pending_started_micros);
        let record = self.post_push_record(
            RecordKind::PostPushFailed,
            txn_seq,
            txn_id,
            Some(duration_ms),
            reason_json(&reason),
        );
        self.inner.write_record(record).await.map_err(map_err)
    }

    /// Raw per-replica setting read (no prefix munging).  Used by
    /// the sync-remote adapter for keys like `last_pulled_seq:<url>`
    /// and `last_pushed_seq:<url>` which must match the sync_steward
    /// key format exactly.  See [`Self::get_setting`] for the
    /// watertown user-facing API (which adds a `"setting:"` prefix).
    pub async fn raw_config_get(&self, key: &str) -> Result<Option<String>, StewardError> {
        self.inner
            .config_get(self.pond_id_uuid(), key)
            .await
            .map_err(map_err)
    }

    /// Raw per-replica setting write (no prefix munging).  Companion
    /// to [`Self::raw_config_get`].
    pub async fn raw_config_set(&mut self, key: &str, value: &str) -> Result<(), StewardError> {
        self.inner
            .config_set(self.pond_id_uuid(), key, value)
            .await
            .map_err(map_err)
    }

    // -------- Queries --------

    /// Prune local-pond lifecycle history at or below `horizon_seq`,
    /// leaving `Setting` rows intact.  Thin wrapper over
    /// [`crate::inner_control::ControlTable::prune_below`] scoped to the local
    /// pond_id.  Returns the number of rows deleted.  The caller must
    /// run a checkpoint + vacuum afterwards to reclaim disk space.
    pub async fn prune_below(&mut self, horizon_seq: i64) -> Result<usize, StewardError> {
        let pond_id = self.pond_id_uuid();
        self.inner
            .prune_below(pond_id, horizon_seq)
            .await
            .map_err(map_err)
    }

    /// Highest committed write sequence number, or 0 if none.  Replaces
    /// the pre-D2 implementation that scanned `transaction_type='write'`
    /// rows directly.
    pub async fn get_last_write_sequence(&self) -> Result<i64, StewardError> {
        let pond_id = pond_id_to_std(&self.pond_metadata.pond_id);
        self.inner
            .last_committed_seq(pond_id)
            .await
            .map_err(map_err)
    }

    /// Incomplete transactions for the local pond_id.  The returned
    /// [`PondTxnMetadata`] carries the original `txn_seq` and `txn_id`
    /// but an empty `args` vector -- the lean schema does not persist
    /// CLI arguments (per the D2 plan).  The trailing `i64` is always 0
    /// (no data_fs_version is recorded for incomplete transactions
    /// because they never reached `DataCommitted`).
    pub async fn find_incomplete_transactions(
        &self,
    ) -> Result<Vec<(PondTxnMetadata, i64)>, StewardError> {
        let pond_id = pond_id_to_std(&self.pond_metadata.pond_id);
        let records = self
            .inner
            .incomplete_transactions(pond_id)
            .await
            .map_err(map_err)?;
        let mut out = Vec::with_capacity(records.len());
        for record in records {
            let txn_id = uuid7::Uuid::from_str(&record.txn_id).map_err(|e| {
                StewardError::ControlTable(format!("Invalid txn_id UUID in control table: {}", e))
            })?;
            let user = tlogfs::PondUserMetadata {
                txn_id,
                args: Vec::new(),
            };
            let txn_meta = PondTxnMetadata::new(record.txn_seq, user);
            out.push((txn_meta, 0));
        }
        Ok(out)
    }

    /// Print the pond identity banner using cached metadata.
    #[allow(clippy::print_stdout)]
    pub fn print_banner(&self) {
        println!();
        pond_metadata_banner(&self.pond_metadata);
        println!();
    }

    // -------- internals --------

    fn base_record(&self, kind: RecordKind, txn_meta: &PondTxnMetadata) -> ControlRecord {
        ControlRecord {
            pond_id: pond_id_to_std(&self.pond_metadata.pond_id),
            record_kind: kind,
            txn_seq: txn_meta.txn_seq,
            txn_id: txn_meta.user.txn_id.to_string(),
            commit_kind: None,
            parent_seq: None,
            duration_ms: None,
            ts_micros: Utc::now().timestamp_micros(),
            metadata_json: "{}".to_string(),
        }
    }

    /// Build a `PostPush*` lifecycle record for the local pond_id.  Unlike
    /// [`Self::base_record`], these are keyed off a raw `(txn_seq, txn_id)`
    /// pair (the sync-remote adapter owns the id) and carry no factory
    /// metadata or `parent_seq`.
    fn post_push_record(
        &self,
        kind: RecordKind,
        txn_seq: i64,
        txn_id: String,
        duration_ms: Option<i64>,
        metadata_json: String,
    ) -> ControlRecord {
        ControlRecord {
            pond_id: self.pond_id_uuid(),
            record_kind: kind,
            txn_seq,
            txn_id,
            commit_kind: None,
            parent_seq: None,
            duration_ms,
            ts_micros: Utc::now().timestamp_micros(),
            metadata_json,
        }
    }
}

// ---- helpers ----

fn pond_id_to_std(id: &uuid7::Uuid) -> StdUuid {
    StdUuid::from_bytes(*id.as_bytes())
}

fn map_err(e: crate::inner_control::StewardError) -> StewardError {
    StewardError::ControlTable(format!("{}", e))
}

/// Serialize an error reason into the `{"reason": ...}` metadata_json shape
/// used by `Failed` and `PostPushFailed` records.
fn reason_json(reason: &str) -> String {
    serde_json::to_string(&serde_json::json!({ "reason": reason })).unwrap_or_else(|_| "{}".into())
}

/// Milliseconds elapsed from `started_micros` until now, clamped to >= 0.
fn elapsed_ms_since(started_micros: i64) -> i64 {
    ((Utc::now().timestamp_micros() - started_micros) / 1000).max(0)
}

async fn seed_pond_metadata(
    inner: &mut InnerControlTable,
    m: &PondMetadata,
) -> Result<(), StewardError> {
    inner
        .config_set(BOOTSTRAP_POND_ID, KEY_STORE_ID, &m.pond_id.to_string())
        .await
        .map_err(map_err)?;
    inner
        .config_set(
            BOOTSTRAP_POND_ID,
            KEY_BIRTH_TIMESTAMP,
            &m.birth_timestamp.to_string(),
        )
        .await
        .map_err(map_err)?;
    inner
        .config_set(BOOTSTRAP_POND_ID, KEY_BIRTHPLACE, &m.birthplace)
        .await
        .map_err(map_err)?;
    inner
        .config_set(BOOTSTRAP_POND_ID, KEY_BIRTH_USERNAME, &m.birth_username)
        .await
        .map_err(map_err)?;
    Ok(())
}

async fn load_pond_metadata(inner: &InnerControlTable) -> Result<PondMetadata, StewardError> {
    let bootstrap = inner
        .config_list(BOOTSTRAP_POND_ID)
        .await
        .map_err(map_err)?;
    let pond_id_str = bootstrap.get(KEY_STORE_ID).ok_or_else(|| {
        StewardError::ControlTable(format!(
            "Control table is missing pond identity (no '{}' setting under bootstrap pond_id)",
            KEY_STORE_ID
        ))
    })?;
    let pond_id = pond_id_str.parse::<uuid7::Uuid>().map_err(|e| {
        StewardError::ControlTable(format!(
            "Invalid '{}' value in control table: {}",
            KEY_STORE_ID, e
        ))
    })?;
    let birth_timestamp = bootstrap
        .get(KEY_BIRTH_TIMESTAMP)
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let birthplace = bootstrap.get(KEY_BIRTHPLACE).cloned().unwrap_or_default();
    let birth_username = bootstrap
        .get(KEY_BIRTH_USERNAME)
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    Ok(PondMetadata {
        pond_id,
        birth_timestamp,
        birthplace,
        birth_username,
    })
}

fn split_config(
    all: HashMap<String, String>,
) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut modes = HashMap::new();
    let mut settings = HashMap::new();
    for (k, v) in all {
        if let Some(name) = k.strip_prefix(FACTORY_MODE_PREFIX) {
            let _previous = modes.insert(name.to_string(), v);
        } else if let Some(name) = k.strip_prefix(SETTING_PREFIX) {
            let _previous = settings.insert(name.to_string(), v);
        }
    }
    (modes, settings)
}

/// Render the pond identity banner to stdout.
#[allow(clippy::print_stdout)]
pub fn pond_metadata_banner(data: &PondMetadata) {
    let datetime = DateTime::from_timestamp(
        data.birth_timestamp / 1_000_000,
        ((data.birth_timestamp % 1_000_000) * 1000) as u32,
    )
    .unwrap_or_else(Utc::now);
    let created_str = datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string();

    let left = vec![
        format!("Pond {}", data.pond_id),
        format!("Created {}", created_str),
    ];

    let birthplace = if data.birthplace.is_empty() {
        "(unspecified)".to_string()
    } else {
        data.birthplace.clone()
    };
    let right = vec![data.birth_username.clone(), birthplace];

    println!(
        "{}",
        utilities::banner::format_banner_from_iters(None, left, right)
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn create_then_open_round_trips_identity_and_modes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("control");

        let metadata = PondMetadata::default();
        let mut table = ControlTable::create(&path, &metadata).await.unwrap();
        assert_eq!(table.pond_metadata().pond_id, metadata.pond_id);

        table.set_factory_mode("remote", "push").await.unwrap();
        assert_eq!(table.get_factory_mode("remote"), Some("push".to_string()));

        let table2 = ControlTable::open(&path).await.unwrap();
        assert_eq!(table2.pond_metadata().pond_id, metadata.pond_id);
        assert_eq!(
            table2.pond_metadata().birth_username,
            metadata.birth_username
        );
        assert_eq!(table2.get_factory_mode("remote"), Some("push".to_string()));
    }

    #[tokio::test]
    async fn settings_roundtrip_via_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("control");
        let metadata = PondMetadata::default();
        let mut table = ControlTable::create(&path, &metadata).await.unwrap();

        table
            .set_setting("hostmount_path", "/mnt/data")
            .await
            .unwrap();
        assert_eq!(
            table.get_setting("hostmount_path"),
            Some("/mnt/data".to_string())
        );

        let table2 = ControlTable::open(&path).await.unwrap();
        assert_eq!(
            table2.get_setting("hostmount_path"),
            Some("/mnt/data".to_string())
        );
        // Settings must not leak into factory_modes via the shared
        // config namespace.
        assert!(table2.get_factory_mode("hostmount_path").is_none());
    }

    #[tokio::test]
    async fn last_write_sequence_tracks_committed_writes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("control");
        let metadata = PondMetadata::default();
        let mut table = ControlTable::create(&path, &metadata).await.unwrap();
        assert_eq!(table.get_last_write_sequence().await.unwrap(), 0);

        let user = tlogfs::PondUserMetadata::new(vec!["init".to_string()]);
        let txn_meta = PondTxnMetadata::new(1, user);
        table
            .record_begin(&txn_meta, None, TransactionType::Write)
            .await
            .unwrap();
        // Begin alone doesn't bump committed seq.
        assert_eq!(table.get_last_write_sequence().await.unwrap(), 0);

        table
            .record_data_committed(&txn_meta, TransactionType::Write, 7, 12, None)
            .await
            .unwrap();
        assert_eq!(table.get_last_write_sequence().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn incomplete_transactions_lists_begin_without_terminal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("control");
        let metadata = PondMetadata::default();
        let mut table = ControlTable::create(&path, &metadata).await.unwrap();

        let user = tlogfs::PondUserMetadata::new(vec!["wip".to_string()]);
        let txn_meta = PondTxnMetadata::new(2, user);
        table
            .record_begin(&txn_meta, None, TransactionType::Write)
            .await
            .unwrap();

        let incomplete = table.find_incomplete_transactions().await.unwrap();
        assert_eq!(incomplete.len(), 1, "begin-without-terminal must appear");
        assert_eq!(incomplete[0].0.txn_seq, 2);
        assert_eq!(incomplete[0].1, 0, "no data_fs_version for incomplete txn");

        // Completing it removes it from the list.
        table
            .record_completed(&txn_meta, TransactionType::Write, 0)
            .await
            .unwrap();
        let still = table.find_incomplete_transactions().await.unwrap();
        assert!(still.is_empty(), "completed txn must not appear incomplete");
    }
}
