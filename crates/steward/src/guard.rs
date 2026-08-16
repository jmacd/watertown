// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Steward Transaction Guard - Wraps tlogfs transaction with audit logging

use crate::{
    PondTxnMetadata, StewardError,
    control_table::{CommitSpine, ControlTable, TransactionType},
    write_lock::WriteLockGuard,
};
use log::{debug, error, info};
use provider::FactoryRegistry;
use provider::registry::ExecutionContext;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tinyfs::FS;
use tlogfs::transaction_guard::TransactionGuard;

/// Reserved directory-entry name of the pond's node-manifest index node under
/// the root.  Paired with the reserved [`tinyfs::INDEX_NODE_UUID`]; excluded
/// from the content-tree fold and node manifest (design
/// `docs/incremental-content-tree-design.md` Section 3).
const INDEX_NODE_NAME: &str = ".pond-node-index";

/// Reserved directory-entry name of the pond's authoritative commit-log node
/// under the root.  Paired with the reserved [`tinyfs::LOG_NODE_UUID`]; each
/// version is one transparency-log leaf (one encoded commit object).  Excluded
/// from the content-tree fold and hidden from enumeration (design
/// `docs/incremental-content-tree-design.md` Section 10, Decision D9).
const LOG_NODE_NAME: &str = ".pond-commit-log";

/// Configuration for a post-commit factory to be executed
struct PostCommitFactoryConfig {
    factory_name: String,
    config_path: String,
    config_bytes: Vec<u8>,
    parent_node_id: tinyfs::FileID,
    factory_mode: String,
}

struct ExpectedContentRoots {
    root_tree_hash: String,
    node_manifest_hash: String,
    node_manifest_root: String,
}

struct PreparedCommit {
    spine: CommitSpine,
    content_roots: ExpectedContentRoots,
}

/// Steward transaction guard wraps TLogFS transaction with control table lifecycle tracking
pub struct StewardTransactionGuard<'a> {
    /// The underlying tlogfs transaction guard
    data_tx: Option<TransactionGuard<'a>>,
    txn_meta: PondTxnMetadata,
    /// Transaction type: read or write
    transaction_type: TransactionType,
    /// Reference to control table for transaction tracking
    control_table: &'a mut ControlTable,
    /// Ship's in-memory allocator, advanced at begin and restored when a write
    /// aborts before the underlying TLogFS transaction commits.
    last_write_seq: &'a mut i64,
    /// Start time for duration tracking
    start_time: std::time::Instant,
    /// Pond path for reloading OpLogPersistence during post-commit
    pond_path: PathBuf,
    /// Track if commit() was called to record failure on drop if not
    committed: bool,
    /// Process-level write lock held for the lifetime of this transaction
    /// when `transaction_type == Write`.  Releases automatically on Drop
    /// (the underlying FD closes, which unlocks at the kernel level).
    /// `None` for read transactions (no exclusion needed).
    #[allow(dead_code)]
    write_lock: Option<WriteLockGuard>,
    /// Number of queued limiter-usage samples written into the pond at the
    /// start of this transaction (Decision L12).  They are dropped from the
    /// control queue only once this transaction commits, so an aborted write
    /// re-emits rather than losing them.
    emitted_usage: std::sync::atomic::AtomicUsize,
    expected_content_roots: Option<ExpectedContentRoots>,
}

impl<'a> StewardTransactionGuard<'a> {
    /// Create a new steward transaction guard
    pub(crate) fn new<P: AsRef<Path>>(
        data_tx: TransactionGuard<'a>,
        txn_meta: &PondTxnMetadata,
        transaction_type: TransactionType,
        control_table: &'a mut ControlTable,
        last_write_seq: &'a mut i64,
        path: P,
        write_lock: Option<WriteLockGuard>,
    ) -> Self {
        Self {
            data_tx: Some(data_tx),
            txn_meta: txn_meta.clone(),
            transaction_type,
            control_table,
            last_write_seq,
            start_time: std::time::Instant::now(),
            pond_path: path.as_ref().to_path_buf(),
            committed: false,
            write_lock,
            emitted_usage: std::sync::atomic::AtomicUsize::new(0),
            expected_content_roots: None,
        }
    }

    /// Get the transaction ID
    #[must_use]
    pub fn txn_meta(&self) -> &PondTxnMetadata {
        &self.txn_meta
    }

    /// Get the transaction type
    #[must_use]
    pub fn transaction_type(&self) -> TransactionType {
        self.transaction_type
    }

    /// Check if this is a write transaction
    #[must_use]
    pub fn is_write_transaction(&self) -> bool {
        self.transaction_type == TransactionType::Write
    }

    /// Check if this is a read transaction
    #[must_use]
    pub fn is_read_transaction(&self) -> bool {
        self.transaction_type == TransactionType::Read
    }

    /// Get access to the control table for querying transaction metadata
    /// This allows accessing control table data without violating borrow rules
    #[must_use]
    pub fn control_table(&self) -> &ControlTable {
        self.control_table
    }

    /// Take the underlying transaction guard (for commit)
    pub(crate) fn take_transaction(&mut self) -> Option<TransactionGuard<'a>> {
        self.data_tx.take()
    }

    /// Get access to the underlying transaction state (for queries)
    ///
    /// Prefer `provider_context()` when all you need is a `ProviderContext`.
    /// Internal only -- external callers should use convenience methods
    /// (`provider_context()`, `get_factory_for_node()`, `query_records()`).
    pub(crate) fn state(&self) -> Result<tlogfs::persistence::State, tlogfs::TLogFSError> {
        self.data_tx
            .as_ref()
            .ok_or_else(|| {
                tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                    "Transaction guard has been consumed".to_string(),
                ))
            })?
            .state()
    }

    /// Get a `ProviderContext` for this transaction.
    ///
    /// This is the preferred way to get provider/factory context. It avoids
    /// exposing tlogfs internals (`State`) to callers that only need to
    /// create table providers or initialize factories.
    pub fn provider_context(&self) -> Result<tinyfs::ProviderContext, tlogfs::TLogFSError> {
        Ok(self.state()?.as_provider_context())
    }

    /// Look up the factory name associated with a filesystem node.
    ///
    /// Returns `Ok(Some(name))` if the node was created by a factory,
    /// `Ok(None)` if it is a static (non-factory) node.
    pub async fn get_factory_for_node(
        &self,
        id: tinyfs::FileID,
    ) -> Result<Option<String>, tlogfs::TLogFSError> {
        self.state()?.get_factory_for_node(id).await
    }

    /// Get the factory name and config bytes for a dynamic node.
    ///
    /// Works for both file-based and directory-based dynamic nodes.
    /// Returns `Ok(Some((factory_name, config_bytes)))` if the node
    /// has factory config, `Ok(None)` otherwise.
    pub async fn get_dynamic_node_config(
        &self,
        id: tinyfs::FileID,
    ) -> Result<Option<(String, Vec<u8>)>, tlogfs::TLogFSError> {
        self.state()?.get_dynamic_node_config(id).await
    }

    /// Query oplog records for a filesystem node.
    ///
    /// Returns the version history entries for the given file.
    pub async fn query_records(
        &self,
        id: tinyfs::FileID,
    ) -> Result<Vec<tlogfs::OplogEntry>, tlogfs::TLogFSError> {
        self.state()?.query_records(id).await
    }

    /// Get the commit history from the underlying Delta table.
    ///
    /// Returns a list of `CommitInfo` entries (most recent first).
    /// Pass `limit` to restrict how many entries are returned.
    pub async fn get_commit_history(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<deltalake::kernel::CommitInfo>, tlogfs::TLogFSError> {
        self.data_persistence()
            .map_err(|e| tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?
            .get_commit_history(limit)
            .await
    }

    /// Get the on-disk store path for the data filesystem.
    pub fn store_path(&self) -> Result<PathBuf, tlogfs::TLogFSError> {
        Ok(self
            .data_persistence()
            .map_err(|e| tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?
            .store_path()
            .clone())
    }

    /// Initialize the root directory in the oplog.
    ///
    /// This is used during pond creation / restoration to set up the
    /// filesystem root. Most callers should use `Ship::create_pond("test-host")` instead.
    pub async fn initialize_root_directory(&self) -> Result<(), tlogfs::TLogFSError> {
        self.state()?.initialize_root_directory().await
    }

    /// Get access to the underlying data persistence layer for read operations
    /// This allows access to the DeltaTable and other query components.
    /// Internal only -- external callers should use convenience methods
    /// (`get_commit_history()`, `store_path()`) instead.
    pub(crate) fn data_persistence(
        &self,
    ) -> Result<&tlogfs::OpLogPersistence, tlogfs::TLogFSError> {
        Ok(self
            .data_tx
            .as_ref()
            .ok_or_else(|| {
                tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                    "Transaction guard has been consumed".to_string(),
                ))
            })?
            .persistence())
    }

    /// Get or create a DataFusion SessionContext with TinyFS ObjectStore registered
    ///
    /// This method ensures a single SessionContext per transaction, preventing
    /// ObjectStore registry conflicts when creating multiple table providers.
    /// Delegates to the underlying TLogFS transaction guard.
    pub async fn session_context(
        &mut self,
    ) -> Result<Arc<datafusion::execution::context::SessionContext>, tlogfs::TLogFSError> {
        self.data_tx
            .as_mut()
            .ok_or_else(|| {
                tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                    "Transaction guard has been consumed".to_string(),
                ))
            })?
            .session_context()
            .await
    }

    /// Abort the transaction and record it as failed
    /// Use this when an error occurs that should be recorded in the control table
    pub async fn abort(mut self, error: impl std::fmt::Display) -> StewardError {
        let error_msg = error.to_string();
        self.rollback(&error_msg).await;
        StewardError::Aborted(error_msg)
    }

    pub(crate) async fn abort_preserving(mut self, error: StewardError) -> StewardError {
        self.rollback(&error.to_string()).await;
        error
    }

    async fn rollback(&mut self, error_msg: &str) {
        let duration_ms = self.start_time.elapsed().as_millis() as i64;
        debug!(
            "Aborting steward transaction {} (seq={}): {}",
            self.txn_meta.user.txn_id, self.txn_meta.txn_seq, error_msg
        );

        // Record the failure in control table — writes only (reads no
        // longer have a matching Begin record).
        if self.transaction_type == TransactionType::Write
            && let Err(e) = self
                .control_table
                .record_failed(
                    &self.txn_meta,
                    self.transaction_type,
                    error_msg.to_string(),
                    duration_ms,
                )
                .await
        {
            log::error!("Failed to record transaction failure: {}", e);
        }

        // Drop the transaction guard without committing (will rollback)
        if self.transaction_type == TransactionType::Write {
            *self.last_write_seq = self.txn_meta.txn_seq - 1;
        }
        self.committed = true; // Mark as handled to avoid Drop warning
        drop(self.data_tx.take());
    }

    /// Commit the transaction with proper steward sequencing
    ///
    /// Returns the data-FS Delta version number on a successful write
    /// commit (`Ok(Some(v))`); returns `Ok(None)` for a read transaction
    /// or a write that produced no changes.
    /// Record that `count` queued usage samples were written into the pond by
    /// this transaction.
    pub fn note_emitted_usage(&self, count: usize) {
        self.emitted_usage
            .store(count, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn expect_content_roots(
        &mut self,
        root_tree_hash: sync_store::content::ObjectHash,
        node_manifest_hash: sync_store::content::ObjectHash,
        node_manifest_root: sync_store::content::ObjectHash,
    ) {
        self.expected_content_roots = Some(ExpectedContentRoots {
            root_tree_hash: root_tree_hash.to_hex(),
            node_manifest_hash: node_manifest_hash.to_hex(),
            node_manifest_root: node_manifest_root.to_hex(),
        });
    }

    async fn prepare_commit(&self) -> Result<Option<PreparedCommit>, StewardError> {
        let mut prepared_commit = None;
        if self.transaction_type == TransactionType::Write {
            let data_tx = self.data_tx.as_ref().ok_or_else(|| {
                StewardError::DataInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                    "Transaction already consumed".to_string(),
                )))
            })?;
            let (pending_records, modified_dirs) = data_tx.state()?.pending_operation_counts();
            if pending_records > 0 || modified_dirs > 0 {
                prepared_commit = self
                    .write_reserved_nodes(
                        data_tx,
                        self.txn_meta.txn_seq,
                        self.txn_meta.user.args.join(" "),
                    )
                    .await?;
            }
        }

        if let Some(expected) = &self.expected_content_roots {
            let actual = if let Some(prepared) = &prepared_commit {
                (
                    prepared.content_roots.root_tree_hash.clone(),
                    prepared.content_roots.node_manifest_hash.clone(),
                    prepared.content_roots.node_manifest_root.clone(),
                )
            } else {
                let data_tx = self.data_tx.as_ref().ok_or_else(|| {
                    StewardError::DataInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                        "Transaction already consumed".to_string(),
                    )))
                })?;
                let pond_id = self.control_table.pond_id_uuid().to_string();
                let current = crate::content_tree::in_txn_content_state(
                    data_tx.persistence().table().clone(),
                    Vec::new(),
                    &pond_id,
                )
                .await?;
                (
                    current.root_tree_hash.to_hex(),
                    current.node_manifest_hash.to_hex(),
                    current.node_manifest_root.to_hex(),
                )
            };
            let mismatch = if actual.0 != expected.root_tree_hash {
                Some(format!(
                    "precommit root tree is {} but expected {}",
                    actual.0, expected.root_tree_hash
                ))
            } else if actual.1 != expected.node_manifest_hash {
                Some(format!(
                    "precommit node manifest hash is {} but expected {}",
                    actual.1, expected.node_manifest_hash
                ))
            } else if actual.2 != expected.node_manifest_root {
                Some(format!(
                    "precommit node manifest Merkle root is {} but expected {}",
                    actual.2, expected.node_manifest_root
                ))
            } else {
                None
            };
            if let Some(error) = mismatch {
                return Err(StewardError::Content(error));
            }
        }

        Ok(prepared_commit)
    }

    pub async fn commit(mut self) -> Result<Option<i64>, StewardError> {
        let args_fmt = format!("{:?}", self.txn_meta.user.args);
        debug!(
            "Committing steward transaction {} {}",
            self.txn_meta.user.txn_id, args_fmt
        );

        // Calculate duration for recording
        let duration_ms = self.start_time.elapsed().as_millis() as i64;

        // Step 1: Transaction metadata was already provided at begin().
        // (Legacy per-import watermark callbacks were removed alongside
        // the chunked-parquet remote factory in D4.5; cross-pond import
        // is planned for D5 via row-level `pond_id` partitioning.)

        // Incremental content tree (Phase 2, Approach A): persist the pond's
        // node manifest into the reserved index node as part of this same
        // transaction, before it is finalized.  Only write transactions that
        // actually changed something get an index-node version; a read
        // transaction (or a no-op write) leaves it untouched.
        let prepared_commit = match self.prepare_commit().await {
            Ok(prepared) => prepared,
            Err(error) => return Err(self.abort_preserving(error).await),
        };

        // Step 2: Extract the underlying transaction guard and commit it.
        let data_tx = self.take_transaction().ok_or_else(|| {
            StewardError::DataInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                "Transaction already consumed".to_string(),
            )))
        })?;
        let commit_result = data_tx.commit().await;

        // Step 3: Record transaction lifecycle in control table based on result
        match commit_result {
            Ok((Some(new_version), persistence)) => {
                // Write transaction committed successfully; the version is
                // the one the FinalizedCommit returned (D5.7a.2) rather
                // than pre_commit_version + 1 arithmetic.

                // VALIDATION: If this was marked as a read transaction but wrote data, fail.
                // Reads no longer leave a Begin record, so there's nothing to terminate in
                // the control table — the in-memory error is sufficient.
                if self.transaction_type == TransactionType::Read {
                    return Err(StewardError::ReadTransactionAttemptedWrite);
                }

                // The post-commit path records a `DataCommitted` control row.
                // Under Decision D9 the authoritative content roots are stamped
                // in-transaction into the commit-log node (by
                // `write_reserved_nodes`), so this path no longer folds the
                // content tree -- step 4b retired that second O(n) fold, leaving
                // the incremental in-transaction fold as the sole content-root
                // computation.  Step 5b then retired the per-partition
                // checksums entirely: the per-directory tree hashes in the
                // content tree subsume them.
                let pond_id = self.control_table.pond_id_uuid();

                // Content-graph spine (Decision D9): the authoritative spine was
                // already stamped into the pond-resident commit-log node, in the
                // same Delta transaction as the data, by `write_reserved_nodes`.
                // The control-table copy recorded below is a disposable,
                // rebuildable cache -- not the source of truth.
                let commit_spine = prepared_commit.map(|prepared| prepared.spine);

                // The leaf appended to the transparency log is the commit
                // object of this commit; a spine is present exactly when the
                // reserved commit-log node got a new leaf this transaction.
                let has_leaf = commit_spine.is_some();

                self.control_table
                    .record_data_committed(
                        &self.txn_meta,
                        self.transaction_type,
                        new_version,
                        duration_ms,
                        commit_spine,
                    )
                    .await
                    .map_err(|e| {
                        StewardError::ControlTable(format!("Failed to record commit: {}", e))
                    })?;
                info!(
                    "Steward transaction {} committed (seq={}, version={})",
                    self.txn_meta.user.txn_id, self.txn_meta.txn_seq, new_version
                );

                // Materialize/reconcile the transparency-log tiles for this
                // commit (Decision D5/D9).  This is a best-effort publishing
                // step after the authoritative Delta commit: a failure here is
                // logged but never unwinds the commit, matching the post-commit
                // factory/remote model below.  It reconciles the tile export
                // against the pond-resident commit-log node, so a previously
                // dropped append (crash, I/O error, unwritable `{POND}/tlog`)
                // self-heals on the next commit.
                if has_leaf {
                    self.materialize_tlog(persistence.table().clone(), pond_id)
                        .await;
                }

                // Mark as committed
                self.committed = true;

                // The usage rows emitted at begin are now durable, so drop
                // them from the control queue.  This happens before the
                // post-commit push below, which will queue fresh samples of
                // its own -- and `drop_emitted` removes a prefix rather than
                // clearing, so those survive.
                let emitted = self
                    .emitted_usage
                    .load(std::sync::atomic::Ordering::Relaxed);
                if emitted > 0 {
                    crate::limiter_usage::drop_emitted(self.control_table, emitted).await;
                }

                // Run post-commit factories for write transactions
                // This happens AFTER commit but uses a NEW transaction
                self.run_post_commit_factories().await;

                // D4: Run post-commit auto-push for /sys/remotes/* entries.
                // Same "after-commit, new transaction" model as factories
                // above; failures are logged but do not undo the commit.
                self.run_post_commit_remotes().await;

                Ok(Some(new_version))
            }
            Ok((None, _persistence)) => {
                // Read-only transaction completed successfully (or write
                // that produced no changes).  Reads no longer leave audit
                // rows in the control table; only write-no-ops do.
                if self.transaction_type == TransactionType::Write {
                    self.control_table
                        .record_completed(&self.txn_meta, self.transaction_type, duration_ms)
                        .await
                        .map_err(|e| {
                            StewardError::ControlTable(format!(
                                "Failed to record completion: {}",
                                e
                            ))
                        })?;
                    debug!(
                        "Write-no-op steward transaction {} completed (seq={})",
                        self.txn_meta.user.txn_id, self.txn_meta.txn_seq
                    );
                } else {
                    debug!(
                        "Read-only steward transaction {} completed (seq={})",
                        self.txn_meta.user.txn_id, self.txn_meta.txn_seq
                    );
                }
                self.committed = true;
                Ok(None)
            }
            Err(e) => {
                // Transaction failed - record error (writes only; reads
                // never recorded a Begin so there's nothing to terminate).
                let error_msg = format!("{}", e);
                if self.transaction_type == TransactionType::Write {
                    *self.last_write_seq = self.txn_meta.txn_seq - 1;
                    self.control_table
                        .record_failed(
                            &self.txn_meta,
                            self.transaction_type,
                            error_msg.clone(),
                            duration_ms,
                        )
                        .await
                        .map_err(|e| {
                            StewardError::ControlTable(format!("Failed to record failure: {}", e))
                        })?;
                }
                self.committed = true;
                Err(StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))
            }
        }
    }

    /// Reconcile this pond's transparency-log tiles with the committed leaf
    /// sequence and re-emit the checkpoint (design Decision D5).
    ///
    /// The authoritative leaf sequence is the ordered `commit_object` bytes of
    /// the spine-bearing `DataCommitted` records in the control table; the tile
    /// log is a derived, re-materializable export of that sequence.  Rather than
    /// trusting `checkpoint.size`, the writer drives its next leaf position from
    /// the committed leaf count: it replays every committed leaf the export is
    /// missing, in commit order, so the export leaf position ends equal to the
    /// commit count.  A dropped append therefore self-heals on the next commit
    /// (a crash between the control record and this call, an I/O error, or an
    /// unwritable `{POND}/tlog` leaves the export lagging until it is replayed).
    /// Tiles ahead of the committed count are harmless and deterministically
    /// re-derived, so the reconciliation only ever extends a lagging export.
    ///
    /// Failures are logged and swallowed: the transparency log is a derived
    /// publishing artifact and must not unwind an already-committed transaction.
    async fn materialize_tlog(&self, table: deltalake::DeltaTable, pond_id: uuid::Uuid) {
        crate::content_tree::materialize_tlog(&self.pond_path, table, pond_id).await
    }

    /// Persist the pond's node manifest into the reserved index node as part of
    /// this write transaction, before it is finalized (design
    /// `docs/incremental-content-tree-design.md` Phase 2, Approach A).
    ///
    /// Write the pond's two reserved nodes -- the derived node-manifest INDEX
    /// node and the authoritative commit-LOG node -- as part of this
    /// transaction, and return the commit spine stamped into the log.
    ///
    /// A single in-transaction fold of the live state (committed rows plus this
    /// transaction's pending records and modified directories) yields the node
    /// manifest and the two content roots at once.  The manifest is written as
    /// a new (collapsing) version of the INDEX node; the commit object -- built
    /// from those roots and chained to the LOG node's current tip -- is appended
    /// as a new version of the LOG node.  Both reserved nodes are excluded from
    /// the fold, so writing them never changes `root_tree_hash`.
    ///
    /// Because the LOG node lands in the same Delta transaction as the data
    /// rows, the pond is self-describing at every committed version: the
    /// authoritative spine lives in the pond, not the disposable control table
    /// (Decision D9).
    async fn write_reserved_nodes(
        &self,
        data_tx: &TransactionGuard<'_>,
        txn_seq: i64,
        commit_args: String,
    ) -> Result<Option<PreparedCommit>, StewardError> {
        use tokio::io::AsyncWriteExt;

        let pond_id = self.control_table.pond_id_uuid();
        let pond_id_str = pond_id.to_string();
        let uncommitted = data_tx.state()?.uncommitted_live_rows().await?;
        let committed_table = data_tx.persistence().table().clone();

        let root = data_tx.root().await?;

        // The prior committed manifest is the incremental fold's baseline; it is
        // absent only at genesis, before the first content-changing commit wrote
        // the index node.  Reading it through the working directory transparently
        // reassembles an externalized (large) manifest.
        let prior_manifest_bytes = if root.exists(INDEX_NODE_NAME).await {
            Some(root.read_file_path_to_vec(INDEX_NODE_NAME).await?)
        } else {
            None
        };

        let inputs = crate::content_tree::incremental_spine_inputs(
            committed_table.clone(),
            prior_manifest_bytes,
            uncommitted.clone(),
            &pond_id_str,
        )
        .await?;

        // Design step 4b oracle: the incremental roots must match a full fold of
        // the same live state.  Always on in debug builds; in release builds it
        // is opt-in via POND_VERIFY_FOLD (see `fold_verification_enabled`) so a
        // high-value pond can validate every commit while other deployments pay
        // just the O(change) incremental fold.
        if crate::content_tree::fold_verification_enabled() {
            let oracle = crate::content_tree::in_txn_spine_inputs(
                committed_table.clone(),
                uncommitted,
                &pond_id_str,
            )
            .await?;
            assert_eq!(
                inputs.root_tree_hash, oracle.root_tree_hash,
                "incremental root_tree_hash diverged from full fold"
            );
            assert_eq!(
                inputs.node_manifest_hash, oracle.node_manifest_hash,
                "incremental node_manifest_hash diverged from full fold"
            );
            assert_eq!(
                inputs.node_manifest_root, oracle.node_manifest_root,
                "incremental node_manifest_root diverged from full fold"
            );
            assert_eq!(
                inputs.manifest_bytes, oracle.manifest_bytes,
                "incremental manifest bytes diverged from full fold"
            );
        }

        // Index node: one full manifest per version, collapsing so it stays at a
        // single live version (Phase 2).
        let mut index_writer = if root.exists(INDEX_NODE_NAME).await {
            root.async_writer_path_collapsing_with_type(
                INDEX_NODE_NAME,
                tinyfs::EntryType::FilePhysicalSeries,
            )
            .await?
        } else {
            let node_id = tinyfs::NodeID::from_hex_string(tinyfs::INDEX_NODE_UUID)
                .map_err(|e| StewardError::Content(format!("reserved index node id: {e}")))?;
            root.create_file_with_id(INDEX_NODE_NAME, node_id).await?
        };
        index_writer.write_all(&inputs.manifest_bytes).await?;
        index_writer.shutdown().await?;

        // Commit-log node: the parent is the current log tip read from the
        // committed table (this transaction's leaf is not written yet), so the
        // new leaf chains onto the previous commit.  Each version is a distinct,
        // permanent leaf, so this is a plain (non-collapsing) append.
        let parent_commit_hash =
            crate::content_tree::log_tip_commit_hash(committed_table, &pond_id_str).await?;
        let spine = crate::content_tree::build_commit_spine(
            parent_commit_hash,
            inputs.root_tree_hash,
            inputs.node_manifest_hash,
            inputs.node_manifest_root,
            &pond_id_str,
            txn_seq,
            commit_args,
        );
        let commit_object = hex::decode(&spine.commit_object)
            .map_err(|e| StewardError::Content(format!("encode commit-log leaf: {e}")))?;
        let mut log_writer = if root.exists(LOG_NODE_NAME).await {
            root.async_writer_path_with_type(LOG_NODE_NAME, tinyfs::EntryType::FilePhysicalSeries)
                .await?
        } else {
            let node_id = tinyfs::NodeID::from_hex_string(tinyfs::LOG_NODE_UUID)
                .map_err(|e| StewardError::Content(format!("reserved log node id: {e}")))?;
            root.create_file_with_id(LOG_NODE_NAME, node_id).await?
        };
        log_writer.write_all(&commit_object).await?;
        log_writer.shutdown().await?;

        Ok(Some(PreparedCommit {
            spine,
            content_roots: ExpectedContentRoots {
                root_tree_hash: inputs.root_tree_hash.to_hex(),
                node_manifest_hash: inputs.node_manifest_hash.to_hex(),
                node_manifest_root: inputs.node_manifest_root.to_hex(),
            },
        }))
    }

    /// Run post-commit factories after a successful write transaction
    /// This discovers and executes factories from /system/run/* in order
    /// Only runs factories configured with "push" mode (skips "pull" mode factories)
    async fn run_post_commit_factories(&mut self) {
        debug!("Starting post-commit factory discovery and execution");

        // Discover post-commit factory configurations
        let factory_configs = match self.discover_post_commit_factories().await {
            Ok(configs) => configs,
            Err(e) => {
                log::warn!("Failed to discover post-commit factories: {}", e);
                return;
            }
        };

        if factory_configs.is_empty() {
            debug!("No post-commit factories found");
            return;
        }

        debug!("Discovered {} factory node(s)", factory_configs.len());

        // Filter factories based on their execution mode
        let mut factories_to_run = Vec::new();

        for mut config in factory_configs {
            // Check factory mode setting - set default "push" for unconfigured factories
            let factory_mode = match self.control_table.get_factory_mode(&config.factory_name) {
                Some(mode) => mode,
                None => {
                    // Default to "push" mode for post-commit factories in /system/run/
                    info!(
                        "Factory '{}' has no mode configured, defaulting to 'push' (automatic execution)",
                        config.factory_name
                    );
                    // Set the default mode in control table for future use
                    if let Err(e) = self
                        .control_table
                        .set_factory_mode(&config.factory_name, "push")
                        .await
                    {
                        error!(
                            "Failed to set default factory mode for '{}': {}",
                            config.factory_name, e
                        );
                        continue; // Skip this factory if we can't set mode
                    }
                    "push".to_string()
                }
            };

            if factory_mode == "pull" {
                info!(
                    "Skipping factory '{}' (mode: pull, only runs on manual sync)",
                    config.factory_name
                );
                continue;
            }

            debug!(
                "Will execute factory '{}' (mode: {})",
                config.factory_name, factory_mode
            );
            config.factory_mode = factory_mode;
            factories_to_run.push(config);
        }

        if factories_to_run.is_empty() {
            info!("No factories configured for post-commit execution");
            return;
        }

        info!(
            "Executing {} post-commit factor{}",
            factories_to_run.len(),
            if factories_to_run.len() == 1 {
                "y"
            } else {
                "ies"
            }
        );

        // Record the full queue of pending tasks in a SINGLE control-table
        // commit rather than one per factory.
        let pending_entries: Vec<(i64, String, String)> = factories_to_run
            .iter()
            .enumerate()
            .map(|(i, config)| {
                (
                    (i + 1) as i64,
                    config.factory_name.clone(),
                    config.config_path.clone(),
                )
            })
            .collect();
        if let Err(e) = self
            .control_table
            .record_post_commit_pending_batch(&self.txn_meta, &pending_entries)
            .await
        {
            log::error!("Failed to record post-commit pending batch: {}", e);
            // Continue despite tracking failure
        }

        let total_factories = factories_to_run.len();

        // Execute each factory independently.  Each execution records its
        // own terminal lifecycle (DataCommitted/Completed + PostPush*) in a
        // single batched control-table commit; the caller only logs.
        for (execution_seq, config) in factories_to_run.into_iter().enumerate() {
            let execution_seq = (execution_seq + 1) as i64; // 1-indexed
            debug!(
                "Executing post-commit factory {}/{}: {} from {} (mode: {})",
                execution_seq,
                total_factories,
                config.factory_name,
                config.config_path,
                config.factory_mode
            );

            // Execute the factory
            match self
                .execute_post_commit_factory(
                    execution_seq,
                    &config.factory_name,
                    &config.config_path,
                    &config.config_bytes,
                    config.parent_node_id,
                    &config.factory_mode,
                )
                .await
            {
                Ok(()) => {
                    info!("Post-commit factory succeeded: {}", config.config_path);
                }
                Err(e) => {
                    log::error!("Post-commit factory failed: {} - {}", config.config_path, e);
                    // Continue to next factory despite failure
                }
            }
        }

        info!("Post-commit factory execution complete");
    }

    /// Discover post-commit factory configurations from /system/run/*
    /// Returns factory configs sorted by config_path
    async fn discover_post_commit_factories(
        &self,
    ) -> Result<Vec<PostCommitFactoryConfig>, StewardError> {
        debug!("Discovering post-commit factories from /system/run/*");

        // Post-commit discovery happens AFTER the transaction is committed
        // We need a NEW read transaction to see the committed data
        // This is a separate operation, not part of the original write transaction
        let data_path = crate::get_data_path(Path::new(&self.pond_path));
        let pond_id = self.control_table.pond_metadata().pond_id.to_string();
        let mut data_persistence = tlogfs::OpLogPersistence::open(&data_path, pond_id)
            .await
            .map_err(StewardError::DataInit)?;
        // Use the CURRENT txn_seq (the one we just committed), not +1
        // Read transactions must use the last write sequence
        let discovery_metadata = PondTxnMetadata::new(
            self.txn_meta.txn_seq,
            tlogfs::PondUserMetadata::new(vec![
                "internal".to_string(),
                "post-commit-discovery".to_string(),
            ]),
        );
        let discovery_tx = data_persistence
            .begin_read(&discovery_metadata)
            .await
            .map_err(StewardError::DataInit)?;

        let fs = FS::new(discovery_tx.state()?)
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        let root = fs
            .root()
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Check if /system/run exists before trying to match
        debug!(
            "Checking for /system/run directory at txn_seq={}",
            self.txn_meta.txn_seq
        );
        match root.resolve_path("/system/run").await {
            Ok((_wd, _lookup)) => {
                debug!("Resolved /system/run");
            }
            Err(e) => {
                debug!("Failed to resolve /system/run: {}", e);
                // Commit the discovery transaction before returning
                _ = discovery_tx.commit().await;
                return Ok(Vec::new());
            }
        }

        // Use collect_matches to find all configs in /system/run/*
        // Returns Vec<(NodePath, Vec<String>)> where first element is path, second is captures
        debug!("Looking for configs matching /system/run/*");
        let matches = root
            .collect_matches("/system/run/*")
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        debug!("Found {} matches for /system/run/*", matches.len());

        // D5.7b.3: scope auto-exec to local pond_id. Configs whose
        // node lives under a cross-pond mount (foreign pond_id) are
        // skipped here -- they remain invokable via explicit
        // `pond run /imports/foo/system/run/whatever`.
        let local_pond_id_str = self.control_table.pond_metadata().pond_id.to_string();
        let local_pond_uuid: uuid7::Uuid = local_pond_id_str.parse().map_err(|e| {
            StewardError::ControlTable(format!(
                "local pond_id `{}` is not a valid uuid7: {}",
                local_pond_id_str, e
            ))
        })?;

        let mut factory_configs = Vec::new();

        for (node_path, _captures) in matches {
            let config_path_str = node_path.path().to_string_lossy().to_string();
            let config_file_id = node_path.id();

            if config_file_id.pond_id() != local_pond_uuid {
                debug!(
                    "Skipping foreign post-commit config {} (pond_id={} != local {})",
                    config_path_str,
                    config_file_id.pond_id(),
                    local_pond_uuid
                );
                continue;
            }

            debug!(
                "Found post-commit config: {} (id={})",
                config_path_str, config_file_id
            );

            // Get parent directory ID (needed for factory context)
            let parent_path = node_path.dirname();
            let (parent_wd, _lookup) = match root.resolve_path(&parent_path).await {
                Ok(result) => result,
                Err(e) => {
                    log::warn!(
                        "Failed to resolve parent path {}: {}",
                        parent_path.display(),
                        e
                    );
                    continue;
                }
            };
            let parent_id = parent_wd.node_path().id();

            // Read the config file using async_reader_path + read_to_end
            let config_bytes = {
                let mut reader = match root.async_reader_path(node_path.path()).await {
                    Ok(r) => r,
                    Err(e) => {
                        log::warn!("Failed to open config {}: {}", config_path_str, e);
                        continue;
                    }
                };

                let mut buffer = Vec::new();
                match tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buffer).await {
                    Ok(bytes_read) => {
                        debug!("Read {} bytes from config {}", bytes_read, config_path_str);
                        debug!("Config content: {}", String::from_utf8_lossy(&buffer));
                        buffer
                    }
                    Err(e) => {
                        log::warn!("Failed to read config {}: {}", config_path_str, e);
                        continue;
                    }
                }
            };

            // Get the factory name from the oplog entry for this config file
            let factory_name = match discovery_tx
                .state()?
                .get_factory_for_node(config_file_id)
                .await
            {
                Ok(Some(name)) => name,
                Ok(None) => {
                    log::warn!("No factory associated with config: {}", config_path_str);
                    continue;
                }
                Err(e) => {
                    log::warn!("Failed to get factory for {}: {}", config_path_str, e);
                    continue;
                }
            };

            factory_configs.push(PostCommitFactoryConfig {
                factory_name,
                config_path: config_path_str,
                config_bytes,
                parent_node_id: parent_id,
                factory_mode: String::new(), // Will be set in run_post_commit_factories
            });
        }

        // Commit the post-commit discovery transaction
        _ = discovery_tx
            .commit()
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Sort by config_path for deterministic execution order
        factory_configs.sort_by(|a, b| a.config_path.cmp(&b.config_path));

        Ok(factory_configs)
    }

    /// Execute a single post-commit factory
    async fn execute_post_commit_factory(
        &mut self,
        execution_seq: i64,
        factory_name: &str,
        config_path: &str,
        config_bytes: &[u8],
        parent_node_id: tinyfs::FileID,
        factory_mode: &str,
    ) -> Result<(), StewardError> {
        debug!(
            "Executing post-commit factory: {} with config {} (mode: {})",
            factory_name, config_path, factory_mode
        );

        // factory_mode is passed as parameter, no need to query again

        debug!("Factory {} has mode: {}", factory_name, factory_mode);

        // Reload OpLogPersistence for a fresh write transaction
        let data_path = crate::get_data_path(&self.pond_path);
        let pond_id = self.control_table.pond_metadata().pond_id.to_string();
        let mut data_persistence = tlogfs::OpLogPersistence::open(&data_path, pond_id)
            .await
            .map_err(StewardError::DataInit)?;

        // Open a new write transaction for factory execution.
        // Use `last_txn_seq + 1` from the freshly-opened persistence rather
        // than `self.txn_meta.txn_seq + 1`: if multiple post-commit factories
        // run in a single outer commit, each successive factory must allocate
        // the next sequence after the previous factory's commit.
        let factory_start = std::time::Instant::now();
        let factory_txn_seq = data_persistence.last_txn_seq() + 1;
        let metadata = PondTxnMetadata::new(
            factory_txn_seq,
            tlogfs::PondUserMetadata::new(vec![
                "internal".to_string(),
                "post-commit-factory".to_string(),
                factory_name.to_string(),
                config_path.to_string(),
            ]),
        );

        // Record `Begin` on the control table BEFORE opening the data
        // transaction so the factory's write has a complete audit trail.
        // Without this row, replication's `Remote::push` sees `NoSuchCommit`
        // for the factory's txn_seq and silently drops the bundle, breaking
        // cross-pond replication of factory-produced data.
        self.control_table
            .record_begin(
                &metadata,
                Some(self.txn_meta.txn_seq),
                TransactionType::Write,
            )
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!(
                    "Failed to record post-commit factory begin: {}",
                    e
                ))
            })?;

        let factory_tx = data_persistence
            .begin_write(&metadata)
            .await
            .map_err(StewardError::DataInit)?;

        // Query pond metadata from control table
        let pond_metadata = self.control_table.get_pond_metadata();

        // Create factory context with pond metadata
        let state = factory_tx.state()?;
        let provider_context = state.as_provider_context();
        let factory_context = provider::FactoryContext::with_metadata(
            provider_context,
            parent_node_id,
            pond_metadata.clone(),
        )
        .with_txn_seq(self.txn_meta.txn_seq);

        // Pass factory mode as args[0]
        let args = vec![factory_mode.to_string()];

        // Expand ${env:VAR} references at runtime so secrets are never
        // persisted in the oplog.
        let expanded_config = if utilities::env_substitution::has_env_refs(
            std::str::from_utf8(config_bytes).unwrap_or(""),
        ) {
            match utilities::env_substitution::substitute_env_vars(
                std::str::from_utf8(config_bytes).map_err(|e| {
                    StewardError::DataInit(tlogfs::TLogFSError::Internal(format!(
                        "Config for {} is not valid UTF-8: {}",
                        config_path, e
                    )))
                })?,
            ) {
                Ok(expanded) => expanded.into_bytes(),
                Err(e) => {
                    log::warn!("Failed to expand env vars in config {}: {}", config_path, e);
                    config_bytes.to_vec()
                }
            }
        } else {
            config_bytes.to_vec()
        };

        // Execute the factory as a ControlWriter.
        let result = FactoryRegistry::execute(
            factory_name,
            &expanded_config,
            factory_context,
            ExecutionContext::control_writer(args),
        )
        .await;

        // Decision D9: every content-changing guarded commit must update the
        // reserved index and commit-log nodes so the pond's node manifest and
        // authoritative spine stay consistent with the data written in the same
        // Delta transaction.  A post-commit factory that produced content is
        // such a commit, so fold its reserved nodes here -- before the commit --
        // exactly as the primary `commit()` path does.  Skipping this left the
        // manifest stale (missing the factory's nodes), which the next ordinary
        // commit's incremental fold detected as a divergence from the full fold.
        let factory_spine = {
            let (pending_records, modified_dirs) = factory_tx.state()?.pending_operation_counts();
            if pending_records > 0 || modified_dirs > 0 {
                self.write_reserved_nodes(
                    &factory_tx,
                    factory_txn_seq,
                    metadata.user.args.join(" "),
                )
                .await?
            } else {
                None
            }
        }
        .map(|prepared| prepared.spine);

        // Commit the post-commit factory execution transaction
        // Metadata was already provided at begin()
        let commit_result = factory_tx.commit().await;
        let duration_ms = factory_start.elapsed().as_millis() as i64;

        // The PostPush outcome reflects the FACTORY's execution result, not
        // the data commit: a factory that errored may still have committed
        // partial data, in which case we record DataCommitted/Completed for
        // the factory transaction but PostPushFailed for the parent.
        let outcome: Result<(), String> = result.as_ref().map(|_| ()).map_err(|e| format!("{}", e));

        match commit_result {
            Ok((Some(new_version), persistence)) => {
                // Partition checksums are retired (Decision D9, step 5b): the
                // per-directory tree hashes in the content tree subsume them.
                //
                // The factory commit appended a commit-log leaf exactly when it
                // wrote reserved nodes (content changed).  Record its spine into
                // the disposable control-table cache and reconcile the tlog.
                let has_leaf = factory_spine.is_some();

                // DataCommitted + Completed (factory txn) + PostPush* (parent)
                // in a single batched control-table commit.
                self.control_table
                    .record_factory_terminal_batch(
                        &metadata,
                        &self.txn_meta,
                        execution_seq,
                        Some(new_version),
                        duration_ms,
                        factory_spine,
                        outcome,
                    )
                    .await
                    .map_err(|e| {
                        StewardError::ControlTable(format!(
                            "Failed to record post-commit factory terminal batch: {}",
                            e
                        ))
                    })?;
                if has_leaf {
                    let pond_id = self.control_table.pond_id_uuid();
                    self.materialize_tlog(persistence.table().clone(), pond_id)
                        .await;
                }
                debug!(
                    "Post-commit factory transaction {} committed (seq={}, version={})",
                    metadata.user.txn_id, factory_txn_seq, new_version
                );
            }
            Ok((None, _persistence)) => {
                // Factory write was a no-op (no records produced).  Record
                // `Completed` + PostPush* so the audit trail terminates the
                // `Begin`; no DataCommitted since nothing was written.
                self.control_table
                    .record_factory_terminal_batch(
                        &metadata,
                        &self.txn_meta,
                        execution_seq,
                        None,
                        duration_ms,
                        None,
                        outcome,
                    )
                    .await
                    .map_err(|e| {
                        StewardError::ControlTable(format!(
                            "Failed to record post-commit factory no-op terminal batch: {}",
                            e
                        ))
                    })?;
                debug!(
                    "Post-commit factory transaction {} was a write no-op (seq={})",
                    metadata.user.txn_id, factory_txn_seq
                );
            }
            Err(commit_err) => {
                // Commit failed - record Failed + PostPushFailed so the Begin
                // row is terminated and the audit log isn't left dangling.
                let err_msg = format!("{}", commit_err);
                if let Err(record_err) = self
                    .control_table
                    .record_factory_aborted_batch(
                        &metadata,
                        &self.txn_meta,
                        execution_seq,
                        err_msg.clone(),
                        duration_ms,
                    )
                    .await
                {
                    log::error!(
                        "Failed to record post-commit factory failure: {} (original error: {})",
                        record_err,
                        err_msg
                    );
                }
                return Err(StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                    commit_err,
                )));
            }
        }

        result.map_err(StewardError::DataInit)
    }

    /// D4 post-commit auto-push: scan `/sys/remotes/*` and push every
    /// pending `txn_seq` to each attachment whose mode is `push` or
    /// `both`.  This is the replacement for the legacy
    /// `/system/run/<N>-remote` factory dispatch.
    ///
    /// Like [`Self::run_post_commit_factories`], failures from any
    /// individual remote are logged but do not roll back the data
    /// commit (the local write has already succeeded).  The legacy
    /// factory-based dispatcher continues to run alongside this method
    /// until D4.5 deletes `crates/remote`.
    async fn run_post_commit_remotes(&mut self) {
        debug!("Starting post-commit /sys/remotes/* discovery");

        let remotes = match self.discover_sys_remotes().await {
            Ok(r) => r,
            Err(e) => {
                log::warn!("Failed to discover /sys/remotes/*: {}", e);
                return;
            }
        };

        if remotes.is_empty() {
            debug!("No /sys/remotes/* entries found");
            return;
        }

        // Filter by mode (raw_config key `remote_mode:<name>`); default is push.
        let mut to_push: Vec<(String, crate::RemoteAttachment)> = Vec::new();
        for (name, attachment) in remotes {
            let mode_key = format!("{}{}", crate::REMOTE_MODE_PREFIX, name);
            let mode_str = match self.control_table.raw_config_get(&mode_key).await {
                Ok(Some(v)) if !v.is_empty() => v,
                _ => "push".to_string(),
            };
            let mode = match crate::RemoteMode::parse(&mode_str) {
                Ok(m) => m,
                Err(e) => {
                    log::warn!(
                        "post-commit auto-push: skipping `{}` (invalid mode `{}`: {})",
                        name,
                        mode_str,
                        e
                    );
                    continue;
                }
            };
            if !mode.pushes() {
                debug!(
                    "post-commit auto-push: skipping `{}` (mode={})",
                    name,
                    mode.as_str()
                );
                continue;
            }
            to_push.push((name, attachment));
        }

        if to_push.is_empty() {
            debug!("No /sys/remotes/* entries are in push/both mode");
            return;
        }

        // Open a fresh Ship for the push.  This is safe because the
        // data transaction has already committed; the fresh Ship sees
        // the committed view and has its own ControlTable handle on
        // the same underlying Delta table.  `self.control_table` is
        // about to become stale but the guard drops immediately after
        // commit() returns.
        let mut steward = match crate::Steward::open_pond(&self.pond_path).await {
            Ok(s) => s,
            Err(e) => {
                log::error!(
                    "post-commit auto-push: failed to reopen pond at {:?}: {}",
                    self.pond_path,
                    e
                );
                return;
            }
        };
        let ship = match steward.as_pond_mut() {
            Some(s) => s,
            None => {
                log::error!("post-commit auto-push: reopened steward is not a pond (internal bug)");
                return;
            }
        };

        for (name, attachment) in to_push {
            info!("post-commit auto-push: {} -> {}", name, attachment.url);
            // One dispatch (Decision A8).  Boxed for the same reason the
            // limiter bind below is: reading a profile opens a read
            // transaction, and the compiler cannot see that a read
            // transaction runs no post-commit work, so the cycle it infers
            // is not a real one.
            let storage_options =
                match Box::pin(crate::storage_profile::prepare_storage(ship, &attachment)).await {
                    Ok(o) => o,
                    Err(e) => {
                        log::error!("post-commit auto-push: {} bad storage options: {}", name, e);
                        continue;
                    }
                };
            // Bind before any network work: a limiter that names a missing
            // node or governs the wrong unit is a misconfiguration, and it
            // should surface without having spent anything first.
            let limit_spec = match attachment.resolved_limits() {
                Ok(v) => v,
                Err(e) => {
                    log::error!("post-commit auto-push: {} bad limits: {}", name, e);
                    continue;
                }
            };
            // Boxed to break an async recursion cycle: this runs from
            // `commit()`, and binding a limiter opens a read transaction whose
            // own `commit()` closes the loop.  The recursion is not real --
            // a read transaction runs no post-commit work -- but the future
            // type is infinitely sized without an indirection here.
            let mut limits = match Box::pin(crate::LimiterSet::open(ship, &limit_spec)).await {
                Ok(l) => l,
                Err(e) => {
                    log::error!(
                        "post-commit auto-push: {} could not bind limiters: {}",
                        name,
                        e
                    );
                    continue;
                }
            };

            // Open inside the budget, not before it: opening a Delta table
            // reads the log, and that is the larger half of a tick's traffic.
            let pushed = crate::open_and_push_to_remote_limited(
                ship,
                &attachment.url,
                storage_options,
                "main",
                &mut limits,
            )
            .await;

            // Record usage regardless of outcome: a push that failed partway
            // still spent what it spent, and an unattended retry every commit
            // would otherwise never be charged for its failures.
            if let Err(e) = limits.commit(ship.control_table_mut()).await {
                log::warn!(
                    "post-commit auto-push: {} failed to record limiter usage: {}",
                    name,
                    e
                );
            }

            match pushed {
                Ok(outcome) => {
                    let tip_hex = outcome.tip.to_hex();
                    info!(
                        "post-commit auto-push: {} done (objects_pushed={}, tip={})",
                        name, outcome.objects_pushed, tip_hex
                    );
                    // Record the per-ref frontier we last pushed (CA3 tip hash,
                    // replacing the retired per-pond seq watermark).
                    if let Err(e) = ship
                        .control_table_mut()
                        .raw_config_set(&format!("last_pushed_tip:{}", attachment.url), &tip_hex)
                        .await
                    {
                        log::warn!(
                            "post-commit auto-push: {} pushed but failed to record last_pushed_tip: {}",
                            name,
                            e
                        );
                    }
                }
                Err(e) => {
                    log::error!("post-commit auto-push: {} failed: {}", name, e);
                    // Continue with the next remote; one bad target
                    // shouldn't poison the others.
                }
            }
        }
    }

    /// Discover all `/sys/remotes/*` entries and parse their YAML.
    /// Returns `(name, attachment)` pairs.  Quietly returns an empty
    /// list if `/sys/remotes` does not exist yet.
    async fn discover_sys_remotes(
        &self,
    ) -> Result<Vec<(String, crate::RemoteAttachment)>, StewardError> {
        let data_path = crate::get_data_path(Path::new(&self.pond_path));
        let pond_id = self.control_table.pond_metadata().pond_id.to_string();
        let mut data_persistence = tlogfs::OpLogPersistence::open(&data_path, pond_id)
            .await
            .map_err(StewardError::DataInit)?;

        let discovery_metadata = PondTxnMetadata::new(
            self.txn_meta.txn_seq,
            tlogfs::PondUserMetadata::new(vec![
                "internal".to_string(),
                "post-commit-remote-discovery".to_string(),
            ]),
        );
        let discovery_tx = data_persistence
            .begin_read(&discovery_metadata)
            .await
            .map_err(StewardError::DataInit)?;

        let fs = FS::new(discovery_tx.state()?)
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
        let root = fs
            .root()
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // If /sys/remotes doesn't exist yet (no `pond remote add` has
        // been run), bail out cleanly.
        if root.resolve_path(crate::SYS_REMOTES_DIR).await.is_err() {
            _ = discovery_tx.commit().await;
            return Ok(Vec::new());
        }

        let pattern = format!("{}/*", crate::SYS_REMOTES_DIR);
        let matches = root
            .collect_matches(&pattern)
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        let mut out: Vec<(String, crate::RemoteAttachment)> = Vec::new();
        for (node_path, _captures) in matches {
            let cfg_path = node_path.path().to_path_buf();
            let name = match cfg_path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            let mut reader = match root.async_reader_path(&cfg_path).await {
                Ok(r) => r,
                Err(e) => {
                    log::warn!(
                        "post-commit auto-push: cannot read {}: {}",
                        cfg_path.display(),
                        e
                    );
                    continue;
                }
            };
            let mut buf = Vec::new();
            if let Err(e) = tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buf).await {
                log::warn!(
                    "post-commit auto-push: read error on {}: {}",
                    cfg_path.display(),
                    e
                );
                continue;
            }

            match crate::RemoteAttachment::from_yaml_bytes(&buf) {
                Ok(att) => out.push((name, att)),
                Err(e) => {
                    log::warn!(
                        "post-commit auto-push: invalid YAML in {}: {}",
                        cfg_path.display(),
                        e
                    );
                    continue;
                }
            }
        }

        _ = discovery_tx
            .commit()
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Deterministic order so logs are easy to read.
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }
}

impl<'a> Deref for StewardTransactionGuard<'a> {
    type Target = FS;

    fn deref(&self) -> &Self::Target {
        self.data_tx
            .as_ref()
            .expect("Transaction guard has been consumed")
    }
}

impl<'a> Drop for StewardTransactionGuard<'a> {
    fn drop(&mut self) {
        if self.data_tx.is_some() && !self.committed {
            if self.transaction_type == TransactionType::Write {
                *self.last_write_seq = self.txn_meta.txn_seq - 1;
            }
            // Transaction was neither committed nor explicitly failed
            // This happens when an error occurs before commit() is called
            // The transaction will rollback but we can't record failure here (Drop isn't async)
            log::warn!(
                "Steward transaction guard dropped without commit - transaction will rollback (txn={}, seq={}). \
                 Note: This transaction will show as INCOMPLETE in control table.",
                self.txn_meta.user.txn_id,
                self.txn_meta.txn_seq,
            );
        }
    }
}
