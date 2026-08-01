// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use super::directory::OpLogDirectory;
use super::error::TLogFSError;
use super::schema::{DirectoryEntry, ForArrow, OplogEntry};
use super::symlink::OpLogSymlink;
use super::transaction_guard::TransactionGuard;
use crate::txn_metadata::{PondTxnMetadata, PondUserMetadata};
use arrow::array::{Array, DictionaryArray};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::UInt16Type;
use async_trait::async_trait;
use chrono::Utc;
use datafusion::execution::context::{SessionConfig, SessionContext};
use deltalake::DeltaTable;
use deltalake::kernel::CommitInfo;
use deltalake::protocol::SaveMode;
use log::{debug, info, warn};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use provider::{FactoryContext, FactoryRegistry};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::ResultExt;
use tinyfs::{
    EntryType, FS, FileID, FileVersionInfo, Node, NodeMetadata, NodeType, Result as TinyFSResult,
    persistence::PersistenceLayer, transaction_guard::TransactionState as TinyFsTransactionState,
};
use tokio::sync::Mutex;
use url::Url;

pub struct OpLogPersistence {
    pub(crate) path: PathBuf,
    pub(crate) table: DeltaTable,
    pub(crate) fs: Option<FS>,
    pub(crate) state: Option<State>,
    /// Per-pond_id transaction-sequence allocator: `pond_id` -> last
    /// committed `txn_seq` for that pond.  Each pond_id has its own seq
    /// space.  Only the LOCAL pond (`self.pond_id`) is advanced by
    /// `begin_write`/`commit`; foreign pond_ids appear here only via
    /// cross-pond import (`sync_last_txn_seq`).  This realizes the
    /// design's per-pond_id namespaced seq spaces at the data-table
    /// layer: importing a fast foreign producer no longer inflates the
    /// local pond's own seq numbering or leaves gaps in its history.
    pub(crate) seqs: HashMap<String, i64>,
    /// Transaction state for enforcing single-writer pattern (shared with tinyfs)
    pub(crate) txn_state: Arc<TinyFsTransactionState>,
    /// Options for large file storage (compression, etc.)
    large_file_options: crate::large_files::LargeFileOptions,
    /// Pond identity UUID - stamped into every OplogEntry at commit time.
    /// For locally-created records, this is the local pond's UUID.
    /// Set at create/open time and never changes.
    pub(crate) pond_id: String,
}

/// In-memory directory state during a transaction
/// Tracks loaded directory content and whether it has been modified
pub struct DirectoryState {
    /// Whether this directory has been modified during this transaction
    pub modified: bool,
    /// Map of child name -> DirectoryEntry for O(1) lookups and duplicate detection
    pub mapping: HashMap<String, tinyfs::DirectoryEntry>,
}

impl DirectoryState {
    /// Create a new unmodified directory state from entries
    fn new_unmodified(entries: Vec<tinyfs::DirectoryEntry>) -> Self {
        let mapping = entries
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect();
        Self {
            modified: false,
            mapping,
        }
    }

    /// Create a new empty unmodified directory state
    fn new_empty() -> Self {
        Self {
            modified: false,
            mapping: HashMap::new(),
        }
    }
}

pub struct InnerState {
    path: PathBuf,
    table: DeltaTable,        // The Delta table for this transaction
    records: Vec<OplogEntry>, // @@@ LINEAR SEARCH
    directories: HashMap<FileID, DirectoryState>,
    /// Track files that exist in directories but haven't been written yet (pending write)
    pending_files: HashMap<FileID, EntryType>,
    /// Track pre-allocated version numbers for async writes in progress
    allocated_versions: HashMap<FileID, Vec<i64>>,
    /// Transaction poisoned flag - if true, commit must fail
    poisoned: bool,
    session_context: Arc<SessionContext>,
    txn_seq: i64,
    /// Options for large file storage (compression, etc.)
    large_file_options: crate::large_files::LargeFileOptions,
    /// Cache of committed oplog records by partition, keyed by
    /// (part_id -> (node_id -> records sorted by timestamp desc)).
    /// Populated on first access per partition via a single SQL query,
    /// avoiding per-file queries when listing directories.
    partition_records_cache: HashMap<tinyfs::PartID, HashMap<tinyfs::NodeID, Vec<OplogEntry>>>,
    /// External parquet files to include as Delta Add actions at commit time.
    /// Used by cross-pond import to register imported files in the same
    /// Delta commit as the normal OpLog records. Each entry is (path, size, part_id).
    external_add_actions: Vec<ExternalAddAction>,
}

/// An external parquet file to register as a Delta Add action at commit time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalAddAction {
    /// Relative path in the object store, e.g.
    /// `"pond_id=<uuid>/part_id=<uuid>/file.parquet"` (D5+).
    pub path: String,
    /// File size in bytes
    pub size: i64,
    /// `pond_id` partition value (UUID of the owning pond; may differ
    /// from the local pond for cross-pond import).
    pub pond_id: String,
    /// `part_id` partition value (UUID of the parent directory).
    pub part_id: String,
}

#[derive(Clone)]
pub struct State {
    inner: Arc<Mutex<InnerState>>,
    /// TinyFS ObjectStore instance - shared with SessionContext
    object_store: Arc<tokio::sync::OnceCell<Arc<crate::TinyFsObjectStore<Self>>>>,
    /// The DataFusion SessionContext - stored outside the lock to avoid deadlocks
    /// This is the same instance stored in inner.session_context
    session_context: Arc<SessionContext>,
    /// Cache for TableProvider instances to avoid repeated ListingTable creation and schema inference
    /// Key: (node_id, part_id, version_selection) -> TableProvider with temporal filtering
    table_provider_cache: Arc<
        std::sync::Mutex<HashMap<TableProviderKey, Arc<dyn datafusion::catalog::TableProvider>>>,
    >,
    /// Transaction state for enforcing single-writer pattern (shared with tinyfs)
    txn_state: Arc<TinyFsTransactionState>,
    /// Options for large file storage (compression, etc.)
    large_file_options: crate::large_files::LargeFileOptions,
    /// Format provider cache directory ({POND}/cache/), computed from data path
    cache_dir: Option<PathBuf>,
    /// Pond root directory ({POND}/), computed from data path
    pond_path: Option<PathBuf>,
    /// Pond identity UUID for this persistence layer
    pond_id: String,
}

// Re-export TableProviderKey from provider for backward compatibility
pub use provider::TableProviderKey;

/// Outcome of [`State::collapse_file_series`].
#[derive(Debug, Clone)]
pub struct CollapseStats {
    /// True if a merged version was written; false for a no-op (fewer than
    /// two live versions to merge).
    pub collapsed: bool,
    /// Number of live (non-superseded, non-empty) versions before the collapse.
    pub versions_before: usize,
    /// Version number of the merged row written, when `collapsed` is true.
    pub merged_version: i64,
    /// Total bytes of merged content.
    pub bytes: u64,
}

/// Identity columns returned by the [`State::list_collapsible_series`]
/// discovery query, deserialized straight from the projected record batch.
#[derive(serde::Deserialize)]
struct CollapseCandidate {
    pond_id: String,
    part_id: String,
    node_id: String,
}

/// How many adjacent same-class versions collapse into one run.
///
/// Re-exported from the shared size-tiered policy so that series collapse and
/// the partial-aggregate cache's segment compaction cannot drift apart.
pub use provider::size_tier::MERGE_FANOUT as COLLAPSE_FANOUT;

/// Choose the contiguous window of live rows to merge, or `None` for a no-op.
///
/// Thin adapter over [`provider::size_tier::choose_merge_window`]: the policy
/// depends on nothing but segment sizes, so all this does is project the sizes
/// out of the oplog rows. `size` is a nullable `i64` on disk; a missing or
/// negative size is treated as zero, which puts the row in the smallest class
/// and so makes it a merge candidate rather than an obstacle.
///
/// `live` must be ordered oldest content first; the returned range indexes it.
fn choose_collapse_window(live: &[&OplogEntry], max_live: usize) -> Option<Range<usize>> {
    let sizes: Vec<u64> = live
        .iter()
        .map(|r| r.size.unwrap_or(0).max(0) as u64)
        .collect();
    provider::size_tier::choose_merge_window(&sizes, max_live)
}

/// One reconstructed write transaction recovered from the data Delta
/// table's commit history by [`OpLogPersistence::reconstruct_txn_history`].
///
/// Carries enough to rebuild the control table's lifecycle records:
/// the original `pond_txn` metadata (txn_seq, txn_id, CLI args,
/// pond_id), the Delta version the commit landed at, and the commit
/// timestamp in microseconds.
#[derive(Debug, Clone)]
pub struct ReconstructedTxn {
    /// The `pond_txn` metadata stamped on the Delta commit.
    pub meta: PondTxnMetadata,
    /// The Delta Lake version at which this commit landed.
    pub delta_version: i64,
    /// Commit timestamp in microseconds since the Unix epoch.
    pub timestamp_micros: i64,
}

/// Decide whether a `FilePhysicalSeries` version is retained by the supplied
/// [`SeriesReadBounds`]. Delegates to [`tinyfs::SeriesReadBounds::retains`] so
/// the read-path filter and the cached-`ListingTable` prune share one predicate.
fn series_version_retained(
    max_event_time: Option<i64>,
    version: i64,
    bounds: &tinyfs::SeriesReadBounds,
) -> bool {
    bounds.retains(max_event_time, version)
}

impl OpLogPersistence {
    /// Get the Delta table for query operations
    #[must_use]
    pub fn table(&self) -> &DeltaTable {
        &self.table
    }

    /// Replace the underlying Delta table (used after maintenance operations
    /// that produce a new table, e.g. vacuum/optimize).
    pub fn set_table(&mut self, table: DeltaTable) {
        self.table = table;
    }

    /// Local pond's last committed transaction sequence (0 if none).
    fn local_seq(&self) -> i64 {
        self.seqs.get(&self.pond_id).copied().unwrap_or(0)
    }

    /// Set the local pond's last committed transaction sequence.
    fn set_local_seq(&mut self, seq: i64) {
        let _ = self.seqs.insert(self.pond_id.clone(), seq);
    }

    /// Get the last committed transaction sequence number for the LOCAL
    /// pond.
    ///
    /// This is the authoritative source for the current transaction sequence.
    /// Use this to determine the next sequence number: `last_txn_seq() + 1`
    #[must_use]
    pub fn last_txn_seq(&self) -> i64 {
        self.local_seq()
    }

    /// Get the last committed transaction sequence number for an arbitrary
    /// `pond_id` (0 if that pond has no committed rows here).  Foreign
    /// pond_ids are populated by cross-pond import.
    #[must_use]
    pub fn last_txn_seq_for(&self, pond_id: &str) -> i64 {
        self.seqs.get(pond_id).copied().unwrap_or(0)
    }

    /// Advance the in-memory per-pond seq allocator for `pond_id` to `seq`
    /// if it is greater than the current value.  No-op otherwise.  Used by
    /// the sync-remote adapter after `apply_pulled_bundle` mirrors a Delta
    /// commit into the local data table: the on-disk commit metadata
    /// carries the new `txn_seq` (and is picked up by the next
    /// `open_or_create`), but the in-memory value must be advanced too.
    ///
    /// Because the allocator is per-pond_id, advancing a FOREIGN pond's
    /// seq (cross-pond import) does not disturb the LOCAL pond's seq
    /// space: local writes continue from `last_txn_seq()` with no gaps.
    /// For a mirror restart (`pond_id == self.pond_id`) this advances the
    /// local allocator as required.
    pub fn sync_last_txn_seq(&mut self, pond_id: &str, seq: i64) {
        let entry = self.seqs.entry(pond_id.to_string()).or_insert(0);
        if seq > *entry {
            debug!(
                "Advancing OpLogPersistence seq[{}] {} -> {} (apply_pulled_bundle)",
                pond_id, *entry, seq
            );
            *entry = seq;
        }
    }

    /// Get the pond identity UUID
    #[must_use]
    pub fn pond_id(&self) -> &str {
        &self.pond_id
    }

    /// Read the pond identity from a Delta data table without fully
    /// instantiating an [`OpLogPersistence`].  Returns `Ok(Some(id))`
    /// when the table holds at least one Add action and we can
    /// identify the local pond's id.
    ///
    /// Returns `Ok(None)` when no Delta table exists at `path`, when
    /// the table exists but has no Add actions yet (a restoration
    /// scaffold awaiting its first bundle), or when the table is
    /// present but lacks a `partition.pond_id` column (a pre-D5
    /// layout already refused by [`open_or_create`]; we simply
    /// decline to recover identity from such a table).
    ///
    /// When multiple distinct `pond_id` values are present (the
    /// cross-pond import case, D5.7b), we walk the Delta commit
    /// history to find a commit that was NOT produced by
    /// `apply_pulled_bundle` (i.e., a local-origin commit), and
    /// return that commit's `pond_id`.  This identifies which of the
    /// pond_ids in the data table is the local one.  If only foreign
    /// commits are found (a brand-new restored pond that hasn't yet
    /// done any local writes), we return `Ok(None)` so the caller
    /// can fall back to the control table cache.
    pub async fn peek_pond_id<P: AsRef<Path>>(path: P) -> Result<Option<String>, TLogFSError> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let url = Url::from_directory_path(path.as_ref())
            .or_else(|_| Url::from_file_path(path.as_ref()))
            .map_err(|_| {
                TLogFSError::Internal(format!("Failed to create URL from path: {}", path_str))
            })?;

        let table = match deltalake::open_table(url).await {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };

        let snapshot = match table.snapshot() {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        let batch = snapshot.add_actions_table(true)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let Some(col_idx) = batch.schema().index_of("partition.pond_id").ok() else {
            return Ok(None);
        };
        let array = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                TLogFSError::Internal("partition.pond_id column is not a StringArray".to_string())
            })?;

        let mut unique: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for i in 0..array.len() {
            if !array.is_null(i) {
                let _inserted = unique.insert(array.value(i).to_string());
            }
        }

        match unique.len() {
            0 => Ok(None),
            1 => Ok(unique.into_iter().next()),
            _ => {
                // Cross-pond import: multiple pond_ids share this data
                // table.  Walk the Delta commit history (newest first)
                // looking for a commit whose user.args do NOT mark it
                // as `apply_pulled_bundle` -- that's a local-origin
                // commit and its stamped pond_id is the local one.
                let history = table.history(None).await?;
                // history is an iterator newest-first; collect so we
                // can scan in reverse order (oldest-first) for the
                // first local-origin commit.
                let mut commits: Vec<CommitInfo> = history.collect();
                commits.reverse();
                for commit in &commits {
                    let Some(meta) = PondTxnMetadata::from_delta_metadata(&commit.info) else {
                        continue;
                    };
                    if meta.pond_id.is_empty() {
                        continue;
                    }
                    let is_apply_pulled = meta.user.args.len() >= 2
                        && meta.user.args[0] == "internal"
                        && meta.user.args[1] == "apply_pulled_bundle";
                    if !is_apply_pulled {
                        return Ok(Some(meta.pond_id));
                    }
                }
                // All commits are foreign-applied (no local writes yet
                // beyond bundle replay).  Let the caller fall back to
                // the control table cache.
                Ok(None)
            }
        }
    }

    /// Reconstruct the write-transaction history of the data Delta table
    /// at `path` from its commit log, for `pond rebuild-control`.
    ///
    /// Walks the Delta commit history (which carries a `pond_txn`
    /// metadata blob on every steward write commit) and returns one
    /// [`ReconstructedTxn`] per commit that carries such metadata,
    /// sorted ascending by `txn_seq`.  Commits without `pond_txn`
    /// (the initial table `CREATE`, compaction/optimize commits, etc.)
    /// are skipped but still consume a version slot, so the derived
    /// `delta_version` stays correct across them.
    ///
    /// `delta_version` is the actual Delta Lake version at which the
    /// commit landed (derived from the table's latest version and the
    /// newest-first ordering of history).  Note that `create_pond`
    /// records the bootstrap txn's `data_delta_version` as `0` even
    /// though its physical commit is version 1; callers that need to
    /// reproduce that convention should special-case the bootstrap.
    ///
    /// Returns an empty vec if the table is missing, unreadable, or
    /// carries no `pond_txn` commits.
    pub async fn reconstruct_txn_history<P: AsRef<Path>>(
        path: P,
    ) -> Result<Vec<ReconstructedTxn>, TLogFSError> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let url = Url::from_directory_path(path.as_ref())
            .or_else(|_| Url::from_file_path(path.as_ref()))
            .map_err(|_| {
                TLogFSError::Internal(format!("Failed to create URL from path: {}", path_str))
            })?;

        let table = match deltalake::open_table(url).await {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };

        let Some(latest) = table.version() else {
            return Ok(Vec::new());
        };

        // `history` is newest-first; the i-th entry is at version
        // `latest - i` (every Delta version has exactly one commit).
        let history: Vec<CommitInfo> = table.history(None).await?.collect();
        let mut out = Vec::new();
        for (i, commit) in history.iter().enumerate() {
            let version = latest - i as i64;
            let Some(meta) = PondTxnMetadata::from_delta_metadata(&commit.info) else {
                continue;
            };
            // Delta commit timestamps are milliseconds since epoch;
            // OplogEntry / control records use microseconds.
            let timestamp_micros = commit.timestamp.unwrap_or(0).saturating_mul(1000);
            out.push(ReconstructedTxn {
                meta,
                delta_version: version,
                timestamp_micros,
            });
        }

        out.sort_by_key(|t| t.meta.txn_seq);
        Ok(out)
    }

    /// Creates a new OpLogPersistence instance with a new table and initializes root
    pub async fn create<P: AsRef<Path>>(
        path: P,
        pond_id: String,
        metadata: PondUserMetadata,
    ) -> Result<Self, TLogFSError> {
        debug!("create called with path: {:?}", path.as_ref());

        Self::open_or_create(path, pond_id, true, Some(metadata)).await
    }

    /// Test-only helper: Create a new pond with synthetic metadata.
    #[cfg(test)]
    pub async fn create_test(path: &str) -> Result<Self, TLogFSError> {
        Self::create(
            path,
            uuid7::uuid7().to_string(),
            PondUserMetadata::new(vec!["test".to_string(), "create".to_string()]),
        )
        .await
    }

    /// Test-only helper: Create a new pond with uncompressed large file storage.
    /// Used for corruption testing where we need to modify raw bytes in parquet files.
    #[cfg(test)]
    pub async fn create_test_uncompressed(path: &str) -> Result<Self, TLogFSError> {
        let mut persistence = Self::create(
            path,
            uuid7::uuid7().to_string(),
            PondUserMetadata::new(vec!["test".to_string(), "create".to_string()]),
        )
        .await?;
        persistence.large_file_options = crate::large_files::LargeFileOptions::uncompressed();
        Ok(persistence)
    }

    /// Test-only helper: Begin a transaction with automatic sequence numbering.
    #[cfg(test)]
    pub async fn begin_test(&mut self) -> Result<TransactionGuard<'_>, TLogFSError> {
        let next_seq = self.local_seq() + 1;
        let metadata =
            PondTxnMetadata::new(next_seq, PondUserMetadata::new(vec!["test".to_string()]));
        self.begin_write(&metadata).await // Tests are write transactions
    }

    /// Opens an existing OpLogPersistence instance
    pub async fn open<P: AsRef<Path>>(path: P, pond_id: String) -> Result<Self, TLogFSError> {
        debug!("open called with path: {:?}", path.as_ref());

        Self::open_or_create(path, pond_id, false, None).await
    }

    /// Create an empty Delta table structure for restoration
    /// This creates the table schema but does NOT initialize the root directory.
    /// Used when restoring from bundles - the first bundle will write transaction #1
    /// which initializes the root directory.
    pub async fn create_empty<P: AsRef<Path>>(
        path: P,
        pond_id: String,
    ) -> Result<Self, TLogFSError> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        debug!(
            "Creating empty table structure for restoration at: {}",
            path_str
        );

        // Create the Delta table structure
        let config: HashMap<String, Option<String>> = vec![(
            "delta.dataSkippingStatsColumns".to_string(),
            Some("node_id,name,parent_id,entry_type,file_type,timestamp,version,blake3,size,min_event_time,max_event_time,extended_attributes,factory,txn_seq".to_string())
        )]
        .into_iter()
        .collect();

        let url = Url::from_directory_path(path.as_ref())
            .or_else(|_| Url::from_file_path(path.as_ref()))
            .map_err(|_| {
                TLogFSError::Internal(format!("Failed to create URL from path: {}", path_str))
            })?;
        let table = DeltaTable::try_from_url(url)
            .await?
            .create()
            .with_columns(OplogEntry::for_delta())
            .with_partition_columns(["pond_id", "part_id"])
            .with_configuration(config)
            .with_save_mode(SaveMode::ErrorIfExists)
            .await?;

        debug!("Created empty Delta table at {}", path_str);

        Ok(Self {
            table,
            path: path.as_ref().to_path_buf(),
            fs: None,
            state: None,
            seqs: HashMap::new(), // No transactions yet - bundles will provide them
            txn_state: Arc::new(TinyFsTransactionState::new()),
            large_file_options: Default::default(),
            pond_id,
        })
    }

    /// @@@ UNCLEAR this should not be public
    pub async fn open_or_create<P: AsRef<Path>>(
        path: P,
        pond_id: String,
        create_new: bool,
        root_metadata: Option<PondUserMetadata>,
    ) -> Result<Self, TLogFSError> {
        // Enable RUST_LOG logging configuration for tests
        let _ = env_logger::try_init();

        let mode = if create_new {
            SaveMode::ErrorIfExists
        } else {
            SaveMode::Append
        };

        // First try to open existing table
        let path_str = path.as_ref().to_string_lossy().to_string();

        // Ensure directory exists before trying to create URL
        std::fs::create_dir_all(path.as_ref())
            .map_err(|e| TLogFSError::Internal(format!("Failed to create directory: {}", e)))?;

        let url = Url::from_directory_path(path.as_ref())
            .or_else(|_| Url::from_file_path(path.as_ref()))
            .map_err(|_| {
                TLogFSError::Internal(format!("Failed to create URL from path: {}", path_str))
            })?;

        let table = match deltalake::open_table(url.clone()).await {
            Ok(existing_table) => {
                debug!("Found existing table at {}", path_str);
                // D5: refuse to open tables with the pre-D5 partition layout.
                // Pre-D5: partition_columns = ["part_id"]; D5+: ["pond_id", "part_id"].
                let part_cols: Vec<String> = existing_table
                    .snapshot()
                    .ok()
                    .map(|s| s.metadata().partition_columns().clone())
                    .unwrap_or_default();
                if part_cols.as_slice() != ["pond_id".to_string(), "part_id".to_string()] {
                    return Err(TLogFSError::LegacyPartitionLayout {
                        path: path.as_ref().to_path_buf(),
                        found: part_cols.join(", "),
                    });
                }

                // Schema migration: add any OplogEntry columns missing from the
                // on-disk table. add_columns() emits a metadata-only Delta commit
                // with no pond_txn, so it is tolerated by last_txn_seq recovery the
                // same way vacuum/optimize commits are. Pre-existing rows read back
                // the new columns as NULL. Idempotent: re-opening a migrated table
                // finds no missing columns and emits no commit.
                let missing_fields: Vec<deltalake::kernel::StructField> = {
                    let existing_schema = existing_table.snapshot()?.schema();
                    OplogEntry::for_delta()
                        .into_iter()
                        .filter(|f| !existing_schema.contains(&f.name))
                        .collect()
                };
                if missing_fields.is_empty() {
                    existing_table
                } else {
                    info!(
                        "Migrating Delta schema at {}: adding column(s) {:?}",
                        path_str,
                        missing_fields.iter().map(|f| &f.name).collect::<Vec<_>>()
                    );
                    existing_table
                        .add_columns()
                        .with_fields(missing_fields)
                        .await?
                }
            }
            Err(open_err) => {
                debug!("no existing table at {}, will create: {open_err}", path_str);
                // Table doesn't exist, create it
                // Configure stats collection to skip the binary 'content' column to avoid warnings
                let config: HashMap<String, Option<String>> = vec![(
                    "delta.dataSkippingStatsColumns".to_string(),
                    // pond_id and part_id are partition columns (in the file path);
                    // partition columns do not need data-skipping stats.
                    Some("node_id,name,parent_id,entry_type,file_type,timestamp,version,blake3,size,min_event_time,max_event_time,extended_attributes,factory,txn_seq,collapsed_through,collapsed_from".to_string())
                )]
                .into_iter()
                .collect();

                let create_result = DeltaTable::try_from_url(url.clone())
                    .await?
                    .create()
                    .with_columns(OplogEntry::for_delta())
                    .with_partition_columns(["pond_id", "part_id"])
                    .with_configuration(config)
                    .with_save_mode(mode)
                    .await;

                match create_result {
                    Ok(table) => table,
                    Err(create_err) => {
                        debug!("failed to create table at {}: {create_err}", path_str);
                        return Err(create_err.into());
                    }
                }
            }
        };

        let mut persistence = Self {
            table: table.clone(),
            path: path.as_ref().to_path_buf(),
            fs: None,
            state: None,
            seqs: HashMap::new(), // Will be updated below
            txn_state: Arc::new(TinyFsTransactionState::new()),
            large_file_options: Default::default(),
            pond_id,
        };

        // Initialize root directory ONLY when creating a new pond
        if create_new {
            debug!("Initializing root directory for new pond at {}", path_str);

            let metadata = PondTxnMetadata::new(1, root_metadata.expect("metadata when new"));

            let tx = persistence.begin_write(&metadata).await?;

            // Actually create the root directory entry
            // IMPORTANT: initialize_root_directory adds entry to records AFTER begin_impl cleared them
            tx.state()
                .map_err(|e| {
                    TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to get state: {}", e)))
                })?
                .initialize_root_directory()
                .await?;

            _ = tx.commit().await.map_err(TLogFSError::TinyFS)?;
        } else {
            // Opening existing table - recover the per-pond_id seq
            // allocator from the data Delta commit history.  This is the
            // authoritative source (not the control table, which is
            // Steward's).
            //
            // We scan ALL commits and bucket each `pond_txn` by its
            // `pond_id`, taking the per-pond MAX `txn_seq` rather than
            // trusting only the single most-recent commit: Delta
            // maintenance operations (vacuum's VACUUM START/END commits,
            // optimize/compaction, checkpoints) append commits that carry
            // no `pond_txn` blob.  After a `pond maintain --compact` the
            // newest commits are vacuum entries, so `history(Some(1))`
            // alone would miss the compaction's `pond_txn`.  Sequences are
            // monotonic per pond, so the per-pond max is the true value.
            //
            // Bucketing per pond_id keeps the LOCAL allocator decoupled
            // from foreign frontiers: a fast cross-pond producer no longer
            // drags `last_txn_seq()` (= local) upward.  A `pond_txn` with
            // an empty pond_id (defensive; current ponds always stamp one)
            // is attributed to the local pond so the local allocator is
            // never under-recovered (which could collide on the next write).
            let history = table.history(None).await?;
            let mut seqs: HashMap<String, i64> = HashMap::new();
            for commit in history {
                if let Some(meta) = PondTxnMetadata::from_delta_metadata(&commit.info) {
                    let key = if meta.pond_id.is_empty() {
                        persistence.pond_id.clone()
                    } else {
                        meta.pond_id.clone()
                    };
                    let entry = seqs.entry(key).or_insert(0);
                    *entry = (*entry).max(meta.txn_seq);
                }
            }
            debug!(
                "Loaded per-pond seq allocator {:?} from Delta metadata at {} (local pond_id={})",
                seqs, path_str, persistence.pond_id,
            );
            persistence.seqs = seqs;
        }

        Ok(persistence)
    }

    pub(crate) fn state(&self) -> Result<State, TLogFSError> {
        if self.state.is_none() {
            panic!(
                "[ERR] CRITICAL BUG: state() called but self.state is None! This should never happen during an active transaction."
            );
        }
        self.state.clone().ok_or(TLogFSError::Missing)
    }

    /// Get commit history from Delta table via State
    pub async fn get_commit_history(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<CommitInfo>, TLogFSError> {
        self.table
            .history(limit)
            .await
            .map(|iter| iter.collect())
            .map_err(TLogFSError::Delta)
    }

    /// Get commit metadata for a specific version
    pub async fn get_commit_metadata(
        &self,
        ts: i64,
    ) -> Result<Option<HashMap<String, serde_json::Value>>, TLogFSError> {
        let history = self.get_commit_history(Some(1)).await?;
        for hist in history.iter() {
            if hist.timestamp == Some(ts) {
                return Ok(Some(hist.info.clone()));
            }
        }
        Ok(None)
    }

    /// Get commit metadata for a specific version
    pub async fn get_last_commit_metadata(
        &self,
    ) -> Result<Option<HashMap<String, serde_json::Value>>, TLogFSError> {
        let history = self.get_commit_history(Some(1)).await?;
        Ok(history.first().as_ref().map(|x| x.info.clone()))
    }

    /// Begin a write transaction - allocates next sequence number
    ///
    /// Write transactions must use sequence number last_txn_seq + 1 and will
    /// commit changes to Delta Lake when committed.
    ///
    /// # Arguments
    /// * `txn_seq` - Transaction sequence number (must be last_txn_seq + 1)
    /// * `metadata` - Transaction metadata (txn_id, CLI args, key/value params)
    ///
    /// # Returns
    /// TransactionGuard that must be explicitly committed or will auto-rollback on drop
    pub async fn begin_write(
        &mut self,
        metadata: &PondTxnMetadata,
    ) -> Result<TransactionGuard<'_>, TLogFSError> {
        // Write transactions must be strictly increasing
        if metadata.txn_seq != self.local_seq() + 1 {
            return Err(TLogFSError::Transaction {
                message: format!(
                    "Write transaction sequence must be exactly +1: attempted txn_seq={} but last_txn_seq={} (expected {})",
                    metadata.txn_seq,
                    self.local_seq(),
                    self.local_seq() + 1
                ),
            });
        }

        self.begin_impl(metadata, true).await
    }

    /// Begin a read transaction - reuses last write sequence for read atomicity
    ///
    /// Read transactions use the last committed write sequence to get a consistent
    /// snapshot. They don't modify data or increment sequences.
    ///
    /// # Arguments
    /// * `txn_seq` - Transaction sequence number (must equal last_txn_seq)
    /// * `metadata` - Transaction metadata (txn_id, CLI args, key/value params)
    ///
    /// # Returns
    /// TransactionGuard that should NOT be committed (drop it to rollback)
    pub async fn begin_read(
        &mut self,
        txn_meta: &PondTxnMetadata,
    ) -> Result<TransactionGuard<'_>, TLogFSError> {
        // Read transactions reuse the last write sequence
        if txn_meta.txn_seq != self.local_seq() {
            return Err(TLogFSError::Transaction {
                message: format!(
                    "Read transaction must use last write sequence: attempted txn_seq={} but last_txn_seq={}",
                    txn_meta.txn_seq,
                    self.local_seq()
                ),
            });
        }

        self.begin_impl(txn_meta, false).await
    }

    /// Internal implementation for begin - shared by begin_write and begin_read
    async fn begin_impl(
        &mut self,
        metadata: &PondTxnMetadata,
        is_write: bool,
    ) -> Result<TransactionGuard<'_>, TLogFSError> {
        // Sanity check - state/fs should be None between transactions
        if self.state.is_some() || self.fs.is_some() {
            panic!("[ALERT] INTERNAL ERROR: state/fs is Some at begin_impl start");
        }

        let inner_state = InnerState::new(
            self.path.clone(),
            self.table.clone(),
            metadata.txn_seq,
            self.large_file_options.clone(),
        )
        .await?;
        let session_context = inner_state.session_context.clone();

        // Compute the format provider cache directory: {POND}/cache/
        // self.path is {POND}/data, so parent is {POND}
        let pond_root = self.path.parent().map(|p| p.to_path_buf());
        let cache_dir = pond_root.as_ref().map(|p| p.join("cache"));

        let state = State {
            inner: Arc::new(Mutex::new(inner_state)),
            object_store: Arc::new(tokio::sync::OnceCell::new()),
            session_context,
            table_provider_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            txn_state: self.txn_state.clone(),
            large_file_options: self.large_file_options.clone(),
            cache_dir,
            pond_path: pond_root,
            pond_id: self.pond_id.clone(),
        };

        // Complete SessionContext setup with ObjectStore registration
        {
            let inner = state.inner.lock().await;
            inner.complete_session_setup(&state).await?;
        }

        state.begin_impl().await?;

        let fs = FS::new(state.clone()).await?;
        self.fs = Some(fs.clone());
        self.state = Some(state);

        // Create the tinyfs transaction guard (this marks the transaction as active)
        let tinyfs_guard = self
            .txn_state
            .begin(fs, Some(metadata.txn_seq))
            .map_err(|e| TLogFSError::Transaction {
                message: format!("Failed to begin tinyfs transaction: {}", e),
            })?;

        Ok(TransactionGuard::new(
            tinyfs_guard,
            self,
            metadata,
            is_write,
        ))
    }

    /// Commit a transaction with metadata and return the committed version
    pub(crate) async fn commit(
        &mut self,
        mut metadata: PondTxnMetadata,
    ) -> Result<Option<i64>, TLogFSError> {
        // Stamp pond_id into commit metadata
        let pond_id = self.pond_id.clone();
        metadata.pond_id = pond_id.clone();

        let new_seq = metadata.txn_seq;
        self.fs = None;
        let res = self
            .state
            .take()
            .ok_or(TLogFSError::Missing)?
            .commit_impl(metadata, self.table.clone(), pond_id)
            .await?;

        let version = match res {
            Some(finalized) => {
                // Install the post-commit snapshot directly into our table
                // handle.  This avoids re-scanning the Delta log from disk
                // and — combined with the steward write lock (D5.7a.1) —
                // guarantees subsequent reads see exactly the state we
                // just wrote, not some racing peer's view.
                let version = finalized.version;
                self.table.state = Some(finalized.snapshot);
                debug!(
                    "[SYNC] Installed post-commit snapshot, version: {}",
                    version
                );
                Some(version)
            }
            None => None,
        };
        self.set_local_seq(new_seq);

        // Note: txn_state clearing is handled by tinyfs::TransactionGuard drop

        Ok(version)
    }

    /// Get the store path for this persistence layer
    #[must_use]
    pub fn store_path(&self) -> &PathBuf {
        &self.path
    }

    /// Read the full content of an externalized large file by its BLAKE3 hash.
    ///
    /// Returns the reconstructed bytes from `_large_files/`. Errors if no
    /// externalized blob with the given hash exists or if it cannot be read.
    pub async fn read_large_file_bytes(&self, blake3: &str) -> Result<Vec<u8>, TLogFSError> {
        use tokio::io::AsyncReadExt;
        let path = crate::large_files::find_large_file_path(&self.path, blake3)
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("locate large file {blake3}: {e}")))?
            .ok_or_else(|| TLogFSError::LargeFileNotFound {
                blake3: blake3.to_string(),
                path: format!("_large_files/blake3={blake3}"),
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "large file not found"),
            })?;
        let mut reader = crate::large_files::ParquetFileReader::new(path.clone())
            .await
            .map_err(|e| TLogFSError::LargeFileNotFound {
                blake3: blake3.to_string(),
                path: path.display().to_string(),
                source: e,
            })?;
        let mut buf = Vec::new();
        let _ = reader
            .read_to_end(&mut buf)
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("read large file {blake3}: {e}")))?;
        Ok(buf)
    }

    /// Open a streaming reader for an externalized large file by BLAKE3 hash.
    ///
    /// Yields raw bytes (the file content, not the parquet wrapper) without
    /// loading the whole blob into memory.  Errors if no externalized blob with
    /// the given hash exists.
    pub async fn open_large_file_reader_by_hash(
        &self,
        blake3: &str,
    ) -> Result<crate::large_files::ParquetFileReader, TLogFSError> {
        let path = crate::large_files::find_large_file_path(&self.path, blake3)
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("locate large file {blake3}: {e}")))?
            .ok_or_else(|| TLogFSError::LargeFileNotFound {
                blake3: blake3.to_string(),
                path: format!("_large_files/blake3={blake3}"),
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "large file not found"),
            })?;
        crate::large_files::ParquetFileReader::new(path.clone())
            .await
            .map_err(|e| TLogFSError::LargeFileNotFound {
                blake3: blake3.to_string(),
                path: path.display().to_string(),
                source: e,
            })
    }

    /// Query OpLog records by transaction sequence for testing
    ///
    /// Returns tuples of (node_id_hex, part_id_hex, version) for verification purposes.
    /// This is a simplified testing API that doesn't return full OplogEntry structs.
    ///
    /// # Arguments
    /// * `txn_seq` - Optional transaction sequence number to filter by
    ///
    /// # Returns
    /// Vector of tuples containing (node_id, part_id, version) as hex strings and i64
    pub async fn query_oplog_by_txn_seq(
        &self,
        txn_seq: Option<i64>,
    ) -> Result<Vec<(String, String, i64)>, TLogFSError> {
        // Create SessionContext and register the oplog table
        let ctx = SessionContext::new();
        _ = ctx
            .register_table("oplog", Arc::new(self.table.clone()))
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to register oplog table: {}", e))
            })?;

        // Build query - select node_id, part_id, version
        let sql = if let Some(seq) = txn_seq {
            format!(
                "SELECT node_id, part_id, version FROM oplog WHERE txn_seq = {} ORDER BY node_id, version",
                seq
            )
        } else {
            "SELECT node_id, part_id, version FROM oplog ORDER BY node_id, version".to_string()
        };

        // Execute query
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to execute query: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to collect results: {}", e)))?;

        let mut records = Vec::new();

        for batch in batches.iter() {
            // Helper to extract string from either StringArray or DictionaryArray
            let get_string_value = |col_idx: usize,
                                    row_idx: usize|
             -> Result<String, TLogFSError> {
                let column = batch.column(col_idx);

                // Try dictionary-encoded first (most common in DataFusion)
                if let Some(dict_array) = column
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt16Type>>()
                {
                    let values = dict_array
                        .values()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            TLogFSError::ArrowMessage("Failed to get dictionary values".to_string())
                        })?;
                    let key = dict_array.keys().value(row_idx);
                    return Ok(values.value(key as usize).to_string());
                }

                // Fall back to plain string array
                if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                    return Ok(string_array.value(row_idx).to_string());
                }

                Err(TLogFSError::ArrowMessage(format!(
                    "Unsupported column type: {:?}",
                    column.data_type()
                )))
            };

            let version_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    TLogFSError::ArrowMessage(format!(
                        "Failed to downcast version, actual type: {:?}",
                        batch.column(2).data_type()
                    ))
                })?;

            for row_idx in 0..batch.num_rows() {
                let node_id = get_string_value(0, row_idx)?;
                let part_id = get_string_value(1, row_idx)?;
                let version = version_array.value(row_idx);

                records.push((node_id, part_id, version));
            }
        }

        Ok(records)
    }
}

impl State {
    /// Get the Delta table for this transaction
    /// This allows factories to access the table for operations like reading Parquet files
    pub async fn table(&self) -> DeltaTable {
        self.inner.lock().await.table.clone()
    }

    /// Get the store path for this transaction
    /// This is used for large file external storage
    pub async fn store_path(&self) -> PathBuf {
        self.inner.lock().await.path.clone()
    }

    /// Query directory entries for a given FileID.
    /// Used by cross-pond import to discover child partition IDs
    /// from directory entries created at mknod time.
    pub async fn query_directory_entries_by_id(
        &self,
        id: &FileID,
    ) -> Result<Vec<DirectoryEntry>, TLogFSError> {
        self.inner.lock().await.query_directory_entries(*id).await
    }

    /// Register a foreign directory in the in-memory directory cache.
    /// This makes the directory immediately usable within the current
    /// transaction (for inserting child entries) without requiring a
    /// commit+reload cycle. The directory is NOT marked as modified,
    /// so it won't be flushed as an empty OpLog record — the real
    /// directory content comes from the imported foreign parquet files.
    pub async fn register_empty_directory(&self, id: FileID) {
        let mut inner = self.inner.lock().await;
        let _ = inner.directories.insert(id, DirectoryState::new_empty());
        // NOT marked as modified — the foreign parquet files contain the
        // real directory records. We only need this in the cache so child
        // insertions work during mknod.
    }

    /// Register an external parquet file for inclusion as a Delta Add action
    /// at commit time. This ensures imported parquet files are part of the
    /// same Delta commit as the normal OpLog records, maintaining the
    /// single-transaction invariant.
    pub async fn add_external_parquet(&self, action: ExternalAddAction) {
        self.inner.lock().await.external_add_actions.push(action);
    }

    /// Get the large file storage options (compression settings, etc.)
    #[must_use]
    pub fn large_file_options(&self) -> &crate::large_files::LargeFileOptions {
        &self.large_file_options
    }

    /// Collapse one contiguous window of live versions of a
    /// `FilePhysicalSeries` node into a single merged run, bounding the number
    /// of segments a reader must open.
    ///
    /// Uses size-tiered (LSM-style) compaction: only [`COLLAPSE_FANOUT`]
    /// adjacent runs of the same size class are merged, and the merged row
    /// records the closed version range `[collapsed_from, collapsed_through]` it
    /// supersedes. Merging a bounded window rather than the whole series is what
    /// makes total write volume `O(N log N)` instead of `O(N^2)`: previously
    /// every collapse rewrote the entire accumulated history to absorb one
    /// window's worth of new bytes.
    ///
    /// The newest versions are deliberately left un-merged. That keeps
    /// per-version `max_event_time` and version numbers intact at the tail,
    /// where bounded readers ([`tinyfs::SeriesReadBounds`]) prune, and it means
    /// the latest row's cumulative bao outboard -- which the append path resumes
    /// from -- is never disturbed.
    ///
    /// Bytes read back from the file are identical before and after, which this
    /// method verifies over the merged window, failing the transaction on any
    /// mismatch.
    ///
    /// Must be called inside an open write transaction; the merged row is
    /// committed with the surrounding transaction. Returns a no-op result when
    /// no window qualifies.
    ///
    /// # Errors
    /// Returns an error if `id` is not a `FilePhysicalSeries`, if reading or
    /// writing the merged content fails, or if the post-collapse content does
    /// not match the pre-collapse content.
    pub async fn collapse_file_series(
        &self,
        id: FileID,
        max_live: usize,
    ) -> Result<CollapseStats, TLogFSError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        if id.entry_type() != EntryType::FilePhysicalSeries {
            return Err(TLogFSError::Transaction {
                message: format!(
                    "collapse_file_series requires a FilePhysicalSeries node, got {:?} for {id}",
                    id.entry_type()
                ),
            });
        }

        // Assess current versions and pick the window to merge.
        let (
            versions_before,
            window,
            temporal,
            inherited_bao,
            newest_version,
            store_path,
            options,
        ) = {
            let mut inner = self.inner.lock().await;
            let records = inner.query_records(id).await?;
            let live: Vec<&OplogEntry> = crate::schema::live_series_entries(&records)
                .into_iter()
                .filter(|r| r.size.unwrap_or(0) > 0)
                .collect();

            let Some(range) = choose_collapse_window(&live, max_live) else {
                return Ok(CollapseStats {
                    collapsed: false,
                    versions_before: live.len(),
                    merged_version: 0,
                    bytes: 0,
                });
            };
            let window: Vec<OplogEntry> = live[range].iter().map(|r| (*r).clone()).collect();

            let newest = window.last().expect("window is non-empty");
            let timestamp_column = newest
                .get_extended_attributes()
                .map(|a| a.timestamp_column().to_string())
                .filter(|s| !s.is_empty());
            let min_event = window.iter().filter_map(|r| r.min_event_time).min();
            let max_event = window.iter().filter_map(|r| r.max_event_time).max();
            let temporal = match (min_event, max_event, timestamp_column) {
                (Some(min), Some(max), Some(col)) => Some((min, max, col)),
                _ => None,
            };

            // The merged run inherits the cumulative bao state of the newest
            // version it absorbs. Collapse regroups an unchanged byte stream, so
            // the series prefix through that version is byte-identical before and
            // after; recomputing it would be both wasteful and, for a run that
            // does not start at version 1, wrong.
            let inherited_bao = newest.bao_outboard.clone();
            let newest_version = newest.version;

            (
                live.len(),
                window,
                temporal,
                inherited_bao,
                newest_version,
                inner.path.clone(),
                inner.large_file_options.clone(),
            )
        };

        let lo = crate::schema::CollapseRange::of(&window[0]).lo;
        let hi = crate::schema::CollapseRange::of(window.last().expect("non-empty")).hi;

        // Read just this window's bytes. Reading the whole series here is what
        // made collapse O(N^2); the window is the only content the merged run
        // stands in for.
        let merged = {
            let inner = self.inner.lock().await;
            let mut buf = Vec::new();
            for record in &window {
                if record.is_large_file() {
                    let mut reader = inner.open_large_file_reader(record).await?;
                    let before = buf.len();
                    _ = reader.read_to_end(&mut buf).await.map_err(|e| {
                        TLogFSError::Internal(format!("collapse: read window content: {e}"))
                    })?;
                    debug_assert!(buf.len() >= before);
                } else {
                    buf.extend_from_slice(record.verified_content_required()?);
                }
            }
            buf
        };

        // Re-write the window's bytes as a single standalone body.
        let mut writer = crate::large_files::HybridWriter::with_options(&store_path, options);
        writer
            .write_all(&merged)
            .await
            .map_err(|e| TLogFSError::Internal(format!("collapse: write merged content: {e}")))?;
        writer
            .flush()
            .await
            .map_err(|e| TLogFSError::Internal(format!("collapse: flush merged content: {e}")))?;
        let result = writer
            .finalize()
            .await
            .map_err(|e| TLogFSError::Internal(format!("collapse: finalize merged: {e}")))?;

        let content_len = result.size;

        // The merged run inherits the cumulative bao state of the newest version
        // it absorbs, with `version_size` restated as the run's own length.
        // Collapse only regroups bytes, so the series prefix through `hi` is
        // unchanged; recomputing it as a fresh first version would be correct
        // only for a run starting at the very beginning of the series, and is
        // wrong for every later run.
        // "No outboard" and "unparseable outboard" are NOT the same case, and
        // must not share a fallback: the fallback below is the FIRST-version
        // baseline, which -- as the comment above states -- is correct only for
        // a run starting at the beginning of the series.  Silently applying it
        // to a run that merely failed to parse its predecessor's state would
        // stamp a wrong baseline onto the merged run, which no later
        // verification could attribute back to here.  A corrupt outboard is a
        // hard error that aborts the collapse instead.
        let bao_outboard = match inherited_bao.as_deref() {
            Some(bytes) => {
                let mut inherited = utilities::bao_outboard::SeriesOutboard::from_bytes(bytes)
                    .map_err(|e| {
                        TLogFSError::Internal(format!(
                            "collapse: node {} version {} has an unreadable bao outboard \
                             ({} bytes): {e}. The merged run must inherit that cumulative \
                             state -- recomputing it as a first version would stamp a wrong \
                             baseline -- so the collapse is aborted, leaving the series \
                             uncollapsed and intact.",
                            id,
                            newest_version,
                            bytes.len(),
                        ))
                    })?;
                inherited.version_size = content_len as u64;
                inherited.to_bytes()
            }
            None => utilities::bao_outboard::SeriesOutboard::from_first_version_state(
                &result.bao_state,
                content_len as u64,
            )
            .to_bytes(),
        };

        let content_ref = if result.content.is_empty()
            && content_len >= crate::large_files::LARGE_FILE_THRESHOLD
        {
            crate::file_writer::ContentRef::Large(result.blake3, content_len as u64)
        } else {
            crate::file_writer::ContentRef::Small(result.content)
        };

        // Enqueue the merged version with the collapse sentinel.
        let merged_version = {
            let mut inner = self.inner.lock().await;
            let version = inner.get_next_version_for_node(id).await?;
            let now = Utc::now().timestamp_micros();
            let txn_seq = inner.txn_seq;

            let mut entry = match (content_ref, &temporal) {
                (crate::file_writer::ContentRef::Small(content), Some((min, max, col))) => {
                    let mut ea = crate::schema::ExtendedAttributes::default();
                    _ = ea.set_timestamp_column(col);
                    OplogEntry::new_file_series(id, now, version, content, *min, *max, ea, txn_seq)
                }
                (crate::file_writer::ContentRef::Small(content), None) => {
                    OplogEntry::new_small_file(id, now, version, content, txn_seq)
                }
                (crate::file_writer::ContentRef::Large(b3, size), Some((min, max, col))) => {
                    let mut ea = crate::schema::ExtendedAttributes::default();
                    _ = ea.set_timestamp_column(col);
                    OplogEntry::new_large_file_series(
                        id,
                        now,
                        version,
                        b3,
                        size as i64,
                        *min,
                        *max,
                        ea,
                        txn_seq,
                    )
                }
                (crate::file_writer::ContentRef::Large(b3, size), None) => {
                    OplogEntry::new_large_file(id, now, version, b3, size as i64, txn_seq)
                }
            };
            entry.set_bao_outboard(bao_outboard);
            entry.collapsed_from = Some(lo);
            entry.collapsed_through = Some(hi);
            inner.records.push(entry);
            version
        };

        // Invariant: the merged run must read back byte-identically. Verifying
        // the run rather than the whole series keeps collapse's I/O proportional
        // to the window it actually rewrote.
        let after = {
            let inner = self.inner.lock().await;
            let record = inner
                .records
                .iter()
                .rev()
                .find(|r| r.node_id == id.node_id() && r.version == merged_version)
                .ok_or_else(|| {
                    TLogFSError::Internal("collapse: merged row missing after enqueue".to_string())
                })?;
            if record.is_large_file() {
                let mut reader = inner.open_large_file_reader(record).await?;
                let mut buf = Vec::new();
                _ = reader
                    .read_to_end(&mut buf)
                    .await
                    .map_err(|e| TLogFSError::Internal(format!("collapse: verify read: {e}")))?;
                buf
            } else {
                record.verified_content_required()?.to_vec()
            }
        };
        if after != merged {
            return Err(TLogFSError::Transaction {
                message: format!(
                    "collapse invariant violated for {id}: merged {} bytes but post-collapse read {} bytes",
                    merged.len(),
                    after.len()
                ),
            });
        }

        Ok(CollapseStats {
            collapsed: true,
            versions_before,
            merged_version,
            bytes: content_len as u64,
        })
    }

    /// Collapse all live versions of a `TablePhysicalSeries` node into a single
    /// merged version so that subsequent reads are O(1) instead of O(versions).
    ///
    /// A table series is read as a DataFusion `ListingTable` with one parquet
    /// file per live version, so every read pays per-version object listing and
    /// footer schema inference. That cost is independent of row count: a 1.9 KB
    /// series with 85 versions reads far slower than an 8.5 KB series with 20.
    /// Collapsing re-encodes the whole series as one parquet file, which turns
    /// that per-version cost into a constant.
    ///
    /// Unlike [`State::collapse_file_series`], which byte-concatenates chained
    /// versions, parquet files cannot be concatenated: the merged version is
    /// produced by reading every live version back through the table provider
    /// (which unions them) and re-encoding the result through a single
    /// `ArrowWriter`. The merged row records `collapsed_through = M - 1`, so the
    /// read path hides every superseded version.
    ///
    /// Because re-encoding legitimately changes the bytes, the post-condition
    /// verified here is logical rather than byte-identity: the merged version
    /// must expose the same row count and the same event-time bounds as the
    /// versions it replaces.
    ///
    /// Must be called inside an open write transaction; the merged row is
    /// committed with the surrounding transaction. Returns a no-op result when
    /// fewer than two live versions exist.
    ///
    /// # Errors
    /// Returns an error if `id` is not a `TablePhysicalSeries`, if the series
    /// carries no timestamp column, if reading or re-encoding fails, or if the
    /// post-collapse row count or temporal bounds do not match the input.
    pub async fn collapse_table_series(
        &self,
        id: FileID,
        max_live: usize,
    ) -> Result<CollapseStats, TLogFSError> {
        use tokio::io::AsyncWriteExt;

        if id.entry_type() != EntryType::TablePhysicalSeries {
            return Err(TLogFSError::Transaction {
                message: format!(
                    "collapse_table_series requires a TablePhysicalSeries node, got {:?} for {id}",
                    id.entry_type()
                ),
            });
        }

        // Assess current versions and pick the window to merge. The lock is
        // released before any table-provider work below, which re-enters the
        // persistence layer to resolve versions.
        let (
            versions_before,
            window_versions,
            lo,
            hi,
            min_event,
            max_event,
            timestamp_column,
            store_path,
            options,
        ) = {
            let mut inner = self.inner.lock().await;
            let records = inner.query_records(id).await?;
            let live: Vec<&OplogEntry> = crate::schema::live_series_entries(&records)
                .into_iter()
                .filter(|r| r.size.unwrap_or(0) > 0)
                .collect();

            let Some(range) = choose_collapse_window(&live, max_live) else {
                return Ok(CollapseStats {
                    collapsed: false,
                    versions_before: live.len(),
                    merged_version: 0,
                    bytes: 0,
                });
            };
            let window = &live[range];

            let newest = *window.last().expect("window is non-empty");
            let timestamp_column = newest
                .get_extended_attributes()
                .map(|a| a.timestamp_column().to_string())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| TLogFSError::Transaction {
                    message: format!(
                        "collapse_table_series: {id} has no timestamp column; a \
                         TablePhysicalSeries cannot be rewritten without one"
                    ),
                })?;
            let min_event = window.iter().filter_map(|r| r.min_event_time).min();
            let max_event = window.iter().filter_map(|r| r.max_event_time).max();

            let lo = window
                .iter()
                .map(|r| crate::schema::CollapseRange::of(r).lo)
                .min()
                .expect("window is non-empty");
            let hi = window
                .iter()
                .map(|r| crate::schema::CollapseRange::of(r).hi)
                .max()
                .expect("window is non-empty");
            let window_versions: Vec<i64> = window.iter().map(|r| r.version).collect();

            (
                live.len(),
                window_versions,
                lo,
                hi,
                min_event,
                max_event,
                timestamp_column,
                inner.path.clone(),
                inner.large_file_options.clone(),
            )
        };

        // Re-encode just this window into a single parquet buffer. The table
        // provider unions the listed per-version parquet files and merges their
        // schemas, so this is the authoritative content for the merged run.
        let (merged, rows_before) = self
            .reencode_table_versions(id, &window_versions, &timestamp_column)
            .await?;

        // Store the merged parquet as a fresh first-version body so the bao-tree
        // cumulative state covers exactly this content.
        let mut writer = crate::large_files::HybridWriter::with_options(&store_path, options);
        writer
            .write_all(&merged)
            .await
            .map_err(|e| TLogFSError::Internal(format!("collapse: write merged parquet: {e}")))?;
        writer
            .flush()
            .await
            .map_err(|e| TLogFSError::Internal(format!("collapse: flush merged parquet: {e}")))?;
        let result = writer.finalize().await.map_err(|e| {
            TLogFSError::Internal(format!("collapse: finalize merged parquet: {e}"))
        })?;

        let content_len = result.size;
        let series_outboard = utilities::bao_outboard::SeriesOutboard::from_first_version_state(
            &result.bao_state,
            content_len as u64,
        );
        let bao_outboard = series_outboard.to_bytes();

        let content_ref = if result.content.is_empty()
            && content_len >= crate::large_files::LARGE_FILE_THRESHOLD
        {
            crate::file_writer::ContentRef::Large(result.blake3, content_len as u64)
        } else {
            crate::file_writer::ContentRef::Small(result.content)
        };

        // A TablePhysicalSeries is invalid without temporal metadata, so the
        // merged row always carries the union of the inputs' event-time bounds.
        let (min_event, max_event) = match (min_event, max_event) {
            (Some(min), Some(max)) => (min, max),
            _ => {
                return Err(TLogFSError::Transaction {
                    message: format!(
                        "collapse_table_series: {id} has live versions without event-time bounds"
                    ),
                });
            }
        };

        // Enqueue the merged version with the collapse sentinel.
        let merged_version = {
            let mut inner = self.inner.lock().await;
            let version = inner.get_next_version_for_node(id).await?;
            let now = Utc::now().timestamp_micros();
            let txn_seq = inner.txn_seq;

            let mut ea = crate::schema::ExtendedAttributes::default();
            _ = ea.set_timestamp_column(&timestamp_column);

            let mut entry = match content_ref {
                crate::file_writer::ContentRef::Small(content) => OplogEntry::new_file_series(
                    id, now, version, content, min_event, max_event, ea, txn_seq,
                ),
                crate::file_writer::ContentRef::Large(b3, size) => {
                    OplogEntry::new_large_file_series(
                        id,
                        now,
                        version,
                        b3,
                        size as i64,
                        min_event,
                        max_event,
                        ea,
                        txn_seq,
                    )
                }
            };
            entry.set_bao_outboard(bao_outboard);
            entry.collapsed_from = Some(lo);
            entry.collapsed_through = Some(hi);
            inner.records.push(entry);
            version
        };

        // Invariant: re-encoding must preserve logical content. Byte-identity
        // does not hold across a parquet rewrite, so compare the row count of
        // the stored merged run against the window it replaced. Scanning only
        // the merged version keeps verification proportional to the window.
        let (after, rows_after) = self
            .reencode_table_versions(id, &[merged_version], &timestamp_column)
            .await?;
        if rows_after != rows_before {
            return Err(TLogFSError::Transaction {
                message: format!(
                    "collapse invariant violated for {id}: merged {rows_before} rows but \
                     post-collapse read {rows_after} rows"
                ),
            });
        }
        drop(after);

        Ok(CollapseStats {
            collapsed: true,
            versions_before,
            merged_version,
            bytes: content_len as u64,
        })
    }

    /// Read the given versions of a table series back through a table provider
    /// and re-encode their union as a single parquet buffer.
    ///
    /// The versions are passed as explicit per-version URLs rather than the
    /// series' "all versions" pattern, so a collapse can re-encode just the
    /// window it intends to merge. DataFusion still infers and merges the schema
    /// across the listed files, which is why this goes through the provider
    /// instead of decoding each version's parquet directly.
    ///
    /// Returns the parquet bytes and the total row count.
    async fn reencode_table_versions(
        &self,
        id: FileID,
        versions: &[i64],
        timestamp_column: &str,
    ) -> Result<(Vec<u8>, usize), TLogFSError> {
        use futures::StreamExt;
        use parquet::arrow::ArrowWriter;

        let context = self.as_provider_context();
        let additional_urls: Vec<String> = versions
            .iter()
            .map(|v| provider::TinyFsPathBuilder::url_specific_version(&id, *v as u64))
            .collect();
        let table_provider = provider::create_table_provider(
            id,
            &context,
            provider::TableProviderOptions {
                version_selection: provider::VersionSelection::AllVersions,
                additional_urls,
                bounds: tinyfs::SeriesReadBounds::NONE,
            },
        )
        .await
        .map_err(|e| TLogFSError::Internal(format!("collapse: build table provider: {e}")))?;

        let session = &context.datafusion_session;
        let df_state = session.state();
        let plan = table_provider
            .scan(&df_state, None, &[], None)
            .await
            .map_err(|e| TLogFSError::Internal(format!("collapse: scan series: {e}")))?;
        let mut stream = plan
            .execute(0, session.task_ctx())
            .map_err(|e| TLogFSError::Internal(format!("collapse: execute scan: {e}")))?;

        let mut rows = 0usize;
        let mut writer: Option<ArrowWriter<Vec<u8>>> = None;
        while let Some(batch) = stream.next().await {
            let batch =
                batch.map_err(|e| TLogFSError::Internal(format!("collapse: read batch: {e}")))?;
            rows += batch.num_rows();
            let writer = match writer {
                Some(ref mut w) => w,
                None => {
                    let w =
                        ArrowWriter::try_new(Vec::new(), batch.schema(), None).map_err(|e| {
                            TLogFSError::Internal(format!("collapse: create parquet writer: {e}"))
                        })?;
                    writer.insert(w)
                }
            };
            writer
                .write(&batch)
                .map_err(|e| TLogFSError::Internal(format!("collapse: write batch: {e}")))?;
        }

        let writer = writer.ok_or_else(|| TLogFSError::Transaction {
            message: format!(
                "collapse_table_series: {id} produced no batches; refusing to \
                 replace live versions with an empty file (timestamp column \
                 '{timestamp_column}')"
            ),
        })?;
        let merged = writer
            .into_inner()
            .map_err(|e| TLogFSError::Internal(format!("collapse: finalize parquet: {e}")))?;

        Ok((merged, rows))
    }

    /// Discover series nodes with more than `threshold` live versions, the
    /// candidates worth passing to [`State::collapse_file_series`] (for
    /// `FilePhysicalSeries`) or [`State::collapse_table_series`] (for
    /// `TablePhysicalSeries`).
    ///
    /// Both physical series kinds are returned because both store append-only
    /// deltas whose versions are disjoint, so both can be merged safely, and
    /// both pay a per-version read cost that grows without bound.
    ///
    /// A live row has `size > 0` and is not covered by any *other* row's
    /// collapse range `[collapsed_from, collapsed_through]`. Loose rows occupy
    /// the degenerate range `[version, version]`, so one anti-join expresses
    /// supersession for both loose rows and superseded merge runs. The query
    /// spans all partitions in the pond.
    ///
    /// # Errors
    /// Returns an error if the discovery query fails or a returned identifier
    /// cannot be parsed back into a [`FileID`].
    pub async fn list_collapsible_series(
        &self,
        threshold: usize,
    ) -> Result<Vec<FileID>, TLogFSError> {
        let inner = self.inner.lock().await;
        let file_series = EntryType::FilePhysicalSeries.as_str();
        let table_series = EntryType::TablePhysicalSeries.as_str();
        let series_types = format!("('{file_series}', '{table_series}')");
        // The reserved commit-log node is a series whose every version is a
        // permanent transparency-log leaf (Decision D9); it must never be
        // collapsed, so it is excluded from candidacy here.
        let log_node = tinyfs::LOG_NODE_UUID;
        let sql = format!(
            "SELECT pond_id, part_id, node_id FROM ( \
                 SELECT t.pond_id AS pond_id, t.part_id AS part_id, t.node_id AS node_id, \
                        t.version AS version, \
                        CASE WHEN t.collapsed_through IS NULL THEN t.version \
                             ELSE COALESCE(t.collapsed_from, 0) END AS lo, \
                        CASE WHEN t.collapsed_through IS NULL THEN t.version \
                             ELSE t.collapsed_through END AS hi \
                 FROM delta_table t \
                 WHERE t.file_type IN {series_types} AND t.size > 0 \
                   AND t.node_id != '{log_node}' \
             ) x \
             LEFT JOIN ( \
                 SELECT part_id AS rpart, node_id AS rnode, version AS rversion, \
                        COALESCE(collapsed_from, 0) AS rlo, collapsed_through AS rhi \
                 FROM delta_table \
                 WHERE file_type IN {series_types} AND collapsed_through IS NOT NULL \
                   AND node_id != '{log_node}' \
             ) r \
             ON x.part_id = r.rpart AND x.node_id = r.rnode AND r.rversion != x.version \
                AND r.rlo <= x.lo AND x.hi <= r.rhi \
             WHERE r.rversion IS NULL \
             GROUP BY pond_id, part_id, node_id \
             HAVING COUNT(*) > {threshold}"
        );

        let df = inner
            .session_context
            .sql(&sql)
            .await
            .map_err(|e| TLogFSError::Transaction {
                message: format!("collapse discovery query failed: {e}"),
            })?;
        let batches = df.collect().await.map_err(|e| TLogFSError::Transaction {
            message: format!("collapse discovery collect failed: {e}"),
        })?;

        let mut ids = Vec::new();
        for batch in &batches {
            let rows: Vec<CollapseCandidate> =
                serde_arrow::from_record_batch(batch).map_err(|e| TLogFSError::Transaction {
                    message: format!("collapse discovery deserialize failed: {e}"),
                })?;
            for row in rows {
                let node_id = tinyfs::NodeID::from_string(&row.node_id).map_err(|e| {
                    TLogFSError::Transaction {
                        message: format!("collapse discovery bad node_id '{}': {e}", row.node_id),
                    }
                })?;
                let part_id = tinyfs::PartID::from_hex_string(&row.part_id).map_err(|e| {
                    TLogFSError::Transaction {
                        message: format!("collapse discovery bad part_id '{}': {e}", row.part_id),
                    }
                })?;
                let pond_id =
                    row.pond_id
                        .parse::<uuid7::Uuid>()
                        .map_err(|e| TLogFSError::Transaction {
                            message: format!(
                                "collapse discovery bad pond_id '{}': {e}",
                                row.pond_id
                            ),
                        })?;
                ids.push(FileID::new_from_ids(part_id, node_id, pond_id));
            }
        }
        Ok(ids)
    }

    /// Initialize root directory - delegates to inner StateImpl
    ///
    /// Should only be called during pond bootstrap from steward or tests
    pub async fn initialize_root_directory(&self) -> Result<(), TLogFSError> {
        let pond_id = self.pond_uuid();
        self.inner
            .lock()
            .await
            .initialize_root_directory(pond_id)
            .await
    }

    async fn begin_impl(&self) -> Result<(), TLogFSError> {
        self.inner.lock().await.begin_impl().await
    }

    async fn commit_impl(
        &mut self,
        metadata: PondTxnMetadata,
        table: DeltaTable,
        pond_id: String,
    ) -> Result<Option<deltalake::kernel::transaction::FinalizedCommit>, TLogFSError> {
        self.inner
            .lock()
            .await
            .commit_impl(metadata, table, pond_id)
            .await
    }

    /// Create an async reader for a file without loading entire content into memory
    pub(crate) async fn async_file_reader(
        &self,
        id: FileID,
    ) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        self.inner.lock().await.async_file_reader(id).await
    }

    /// As [`Self::async_file_reader`], but prunes `FilePhysicalSeries` versions
    /// per the supplied [`tinyfs::SeriesReadBounds`] (event-time lower bound
    /// and/or version watermark). See [`InnerState::async_file_reader_bounded`].
    pub(crate) async fn async_file_reader_bounded(
        &self,
        id: FileID,
        bounds: tinyfs::SeriesReadBounds,
    ) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        self.inner
            .lock()
            .await
            .async_file_reader_bounded(id, bounds)
            .await
    }

    /// Add an arbitrary OplogEntry record to pending transaction state
    /// This is used for metadata-only operations like temporal bounds setting
    pub async fn add_oplog_entry(&self, entry: OplogEntry) -> Result<(), TLogFSError> {
        self.inner.lock().await.records.push(entry);
        Ok(())
    }

    /// Get count of pending operations (records + modified directories)
    ///
    /// This is used for diagnostics when a transaction is dropped without commit.
    /// Returns (pending_records, modified_directories) counts.
    #[must_use]
    pub fn pending_operation_counts(&self) -> (usize, usize) {
        // Use try_lock to avoid blocking in Drop - if locked, return (0,0) as unknown
        match self.inner.try_lock() {
            Ok(guard) => {
                let record_count = guard.records.len();
                let modified_dirs = guard.directories.values().filter(|d| d.modified).count();
                (record_count, modified_dirs)
            }
            Err(_) => (0, 0), // Can't get counts if lock is held
        }
    }

    /// Synthesize the uncommitted live rows of the current transaction: every
    /// pending file/series record plus a full-snapshot row for each in-memory
    /// directory modified this transaction.  Combined with the committed Delta
    /// rows, this is exactly the post-commit live state, so the steward content
    /// fold can run before the transaction is finalized (design
    /// `docs/incremental-content-tree-design.md` Section 4, Approach A).
    ///
    /// Read-only: it neither flushes, allocates versions, nor mutates any
    /// transaction state.  Synthesized directory rows carry `version = i64::MAX`
    /// so a latest-wins fold prefers them over any committed row for the same
    /// node; the value is otherwise unused because the fold hashes directory
    /// content, not version numbers.
    pub async fn uncommitted_live_rows(&self) -> Result<Vec<OplogEntry>, TLogFSError> {
        let inner = self.inner.lock().await;
        let mut rows: Vec<OplogEntry> = inner.records.clone();
        let now = Utc::now().timestamp_micros();
        for (dir_id, dir_state) in &inner.directories {
            if !dir_state.modified {
                continue;
            }
            let entries: Vec<DirectoryEntry> = dir_state.mapping.values().cloned().collect();
            let content = inner.serialize_directory_entries(&entries)?;
            rows.push(OplogEntry::new_directory_full_snapshot(
                *dir_id,
                now,
                i64::MAX,
                content,
                inner.txn_seq,
            ));
        }
        Ok(rows)
    }

    /// Get the factory name for a specific node from the oplog
    /// Returns None if the node has no associated factory (static files/directories)
    pub async fn get_factory_for_node(&self, id: FileID) -> Result<Option<String>, TLogFSError> {
        self.inner.lock().await.get_factory_for_node(id).await
    }

    /// Get the factory name and config bytes for a dynamic node.
    ///
    /// Works for both file-based and directory-based dynamic nodes.
    pub async fn get_dynamic_node_config(
        &self,
        id: FileID,
    ) -> Result<Option<(String, Vec<u8>)>, TLogFSError> {
        self.inner.lock().await.get_dynamic_node_config(id).await
    }

    /// Create a ProviderContext from this State
    ///
    /// This allows State to be used with factories that expect a ProviderContext.
    /// Create a ProviderContext from this State with concrete values (no trait objects!)
    /// This is synchronous and lock-free - it accesses session_context directly
    #[must_use]
    pub fn as_provider_context(&self) -> provider::ProviderContext {
        // Create provider context with State as the persistence layer
        let mut ctx = provider::ProviderContext::new(
            self.session_context.clone(),
            Arc::new(self.clone()) as Arc<dyn PersistenceLayer>,
        );
        // Attach cache_dir if available (tlogfs has a real filesystem)
        if let Some(ref cache_dir) = self.cache_dir {
            ctx = ctx.with_cache_dir(cache_dir.clone());
        }
        // Attach pond_path if available
        if let Some(ref pond_path) = self.pond_path {
            ctx = ctx.with_pond_path(pond_path.clone());
        }
        ctx
    }
}

#[async_trait]
impl PersistenceLayer for State {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn transaction_state(&self) -> Arc<TinyFsTransactionState> {
        self.txn_state.clone()
    }

    fn pond_uuid(&self) -> uuid7::Uuid {
        self.pond_id
            .parse::<uuid7::Uuid>()
            .expect("pond_id must be a valid UUID")
    }

    async fn load_node(&self, id: FileID) -> TinyFSResult<Node> {
        self.inner.lock().await.load_node(id, self.clone()).await
    }

    async fn store_node(&self, node: &Node) -> TinyFSResult<()> {
        self.inner.lock().await.store_node(node).await
    }

    async fn create_file_node(&self, id: FileID) -> TinyFSResult<Node> {
        self.inner
            .lock()
            .await
            .create_file_node(id, self.clone())
            .await
    }

    async fn create_directory_node(&self, id: FileID) -> TinyFSResult<Node> {
        self.inner
            .lock()
            .await
            .create_directory_node(id, self.clone())
            .await
    }

    async fn initialize_foreign_root(&self, pond_id: uuid7::Uuid) -> TinyFSResult<()> {
        self.inner
            .lock()
            .await
            .initialize_root_directory(pond_id)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }

    async fn create_symlink_node(&self, id: FileID, target: &Path) -> TinyFSResult<Node> {
        self.inner
            .lock()
            .await
            .create_symlink_node(id, target, self.clone())
            .await
    }

    async fn create_dynamic_node(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> TinyFSResult<Node> {
        self.inner
            .lock()
            .await
            .create_dynamic_node(id, factory_type, config_content, self.clone())
            .await
            .map_err(error_utils::to_tinyfs_error)
    }

    async fn get_dynamic_node_config(&self, id: FileID) -> TinyFSResult<Option<(String, Vec<u8>)>> {
        self.inner
            .lock()
            .await
            .get_dynamic_node_config(id)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }

    async fn update_dynamic_node_config(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> TinyFSResult<()> {
        self.inner
            .lock()
            .await
            .update_dynamic_node_config(id, factory_type, config_content)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }

    async fn metadata(&self, id: FileID) -> TinyFSResult<NodeMetadata> {
        self.inner.lock().await.metadata(id).await
    }

    async fn list_file_versions(&self, id: FileID) -> TinyFSResult<Vec<FileVersionInfo>> {
        self.inner.lock().await.list_file_versions(id).await
    }

    async fn read_file_version(&self, id: FileID, version: u64) -> TinyFSResult<Vec<u8>> {
        self.inner.lock().await.read_file_version(id, version).await
    }

    async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> TinyFSResult<()> {
        self.inner
            .lock()
            .await
            .set_extended_attributes(id, attributes)
            .await
    }
}

impl State {
    /// Load symlink target path
    pub async fn load_symlink_target(&self, id: FileID) -> TinyFSResult<PathBuf> {
        self.inner.lock().await.load_symlink_target(id).await
    }

    /// Query oplog records for a node (used by directory operations)
    pub async fn query_records(&self, id: FileID) -> Result<Vec<OplogEntry>, TLogFSError> {
        self.inner.lock().await.query_records(id).await
    }

    /// Update directory content (used by OpLogDirectory::insert)
    /// DEPRECATED: This writes immediately. Use ensure_directory_loaded + insert_directory_entry instead.
    pub async fn update_directory_content(
        &self,
        id: FileID,
        content: Vec<u8>,
    ) -> Result<(), TLogFSError> {
        self.inner
            .lock()
            .await
            .update_directory_content(id, content)
            .await
    }

    /// Ensure directory is loaded into in-memory state
    /// If not present, loads from OpLog and caches with modified: false
    pub async fn ensure_directory_loaded(&self, id: FileID) -> Result<(), TLogFSError> {
        self.inner.lock().await.ensure_directory_loaded(id).await
    }

    /// Get a directory entry from in-memory state
    /// Directory must be loaded first via ensure_directory_loaded
    pub async fn get_directory_entry(
        &self,
        dir_id: FileID,
        entry_name: &str,
    ) -> Result<Option<tinyfs::DirectoryEntry>, TLogFSError> {
        Ok(self
            .inner
            .lock()
            .await
            .get_directory_entry(dir_id, entry_name))
    }

    /// Get all directory entries for a directory
    /// Directory must be loaded first via ensure_directory_loaded
    pub async fn get_all_directory_entries(
        &self,
        dir_id: FileID,
    ) -> Result<Vec<tinyfs::DirectoryEntry>, TLogFSError> {
        Ok(self.inner.lock().await.get_all_directory_entries(dir_id))
    }

    /// Pre-allocate a version number for an async write
    /// This reserves the version so that concurrent writes get sequential versions
    /// The actual content write happens later in shutdown()
    pub async fn allocate_version_for_write(&self, id: FileID) -> Result<i64, TLogFSError> {
        self.inner.lock().await.allocate_version_for_write(id).await
    }

    /// Mark transaction as poisoned (failed write)
    /// A poisoned transaction cannot be committed
    pub async fn poison_transaction(&self, reason: String) {
        self.inner.lock().await.poison_transaction(reason).await
    }

    /// Store file content reference (called from OpLogFileWriter::shutdown)
    /// If pre_allocated_version is provided, uses that instead of calculating a new version
    pub async fn store_file_content_ref(
        &self,
        id: FileID,
        content_ref: crate::file_writer::ContentRef,
        metadata: crate::file_writer::FileMetadata,
        pre_allocated_version: Option<i64>,
        bao_outboard: Option<Vec<u8>>,
        collapsed_through: Option<i64>,
    ) -> Result<(), TLogFSError> {
        self.inner
            .lock()
            .await
            .store_file_content_ref(
                id,
                content_ref,
                metadata,
                pre_allocated_version,
                bao_outboard,
                collapsed_through,
            )
            .await
    }

    /// Insert a directory entry into in-memory state
    /// Directory must be loaded first via ensure_directory_loaded
    /// Checks for duplicates and marks directory as modified
    pub async fn insert_directory_entry(
        &self,
        dir_id: FileID,
        entry: tinyfs::DirectoryEntry,
    ) -> Result<(), TLogFSError> {
        self.inner
            .lock()
            .await
            .insert_directory_entry(dir_id, entry)
    }

    /// Remove a directory entry by name
    /// Returns the removed entry if it existed, None if not found
    /// Marks directory as modified
    pub async fn remove_directory_entry(
        &self,
        dir_id: FileID,
        name: &str,
    ) -> Result<Option<tinyfs::DirectoryEntry>, TLogFSError> {
        self.inner.lock().await.remove_directory_entry(dir_id, name)
    }

    /// Get the shared DataFusion SessionContext
    ///
    /// This method ensures a single SessionContext across all operations using this State,
    /// preventing ObjectStore registry conflicts and ensuring consistent configuration.
    /// This is the method SqlDerived should use instead of creating its own SessionContext.
    pub async fn session_context(&self) -> Result<Arc<SessionContext>, TLogFSError> {
        let inner = self.inner.lock().await;
        Ok(inner.session_context.clone())
    }

    /// Get the TinyFS ObjectStore instance if it has been created
    /// This provides direct access to the same ObjectStore that DataFusion uses
    #[must_use]
    pub fn object_store(&self) -> Option<Arc<crate::TinyFsObjectStore<Self>>> {
        self.object_store.get().cloned()
    }

    /// Get cached TableProvider by key (for TLogFS query optimization)
    #[must_use]
    pub fn get_table_provider_cache(
        &self,
        key: &TableProviderKey,
    ) -> Option<Arc<dyn datafusion::catalog::TableProvider>> {
        self.table_provider_cache
            .lock()
            .expect("Failed to acquire table provider cache lock")
            .get(key)
            .cloned()
    }

    /// Set cached TableProvider by key (for TLogFS query optimization)
    pub fn set_table_provider_cache(
        &self,
        key: TableProviderKey,
        value: Arc<dyn datafusion::catalog::TableProvider>,
    ) {
        _ = self
            .table_provider_cache
            .lock()
            .expect("Failed to acquire table provider cache lock")
            .insert(key, value);
    }

}

/// Parse Parquet `content`, resolve the timestamp column (explicit or
/// auto-detected), and compute the global (min, max) timestamp across all row
/// batches. Returns the extended attributes carrying the timestamp column name
/// alongside the temporal bounds, normalized to microseconds.
///
/// Shared by the FileSeries write paths so they don't each re-open the Parquet
/// reader and re-scan batches.
fn extract_series_temporal_metadata(
    content: &[u8],
    timestamp_column: Option<&str>,
) -> Result<(super::schema::ExtendedAttributes, i64, i64), TLogFSError> {
    use super::schema::{
        ExtendedAttributes, detect_timestamp_column, extract_temporal_range_from_batch,
    };
    use tokio_util::bytes::Bytes;

    let bytes = Bytes::from(content.to_vec());
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create Parquet reader: {e}")))?
        .build()
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to build Parquet reader: {e}")))?;

    let mut all_batches = Vec::new();
    for batch_result in reader {
        all_batches.push(
            batch_result
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read batch: {e}")))?,
        );
    }

    let schema = all_batches
        .first()
        .ok_or_else(|| TLogFSError::ArrowMessage("No data in Parquet file".to_string()))?
        .schema();

    let time_col = match timestamp_column {
        Some(col) => col.to_string(),
        None => detect_timestamp_column(&schema)?,
    };

    let mut global_min = i64::MAX;
    let mut global_max = i64::MIN;
    for batch in &all_batches {
        let (batch_min, batch_max) = extract_temporal_range_from_batch(batch, &time_col)?;
        global_min = global_min.min(batch_min);
        global_max = global_max.max(batch_max);
    }

    let mut extended_attrs = ExtendedAttributes::default();
    _ = extended_attrs.set_timestamp_column(&time_col);

    Ok((extended_attrs, global_min, global_max))
}

impl InnerState {
    async fn new<P: AsRef<Path>>(
        path: P,
        table: DeltaTable,
        txn_seq: i64,
        large_file_options: crate::large_files::LargeFileOptions,
    ) -> Result<Self, TLogFSError> {
        // Create the SessionContext with caching enabled (64MiB limit)
        use datafusion::execution::{
            cache::{
                cache_manager::CacheManagerConfig,
                cache_unit::{DefaultFileStatisticsCache, DefaultListFilesCache},
            },
            memory_pool::FairSpillPool,
            runtime_env::RuntimeEnvBuilder,
        };

        // Enable DataFusion file statistics and list files caching (64MiB total)
        let file_stats_cache = Arc::new(DefaultFileStatisticsCache::default());
        let list_files_cache = Arc::new(DefaultListFilesCache::default());

        let cache_config = CacheManagerConfig::default()
            .with_files_statistics_cache(Some(file_stats_cache))
            .with_list_files_cache(Some(list_files_cache));

        // Use FairSpillPool instead of GreedyMemoryPool: divides memory fairly
        // among all spillable consumers, triggering spill-to-disk instead of OOM
        // when any consumer exceeds its fair share.  POND_MEMORY_LIMIT_MB lets an
        // operator raise the cap on roomy hosts or lower it on small boards like
        // the 2GB BeaglePlay; unset or unparseable falls back to 512 MiB.
        let pool_mb = std::env::var("POND_MEMORY_LIMIT_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|m| *m >= 64)
            .unwrap_or(512);
        let pool = Arc::new(FairSpillPool::new(pool_mb * 1024 * 1024));

        let runtime_env = RuntimeEnvBuilder::new()
            .with_cache_manager(cache_config)
            .with_memory_pool(pool)
            .build_arc()
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create runtime environment: {}", e))
            })?;

        let mut session_config = SessionConfig::default()
            .with_target_partitions(2) // Limit parallelism to reduce memory pressure
            .with_information_schema(true);

        // Enable late-materialization filter pushdown in Parquet reader.
        // Filters are evaluated during decode: timestamp column decoded first,
        // non-matching rows skipped before decoding remaining columns.
        // Critical for time-scoped queries (WHERE timestamp >= cutoff).
        session_config
            .options_mut()
            .execution
            .parquet
            .pushdown_filters = true;
        session_config
            .options_mut()
            .execution
            .parquet
            .reorder_filters = true;

        // Per-version cache files have non-overlapping time ranges.
        // This enables bin-packing that groups files with non-overlapping
        // statistics into ordered streams, eliminating merge-sort.
        session_config
            .options_mut()
            .execution
            .split_file_groups_by_statistics = true;

        // Read Parquet footer in a single I/O (last 64KB) instead of two
        // separate reads (8-byte footer length, then metadata).
        session_config
            .options_mut()
            .execution
            .parquet
            .metadata_size_hint = Some(64 * 1024);

        let ctx = Arc::new(SessionContext::new_with_config_rt(
            session_config,
            runtime_env,
        ));

        debug!(
            "[LIST] ENABLED DataFusion: FairSpillPool {pool_mb} MiB, pushdown_filters, reorder_filters, split_file_groups_by_statistics, metadata_size_hint=64KB, parallelism=2"
        );

        // Register the fundamental delta_table for direct DeltaTable queries
        debug!(
            "[LIST] REGISTERING fundamental table 'delta_table' in State constructor, Delta table version={:?}",
            table.version()
        );
        _ = ctx
            .register_table("delta_table", Arc::new(table.clone()))
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to register delta_table: {}", e))
            })?;

        // Note: TinyFS ObjectStore registration will be done later when State is available

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            table,
            records: Vec::new(),
            directories: HashMap::new(),
            pending_files: HashMap::new(),
            allocated_versions: HashMap::new(),
            poisoned: false,
            session_context: ctx,
            txn_seq,
            large_file_options,
            partition_records_cache: HashMap::new(),
            external_add_actions: Vec::new(),
        })
    }

    /// Complete SessionContext setup after State is available
    /// This registers the TinyFS ObjectStore which requires a State reference
    async fn complete_session_setup(&self, state: &State) -> Result<(), TLogFSError> {
        // Register the TinyFS ObjectStore with the context
        let _object_store =
            provider::register_tinyfs_object_store(&self.session_context, state.clone()).map_err(
                |e| TLogFSError::ArrowMessage(format!("Failed to register object store: {}", e)),
            )?;
        debug!("[OK] Completed SessionContext setup with TinyFS ObjectStore");
        Ok(())
    }

    /// Initialize root directory - should only be called during pond bootstrap
    ///
    /// This is a special operation that creates the root directory entry.
    /// Should only be called from:
    /// - Steward layer during pond initialization (with proper metadata)
    /// - Tests (with test metadata)
    pub async fn initialize_root_directory(
        &mut self,
        pond_id: uuid7::Uuid,
    ) -> Result<(), TLogFSError> {
        let root_id = FileID::root_for(pond_id);

        // Initialize root directory in memory as empty (will be written to OpLog at commit)
        _ = self
            .directories
            .insert(root_id, DirectoryState::new_empty());
        // Mark as modified so flush writes it
        if let Some(dir_state) = self.directories.get_mut(&root_id) {
            dir_state.modified = true;
        }

        debug!(
            "Initialized root directory {} in memory, marked as modified",
            root_id
        );

        Ok(())
    }

    /// Ensure directory is loaded into in-memory state
    /// If not present, loads from OpLog and caches with modified: false
    async fn ensure_directory_loaded(&mut self, id: FileID) -> Result<(), TLogFSError> {
        // Check if already loaded
        if self.directories.contains_key(&id) {
            return Ok(());
        }

        // Load from OpLog
        let entries = self.query_directory_entries(id).await?;

        // Cache in memory with modified: false
        _ = self
            .directories
            .insert(id, DirectoryState::new_unmodified(entries));
        Ok(())
    }

    /// Get a directory entry from in-memory state
    /// Directory must be loaded first via ensure_directory_loaded
    fn get_directory_entry(
        &self,
        dir_id: FileID,
        entry_name: &str,
    ) -> Option<tinyfs::DirectoryEntry> {
        self.directories
            .get(&dir_id)
            .and_then(|dir_state| dir_state.mapping.get(entry_name).cloned())
    }

    /// Get all directory entries from in-memory state
    /// Directory must be loaded first via ensure_directory_loaded
    fn get_all_directory_entries(&self, dir_id: FileID) -> Vec<tinyfs::DirectoryEntry> {
        self.directories
            .get(&dir_id)
            .map(|dir_state| dir_state.mapping.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Insert a directory entry into in-memory state
    /// Directory must be loaded first via ensure_directory_loaded
    /// Checks for duplicates and marks directory as modified
    fn insert_directory_entry(
        &mut self,
        dir_id: FileID,
        entry: tinyfs::DirectoryEntry,
    ) -> Result<(), TLogFSError> {
        let dir_state = self
            .directories
            .get_mut(&dir_id)
            .ok_or_else(|| TLogFSError::Internal(format!("Directory {} not loaded", dir_id)))?;

        // Check for duplicate entry (O(1) lookup)
        if dir_state.mapping.contains_key(&entry.name) {
            return Err(TLogFSError::Internal(format!(
                "Directory entry '{}' already exists in directory {}",
                entry.name, dir_id
            )));
        }

        // Insert new entry and mark as modified
        _ = dir_state.mapping.insert(entry.name.clone(), entry);
        dir_state.modified = true;

        debug!(
            "Inserted entry into directory {}, marked as modified",
            dir_id
        );
        Ok(())
    }

    /// Remove a directory entry from in-memory state
    /// Directory must be loaded first via ensure_directory_loaded
    /// Returns the removed entry if found, marks directory as modified
    fn remove_directory_entry(
        &mut self,
        dir_id: FileID,
        name: &str,
    ) -> Result<Option<tinyfs::DirectoryEntry>, TLogFSError> {
        let dir_state = self
            .directories
            .get_mut(&dir_id)
            .ok_or_else(|| TLogFSError::Internal(format!("Directory {} not loaded", dir_id)))?;

        // Remove entry if it exists
        let removed = dir_state.mapping.remove(name);

        if removed.is_some() {
            dir_state.modified = true;
            debug!(
                "Removed entry '{}' from directory {}, marked as modified",
                name, dir_id
            );
        }

        Ok(removed)
    }

    /// Begin a new transaction
    async fn begin_impl(&mut self) -> Result<(), TLogFSError> {
        // Clear any stale state (should be clean already, but just in case)
        self.records.clear();
        self.directories.clear();

        debug!("Started transaction");
        Ok(())
    }

    /// Create a hybrid writer for streaming file content
    async fn create_hybrid_writer(&self) -> crate::large_files::HybridWriter {
        crate::large_files::HybridWriter::with_options(
            self.path.clone(),
            self.large_file_options.clone(),
        )
    }

    /// Store file content from hybrid writer result
    async fn store_file_from_hybrid_writer(
        &mut self,
        id: FileID,
        result: crate::large_files::HybridWriterResult,
    ) -> Result<(), TLogFSError> {
        // Get proper version number immediately - no placeholders
        let version = self.get_next_version_for_node(id).await?;
        let entry = if result.size < crate::large_files::LARGE_FILE_THRESHOLD {
            // Small file: store content directly in Delta Lake
            let now = Utc::now().timestamp_micros();
            OplogEntry::new_small_file(id, now, version, result.content, self.txn_seq)
        } else {
            // Large file: content already stored, just create OplogEntry with BLAKE3
            let size = result.size;
            debug!("Storing large file: {size} bytes");
            let now = Utc::now().timestamp_micros();

            match id.entry_type() {
                EntryType::TablePhysicalSeries | EntryType::TableDynamic => {
                    // For FileSeries, extract temporal metadata from Parquet content
                    let (extended_attrs, global_min, global_max) =
                        extract_series_temporal_metadata(&result.content, None)?;

                    // Create large FileSeries entry with temporal metadata and size
                    OplogEntry::new_large_file_series(
                        id,
                        now,
                        version,
                        result.blake3,
                        result.size as i64, // Cast to i64 to match Delta Lake protocol
                        global_min,
                        global_max,
                        extended_attrs,
                        self.txn_seq,
                    )
                }
                _ => {
                    // For other entry types, use generic large file constructor
                    OplogEntry::new_large_file(
                        id,
                        now,
                        version,
                        result.blake3,
                        result.size as i64, // Cast to i64 to match Delta Lake protocol
                        self.txn_seq,
                    )
                }
            }
        };
        self.records.push(entry);
        Ok(())
    }

    /// Store file content with automatic size-based strategy (small inline vs large external)
    pub async fn store_file_content_with_type(
        &mut self,
        id: FileID,
        content: &[u8],
    ) -> Result<(), TLogFSError> {
        use crate::large_files::should_store_as_large_file;

        if should_store_as_large_file(content) {
            // Use hybrid writer for large files
            let mut writer = self.create_hybrid_writer().await;
            use tokio::io::AsyncWriteExt;
            writer.write_all(content).await?;
            writer.shutdown().await?;
            let result = writer.finalize().await?;
            self.store_file_from_hybrid_writer(id, result).await
        } else {
            // For small files, always use direct storage regardless of type
            // This avoids Parquet parsing issues for small FileSeries test files
            self.store_small_file_with_type(id, content).await
        }
    }

    /// Store small file directly in Delta Lake with specific entry type
    async fn store_small_file_with_type(
        &mut self,
        id: FileID,
        content: &[u8],
    ) -> Result<(), TLogFSError> {
        // Get proper version number immediately - no placeholders
        let version = self.get_next_version_for_node(id).await?;
        let now = Utc::now().timestamp_micros();
        let entry = OplogEntry::new_small_file(id, now, version, content.to_vec(), self.txn_seq);

        self.records.push(entry);
        Ok(())
    }

    /// Get the next version number for a specific node (current max + 1)
    async fn get_next_version_for_node(&mut self, id: FileID) -> Result<i64, TLogFSError> {
        debug!("get_next_version_for_node called for node_id={id}");

        // Query all records for this node and find the maximum version
        match self.query_records(id).await {
            Ok(records) => {
                let record_count = records.len();
                debug!("get_next_version_for_node found {record_count} existing records");

                // Log all versions found for debugging duplicates
                if !records.is_empty() {
                    let versions: Vec<i64> = records.iter().map(|r| r.version).collect();
                    debug!(
                        "get_next_version_for_node existing versions for {id}: {:?}",
                        versions
                    );
                }

                let next_version = if records.is_empty() {
                    // This is a new node - check allocated_versions first
                    let allocated = self.allocated_versions.get(&id).and_then(|v| v.last());
                    if let Some(&max_allocated) = allocated {
                        // Someone already allocated versions for this file
                        debug!(
                            "get_next_version_for_node: new node with allocated versions, max_allocated={max_allocated}, returning next_version={}",
                            max_allocated + 1
                        );
                        max_allocated + 1
                    } else {
                        debug!("get_next_version_for_node: new node, starting with version 1");
                        1
                    }
                } else {
                    // This is an existing node Find max version from both records and allocated.
                    let max_record_version = records
                        .iter()
                        .map(|r| r.version)
                        .max()
                        .expect("records is non-empty");

                    let max_allocated = self
                        .allocated_versions
                        .get(&id)
                        .and_then(|v| v.last())
                        .copied()
                        .unwrap_or(0);

                    let max_version = std::cmp::max(max_record_version, max_allocated);
                    let next_version = max_version + 1;
                    debug!(
                        "get_next_version_for_node: existing node with max_record_version={max_record_version}, max_allocated={max_allocated}, returning next_version={next_version}"
                    );
                    next_version
                };

                Ok(next_version)
            }
            Err(e) => {
                let error_str = format!("{:?}", e);
                debug!("get_next_version_for_node query failed: {error_str}");
                // Critical error: cannot determine proper version sequence
                Err(TLogFSError::ArrowMessage(format!(
                    "Cannot determine next version for node {id}: query failed: {e}"
                )))
            }
        }
    }

    /// Pre-allocate version number for async write
    /// This is called when async_writer() is created, BEFORE any data is written
    async fn allocate_version_for_write(&mut self, id: FileID) -> Result<i64, TLogFSError> {
        let next_version = self.get_next_version_for_node(id).await?;

        // Track this allocated version
        self.allocated_versions
            .entry(id)
            .or_default()
            .push(next_version);

        debug!("Allocated version {next_version} for file {id} (pending write)");
        Ok(next_version)
    }

    /// Poison the transaction due to write failure
    async fn poison_transaction(&mut self, reason: String) {
        if !self.poisoned {
            warn!("[TEST] TRANSACTION POISONED: {reason}");
            self.poisoned = true;
        }
    }

    /// Store FileSeries with temporal metadata extraction from Parquet data
    /// This method extracts min/max timestamps from the specified time column
    pub async fn store_file_series_from_parquet(
        &mut self,
        id: FileID,
        content: &[u8],
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64), TLogFSError> {
        // Get the next version number for this node
        let next_version = self.get_next_version_for_node(id).await?;

        // Extract temporal metadata (timestamp column + global min/max) from the
        // Parquet content.
        let (extended_attrs, global_min, global_max) =
            extract_series_temporal_metadata(content, timestamp_column)?;

        // Store the FileSeries using the unified hybrid writer pattern
        use crate::large_files::should_store_as_large_file;

        let content_len = content.len();
        let is_large_file = should_store_as_large_file(content);
        debug!(
            "store_file_series_from_parquet decision: content_len={content_len}, is_large_file={is_large_file}"
        );

        if is_large_file {
            // Use hybrid writer for large files (same pattern as store_file_content_with_type)
            let mut writer = self.create_hybrid_writer().await;
            use tokio::io::AsyncWriteExt;
            writer.write_all(content).await?;
            writer.shutdown().await?;
            let result = writer.finalize().await?;

            // Extract hybrid writer result data
            let sha256 = result.blake3.clone();
            let size = result.size as i64;
            let now = Utc::now().timestamp_micros();

            debug!("Stored large FileSeries via HybridWriter: {size} bytes, BLAKE3: {sha256}");

            let entry = OplogEntry::new_large_file_series(
                id,
                now,
                next_version, // Use proper version counter
                sha256,
                size,
                global_min,
                global_max,
                extended_attrs,
                self.txn_seq,
            );

            // Store metadata in pending records (content is external)
            self.records.push(entry);
        } else {
            // Store as small FileSeries
            let now = Utc::now().timestamp_micros();
            let content_size = content.len();

            debug!(
                "store_file_series_from_parquet - storing as small FileSeries with {content_size} bytes content"
            );

            let entry = OplogEntry::new_file_series(
                id,
                now,
                next_version, // Use proper version counter
                content.to_vec(),
                global_min,
                global_max,
                extended_attrs,
                self.txn_seq,
            );

            let entry_content_size = entry.content.as_ref().map(|c| c.len()).unwrap_or(0);
            debug!(
                "store_file_series_from_parquet - created OplogEntry with content size: {entry_content_size}"
            );

            self.records.push(entry);
        }

        Ok((global_min, global_max))
    }

    /// Create an async reader for a file without loading entire content into memory
    /// Resolve a large-file [`OplogEntry`] to a streaming, BLAKE3-verified
    /// parquet reader by locating the externalized blob under `_large_files/`.
    async fn open_large_file_reader(
        &self,
        record: &OplogEntry,
    ) -> Result<crate::large_files::ParquetFileReader, TLogFSError> {
        let sha256 = record.blake3.as_ref().ok_or_else(|| {
            TLogFSError::ArrowMessage("Large file entry missing BLAKE3".to_string())
        })?;

        // Find the file in either flat or hierarchical structure
        let large_file_path = crate::large_files::find_large_file_path(&self.path, sha256)
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Error searching for large file: {e}")))?
            .ok_or_else(|| TLogFSError::LargeFileNotFound {
                blake3: sha256.clone(),
                path: format!("_large_files/blake3={sha256}"),
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Large file not found in any location",
                ),
            })?;

        debug!("Reading large file from parquet: {large_file_path:?}");

        crate::large_files::ParquetFileReader::new(large_file_path.clone())
            .await
            .map_err(|e| TLogFSError::LargeFileNotFound {
                blake3: sha256.clone(),
                path: large_file_path.display().to_string(),
                source: e,
            })
    }

    pub async fn async_file_reader(
        &mut self,
        id: FileID,
    ) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        self.async_file_reader_bounded(id, tinyfs::SeriesReadBounds::NONE)
            .await
    }

    /// As [`Self::async_file_reader`], but for `FilePhysicalSeries` nodes the
    /// supplied [`tinyfs::SeriesReadBounds`] prune versions before
    /// concatenation, so a windowed reader never opens old history. Versions
    /// lacking recorded event-time bounds are always retained by the event-time
    /// predicate (a missing bound must never drop data).
    /// [`tinyfs::SeriesReadBounds::NONE`] reads the full series, unchanged.
    pub async fn async_file_reader_bounded(
        &mut self,
        id: FileID,
        bounds: tinyfs::SeriesReadBounds,
    ) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        let records = self.query_records(id).await?;

        // Check if this is a FilePhysicalSeries - concatenate all versions oldest-to-newest
        if let Some(first_record) = records.first()
            && first_record.file_type == EntryType::FilePhysicalSeries
        {
            return self.async_file_reader_series(&records, bounds).await;
        }

        // Find the latest record with actual content (skip empty temporal override versions)
        let record = records
            .iter()
            .find(|r| r.size.unwrap_or(0) > 0) // Skip 0-byte versions
            .ok_or_else(|| {
                TLogFSError::ArrowMessage(format!("No non-empty versions found for file {id}"))
            })?;

        if record.is_large_file() {
            // Large file: stream from the verified parquet blob
            let reader = self.open_large_file_reader(record).await?;
            debug!("Created streaming verified reader for large file");
            Ok(Box::pin(reader))
        } else {
            // Small file: use verified content accessor which checks BLAKE3 hash
            let content = record.verified_content_required()?.to_vec();
            debug!("Verified small file ({} bytes)", content.len());

            Ok(Box::pin(std::io::Cursor::new(content)))
        }
    }

    /// Create an async reader for a FilePhysicalSeries that concatenates all versions
    ///
    /// FilePhysicalSeries entries have "all-version" semantics where each version
    /// appends data. When reading, all versions are concatenated in oldest-to-newest order.
    async fn async_file_reader_series(
        &self,
        records: &[OplogEntry],
        bounds: tinyfs::SeriesReadBounds,
    ) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        use tinyfs::chained_reader::ChainedReader;

        // Rows inside a merged collapse row's [collapsed_from, collapsed_through]
        // range have been superseded and must be skipped; otherwise their bytes
        // would be double-counted alongside the merged row that replaced them.
        // `live_series_entries` returns rows in series byte order (by range
        // start), which for merged rows is *not* their version number.
        let mut valid_records: Vec<&OplogEntry> = crate::schema::live_series_entries(records)
            .into_iter()
            .filter(|r| r.size.unwrap_or(0) > 0) // Skip 0-byte versions
            // Bounded pruning: skip versions below the event-time lower bound
            // and/or at-or-below the version watermark. Versions without a
            // recorded `max_event_time` are retained by the event-time
            // predicate (a missing bound must never drop data). A merged row is
            // pruned on the newest version it covers, so a run is retained
            // whenever any version inside it would have been.
            .filter(|r| {
                series_version_retained(
                    r.max_event_time,
                    crate::schema::CollapseRange::of(r).hi,
                    &bounds,
                )
            })
            .collect();
        valid_records.sort_by_key(|r| crate::schema::CollapseRange::of(r).sort_key());

        if valid_records.is_empty() {
            // Return empty reader for empty series
            return Ok(Box::pin(ChainedReader::from_bytes(vec![])));
        }

        // Collect readers and sizes for each version
        let mut readers: Vec<Pin<Box<dyn tokio::io::AsyncRead + Send>>> =
            Vec::with_capacity(valid_records.len());
        let mut sizes: Vec<u64> = Vec::with_capacity(valid_records.len());

        for record in valid_records {
            if record.is_large_file() {
                // Large file: stream from the verified parquet blob
                let reader = self.open_large_file_reader(record).await?;
                sizes.push(record.size.unwrap_or(0) as u64);
                readers.push(Box::pin(reader));
            } else {
                // Small file: use verified content accessor
                let content = record.verified_content_required()?.to_vec();
                sizes.push(content.len() as u64);
                readers.push(Box::pin(std::io::Cursor::new(content)));
            }
        }

        debug!(
            "Created ChainedReader for FilePhysicalSeries with {} versions, total {} bytes",
            readers.len(),
            sizes.iter().sum::<u64>()
        );

        Ok(Box::pin(ChainedReader::new(readers, sizes)))
    }

    /// Commit pending records to Delta Lake
    async fn commit_impl(
        &mut self,
        metadata: PondTxnMetadata,
        table: DeltaTable,
        pond_id: String,
    ) -> Result<Option<deltalake::kernel::transaction::FinalizedCommit>, TLogFSError> {
        // Check if transaction is poisoned (failed write)
        if self.poisoned {
            return Err(TLogFSError::Transaction {
                message: "Cannot commit poisoned transaction - a write operation failed"
                    .to_string(),
            });
        }

        self.flush_directory_operations().await?;

        let mut records = std::mem::take(&mut self.records);

        if records.is_empty() && self.external_add_actions.is_empty() {
            debug!("Committing read-only transaction");
            return Ok(None);
        }

        // Collect external add actions before any writing
        let external_actions = std::mem::take(&mut self.external_add_actions);
        let _has_external = !external_actions.is_empty();
        let has_records = !records.is_empty();

        if has_records {
            for record in &mut records {
                if record.pond_id.is_empty() {
                    record.pond_id = pond_id.clone();
                }
            }
        }

        let record_count = records.len();
        let external_count = external_actions.len();
        info!(
            "Committing {} record(s) + {} external file(s) in {:?}",
            record_count, external_count, self.path
        );

        // Build all Add actions for a single Delta commit
        use deltalake::kernel::Action;
        use deltalake::kernel::models::Add;
        use deltalake::kernel::transaction::CommitBuilder;
        use deltalake::protocol::SaveMode;

        let mut all_actions: Vec<Action> = Vec::new();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        // Phase 1: Write OpLog records as parquet to the object store
        if has_records {
            let batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &records)?;
            drop(records);

            let store = table.object_store();

            // Group by (pond_id, part_id) for partitioned writes.
            let part_id_col = batch.column_by_name("part_id").expect("part_id column");
            let part_id_arr = part_id_col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("part_id is StringArray");
            let pond_id_col = batch.column_by_name("pond_id").expect("pond_id column");
            let pond_id_arr = pond_id_col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("pond_id is StringArray");

            let mut groups: Vec<(String, String)> = Vec::new();
            for i in 0..batch.num_rows() {
                let pond = pond_id_arr.value(i).to_string();
                let part = part_id_arr.value(i).to_string();
                let key = (pond, part);
                if !groups.contains(&key) {
                    groups.push(key);
                }
            }

            for (pond, part) in &groups {
                let mask: arrow::array::BooleanArray = (0..batch.num_rows())
                    .map(|i| {
                        Some(
                            pond_id_arr.value(i) == pond.as_str()
                                && part_id_arr.value(i) == part.as_str(),
                        )
                    })
                    .collect();
                let filtered = arrow::compute::filter_record_batch(&batch, &mask)?;

                // Remove pond_id and part_id columns (partition columns, stored in path).
                let indices: Vec<usize> = filtered
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| f.name() != "part_id" && f.name() != "pond_id")
                    .map(|(i, _)| i)
                    .collect();
                let filtered = filtered.project(&indices)?;

                let mut parquet_buf = Vec::new();
                let mut writer = parquet::arrow::ArrowWriter::try_new(
                    &mut parquet_buf,
                    filtered.schema(),
                    None,
                )?;
                writer.write(&filtered)?;
                let _file_metadata = writer.close()?;

                let file_name = format!(
                    "pond_id={}/part_id={}/part-00000-{}-c000.snappy.parquet",
                    pond,
                    part,
                    uuid7::uuid7()
                );
                let file_size = parquet_buf.len() as i64;

                let obj_path = object_store::path::Path::from(file_name.as_str());
                _ = store
                    .put(&obj_path, parquet_buf.into())
                    .await
                    .map_err(|e| {
                        TLogFSError::Internal(format!("Failed to write parquet: {}", e))
                    })?;

                all_actions.push(Action::Add(Add {
                    path: file_name,
                    partition_values: HashMap::from([
                        ("pond_id".to_string(), Some(pond.clone())),
                        ("part_id".to_string(), Some(part.clone())),
                    ]),
                    size: file_size,
                    modification_time: now_ms,
                    data_change: true,
                    stats: None,
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                }));
            }
        } else {
            drop(records);
        }

        // Phase 2: Add external parquet files (already in object store)
        for ea in &external_actions {
            all_actions.push(Action::Add(Add {
                path: ea.path.clone(),
                partition_values: HashMap::from([
                    ("pond_id".to_string(), Some(ea.pond_id.clone())),
                    ("part_id".to_string(), Some(ea.part_id.clone())),
                ]),
                size: ea.size,
                modification_time: now_ms,
                data_change: true,
                stats: None,
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            }));
        }

        // Phase 3: Single Delta commit with all actions
        let operation = deltalake::protocol::DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: Some(vec!["pond_id".to_string(), "part_id".to_string()]),
            predicate: None,
        };

        let snapshot_ref: Option<&dyn deltalake::kernel::transaction::TableReference> = table
            .snapshot()
            .ok()
            .map(|s| s as &dyn deltalake::kernel::transaction::TableReference);

        // Build commit metadata (pond_txn)
        let commit_metadata = metadata.to_delta_metadata();

        let finalized = CommitBuilder::default()
            .with_actions(all_actions)
            .with_app_metadata(commit_metadata)
            .build(snapshot_ref, table.log_store().clone(), operation)
            .await?;

        debug!(
            "[OK] Single Delta commit at version {}: {} record(s) + {} external file(s)",
            finalized.version, record_count, external_count,
        );

        self.records.clear();
        self.directories.clear();

        Ok(Some(finalized))
    }

    /// Serialize DirectoryEntry records as Arrow IPC bytes
    fn serialize_directory_entries(
        &self,
        entries: &[DirectoryEntry],
    ) -> Result<Vec<u8>, TLogFSError> {
        // For empty directories, use truly empty content (0 bytes) instead of Arrow IPC with schema
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        serialization::serialize_to_arrow_ipc(entries)
    }

    /// Deserialize DirectoryEntry records from Arrow IPC bytes into HashMap by name
    fn deserialize_directory_entries(
        &self,
        content: &[u8],
    ) -> Result<HashMap<String, DirectoryEntry>, TLogFSError> {
        // Handle empty directories (0 bytes of content)
        if content.is_empty() {
            return Ok(HashMap::new());
        }
        debug!(
            "deserialize_directory_entries: processing {} bytes for directory entries",
            content.len()
        );
        let entries: Vec<DirectoryEntry> = serialization::deserialize_from_arrow_ipc(content)?;
        let map: HashMap<String, DirectoryEntry> =
            entries.into_iter().map(|e| (e.name.clone(), e)).collect();
        debug!(
            "deserialize_directory_entries: successfully deserialized {} directory entries",
            map.len()
        );
        Ok(map)
    }

    /// Store file content reference with transaction context (used by transaction guard NewFileWriter)
    /// If pre_allocated_version is Some, uses that version instead of calculating a new one
    pub async fn store_file_content_ref(
        &mut self,
        id: FileID,
        content_ref: crate::file_writer::ContentRef,
        metadata: crate::file_writer::FileMetadata,
        pre_allocated_version: Option<i64>,
        bao_outboard: Option<Vec<u8>>,
        collapsed_through: Option<i64>,
    ) -> Result<(), TLogFSError> {
        debug!("store_file_content_ref_transactional called for node_id={id}");

        // Create OplogEntry from content reference
        let now = Utc::now().timestamp_micros();

        // Use pre-allocated version if provided, otherwise calculate next version
        let version = if let Some(allocated) = pre_allocated_version {
            debug!("Using pre-allocated version {allocated} for file {id}");
            allocated
        } else {
            // Legacy path: calculate version now (for backward compatibility)
            // Check if there's already an entry for this node in this transaction
            let existing_entry = self
                .records
                .iter()
                .find(|e| e.node_id == id.node_id() && e.part_id == id.part_id());

            if existing_entry.is_some() {
                // There's already a version in this transaction.
                // ALWAYS get next version - never reuse (this is an immutable log).
                self.get_next_version_for_node(id).await?
            } else {
                // New entry - calculate next version
                self.get_next_version_for_node(id).await?
            }
        };

        let txn_seq = self.txn_seq; // Capture for use in match arms

        let entry = match content_ref {
            crate::file_writer::ContentRef::Small(content) => {
                // Small file: store content inline
                match id.entry_type() {
                    EntryType::TablePhysicalSeries | EntryType::TableDynamic => {
                        // FileSeries needs temporal metadata
                        match metadata {
                            crate::file_writer::FileMetadata::Series {
                                min_timestamp,
                                max_timestamp,
                                timestamp_column,
                            } => {
                                use crate::schema::ExtendedAttributes;
                                let mut extended_attrs = ExtendedAttributes::default();
                                _ = extended_attrs.set_timestamp_column(&timestamp_column);

                                OplogEntry::new_file_series(
                                    id,
                                    now,
                                    version, // Use proper version counter
                                    content,
                                    min_timestamp,
                                    max_timestamp,
                                    extended_attrs,
                                    txn_seq,
                                )
                            }
                            _ => {
                                return Err(TLogFSError::Transaction {
                                    message: "FileSeries requires Series metadata".to_string(),
                                });
                            }
                        }
                    }
                    EntryType::FilePhysicalSeries => {
                        // FilePhysicalSeries: store temporal metadata if provided,
                        // otherwise store as a regular small file.
                        match metadata {
                            crate::file_writer::FileMetadata::Series {
                                min_timestamp,
                                max_timestamp,
                                timestamp_column,
                            } => {
                                use crate::schema::ExtendedAttributes;
                                let mut extended_attrs = ExtendedAttributes::default();
                                _ = extended_attrs.set_timestamp_column(&timestamp_column);

                                OplogEntry::new_file_series(
                                    id,
                                    now,
                                    version,
                                    content,
                                    min_timestamp,
                                    max_timestamp,
                                    extended_attrs,
                                    txn_seq,
                                )
                            }
                            _ => OplogEntry::new_small_file(id, now, version, content, txn_seq),
                        }
                    }
                    _ => {
                        // Regular small file
                        OplogEntry::new_small_file(
                            id, now, version, // Use proper version counter
                            content, txn_seq,
                        )
                    }
                }
            }
            crate::file_writer::ContentRef::Large(sha256, size) => {
                // Large file: store reference
                match id.entry_type() {
                    EntryType::TablePhysicalSeries | EntryType::TableDynamic => {
                        // Large FileSeries needs temporal metadata
                        match metadata {
                            crate::file_writer::FileMetadata::Series {
                                min_timestamp,
                                max_timestamp,
                                timestamp_column,
                            } => {
                                use crate::schema::ExtendedAttributes;
                                let mut extended_attrs = ExtendedAttributes::default();
                                _ = extended_attrs.set_timestamp_column(&timestamp_column);

                                OplogEntry::new_large_file_series(
                                    id,
                                    now,
                                    version, // Use proper version counter
                                    sha256,
                                    size as i64,
                                    min_timestamp,
                                    max_timestamp,
                                    extended_attrs,
                                    txn_seq,
                                )
                            }
                            _ => {
                                return Err(TLogFSError::Transaction {
                                    message: "Large FileSeries requires Series metadata"
                                        .to_string(),
                                });
                            }
                        }
                    }
                    EntryType::FilePhysicalSeries => {
                        // Large FilePhysicalSeries: store temporal metadata
                        // if provided, otherwise store as a regular large file.
                        match metadata {
                            crate::file_writer::FileMetadata::Series {
                                min_timestamp,
                                max_timestamp,
                                timestamp_column,
                            } => {
                                use crate::schema::ExtendedAttributes;
                                let mut extended_attrs = ExtendedAttributes::default();
                                _ = extended_attrs.set_timestamp_column(&timestamp_column);

                                OplogEntry::new_large_file_series(
                                    id,
                                    now,
                                    version,
                                    sha256,
                                    size as i64,
                                    min_timestamp,
                                    max_timestamp,
                                    extended_attrs,
                                    txn_seq,
                                )
                            }
                            _ => OplogEntry::new_large_file(
                                id,
                                now,
                                version,
                                sha256,
                                size as i64,
                                txn_seq,
                            ),
                        }
                    }
                    _ => {
                        // Regular large file
                        OplogEntry::new_large_file(
                            id,
                            now,
                            version, // Use proper version counter
                            sha256,
                            size as i64,
                            txn_seq,
                        )
                    }
                }
            }
        };

        // IMMUTABLE LOG: ALWAYS append, never replace
        // Every write creates a new version, even multiple writes in the same transaction
        debug!(
            "Appending new version {} for node {id} (immutable log - no replacements)",
            entry.version
        );

        // Remove from pending_files since it now has actual content
        _ = self.pending_files.remove(&id);

        // Set bao_outboard if provided
        let mut entry = entry;
        if let Some(outboard) = bao_outboard {
            entry.set_bao_outboard(outboard);
        }
        // A pull replicating a source-side collapse stamps the merged version so
        // the read path and content fold both drop the superseded predecessors.
        if let Some(sentinel) = collapsed_through {
            entry.collapsed_through = Some(sentinel);
        }

        self.records.push(entry);

        debug!("Stored file content reference for node {id}");
        Ok(())
    }

    /// Query directory entries for a parent node (OPTIMIZED: LATEST VERSION ONLY)
    ///
    /// [GO] MAJOR PERFORMANCE IMPROVEMENT: This function now reads ONLY the latest version
    /// instead of iterating through all historical versions. This is O(1) instead of O(N)
    /// where N = number of transactions that modified the directory.
    ///
    /// Uses SQL "ORDER BY version DESC LIMIT 1" to fetch exactly one record from Delta Lake.
    async fn query_directory_entries(
        &mut self,
        id: FileID,
    ) -> Result<Vec<DirectoryEntry>, TLogFSError> {
        // [GO] CRITICAL: Use specialized query that fetches ONLY latest record via SQL LIMIT 1
        let latest_record = self
            .query_latest_directory_record(id)
            .await?
            .ok_or_else(|| TLogFSError::NodeNotFound {
                path: PathBuf::from(format!("Directory {}", id)),
            })?;

        debug!(
            "[GO] query_directory_entries: read ONLY version {} for part_id={} (SQL LIMIT 1)",
            latest_record.version, id
        );

        // Validate format field (future-proofing)
        use super::schema::StorageFormat;
        if latest_record.format != StorageFormat::FullDir {
            warn!(
                "Directory {} has unexpected format {:?}, expected FullDir",
                id, latest_record.format
            );
            // Don't fail - allow Inline format for backward compatibility during transition
        }

        // [OK] Single record fetched via SQL LIMIT 1
        // [OK] Single deserialize - complete directory state as HashMap
        // [OK] No iteration through history
        // [OK] No deduplication across versions
        // [OK] Constant time regardless of transaction count
        if let Some(content) = &latest_record.content {
            debug!(
                "query_directory_entries: deserializing {} bytes for directory {}",
                content.len(),
                id
            );
            let entries_map = self.deserialize_directory_entries(content).map_err(|e| {
                TLogFSError::ArrowMessage(format!(
                    "Failed to deserialize directory content for part_id={}, version={}: {}",
                    id, latest_record.version, e
                ))
            })?;
            debug!(
                "[OK] query_directory_entries: loaded {} entries from snapshot (version {})",
                entries_map.len(),
                latest_record.version
            );
            Ok(entries_map.into_values().collect())
        } else {
            // Empty directory
            Ok(Vec::new())
        }
    }

    /// Flush all pending directory operations to the oplog
    ///
    /// New architecture: In-memory directory state with commit-time flush.
    /// Only writes directories that have been modified during this transaction.
    async fn flush_directory_operations(&mut self) -> Result<(), TLogFSError> {
        let modified_count = self.directories.values().filter(|ds| ds.modified).count();
        debug!(
            "flush_directory_operations: starting, txn_seq={}, directories.len()={}, modified={}",
            self.txn_seq,
            self.directories.len(),
            modified_count
        );

        let pending_dirs = std::mem::take(&mut self.directories);

        // Iterate all directories and write only those marked as modified
        for (dir_id, dir_state) in pending_dirs {
            if !dir_state.modified {
                continue; // Skip unmodified directories
            }

            debug!(
                "flush_directory_operations: writing modified directory {} with {} entries",
                dir_id,
                dir_state.mapping.len()
            );

            // Get next version for this directory
            let next_version = self.get_next_version_for_node(dir_id).await?;

            // Convert HashMap to Vec for serialization, updating version_last_modified
            let all_entries: Vec<DirectoryEntry> = dir_state
                .mapping
                .values()
                .map(|entry| {
                    // Update version_last_modified to current version,
                    // preserving pond_id for cross-pond import entries.
                    let mut new_entry = DirectoryEntry::new(
                        entry.name.clone(),
                        entry.child_node_id,
                        entry.entry_type,
                        next_version,
                    );
                    new_entry.pond_id = entry.pond_id.clone();
                    new_entry
                })
                .collect();

            debug!(
                "Serializing {} entries for directory {} at version {}",
                all_entries.len(),
                dir_id,
                next_version
            );

            // Serialize full snapshot
            let content_bytes = self.serialize_directory_entries(&all_entries)?;

            // Create full snapshot OplogEntry
            let now = Utc::now().timestamp_micros();
            let mut record = OplogEntry::new_directory_full_snapshot(
                dir_id,
                now,
                next_version,
                content_bytes,
                self.txn_seq,
            );
            // Stamp the row's pond_id from the directory's own FileID so a
            // cross-pond import's foreign-rooted rows persist under the foreign
            // pond_id partition rather than the local committing pond.
            record.pond_id = dir_id.pond_id().to_string();
            debug!(
                "[NOTE] FLUSH CREATING RECORD: part_id={}, node_id={}, file_type={:?}, version={}, content_len={}, format={:?}",
                record.part_id,
                record.node_id,
                record.file_type,
                record.version,
                record.content.as_ref().map(|c| c.len()).unwrap_or(0),
                record.format
            );

            self.records.push(record);
        }

        debug!(
            "flush_directory_operations: complete, wrote {} modified directories",
            modified_count
        );

        Ok(())
    }
    /// Create a dynamic directory node with factory configuration
    /// Create a dynamic node with factory configuration (used by `mknod` command)
    ///
    /// NOTE: Most dynamic nodes (created by DynamicDirDirectory, TemplateDirectory, etc.)
    /// do NOT need this method - they are created on-the-fly from configuration when
    /// their parent directory is accessed. This method is only for explicitly creating
    /// persistent dynamic nodes via `mknod`.
    ///
    /// TODO: Complete implementation - for now, callers should use FactoryRegistry directly
    async fn create_dynamic_node(
        &mut self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
        state: State,
    ) -> Result<Node, TLogFSError> {
        debug!(
            "create_dynamic_node: id={}, entry_type={:?}, factory_type='{}', txn_seq={}",
            id,
            id.entry_type(),
            factory_type,
            self.txn_seq
        );

        let now = Utc::now().timestamp_micros();
        let next_version = self.get_next_version_for_node(id).await?;

        // Create dynamic node OplogEntry with factory field
        let oplog_entry = OplogEntry::new_dynamic_node(
            id,
            now,
            next_version,
            factory_type,
            config_content.clone(),
            self.txn_seq,
        );

        debug!(
            "[NOTE] CREATING DYNAMIC NODE OPLOG ENTRY: part_id={}, node_id={}, file_type={:?}, version={}, factory={:?}, content_len={:?}, extended_attrs={:?}, format={:?}, txn_seq={}",
            oplog_entry.part_id,
            oplog_entry.node_id,
            oplog_entry.file_type,
            oplog_entry.version,
            oplog_entry.factory,
            oplog_entry.content.as_ref().map(|c| c.len()),
            oplog_entry
                .extended_attributes
                .as_ref()
                .map(|s| &s[0..s.len().min(100)]),
            oplog_entry.format,
            oplog_entry.txn_seq
        );

        // Add to pending records
        self.records.push(oplog_entry);

        // Create in-memory node - dynamic nodes are treated as regular files/directories
        // with factory metadata attached. Don't initialize factory here - that's done
        // by post-commit discovery or explicit FactoryRegistry calls.
        //
        // Note: OpLog entry created above contains:
        // - FileDynamic: config in content field
        // - DirectoryDynamic: config in extended_attributes, empty content for directory entries
        let node = match id.entry_type() {
            EntryType::FileDynamic | EntryType::TableDynamic => {
                // Create as regular file node - factory metadata in OpLog
                // FileDynamic: non-queryable factory files (templates, configs)
                // TableDynamic: SQL-queryable factory files (synthetic-timeseries, timeseries-join, etc.)
                node_factory::create_file_node(id, state)
            }
            EntryType::DirectoryDynamic => {
                // For dynamic directories, the OpLog entry was created with empty content
                // (which represents an empty directory). We still need to track it in
                // the directories HashMap so subsequent operations (adding children) work.
                debug!(
                    "create_dynamic_node: tracking empty dynamic directory {} in memory",
                    id
                );
                let dir_state = DirectoryState::new_empty();
                // Don't mark as modified - OpLog entry already created above with empty content
                _ = self.directories.insert(id, dir_state);

                // Create directory node
                node_factory::create_directory_node(id, state)
            }
            _ => {
                return Err(TLogFSError::Transaction {
                    message: format!(
                        "create_dynamic_node called with non-dynamic EntryType: {:?}",
                        id.entry_type()
                    ),
                });
            }
        };

        node.map_err(TLogFSError::TinyFS)
    }

    /// Get dynamic node configuration if the node is dynamic
    /// Uses the same query pattern as the rest of the persistence layer
    pub async fn get_dynamic_node_config(
        &mut self,
        id: FileID,
    ) -> Result<Option<(String, Vec<u8>)>, TLogFSError> {
        // First check pending records (for nodes created in current transaction)
        for record in &self.records {
            if record.node_id == id.node_id()
                && record.part_id == id.part_id()
                && let Some(factory_type) = &record.factory
            {
                // Config is stored in content field for all dynamic node types
                // (new_dynamic_node stores config in content for both files and directories)
                let config_content = record.verified_content_required()?.to_vec();
                return Ok(Some((factory_type.clone(), config_content)));
            }
        }

        // Then check committed records (for existing nodes)
        let records = self.query_records(id).await?;

        if let Some(record) = records.first()
            && let Some(factory_type) = &record.factory
        {
            let config_content = record.verified_content_required()?.to_vec();
            Ok(Some((factory_type.clone(), config_content)))
        } else {
            Ok(None)
        }
    }

    /// Update the configuration of an existing dynamic node
    /// This creates a new OplogEntry with the updated configuration
    pub async fn update_dynamic_node_config(
        &mut self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<(), TLogFSError> {
        let now = Utc::now().timestamp_micros();

        // Verify the node exists and is a dynamic node by checking if it has a factory field
        // We don't try to parse the existing config since it might be malformed (that's why we're overwriting)
        let has_factory = self
            .records
            .iter()
            .any(|r| r.node_id == id.node_id() && r.part_id == id.part_id() && r.factory.is_some());

        if !has_factory {
            // Check committed records
            let records = self.query_records(id).await?;
            if !records
                .first()
                .map(|r| r.factory.is_some())
                .unwrap_or(false)
            {
                return Err(TLogFSError::NodeNotFound {
                    path: format!("id:{}", id).into(),
                });
            }
        }

        // Get the current version from existing records to increment it
        let records = self.query_records(id).await?;
        let current_version = records.first().map(|r| r.version).unwrap_or(0);
        let new_version = current_version + 1;

        let entry = OplogEntry::new_dynamic_node(
            id,
            now,
            new_version,
            factory_type,
            config_content,
            self.txn_seq,
        );

        // Add to pending records
        self.records.push(entry);

        Ok(())
    }

    /// Query for ONLY the latest record for any node type (O(1) performance)
    async fn query_latest_record(&mut self, id: FileID) -> Result<OplogEntry, TLogFSError> {
        let pond_id_str = id.pond_id().to_string();

        // Step 1: Check pending records in memory FIRST
        // Pending records have empty pond_id (stamped at commit), so match
        // only by part_id+node_id. This is safe because pending records
        // are always from the local pond's current transaction.
        let pending_record = self
            .records
            .iter()
            .filter(|r| {
                // @@@ Linear!
                r.part_id == id.part_id() && r.node_id == id.node_id()
            })
            .max_by_key(|r| r.version)
            .cloned();

        // If a record is found pending, it must be later than persistence storage.
        if let Some(pending) = pending_record {
            return Ok(pending);
        }

        // Step 2: Use the partition cache to get the latest committed record.
        // Filter by pond_id to isolate records from different ponds that
        // share the same partition (e.g., root partition in cross-pond import).
        self.ensure_partition_cached(id.part_id()).await?;

        match self
            .partition_records_cache
            .get(&id.part_id())
            .and_then(|by_node| by_node.get(&id.node_id()))
            .and_then(|records| records.iter().find(|r| r.pond_id == pond_id_str).cloned())
        {
            Some(record) => Ok(record),
            None => {
                // Distinguish "partition has no data" (foreign partition not imported)
                // from "node not found within a populated partition"
                let partition_has_data = self
                    .partition_records_cache
                    .get(&id.part_id())
                    .map(|by_node| !by_node.is_empty())
                    .unwrap_or(false);

                if partition_has_data {
                    Err(TLogFSError::PartitionNotFound {
                        part_id: id.part_id().to_string(),
                        node_id: id.node_id().to_string(),
                        hint: "Node not found in this partition.".to_string(),
                    })
                } else {
                    Err(TLogFSError::PartitionNotFound {
                        part_id: id.part_id().to_string(),
                        node_id: id.node_id().to_string(),
                        hint: "This partition contains no data. If this is an imported \
                               directory from a foreign pond, the partition may not have \
                               been included in the import. Use source_path with /** \
                               to import recursively."
                            .to_string(),
                    })
                }
            }
        }
    }

    /// Query for ONLY the latest directory record (O(1) performance)
    ///
    /// This function uses SQL LIMIT 1 to fetch only the most recent directory version,
    /// avoiding the O(N) cost of reading all historical versions.
    ///
    /// IMPORTANT: This function checks BOTH committed (Delta Lake) and pending (self.records) state.
    /// During a transaction, pending directory records in self.records take precedence over committed ones.
    async fn query_latest_directory_record(
        &mut self,
        id: FileID,
    ) -> Result<Option<OplogEntry>, TLogFSError> {
        let pond_id_str = id.pond_id().to_string();

        // Step 1: Check pending records in memory FIRST
        // During flush_directory_operations, newly created directory snapshots are in self.records
        let pending_record = self
            .records
            .iter()
            .filter(|r| r.part_id == id.part_id() && r.node_id == id.node_id())
            .max_by_key(|r| r.version)
            .cloned();

        // Step 2: Use partition cache for committed records, scoped by pond_id
        self.ensure_partition_cached(id.part_id()).await?;

        let committed_record = self
            .partition_records_cache
            .get(&id.part_id())
            .and_then(|by_node| by_node.get(&id.node_id()))
            .and_then(|records| {
                records
                    .iter()
                    .find(|r| r.pond_id == pond_id_str && r.file_type.is_directory())
                    .cloned()
            });

        // Step 3: Return the latest between committed and pending (pending wins if both exist)
        match (committed_record, pending_record) {
            (Some(c), Some(p)) => Ok(Some(if p.version > c.version { p } else { c })),
            (Some(c), None) => Ok(Some(c)),
            (None, Some(p)) => Ok(Some(p)),
            (None, None) => Ok(None),
        }
    }

    /// Load all committed records for a partition into the cache with a
    /// single SQL query.  Subsequent `query_records()` calls for any node
    /// in this partition will hit the cache instead of running SQL.
    async fn ensure_partition_cached(
        &mut self,
        part_id: tinyfs::PartID,
    ) -> Result<(), TLogFSError> {
        if self.partition_records_cache.contains_key(&part_id) {
            return Ok(());
        }

        let sql = format!(
            "SELECT * FROM delta_table WHERE part_id = '{}' ORDER BY timestamp DESC",
            part_id
        );

        let query_start = std::time::Instant::now();
        let batches = self
            .session_context
            .sql(&sql)
            .await
            .map_err(TLogFSError::DataFusion)?
            .collect()
            .await
            .map_err(TLogFSError::DataFusion)?;

        let mut by_node: HashMap<tinyfs::NodeID, Vec<OplogEntry>> = HashMap::new();
        for batch in batches {
            let records: Vec<OplogEntry> = serde_arrow::from_record_batch(&batch)?;
            for record in records {
                by_node.entry(record.node_id).or_default().push(record);
            }
        }

        let node_count = by_node.len();
        let elapsed = query_start.elapsed().as_millis();
        debug!(
            "ensure_partition_cached: loaded {node_count} nodes for part_id={part_id} in {elapsed}ms"
        );

        _ = self.partition_records_cache.insert(part_id, by_node);
        Ok(())
    }

    /// Query records from both committed (Delta Lake) and pending (in-memory) data
    /// This ensures TinyFS operations can see pending data before commit
    ///
    /// SECURITY: Always requires node_id to enforce proper data isolation between nodes
    async fn query_records(&mut self, id: FileID) -> Result<Vec<OplogEntry>, TLogFSError> {
        // Performance tracing - enable with RUST_LOG=trace and redirect stderr
        let mut trace = utilities::perf_trace::PerfTrace::start("query_records");
        let caller = utilities::perf_trace::extract_caller(
            "tlogfs::persistence::InnerState::",
            "query_records",
        );
        trace.param("caller", &caller);
        trace.param("id", id);

        let pond_id_str = id.pond_id().to_string();

        // Step 1: Get committed records, scoped by pond_id
        let query_start = std::time::Instant::now();

        self.ensure_partition_cached(id.part_id()).await?;

        let committed_records: Vec<OplogEntry> = self
            .partition_records_cache
            .get(&id.part_id())
            .and_then(|by_node| by_node.get(&id.node_id()))
            .map(|records| {
                records
                    .iter()
                    .filter(|r| r.pond_id == pond_id_str)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        trace.metric("query_ms", query_start.elapsed().as_millis() as u64);
        trace.metric("committed_count", committed_records.len() as u64);

        // Step 2: Get pending records from memory (node-scoped)
        let pending_start = std::time::Instant::now();
        let records = {
            self.records
                .iter()
                .filter(|record| record.part_id == id.part_id() && record.node_id == id.node_id())
                .cloned()
                .collect::<Vec<_>>()
        };
        trace.metric("memory_scan_us", pending_start.elapsed().as_micros() as u64);
        trace.metric("pending_count", records.len() as u64);

        // Step 3: Combine and sort by timestamp
        let mut all_records = committed_records;
        all_records.extend(records);
        all_records.sort_by_key(|r| std::cmp::Reverse(r.timestamp));

        trace.metric("total_count", all_records.len() as u64);

        Ok(all_records)
    }

    async fn load_node(&mut self, id: FileID, state: State) -> TinyFSResult<Node> {
        debug!("load_node {id:?}");

        // [SEARCH] CRITICAL FIX: Check if this is a directory in self.directories first
        // During a transaction, directories created by store_node() exist in self.directories
        // but don't have OpLog records in self.records until flush_directory_operations() is called at commit
        if self.directories.contains_key(&id) {
            debug!(
                "load_node: found directory {} in self.directories (not yet flushed to OpLog)",
                id
            );
            // Create directory node directly - it exists in memory
            return node_factory::create_directory_node(id, state);
        }

        // [GO] OPTIMIZATION: Query only the latest record (O(1) instead of O(N))
        match self.query_latest_record(id).await {
            Ok(record) => {
                debug!("load_node: found latest record version {}", record.version);
                // Use node factory to create the appropriate node type
                node_factory::create_node_from_oplog_entry(&record, id, state).await
            }
            Err(e) => {
                let error_msg = e.to_string();
                debug!("query_latest_record failed with error: {error_msg}");
                Err(error_utils::to_tinyfs_error(e))
            }
        }
    }

    async fn get_factory_for_node(&mut self, id: FileID) -> Result<Option<String>, TLogFSError> {
        debug!("get_factory_for_node {id}");

        // Query Delta Lake for the most recent record for this node
        let records = self
            .query_records(id)
            .await
            .map_err(|e| TLogFSError::TinyFS(error_utils::to_tinyfs_error(e)))?;

        if let Some(record) = records.first() {
            // Return the factory from the most recent oplog record
            Ok(record.factory.clone())
        } else {
            // Check pending records
            let pending_record = self
                .records
                .iter()
                .find(|entry| entry.node_id == id.node_id() && entry.part_id == id.part_id())
                .cloned();

            if let Some(record) = pending_record {
                Ok(record.factory.clone())
            } else {
                // Node doesn't exist
                Err(TLogFSError::TinyFS(tinyfs::Error::NotFound(PathBuf::from(
                    format!("Node {} not found", id),
                ))))
            }
        }
    }

    async fn store_node(&mut self, node: &Node) -> TinyFSResult<()> {
        debug!(
            "TRANSACTION: OpLogPersistence::store_node() - node: {}",
            node.id
        );

        let id = node.id();

        // Create OplogEntry based on node type
        let content = match &node.node_type {
            NodeType::File(_file_handle) => {
                // Files should NOT create empty placeholder records
                // Track the file as pending - it exists in the directory but has no content yet
                // When shutdown() writes actual content, it will create the first version
                debug!(
                    "TRANSACTION: store_node() - file {} registered as pending, no oplog record yet",
                    id
                );
                _ = self.pending_files.insert(id, id.entry_type());
                return Ok(());
            }
            NodeType::Directory(_) => {
                // Check if directory already loaded in memory
                // If present, skip - will be written by flush_directory_operations if modified
                if self.directories.contains_key(&id) {
                    debug!(
                        "TRANSACTION: store_node() - directory {} already tracked in memory, skipping",
                        id
                    );
                    return Ok(());
                }

                // Directory not yet tracked - initialize empty directory in memory and mark as modified
                // This ensures flush_directory_operations will write the directory to OpLog
                debug!(
                    "TRANSACTION: store_node() - initializing empty directory {} in memory, marked as modified",
                    id
                );
                let mut dir_state = DirectoryState::new_empty();
                dir_state.modified = true; // Mark as modified so it gets written at commit
                _ = self.directories.insert(id, dir_state);
                return Ok(());
            }
            NodeType::Symlink(symlink_handle) => {
                let target = symlink_handle
                    .readlink()
                    .await
                    .map_other_context("Symlink readlink error")?;
                target.to_string_lossy().as_bytes().to_vec()
            }
        };

        let now = Utc::now().timestamp_micros();

        let next_version = self
            .get_next_version_for_node(id)
            .await
            .map_err(error_utils::to_tinyfs_error)?;

        let oplog_entry = OplogEntry::new_inline(id, now, next_version, content, self.txn_seq);

        self.records.push(oplog_entry);
        Ok(())
    }

    async fn load_symlink_target(&mut self, id: FileID) -> TinyFSResult<PathBuf> {
        let records = self
            .query_records(id)
            .await
            .map_err(error_utils::to_tinyfs_error)?;

        if let Some(record) = records.first() {
            if record.file_type == EntryType::Symlink {
                let content = record
                    .verified_content_required()
                    .map_err(error_utils::to_tinyfs_error)?;
                let target_str = String::from_utf8(content.to_vec())
                    .map_other_context("Invalid UTF-8 in symlink target")?;
                Ok(PathBuf::from(target_str))
            } else {
                Err(tinyfs::Error::Other(
                    "Expected symlink node type".to_string(),
                ))
            }
        } else {
            Err(tinyfs::Error::NotFound(PathBuf::from(format!(
                "Symlink {id} not found"
            ))))
        }
    }

    /// Update directory content with new encoded entries
    /// Called by OpLogDirectory::insert to persist directory changes
    async fn update_directory_content(
        &mut self,
        id: FileID,
        content: Vec<u8>,
    ) -> Result<(), TLogFSError> {
        let now = Utc::now().timestamp_micros();

        let next_version = self.get_next_version_for_node(id).await?;

        let oplog_entry = OplogEntry::new_inline(id, now, next_version, content, self.txn_seq);

        self.records.push(oplog_entry);
        Ok(())
    }

    async fn create_file_node(&mut self, id: FileID, state: State) -> TinyFSResult<Node> {
        // Create file node in memory only - no immediate persistence
        // The caller (create_file_path_streaming_with_type) will call store_node() to persist
        // NO double-write: don't call store_file_content_with_type here!
        node_factory::create_file_node(id, state)
    }

    async fn create_directory_node(&self, id: FileID, state: State) -> TinyFSResult<Node> {
        node_factory::create_directory_node(id, state)
    }

    async fn create_symlink_node(
        &mut self,
        id: FileID,
        target: &Path,
        state: State,
    ) -> TinyFSResult<Node> {
        // Create symlink node in memory only - store_node will persist it
        node_factory::create_symlink_node(id, target, state)
    }

    async fn metadata(&mut self, id: FileID) -> TinyFSResult<NodeMetadata> {
        debug!("metadata: querying id={id}");

        // Check if this is a pending file (created but not yet written)
        if let Some(&entry_type) = self.pending_files.get(&id) {
            debug!("metadata: found pending file {id}, returning default metadata");
            return Ok(NodeMetadata {
                entry_type,
                size: Some(0),
                blake3: None,
                bao_outboard: None, // No bao-tree data yet
                version: 0,         // No version yet - will be 1 when first written
                timestamp: Utc::now().timestamp_micros(),
            });
        }

        // Query Delta Lake for the most recent record for this node using the correct partition
        let records = self
            .query_records(id)
            .await
            .map_err(error_utils::to_tinyfs_error)?;

        let record_count = records.len();
        debug!("metadata: found {record_count} records");

        // Debug: log all records to understand the issue
        for (i, record) in records.iter().enumerate() {
            let file_type_str = format!("{:?}", record.file_type);
            let version = record.version;
            let timestamp = record.timestamp;
            debug!(
                "metadata: record[{i}] - file_type={file_type_str}, version={version}, timestamp={timestamp}"
            );
        }

        // For a FilePhysicalSeries the newest *content* is not necessarily the
        // highest *version*: size-tiered collapse appends a merged run with a
        // fresh (highest) version number while that run stands for versions in
        // the middle of the byte stream. The append path resumes its cumulative
        // bao state from this record, so it must be the row covering the end of
        // the stream -- the last live row in range order -- not `records.first()`.
        let newest = if id.entry_type() == EntryType::FilePhysicalSeries {
            crate::schema::live_series_entries(&records)
                .into_iter()
                .rfind(|r| r.size.unwrap_or(0) > 0)
                .or_else(|| records.first())
        } else {
            records.first()
        };

        if let Some(record) = newest {
            // Use the record directly - it's already an OplogEntry with metadata() method
            let file_type_str = format!("{:?}", record.file_type);
            debug!(
                "metadata: returning consolidated metadata from OplogEntry - using file_type={file_type_str}"
            );
            Ok(record.metadata())
        } else {
            debug!("metadata: no records found");
            // Node doesn't exist
            Err(tinyfs::Error::not_found(format!("Node {id}")))
        }
    }

    // Versioning operations implementation

    async fn list_file_versions(&mut self, id: FileID) -> TinyFSResult<Vec<FileVersionInfo>> {
        debug!("list_file_versions called for id={id}");

        assert!(
            !id.entry_type().is_dynamic(),
            "list_file_versions called with dynamic FileID {id} (entry_type={:?}). \
             Dynamic files are ephemeral and have no oplog records. \
             Callers must handle dynamic files without querying the persistence layer.",
            id.entry_type()
        );

        let mut records = self
            .query_records(id)
            .await
            .map_err(error_utils::to_tinyfs_error)?;

        let record_count = records.len();
        debug!("list_file_versions found {record_count} records for node {id}");

        // Sort records by version number (which should match timestamp order anyway)
        records.sort_by_key(|record| record.version);

        // Hide versions superseded by a collapse merge row: a merged
        // FilePhysicalSeries row covers the closed version range
        // [collapsed_from, collapsed_through], so rows inside that range are no
        // longer independently readable. Rows of other entry types carry no
        // range and are left unchanged.
        let live: std::collections::HashSet<i64> = crate::schema::live_series_entries(&records)
            .into_iter()
            .map(|r| r.version)
            .collect();

        let version_infos = records
            .into_iter()
            .filter(|record| live.contains(&record.version))
            .map(|record| {
                // Use the actual database version number, not a re-enumerated logical version
                let version = record.version as u64;

                // For large files, size represents the ORIGINAL content size (before chunking)
                // The actual parquet file on disk will be different due to compression
                // but DataFusion needs to know the reconstructed size
                let size = if record.is_large_file() {
                    record.size.unwrap_or(0)
                } else {
                    record.content.as_ref().map(|c| c.len() as i64).unwrap_or(0)
                };

                // Extract extended metadata for series files. Both TablePhysical
                // series (is_series_file) and FilePhysicalSeries (e.g. jsonlogs
                // journals) record per-version min/max_event_time via
                // new_file_series; expose it so bounded consumers (the cached
                // ListingTable prune) can skip versions outside the hot window.
                let extended_metadata = if record.file_type.is_series_file()
                    || record.file_type == EntryType::FilePhysicalSeries
                {
                    let mut metadata = HashMap::new();
                    if let (Some(min_time), Some(max_time)) =
                        (record.min_event_time, record.max_event_time)
                    {
                        _ = metadata.insert("min_event_time".to_string(), min_time.to_string());
                        _ = metadata.insert("max_event_time".to_string(), max_time.to_string());
                    }
                    if let Some(attrs) = &record.extended_attributes {
                        _ = metadata.insert("extended_attributes".to_string(), attrs.clone());
                    }
                    Some(metadata)
                } else {
                    None
                };

                FileVersionInfo {
                    version,
                    timestamp: record.timestamp,
                    size: size as u64, // Cast back to u64 for tinyfs interface
                    blake3: record.blake3.clone(),
                    entry_type: record.file_type,
                    extended_metadata,
                }
            })
            .collect();

        Ok(version_infos)
    }

    async fn read_file_version(&self, id: FileID, version: u64) -> TinyFSResult<Vec<u8>> {
        // OPTIMIZATION: Query for specific version instead of fetching all versions
        // Query for specific version only
        let sql = format!(
            "SELECT * FROM delta_table WHERE part_id = '{}' AND node_id = '{}' AND version = {} LIMIT 1",
            id.part_id(),
            id.node_id(),
            version
        );

        let records = match self.session_context.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let mut records = Vec::new();
                    for batch in batches {
                        let batch_records: Vec<OplogEntry> = serde_arrow::from_record_batch(&batch)
                            .map_other_context("Failed to deserialize record")?;
                        records.extend(batch_records);
                    }
                    records
                }
                Err(e) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Query execution failed: {}",
                        e
                    )));
                }
            },
            Err(e) => {
                return Err(tinyfs::Error::Other(format!("SQL parse failed: {}", e)));
            }
        };

        // Check pending records too (for uncommitted writes)
        let pending_record = self
            .records
            .iter()
            .find(|r| {
                r.part_id == id.part_id()
                    && r.node_id == id.node_id()
                    && r.version == version as i64
            })
            .cloned();

        let target_record = {
            pending_record
                .or_else(|| records.into_iter().next())
                .ok_or_else(|| {
                    tinyfs::Error::NotFound(PathBuf::from(format!(
                        "Version {version} of file {id} not found",
                    )))
                })?
        };

        // Load content based on file type
        if target_record.is_large_file() {
            // Large file: read from external storage
            let sha256 = target_record.blake3.as_ref().ok_or_else(|| {
                tinyfs::Error::Other("Large file entry missing BLAKE3".to_string())
            })?;

            let large_file_path = crate::large_files::find_large_file_path(&self.path, sha256)
                .await
                .map_other_context("Error searching for large file")?
                .ok_or_else(|| {
                    tinyfs::Error::NotFound(PathBuf::from(format!(
                        "Large file with BLAKE3 {} not found",
                        sha256
                    )))
                })?;

            // Large files are stored as chunked parquet - use ParquetFileReader to reconstruct
            use tokio::io::AsyncReadExt;
            let mut reader = crate::large_files::ParquetFileReader::new(large_file_path)
                .await
                .map_other_context("Failed to open large file reader")?;

            let mut content = Vec::new();
            let _ = reader
                .read_to_end(&mut content)
                .await
                .map_other_context("Failed to read large file")?;

            Ok(content)
        } else {
            // Small file: content stored inline
            target_record
                .content
                .ok_or_else(|| tinyfs::Error::Other("Small file entry missing content".to_string()))
        }
    }

    async fn set_extended_attributes(
        &mut self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> TinyFSResult<()> {
        debug!("set_extended_attributes searching for node_id={id}");
        let records_count = self.records.len();
        debug!("set_extended_attributes current pending records count: {records_count}");

        // Find the pending record with max version for this node/part in current transaction
        let mut max_version = -1;
        let mut target_index = None;

        for (index, record) in self.records.iter().enumerate() {
            let record_node = &record.node_id;
            let record_part = &record.part_id;
            let record_version = record.version;
            debug!(
                "set_extended_attributes checking record[{index}]: node_id={record_node}, part_id={record_part}, version={record_version}"
            );

            if record.node_id == id.node_id() && record.part_id == id.part_id() {
                debug!(
                    "set_extended_attributes found matching record at index {index} with version {record_version}"
                );
                if record.version > max_version {
                    max_version = record.version;
                    target_index = Some(index);
                }
            }
        }

        let index = target_index.ok_or_else(|| {
            tinyfs::Error::Other(format!(
                "No pending version found for node {} - extended attributes can only be set on files created in the current transaction",
                id
            ))
        })?;

        let attrs_count = attributes.len();
        info!(
            "set_extended_attributes processing attributes for node {id} at index {index}, attrs_count: {attrs_count}"
        );

        // Store remaining attributes as JSON (if any)
        if !attributes.is_empty() {
            let attributes_json = serde_json::to_string(&attributes)
                .map_other_context("Failed to serialize extended attributes")?;
            self.records[index].extended_attributes = Some(attributes_json);
        }

        Ok(())
    }
}

/// Serialization utilities for Arrow IPC format
mod serialization {
    use super::*;
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

    /// Generic serialization function for Arrow IPC format
    pub fn serialize_to_arrow_ipc<T>(items: &[T]) -> Result<Vec<u8>, TLogFSError>
    where
        T: Clone + ForArrow + Serialize,
    {
        let batch = serde_arrow::to_record_batch(&T::for_arrow(), &items.to_vec())?;

        let mut buffer = Vec::new();
        let options = IpcWriteOptions::default();
        let mut writer =
            StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)
                .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;

        Ok(buffer)
    }

    /// Generic deserialization function for Arrow IPC format
    pub fn deserialize_from_arrow_ipc<T>(content: &[u8]) -> Result<Vec<T>, TLogFSError>
    where
        for<'de> T: Deserialize<'de>,
    {
        // Debug logging with size validation - catch corrupted IPC streams early
        let content_size = content.len();
        debug!(
            "deserialize_from_arrow_ipc: processing {} bytes of IPC data",
            content_size
        );

        // Fail fast on unreasonably large content for directory entries
        const MAX_REASONABLE_DIRECTORY_SIZE: usize = 10 * 1024 * 1024; // 10MB should be plenty for directory metadata
        if content_size > MAX_REASONABLE_DIRECTORY_SIZE {
            return Err(TLogFSError::ArrowMessage(format!(
                "[ALERT] CORRUPTED IPC DATA: Content size {} bytes exceeds reasonable limit of {} bytes. \
                This indicates corrupted Arrow IPC stream data. \
                Directory metadata should never be this large.",
                content_size, MAX_REASONABLE_DIRECTORY_SIZE
            )));
        }

        // Log first few bytes for corruption diagnosis
        if content_size >= 8 {
            let header_bytes = &content[0..8];
            debug!(
                "deserialize_from_arrow_ipc: IPC header bytes: {:02x?}",
                header_bytes
            );
        } else {
            warn!(
                "deserialize_from_arrow_ipc: Content too small ({} bytes) for valid IPC stream",
                content_size
            );
        }

        let mut reader =
            StreamReader::try_new(std::io::Cursor::new(content), None).map_err(|e| {
                TLogFSError::ArrowMessage(format!(
                    "Failed to create IPC StreamReader for {} bytes: {}. \
                This may indicate corrupted Arrow IPC stream data.",
                    content_size, e
                ))
            })?;

        if let Some(batch) = reader.next() {
            let batch = batch.map_err(|e| {
                TLogFSError::ArrowMessage(format!(
                    "Failed to read IPC batch from {} bytes: {}. \
                This may indicate corrupted Arrow IPC stream data.",
                    content_size, e
                ))
            })?;

            debug!(
                "deserialize_from_arrow_ipc: Successfully read batch with {} rows, {} columns",
                batch.num_rows(),
                batch.num_columns()
            );

            let entries: Vec<T> = serde_arrow::from_record_batch(&batch)?;
            Ok(entries)
        } else {
            debug!("deserialize_from_arrow_ipc: No batches found in IPC stream");
            Ok(Vec::new())
        }
    }

    // This function is no longer needed after Phase 2 abstraction consolidation
    // It was part of the old Record-based approach that caused double-nesting issues
}

/// Error handling utilities to reduce boilerplate
mod error_utils {
    use super::*;

    /// Convert TLogFSError to TinyFSResult
    pub fn to_tinyfs_error(e: TLogFSError) -> tinyfs::Error {
        // @@@ No
        tinyfs::Error::Other(e.to_string())
    }
}

/// Node creation utilities to reduce duplication
mod node_factory {
    use super::*;

    /// Create a file node
    pub fn create_file_node(id: FileID, state: State) -> Result<Node, tinyfs::Error> {
        let oplog_file = crate::file::OpLogFile::new(id, state);
        let file_handle = crate::file::OpLogFile::create_handle(oplog_file);
        Ok(Node::new(id, NodeType::File(file_handle)))
    }

    /// Create a directory node
    pub fn create_directory_node(id: FileID, state: State) -> Result<Node, tinyfs::Error> {
        debug!("create directory {id}");
        let oplog_dir = OpLogDirectory::new(id, state);
        let dir_handle = OpLogDirectory::create_handle(oplog_dir);
        Ok(Node::new(id, NodeType::Directory(dir_handle)))
    }

    /// Create a symlink node with the given target
    pub fn create_symlink_node(
        id: FileID,
        target: &Path,
        state: State,
    ) -> Result<Node, tinyfs::Error> {
        let oplog_symlink = OpLogSymlink::new(id, target.to_path_buf(), state);
        let symlink_handle = OpLogSymlink::create_handle(oplog_symlink);
        Ok(Node::new(id, NodeType::Symlink(symlink_handle)))
    }

    /// Create a node from an OplogEntry
    pub async fn create_node_from_oplog_entry(
        oplog_entry: &OplogEntry,
        id: FileID,
        state: State,
    ) -> Result<Node, tinyfs::Error> {
        // Handle static nodes (traditional TLogFS nodes)
        match oplog_entry.file_type {
            EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic => {
                assert!(id.entry_type().is_dynamic());
                assert!(oplog_entry.factory.is_some());

                let factory_type = oplog_entry.factory.as_ref().expect("factory");
                return create_dynamic_node_from_oplog_entry(oplog_entry, id, state, factory_type)
                    .await;
            }
            EntryType::FilePhysicalVersion
            | EntryType::FilePhysicalSeries
            | EntryType::TablePhysicalVersion
            | EntryType::TablePhysicalSeries => {
                let oplog_file = crate::file::OpLogFile::new(id, state);
                let file_handle = crate::file::OpLogFile::create_handle(oplog_file);
                Ok(Node::new(id, NodeType::File(file_handle)))
            }
            EntryType::DirectoryPhysical => {
                let oplog_dir = OpLogDirectory::new(id, state);
                let dir_handle = OpLogDirectory::create_handle(oplog_dir);
                Ok(Node::new(id, NodeType::Directory(dir_handle)))
            }
            EntryType::Symlink => {
                let oplog_symlink = OpLogSymlink::new_from_persistence(id, state);
                let symlink_handle = OpLogSymlink::create_handle(oplog_symlink);
                Ok(Node::new(id, NodeType::Symlink(symlink_handle)))
            }
        }
    }

    /// Create a dynamic node from an OplogEntry with factory type
    async fn create_dynamic_node_from_oplog_entry(
        oplog_entry: &OplogEntry,
        // @@@ Unclear about this, used to pass a name, need the parent ??
        // @@@ OH parent_id has parent identity, maybe, below node_id.to_string()
        id: FileID,
        state: State,
        factory_type: &str,
    ) -> Result<Node, tinyfs::Error> {
        // Note: Node caching is now handled by CachingPersistence decorator in tinyfs
        // No need for manual caching here

        // Get verified configuration from the oplog entry
        // For ALL dynamic nodes, config is stored as-is in content field (original YAML bytes)
        let config_content = oplog_entry
            .verified_content_required()
            .map_err(error_utils::to_tinyfs_error)?;

        // Create context with all template variables (vars, export, and any other keys)
        let mut context = FactoryContext::new(state.as_provider_context(), id);

        // For cross-pond imports: if this node belongs to a foreign pond,
        // set the effective_root so factory path resolution stays within
        // the imported tree. Without this, absolute paths like
        // /sensors/station_a would resolve from the consumer's root.
        //
        // We construct a synthetic NodePath for the foreign root rather
        // than loading it via state.load_node() — that would deadlock
        // because we are called from within the inner lock.
        if id.pond_id() != state.pond_uuid() {
            let foreign_root_id = FileID::root_for(id.pond_id());
            let root_node = create_directory_node(foreign_root_id, state.clone())?;
            let root_np = tinyfs::NodePath {
                node: root_node,
                path: "/".into(),
            };
            context = context.with_effective_root(root_np);
        }

        debug!(
            "[SEARCH] create_dynamic_node_from_oplog_entry: factory='{}', entry_type={:?}, config_len={}",
            factory_type,
            oplog_entry.file_type,
            config_content.len()
        );

        // Use context-aware factory registry to create the appropriate node type
        let node_type = match oplog_entry.file_type {
            EntryType::DirectoryDynamic => {
                debug!(
                    "[SEARCH] Calling FactoryRegistry::create_directory for '{}'",
                    factory_type
                );
                let dir_handle = FactoryRegistry::create_directory(
                    factory_type,
                    config_content,
                    context.clone(),
                )
                .map_err(|e| {
                    debug!("[ERR] FactoryRegistry::create_directory failed: {}", e);
                    e
                })?;
                debug!("[OK] FactoryRegistry::create_directory succeeded");
                NodeType::Directory(dir_handle)
            }
            EntryType::FileDynamic | EntryType::TableDynamic => {
                // Check if this is an executable factory
                if let Some(factory) = FactoryRegistry::get_factory(factory_type) {
                    if factory.create_file.is_some() {
                        // File factory - call create_file
                        debug!(
                            "[SEARCH] Calling FactoryRegistry::create_file for '{}'",
                            factory_type
                        );
                        let file_handle = FactoryRegistry::create_file(
                            factory_type,
                            config_content,
                            context.clone(),
                        )
                        .await
                        .map_err(|e| {
                            debug!("[ERR] FactoryRegistry::create_file failed: {}", e);
                            e
                        })?;
                        debug!("[OK] FactoryRegistry::create_file succeeded");
                        NodeType::File(file_handle)
                    } else {
                        // Executable factory - config IS the file content
                        debug!(
                            "[SEARCH] Executable factory '{}' - using config as file content",
                            factory_type
                        );
                        let config_file = provider::ConfigFile::new(config_content.to_vec());
                        NodeType::File(config_file.create_handle())
                    }
                } else {
                    return Err(tinyfs::Error::Other(format!(
                        "Unknown factory: {}",
                        factory_type
                    )));
                }
            }
            _ => {
                // Unknown entry type - shouldn't happen
                let config_file = provider::ConfigFile::new(config_content.to_vec());
                let file_handle = config_file.create_handle();
                NodeType::File(file_handle)
            }
        };

        let node = Node::new(id, node_type);

        // Note: Node caching is now handled by CachingPersistence decorator in tinyfs
        // The node will be automatically cached when returned

        Ok(node)
    }
}

#[cfg(test)]
mod series_bounds_tests {
    use super::series_version_retained;
    use tinyfs::SeriesReadBounds;

    // Version number is irrelevant to the event-time predicate; use a fixed one.
    const V: i64 = 1;

    #[test]
    fn no_bound_retains_all_versions() {
        // NONE reads the full series regardless of recorded bounds.
        assert!(series_version_retained(Some(0), V, &SeriesReadBounds::NONE));
        assert!(series_version_retained(
            Some(i64::MAX),
            V,
            &SeriesReadBounds::NONE
        ));
        assert!(series_version_retained(None, V, &SeriesReadBounds::NONE));
    }

    #[test]
    fn event_time_bound_prunes_versions_strictly_below() {
        let b = SeriesReadBounds::from_event_time_lo(1_000);
        // Below the bound: pruned.
        assert!(!series_version_retained(Some(999), V, &b));
        // At the bound: retained (inclusive).
        assert!(series_version_retained(Some(1_000), V, &b));
        // Above the bound: retained.
        assert!(series_version_retained(Some(1_001), V, &b));
    }

    #[test]
    fn null_event_time_versions_are_always_retained() {
        // Critical invariant: a version without a recorded max_event_time must
        // never be pruned by the event-time predicate, or windowed reads would
        // silently drop data.
        assert!(series_version_retained(
            None,
            V,
            &SeriesReadBounds::from_event_time_lo(1_000)
        ));
        assert!(series_version_retained(
            None,
            V,
            &SeriesReadBounds::from_event_time_lo(i64::MAX)
        ));
    }

    #[test]
    fn version_watermark_is_exclusive() {
        let b = SeriesReadBounds::from_version_gt(5);
        // At or below the watermark: pruned (already folded).
        assert!(!series_version_retained(Some(0), 4, &b));
        assert!(!series_version_retained(Some(0), 5, &b));
        // Above the watermark: retained (unseen), even with NULL event time.
        assert!(series_version_retained(Some(0), 6, &b));
        assert!(series_version_retained(None, 6, &b));
    }

    #[test]
    fn bounds_combine_with_and() {
        // Both an event-time floor and a version watermark: a version must
        // satisfy BOTH to be retained.
        let b = SeriesReadBounds {
            event_time_lo: Some(1_000),
            version_gt: Some(5),
        };
        // New enough version but too-old event time -> pruned.
        assert!(!series_version_retained(Some(999), 6, &b));
        // Recent event time but already-folded version -> pruned.
        assert!(!series_version_retained(Some(2_000), 5, &b));
        // Satisfies both -> retained.
        assert!(series_version_retained(Some(2_000), 6, &b));
        // NULL event time is retained by the event-time predicate, so version
        // gate alone decides.
        assert!(series_version_retained(None, 6, &b));
        assert!(!series_version_retained(None, 5, &b));
    }
}

#[cfg(test)]
mod collapse_window_tests {
    use super::{COLLAPSE_FANOUT, choose_collapse_window};
    use crate::schema::OplogEntry;
    use provider::size_tier::size_class;

    /// A live row of the given byte size; only `size` affects window choice.
    fn row(size: i64) -> OplogEntry {
        let mut e = OplogEntry::new_small_file(
            tinyfs::FileID::new_in_partition(
                tinyfs::FileID::root_for(tinyfs::local_pond_uuid()).part_id(),
                tinyfs::EntryType::FilePhysicalSeries,
                tinyfs::local_pond_uuid(),
            ),
            0,
            1,
            Vec::new(),
            0,
        );
        e.size = Some(size);
        e
    }

    fn window(sizes: &[i64], max_live: usize) -> Option<std::ops::Range<usize>> {
        let rows: Vec<OplogEntry> = sizes.iter().map(|s| row(*s)).collect();
        let live: Vec<&OplogEntry> = rows.iter().collect();
        choose_collapse_window(&live, max_live)
    }

    #[test]
    fn fewer_than_two_live_rows_is_a_noop() {
        assert_eq!(window(&[], 1), None);
        assert_eq!(window(&[100], 1), None);
    }

    #[test]
    fn size_classes_are_powers_of_the_fanout() {
        assert_eq!(size_class(0), 0);
        assert_eq!(size_class(COLLAPSE_FANOUT as u64 - 1), 0);
        assert_eq!(size_class(COLLAPSE_FANOUT as u64), 1);
        assert_eq!(size_class((COLLAPSE_FANOUT * COLLAPSE_FANOUT) as u64), 2);
    }

    /// The tiering rule: the oldest run of FANOUT same-class neighbours wins,
    /// even when an older, larger run sits in front of it.
    #[test]
    fn prefers_the_oldest_same_class_group() {
        // One big run, then ten small same-class rows.
        let mut sizes = vec![1_000_000];
        sizes.extend(std::iter::repeat_n(5, COLLAPSE_FANOUT));
        assert_eq!(window(&sizes, usize::MAX), Some(1..1 + COLLAPSE_FANOUT));
    }

    /// Without a same-class group and within `max_live`, nothing is merged:
    /// rewriting bytes to no purpose is worse than a few extra segments.
    #[test]
    fn ragged_classes_within_the_bound_are_left_alone() {
        let sizes: Vec<i64> = (0..6).map(|i| 10i64.pow(i)).collect();
        assert_eq!(window(&sizes, 100), None);
    }

    /// The backstop must NOT anchor at index 0.
    ///
    /// After any previous collapse the oldest row is the large accumulated run,
    /// so merging from index 0 rewrites it to absorb a few small rows -- the
    /// `O(N^2)` behaviour size classes exist to prevent. Callers pass the same
    /// number as both the candidacy threshold and `max_live`, so this path is
    /// reached far more often than "drifted past the bound" suggests.
    #[test]
    fn backstop_merges_the_cheapest_window_not_the_oldest() {
        // A huge accumulated run, then rows whose classes are deliberately
        // ragged so no same-class group of FANOUT exists.
        let mut sizes = vec![100_000_000];
        for i in 0..(COLLAPSE_FANOUT * 2) {
            sizes.push(10i64.pow((i % 5) as u32 + 1));
        }
        let chosen = window(&sizes, 2).expect("backstop must fire past the bound");
        assert_ne!(
            chosen.start, 0,
            "must not rewrite the large accumulated run"
        );
        assert_eq!(chosen.len(), COLLAPSE_FANOUT);

        // And it really is the minimum-cost window.
        let cheapest = (0..=sizes.len() - COLLAPSE_FANOUT)
            .min_by_key(|&s| sizes[s..s + COLLAPSE_FANOUT].iter().sum::<i64>())
            .unwrap();
        assert_eq!(chosen.start, cheapest);
    }

    /// Ties resolve to the oldest window, so a uniform series still compacts
    /// front to back rather than wandering.
    #[test]
    fn equal_cost_windows_resolve_to_the_oldest() {
        let sizes: Vec<i64> = std::iter::repeat_n(7, COLLAPSE_FANOUT + 3).collect();
        // All one class, so the same-class rule fires first -- also at 0.
        assert_eq!(window(&sizes, 2), Some(0..COLLAPSE_FANOUT));
    }

    /// Fewer live rows than the fanout still collapses under the backstop,
    /// merging everything available.
    #[test]
    fn backstop_handles_a_series_shorter_than_the_fanout() {
        let sizes = vec![1, 1_000, 1_000_000];
        assert_eq!(window(&sizes, 1), Some(0..3));
    }
}
