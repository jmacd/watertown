// SPDX-License-Identifier: Apache-2.0

//! The [`Store`] type: a Delta-Lake-backed key-value store.
//!
//! See `lib.rs` for the public crate documentation.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Int64Array, RecordBatch, StringArray, StringViewArray,
};
use chrono::Utc;
use datafusion::execution::context::SessionContext;
use deltalake::DeltaTable;
use deltalake::kernel::schema::partitions::{PartitionFilter, PartitionValue};
use deltalake::operations::optimize::OptimizeType;
use deltalake::protocol::SaveMode;
use log::{debug, warn};
use url::Url;
use uuid::Uuid;

use crate::checksum;
use crate::error::{Result, StoreError};
use crate::schema;

/// A single operation applied to the store within an [`Store::apply_batch`] call.
#[derive(Debug, Clone)]
pub enum Op {
    /// Set `(partition, item_key)` to `value`.
    Put {
        /// Partition key.
        partition: String,
        /// Item key within the partition.
        key: String,
        /// Value bytes.
        value: Vec<u8>,
    },
    /// Mark `(partition, item_key)` as deleted (tombstone).
    Delete {
        /// Partition key.
        partition: String,
        /// Item key within the partition.
        key: String,
    },
}

/// A Delta-Lake-backed key-value store.
///
/// Construction:
///
/// - [`Store::create`] makes a fresh table at `path` (errors if path exists).
/// - [`Store::open`] opens an existing table.
///
/// Mutation goes through [`Store::apply_batch`], which writes one Delta
/// commit containing all the supplied operations.  Convenience wrappers
/// [`Store::put`] and [`Store::delete`] auto-allocate the next `txn_seq`
/// and apply a single-op batch.
///
/// Reads ([`Store::get`], [`Store::list`], [`Store::partitions`]) execute
/// DataFusion queries against the current table state.
pub struct Store {
    path: PathBuf,
    table: DeltaTable,
    /// Re-created and re-registered each time the table version changes.
    session_ctx: Arc<SessionContext>,
}

const TABLE_NAME: &str = "source";

/// Write a checkpoint every this many versions, matching the Delta default
/// and `steward::maintenance`, which does the same for local tables.
const CHECKPOINT_INTERVAL: u64 = 10;

/// Merge accumulated small files every this many commits.  Compaction rewrites
/// data, so it is not free; this trades an occasional rewrite against a lookup
/// cost that would otherwise grow with every write.
const COMPACT_INTERVAL: u64 = 25;

impl Store {
    /// Create a new store at `path`.  The directory is created if missing.
    /// Errors if a Delta table already exists there.
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;

        let url = url_from_path(&path)?;
        debug!("creating store at {}", url);

        let table = DeltaTable::try_from_url(url)
            .await?
            .create()
            .with_columns(schema::delta_columns())
            .with_partition_columns(schema::partition_columns())
            .with_save_mode(SaveMode::ErrorIfExists)
            .await?;

        let session_ctx = build_session_ctx(&table)?;

        Ok(Self {
            path,
            table,
            session_ctx,
        })
    }

    /// Open an existing store at `path`.  Errors if no Delta table exists
    /// there.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let url = url_from_path(&path)?;
        debug!("opening store at {}", url);

        let table = deltalake::open_table(url).await?;
        let session_ctx = build_session_ctx(&table)?;

        Ok(Self {
            path,
            table,
            session_ctx,
        })
    }

    /// Create a fresh store at `url` with the given `storage_options` (e.g.,
    /// S3 credentials).  Errors if a Delta table already exists there.
    /// For S3 URLs, `deltalake_aws::register_handlers(None)` must have run
    /// once at process startup.
    pub async fn create_at_url(
        url: &str,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let parsed = Url::parse(url).map_err(|_| StoreError::InvalidPath(url.to_string()))?;
        debug!("creating store at {}", parsed);
        if parsed.scheme() == "file"
            && let Ok(p) = parsed.to_file_path()
        {
            std::fs::create_dir_all(&p)?;
        }
        let table = DeltaTable::try_from_url_with_storage_options(parsed, storage_options)
            .await?
            .create()
            .with_columns(schema::delta_columns())
            .with_partition_columns(schema::partition_columns())
            .with_save_mode(SaveMode::ErrorIfExists)
            .await?;
        let session_ctx = build_session_ctx(&table)?;
        Ok(Self {
            path: PathBuf::from(url),
            table,
            session_ctx,
        })
    }

    /// Open an existing store at `url` with `storage_options`.
    pub async fn open_at_url(
        url: &str,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let parsed = Url::parse(url).map_err(|_| StoreError::InvalidPath(url.to_string()))?;
        debug!("opening store at {}", parsed);
        let table = deltalake::open_table_with_storage_options(parsed, storage_options).await?;
        let session_ctx = build_session_ctx(&table)?;
        Ok(Self {
            path: PathBuf::from(url),
            table,
            session_ctx,
        })
    }

    /// On-disk path the store was opened/created from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The backing object store rooted at the Delta table directory.  Used to
    /// read and write sibling content under prefixes outside the Delta log
    /// (e.g. a content-addressed large-blob store), keeping multi-gigabyte
    /// values out of the Delta row table.
    pub fn object_store(&self) -> Arc<dyn object_store::ObjectStore> {
        self.table.object_store()
    }

    /// Current Delta table version (incremented on every successful
    /// commit, including writes that resulted in zero new rows).
    pub fn delta_version(&self) -> i64 {
        self.table.version().unwrap_or(0)
    }

    /// The `txn_seq` recorded for `pond_id` as a Delta app-transaction, or
    /// `None` for a store last written before commits carried one.
    ///
    /// This lives in the commit log and is folded into every checkpoint, so
    /// reading it opens no data files.
    pub async fn app_transaction_version(&self, pond_id: Uuid) -> Result<Option<i64>> {
        Ok(self
            .table
            .snapshot()?
            .transaction_version(self.table.log_store().as_ref(), pond_id.to_string())
            .await?)
    }

    /// The largest `txn_seq` ever committed to this store for `pond_id`,
    /// or `0` if `pond_id` has never written here.  Use this to allocate
    /// the next `txn_seq` for that pond when calling [`Store::apply_batch`].
    ///
    /// Read from the Delta app-transaction the commit records, which lives in
    /// the log and is folded into each checkpoint, so answering costs a
    /// bounded number of requests no matter how much the pond has written.
    ///
    /// The obvious `SELECT MAX(txn_seq)` instead reads a column from every
    /// data file the pond owns.  That made the price of a push grow with the
    /// number of pushes before it: a traced push of water-prod spent 584 of
    /// its 1198 requests doing exactly this, to learn one number.  It survives
    /// only as a fallback for remotes written before commits carried the
    /// app-transaction, and each such store leaves the fallback behind the
    /// first time it is written to.
    pub async fn last_txn_seq(&self, pond_id: Uuid) -> Result<i64> {
        if let Some(version) = self.app_transaction_version(pond_id).await? {
            return Ok(version);
        }

        let sql = format!(
            "SELECT MAX({seq}) AS m FROM {table} WHERE {pid} = '{pid_v}'",
            seq = schema::col::TXN_SEQ,
            table = TABLE_NAME,
            pid = schema::col::POND_ID,
            pid_v = pond_id,
        );
        let batches = self.session_ctx.sql(&sql).await?.collect().await?;
        let mut max: i64 = 0;
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    StoreError::Invariant("last_txn_seq: column 0 is not Int64".into())
                })?;
            if !arr.is_null(0) {
                max = max.max(arr.value(0));
            }
        }
        Ok(max)
    }

    /// Apply a batch of operations as one Delta commit.  All operations
    /// share the supplied `pond_id`, `txn_seq`, and `ts_micros` -- one
    /// batch represents one transaction by one pond.
    ///
    /// Within a single batch, multiple operations against the same
    /// `(partition, key)` are coalesced -- the LAST operation in `ops`
    /// wins.  This matches the semantics of a transaction that overwrites
    /// the same key multiple times.
    pub async fn apply_batch(
        &mut self,
        pond_id: Uuid,
        txn_seq: i64,
        ts_micros: i64,
        ops: Vec<Op>,
    ) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        // Coalesce by (partition, key) keeping the last-seen op.
        let mut by_key: std::collections::HashMap<(String, String), Op> =
            std::collections::HashMap::new();
        for op in ops {
            let k = match &op {
                Op::Put { partition, key, .. } => (partition.clone(), key.clone()),
                Op::Delete { partition, key } => (partition.clone(), key.clone()),
            };
            _ = by_key.insert(k, op);
        }

        let n = by_key.len();
        let mut pond_ids = Vec::with_capacity(n);
        let mut partition_keys = Vec::with_capacity(n);
        let mut item_keys = Vec::with_capacity(n);
        let mut deleted = Vec::with_capacity(n);
        let mut values: Vec<Vec<u8>> = Vec::with_capacity(n);
        let mut blake3s: Vec<Vec<u8>> = Vec::with_capacity(n);

        let pond_id_str = pond_id.to_string();

        for op in by_key.into_values() {
            match op {
                Op::Put {
                    partition,
                    key,
                    value,
                } => {
                    let hash = blake3::hash(&value);
                    pond_ids.push(pond_id_str.clone());
                    partition_keys.push(partition);
                    item_keys.push(key);
                    deleted.push(false);
                    blake3s.push(hash.as_bytes().to_vec());
                    values.push(value);
                }
                Op::Delete { partition, key } => {
                    // Tombstone: empty value, BLAKE3 of empty bytes.
                    let empty: Vec<u8> = Vec::new();
                    let hash = blake3::hash(&empty);
                    pond_ids.push(pond_id_str.clone());
                    partition_keys.push(partition);
                    item_keys.push(key);
                    deleted.push(true);
                    blake3s.push(hash.as_bytes().to_vec());
                    values.push(empty);
                }
            }
        }

        let txn_seqs = vec![txn_seq; n];
        let timestamps = vec![ts_micros; n];

        let schema = schema::arrow_schema();
        let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
        let blake3_refs: Vec<&[u8]> = blake3s.iter().map(|v| v.as_slice()).collect();
        let pond_id_refs: Vec<&str> = pond_ids.iter().map(|s| s.as_str()).collect();
        let partition_refs: Vec<&str> = partition_keys.iter().map(|s| s.as_str()).collect();
        let item_refs: Vec<&str> = item_keys.iter().map(|s| s.as_str()).collect();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(pond_id_refs)),
                Arc::new(StringArray::from(partition_refs)),
                Arc::new(StringArray::from(item_refs)),
                Arc::new(Int64Array::from(txn_seqs)),
                Arc::new(BooleanArray::from(deleted)),
                Arc::new(BinaryArray::from(value_refs)),
                Arc::new(BinaryArray::from(blake3_refs)),
                Arc::new(Int64Array::from(timestamps)),
            ],
        )?;

        // Record the pond's `txn_seq` as a Delta app-transaction so the next
        // allocation can read it from the log instead of scanning every data
        // file the pond owns.  See [`Store::last_txn_seq`].
        let new_table = self
            .table
            .clone()
            .write(vec![batch])
            .with_commit_properties(
                deltalake::kernel::transaction::CommitProperties::default()
                    .with_application_transaction(deltalake::kernel::Transaction::new(
                        pond_id.to_string(),
                        txn_seq,
                    )),
            )
            .await?;
        debug!(
            "store apply_batch: pond {} version {:?} -> {:?}, n_rows={}",
            pond_id,
            self.table.version(),
            new_table.version(),
            n
        );
        self.table = new_table;

        // Re-build session context against the new table version.
        self.session_ctx = build_session_ctx(&self.table)?;
        self.checkpoint_if_due().await;
        self.compact_if_due(pond_id).await;

        Ok(())
    }

    /// Write a checkpoint when the version reaches an interval boundary.
    ///
    /// Without this a remote's `_delta_log/_last_checkpoint` never appears,
    /// and every reader replays the entire commit history to learn the
    /// current state.  A traced push of a production pond spent 609 of its
    /// 1198 requests re-reading `_delta_log` for exactly this reason, and the
    /// count grows with the table's age rather than with the work being done
    /// -- a bill for how long the pond has existed.  A checkpoint collapses
    /// that replay into one read.
    ///
    /// Best-effort: a checkpoint is an optimisation, and failing to write one
    /// must not fail the commit that earned it.
    async fn checkpoint_if_due(&mut self) {
        let version = self.table.version().unwrap_or(0);
        if version <= 0 || !(version as u64).is_multiple_of(CHECKPOINT_INTERVAL) {
            return;
        }
        match deltalake::checkpoints::create_checkpoint(&self.table, None).await {
            Ok(()) => debug!("store checkpoint written at version {version}"),
            Err(e) => warn!("store checkpoint failed at version {version}: {e}"),
        }
    }

    /// Merge this pond's small parquet files on an interval.
    ///
    /// Every commit lands one small file per partition it touches, and a point
    /// lookup opens every file in the partition, so left alone the price of
    /// reading one key grows with the number of writes before it.  Delta's
    /// compaction merges only files below the target size, so a merged file is
    /// rewritten a bounded number of times as it grows rather than on every
    /// pass.
    ///
    /// Scoped to `pond_id`, which is a partition column, so no other pond's
    /// files are opened, and required not to change any partition's logical
    /// content.
    ///
    /// Best-effort: merging files is an optimisation, and failing to merge
    /// them must not fail the commit that earned the attempt.
    async fn compact_if_due(&mut self, pond_id: Uuid) {
        let version = self.delta_version();
        if version <= 0 || !u64::try_from(version).is_ok_and(|v| v.is_multiple_of(COMPACT_INTERVAL))
        {
            return;
        }
        match self.compact(pond_id, None).await {
            Ok(m) => debug!(
                "store compact at version {version}: files +{}/-{}",
                m.num_files_added, m.num_files_removed
            ),
            Err(e) => warn!("store compact failed at version {version}: {e}"),
        }
    }

    /// Convenience: write a single Put using the next available `txn_seq`
    /// for `pond_id`.  Prefer [`Store::apply_batch`] when writing
    /// multiple operations.
    pub async fn put(
        &mut self,
        pond_id: Uuid,
        partition: &str,
        key: &str,
        value: Vec<u8>,
    ) -> Result<i64> {
        let txn_seq = self.last_txn_seq(pond_id).await? + 1;
        let ts = Utc::now().timestamp_micros();
        self.apply_batch(
            pond_id,
            txn_seq,
            ts,
            vec![Op::Put {
                partition: partition.to_string(),
                key: key.to_string(),
                value,
            }],
        )
        .await?;
        Ok(txn_seq)
    }

    /// Convenience: write a single Delete using the next available
    /// `txn_seq` for `pond_id`.
    pub async fn delete(&mut self, pond_id: Uuid, partition: &str, key: &str) -> Result<i64> {
        let txn_seq = self.last_txn_seq(pond_id).await? + 1;
        let ts = Utc::now().timestamp_micros();
        self.apply_batch(
            pond_id,
            txn_seq,
            ts,
            vec![Op::Delete {
                partition: partition.to_string(),
                key: key.to_string(),
            }],
        )
        .await?;
        Ok(txn_seq)
    }

    /// Read the current value of `(pond_id, partition, key)`, or `None`
    /// if the item does not exist or has been tombstoned.
    pub async fn get(&self, pond_id: Uuid, partition: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let sql = format!(
            "SELECT {value}, {deleted} FROM {table} \
             WHERE {pid} = '{pid_v}' AND {pk} = '{p}' AND {ik} = '{k}' \
             ORDER BY {seq} DESC LIMIT 1",
            value = schema::col::VALUE,
            deleted = schema::col::DELETED,
            table = TABLE_NAME,
            pid = schema::col::POND_ID,
            pid_v = pond_id,
            pk = schema::col::PARTITION_KEY,
            ik = schema::col::ITEM_KEY,
            seq = schema::col::TXN_SEQ,
            p = sql_escape(partition),
            k = sql_escape(key),
        );

        let batches = self.session_ctx.sql(&sql).await?.collect().await?;
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let value = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| StoreError::Invariant("get: value is not Binary".into()))?;
            let deleted = batch
                .column(1)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| StoreError::Invariant("get: deleted is not Boolean".into()))?;
            if deleted.value(0) {
                return Ok(None);
            }
            return Ok(Some(value.value(0).to_vec()));
        }
        Ok(None)
    }

    /// Return all live `(item_key, value)` pairs in `(pond_id, partition)`,
    /// sorted by `item_key`.
    pub async fn list(&self, pond_id: Uuid, partition: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let sql = format!(
            "SELECT {ik}, {value} \
             FROM ( \
                SELECT {ik}, {value}, {deleted}, \
                       ROW_NUMBER() OVER ( \
                         PARTITION BY {ik} ORDER BY {seq} DESC \
                       ) AS rn \
                FROM {table} \
                WHERE {pid} = '{pid_v}' AND {pk} = '{p}' \
             ) \
             WHERE rn = 1 AND NOT {deleted} \
             ORDER BY {ik}",
            ik = schema::col::ITEM_KEY,
            value = schema::col::VALUE,
            deleted = schema::col::DELETED,
            seq = schema::col::TXN_SEQ,
            table = TABLE_NAME,
            pid = schema::col::POND_ID,
            pid_v = pond_id,
            pk = schema::col::PARTITION_KEY,
            p = sql_escape(partition),
        );

        let batches = self.session_ctx.sql(&sql).await?.collect().await?;
        let mut out = Vec::new();
        for batch in batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| StoreError::Invariant("list: item_key is not Utf8".into()))?;
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| StoreError::Invariant("list: value is not Binary".into()))?;
            for i in 0..batch.num_rows() {
                out.push((keys.value(i).to_string(), values.value(i).to_vec()));
            }
        }
        Ok(out)
    }

    /// Return the set of partition keys that contain at least one row
    /// (live or tombstoned) for `pond_id`, sorted.
    pub async fn partitions(&self, pond_id: Uuid) -> Result<Vec<String>> {
        // CAST to Utf8 because Delta partition columns sometimes surface
        // as Dictionary, LargeUtf8, or Utf8View in the Arrow result.
        let sql = format!(
            "SELECT DISTINCT CAST({pk} AS VARCHAR) AS pk \
             FROM {table} WHERE {pid} = '{pid_v}' ORDER BY pk",
            pk = schema::col::PARTITION_KEY,
            table = TABLE_NAME,
            pid = schema::col::POND_ID,
            pid_v = pond_id,
        );
        let batches = self.session_ctx.sql(&sql).await?.collect().await?;
        let mut out = Vec::new();
        for batch in batches {
            collect_string_column(batch.column(0).as_ref(), &mut out)?;
        }
        Ok(out)
    }

    /// Return the set of `(pond_id, partition_key)` pairs in the store.
    /// Useful for cross-pond inspection (e.g., listing all distinct
    /// pond identities present in a multi-pond consumer).
    pub async fn all_partitions(&self) -> Result<Vec<(Uuid, String)>> {
        let sql = format!(
            "SELECT DISTINCT CAST({pid} AS VARCHAR) AS pid, \
                             CAST({pk} AS VARCHAR) AS pk \
             FROM {table} ORDER BY pid, pk",
            pid = schema::col::POND_ID,
            pk = schema::col::PARTITION_KEY,
            table = TABLE_NAME,
        );
        let batches = self.session_ctx.sql(&sql).await?.collect().await?;
        let mut out = Vec::new();
        for batch in batches {
            let mut pids: Vec<String> = Vec::new();
            let mut pks: Vec<String> = Vec::new();
            collect_string_column(batch.column(0).as_ref(), &mut pids)?;
            collect_string_column(batch.column(1).as_ref(), &mut pks)?;
            for (pid_str, pk) in pids.into_iter().zip(pks) {
                let pid = Uuid::parse_str(&pid_str).map_err(|e| {
                    StoreError::Invariant(format!(
                        "all_partitions: pond_id `{}` is not a valid UUID: {}",
                        pid_str, e
                    ))
                })?;
                out.push((pid, pk));
            }
        }
        Ok(out)
    }

    /// Return all live (post-tombstone) `(item_key, value_blake3)`
    /// pairs in `(pond_id, partition)`, sorted by `item_key`.
    ///
    /// This is the canonical input to a [`PartitionChecksum`]; callers
    /// generally use [`Store::compute_partition_checksum`] instead.
    pub async fn partition_leaves(
        &self,
        pond_id: Uuid,
        partition: &str,
    ) -> Result<Vec<(String, [u8; 32])>> {
        let sql = format!(
            "SELECT {ik}, {blake3} \
             FROM ( \
                SELECT {ik}, {blake3}, {deleted}, \
                       ROW_NUMBER() OVER ( \
                         PARTITION BY {ik} ORDER BY {seq} DESC \
                       ) AS rn \
                FROM {table} \
                WHERE {pid} = '{pid_v}' AND {pk} = '{p}' \
             ) \
             WHERE rn = 1 AND NOT {deleted} \
             ORDER BY {ik}",
            ik = schema::col::ITEM_KEY,
            blake3 = schema::col::VALUE_BLAKE3,
            deleted = schema::col::DELETED,
            seq = schema::col::TXN_SEQ,
            table = TABLE_NAME,
            pid = schema::col::POND_ID,
            pid_v = pond_id,
            pk = schema::col::PARTITION_KEY,
            p = sql_escape(partition),
        );
        let batches = self.session_ctx.sql(&sql).await?.collect().await?;
        let mut out: Vec<(String, [u8; 32])> = Vec::new();
        for batch in batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    StoreError::Invariant("partition_leaves: item_key not Utf8".into())
                })?;
            let blake3s = batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    StoreError::Invariant("partition_leaves: blake3 not Binary".into())
                })?;
            for i in 0..batch.num_rows() {
                let key = keys.value(i).to_string();
                let bytes = blake3s.value(i);
                let arr = checksum::array_from_slice(bytes).ok_or_else(|| {
                    StoreError::Invariant(format!(
                        "partition_leaves: value_blake3 has length {} (expected 32)",
                        bytes.len()
                    ))
                })?;
                out.push((key, arr));
            }
        }
        Ok(out)
    }

    /// Compute the per-partition content checksum using the supplied
    /// strategy.
    ///
    /// Convenience wrapper: reads [`Store::partition_leaves`] and feeds
    /// them through the strategy.
    pub async fn compute_partition_checksum<C>(
        &self,
        pond_id: Uuid,
        partition: &str,
        strategy: &C,
    ) -> Result<checksum::Checksum>
    where
        C: checksum::PartitionChecksum + ?Sized,
    {
        let leaves = self.partition_leaves(pond_id, partition).await?;
        let refs: Vec<checksum::Leaf<'_>> = leaves
            .iter()
            .map(|(k, h)| checksum::Leaf {
                key: k.as_str(),
                value_blake3: h,
            })
            .collect();
        Ok(strategy.compute(&refs))
    }

    /// Run Delta Lake's `optimize(Compact)` over the table for `pond_id`,
    /// optionally restricted to a single `partition_key` value via
    /// `filter`.
    ///
    /// Compaction merges small parquet files into bigger ones to reduce
    /// per-table overhead.  It must NOT change the logical content of
    /// any partition; the steward asserts this by snapshotting per-
    /// partition checksums before and after.
    ///
    /// Because `pond_id` is a Delta partition column, restricting compact
    /// to one pond_id is a hard guarantee that no foreign rows are
    /// touched -- foreign data files are never opened.
    ///
    /// Returns metrics describing how many files were added/removed.
    /// Both being zero means optimize found nothing to do (no rows for
    /// this pond_id, nonexistent filter, or already-optimal layout).
    pub async fn compact(&mut self, pond_id: Uuid, filter: Option<&str>) -> Result<CompactMetrics> {
        let mut filters: Vec<PartitionFilter> = vec![PartitionFilter {
            key: schema::col::POND_ID.to_string(),
            value: PartitionValue::Equal(pond_id.to_string()),
        }];
        if let Some(value) = filter {
            filters.push(PartitionFilter {
                key: schema::col::PARTITION_KEY.to_string(),
                value: PartitionValue::Equal(value.to_string()),
            });
        }

        let (new_table, metrics) = self
            .table
            .clone()
            .optimize()
            .with_type(OptimizeType::Compact)
            .with_filters(&filters)
            .await?;

        debug!(
            "store compact: pond {} filter={:?} version {:?} -> {:?} files +{}/-{}",
            pond_id,
            filter,
            self.table.version(),
            new_table.version(),
            metrics.num_files_added,
            metrics.num_files_removed,
        );

        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        self.checkpoint_if_due().await;

        Ok(CompactMetrics {
            num_files_added: metrics.num_files_added,
            num_files_removed: metrics.num_files_removed,
        })
    }

    /// Return the object-store paths of every parquet data file
    /// referenced by the current table version, sorted.  Useful for
    /// verifying that compaction with a partition filter only touched
    /// the requested partition.
    pub fn data_files(&self) -> Result<Vec<String>> {
        let mut out: Vec<String> = self.table.get_file_uris()?.collect();
        out.sort();
        Ok(out)
    }

    /// Read the Add and Remove file actions recorded in the Delta
    /// commit log entry for a specific `version`.  Returns
    /// `(adds, removes)`.  `Metadata`, `Protocol`, `CommitInfo`,
    /// `Cdc`, `Txn`, and `DomainMetadata` actions are filtered out --
    /// `remote-push` only cares about file deltas.
    ///
    /// `version` must be a real Delta version on this table; an out-of-
    /// range version returns [`StoreError::Invariant`].
    ///
    /// Paths in the returned `AddPath`/`RemovePath` are URI-decoded
    /// strings relative to the table root (delta-rs's deserializer
    /// already percent-decodes them; consumers can pass them straight
    /// into `object_store::path::Path::from`).
    pub async fn actions_at_version(
        &self,
        version: i64,
    ) -> Result<(Vec<AddPath>, Vec<RemovePath>)> {
        let bytes = self
            .table
            .log_store()
            .read_commit_entry(version)
            .await?
            .ok_or_else(|| {
                StoreError::Invariant(format!("no Delta commit log entry at version {}", version))
            })?;
        let actions = deltalake::logstore::get_actions(version, &bytes)?;
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for action in actions {
            match action {
                deltalake::kernel::Action::Add(a) => adds.push(AddPath {
                    path: a.path,
                    size: a.size,
                }),
                deltalake::kernel::Action::Remove(r) => removes.push(RemovePath { path: r.path }),
                _ => {}
            }
        }
        Ok((adds, removes))
    }

    /// Commit a set of explicit `Add` and `Remove` actions to the
    /// data store as a single Delta version.  Used by
    /// `Steward::apply_pulled_bundle` for `remote-pull`: the consumer
    /// receives parquet bytes from the remote and needs to register
    /// them with its local Delta table without going through the
    /// regular `apply_batch` path (which would generate fresh
    /// parquet files instead of mirroring the source's).
    ///
    /// `op` describes the operation for the commit log; pass
    /// `DeltaOperation::Write { mode: Append, .. }` for Write
    /// bundles or `DeltaOperation::Optimize { .. }` for Compact
    /// bundles.
    ///
    /// Returns the new Delta version on the data store after the
    /// commit.
    pub async fn commit_actions(
        &mut self,
        actions: Vec<deltalake::kernel::Action>,
        op: deltalake::protocol::DeltaOperation,
    ) -> Result<i64> {
        use deltalake::kernel::transaction::CommitBuilder;
        let snapshot = self.table.snapshot()?;
        let log_store = self.table.log_store();
        let _result = CommitBuilder::default()
            .with_actions(actions)
            .build(Some(snapshot), log_store, op)
            .await?;
        self.table.update_state().await?;
        self.session_ctx = build_session_ctx(&self.table)?;
        self.checkpoint_if_due().await;
        Ok(self.table.version().unwrap_or(0))
    }

    /// Run delta-rs vacuum on this store to reclaim disk space from
    /// parquet files that no longer have any active Add reference
    /// (e.g., merged-away files after compact, or files removed by
    /// pulled compact bundles).
    ///
    /// Uses retention period = 0 with `enforce_retention_duration =
    /// false` to make vacuum synchronous-friendly for the prototype.
    /// Production deployments should pick a real retention period
    /// (the Delta default is 7 days).
    ///
    /// Returns the count of files reclaimed.
    pub async fn vacuum(&mut self) -> Result<usize> {
        let (new_table, metrics) = self
            .table
            .clone()
            .vacuum()
            .with_retention_period(chrono::Duration::seconds(0))
            .with_enforce_retention_duration(false)
            .await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok(metrics.files_deleted.len())
    }

    /// Delete all rows belonging to `pond_id` from the data store.
    /// Used by the per-pond_id `restart_from_compact` recovery
    /// pathway when a foreign import has fallen behind the source's
    /// retention or has otherwise diverged.
    ///
    /// Because `pond_id` is a Delta partition column, this delete
    /// operates at file granularity: only files in
    /// `pond_id=<pond_id>/...` partitions are removed.  Other
    /// pond_ids' files are not touched.
    ///
    /// Returns the new Delta version after the delete commit.
    pub async fn drop_pond_data(&mut self, pond_id: Uuid) -> Result<i64> {
        let predicate = format!("{} = '{}'", schema::col::POND_ID, pond_id);
        let (new_table, _metrics) = self
            .table
            .clone()
            .delete()
            .with_predicate(predicate)
            .await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        self.checkpoint_if_due().await;
        Ok(self.table.version().unwrap_or(0))
    }
}

/// A file added by a Delta commit, as reported by
/// [`Store::actions_at_version`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPath {
    /// URI-decoded path relative to the table root.
    pub path: String,
    /// File size in bytes.
    pub size: i64,
}

/// A file removed by a Delta commit, as reported by
/// [`Store::actions_at_version`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovePath {
    /// URI-decoded path relative to the table root.
    pub path: String,
}

/// Metrics returned by [`Store::compact`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactMetrics {
    /// Number of new (merged) parquet files Delta wrote.
    pub num_files_added: u64,
    /// Number of small parquet files Delta marked Removed.
    pub num_files_removed: u64,
}

impl CompactMetrics {
    /// `true` iff Delta did not actually change the file set.
    pub fn is_noop(&self) -> bool {
        self.num_files_added == 0 && self.num_files_removed == 0
    }
}

fn url_from_path(path: &Path) -> Result<Url> {
    Url::from_directory_path(path)
        .or_else(|_| Url::from_file_path(path))
        .map_err(|_| StoreError::InvalidPath(path.display().to_string()))
}

fn build_session_ctx(table: &DeltaTable) -> Result<Arc<SessionContext>> {
    let ctx = SessionContext::new();
    _ = ctx.register_table(TABLE_NAME, Arc::new(table.clone()))?;
    Ok(Arc::new(ctx))
}

/// Quote a value for SQL string literal embedding.  Doubles single quotes.
/// (DataFusion's `sql()` API doesn't expose bind parameters; for the
/// prototype this is fine because partition/item keys are not user-
/// supplied attack vectors.)
fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

/// Append the contents of a string column to `out`, accepting both the
/// `Utf8` (`StringArray`) and `Utf8View` (`StringViewArray`) Arrow
/// representations that DataFusion may produce for `CAST AS VARCHAR`.
fn collect_string_column(col: &dyn Array, out: &mut Vec<String>) -> Result<()> {
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                out.push(arr.value(i).to_string());
            }
        }
        return Ok(());
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                out.push(arr.value(i).to_string());
            }
        }
        return Ok(());
    }
    Err(StoreError::Invariant(format!(
        "expected Utf8 or Utf8View column, got {:?}",
        col.data_type()
    )))
}
