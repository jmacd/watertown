// SPDX-License-Identifier: Apache-2.0

//! Control table: lifecycle records and settings.
//!
//! The control table is a Delta Lake table peer of the data store.  It
//! holds two kinds of payloads in the same row schema:
//!
//! 1. **Lifecycle records**: one row per state transition of a
//!    transaction (Begin, DataCommitted, Failed, Completed,
//!    PostPushPending/Started/Completed/Failed).
//! 2. **Settings**: key/value configuration written by `Steward::config_set`.
//!    Stored as a `Setting` record kind with the key in `txn_id` and
//!    the value in `metadata_json`.  Latest write per key wins.
//!
//! The schema is intentionally narrow.  Ad-hoc fields go through
//! `metadata_json`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray, StringViewArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use chrono::Utc;
use datafusion::execution::context::SessionContext;
use deltalake::DeltaTable;
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use deltalake::protocol::SaveMode;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use super::error::{Result, StewardError};

/// Lifecycle record kinds.  Stored as a string in the control table for
/// readability and forward-compatibility (new variants don't break old
/// readers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    /// Transaction has been allocated a `txn_seq` but no data is committed yet.
    Begin,
    /// A data write or compact transaction committed successfully.
    DataCommitted,
    /// Transaction failed before producing data.
    Failed,
    /// A read transaction or no-op write transaction completed.
    Completed,
    /// A post-commit push has been scheduled.
    PostPushPending,
    /// Push started.
    PostPushStarted,
    /// Push completed successfully.
    PostPushCompleted,
    /// Push failed.
    PostPushFailed,
    /// A configuration key/value setting (latest write wins).
    Setting,
}

impl RecordKind {
    /// Stable serialized name.
    pub fn as_str(&self) -> &'static str {
        match self {
            RecordKind::Begin => "begin",
            RecordKind::DataCommitted => "data_committed",
            RecordKind::Failed => "failed",
            RecordKind::Completed => "completed",
            RecordKind::PostPushPending => "post_push_pending",
            RecordKind::PostPushStarted => "post_push_started",
            RecordKind::PostPushCompleted => "post_push_completed",
            RecordKind::PostPushFailed => "post_push_failed",
            RecordKind::Setting => "setting",
        }
    }
}

/// A transaction is either a normal write or a compaction.  Recorded on
/// `DataCommitted` records to disambiguate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitKind {
    /// User data write.
    Write,
    /// Delta-level compaction; logical content unchanged.
    Compact,
}

impl CommitKind {
    /// Stable serialized name.
    pub fn as_str(&self) -> &'static str {
        match self {
            CommitKind::Write => "write",
            CommitKind::Compact => "compact",
        }
    }
}

/// One control-table row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlRecord {
    /// Pond identity that owns this record.  For local lifecycle and
    /// PostPush records, this is the local pond's id.  For records
    /// mirroring a foreign pond (i.e., a `DataCommitted` written by
    /// `apply_pulled_bundle`), this is the foreign pond's id.  For
    /// `Setting` records (per-replica config), this is the local pond
    /// instance's id.  Stored as the canonical lowercase-hyphenated
    /// UUID string.
    pub pond_id: Uuid,
    /// Lifecycle kind.
    pub record_kind: RecordKind,
    /// Transaction sequence (or `0` for [`RecordKind::Setting`]).
    /// Per-pond_id namespaced -- two different pond_ids may share a
    /// `txn_seq` value without collision.
    pub txn_seq: i64,
    /// Transaction id (UUID v4) for lifecycle records, or the setting
    /// key for `Setting` records.
    pub txn_id: String,
    /// Set on `DataCommitted`.
    pub commit_kind: Option<CommitKind>,
    /// `None` means root (first commit).  Set on `DataCommitted`.
    pub parent_seq: Option<i64>,
    /// Set on `DataCommitted`/`Failed`/`Completed`/`PostPush*`.
    pub duration_ms: Option<i64>,
    /// UTC microseconds since unix epoch.
    pub ts_micros: i64,
    /// Free-form metadata.  Defaults to `"{}"`.
    pub metadata_json: String,
}

/// JSON form of the `metadata_json` payload on `DataCommitted` records.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DataCommittedMetadata {
    /// The data store's Delta Lake version at the moment this commit
    /// was recorded.  `remote-push` reads this to locate the
    /// commit-log entry whose Add/Remove actions describe the bundle.
    /// Older records (written before this field existed) deserialize
    /// to `0`; treat that as "unknown".
    #[serde(default)]
    pub data_delta_version: i64,
    /// Hex BLAKE3 of this commit's root directory tree (the SPACE root).
    /// `None` on records written before the content-graph spine existed,
    /// and on commits that do not stamp a spine (compaction, factory
    /// sub-transactions, control rebuilds).  See
    /// `docs/content-addressed-pond-design.md` Section 5.3.
    #[serde(default)]
    pub root_tree_hash: Option<String>,
    /// Hex BLAKE3 of the previous commit on this pond's linear chain, or
    /// `None` for the genesis commit or an unstamped record.
    #[serde(default)]
    pub parent_commit_hash: Option<String>,
    /// Hex BLAKE3 of this commit object (root_tree_hash + parent + provenance);
    /// the TIME-log leaf identity.  `None` on unstamped records.
    #[serde(default)]
    pub commit_hash: Option<String>,
    /// Hex of the encoded commit object bytes via `Commit::encode`, so the
    /// commit object can be reproduced verbatim for content-addressed push
    /// without recomputing its non-deterministic provenance such as commit time.
    /// `None` on unstamped records.
    #[serde(default)]
    pub commit_object: Option<String>,
}

/// SQL table name under which the control table is registered with the
/// DataFusion `SessionContext` returned by [`ControlTable::session_ctx`].
/// External callers building ad-hoc SQL against the control table should
/// reference this constant rather than hard-coding `"control"`.
pub const TABLE_NAME: &str = "control";

const COL_POND_ID: &str = "pond_id";
const COL_RECORD_KIND: &str = "record_kind";
const COL_TXN_SEQ: &str = "txn_seq";
const COL_TXN_ID: &str = "txn_id";
const COL_COMMIT_KIND: &str = "commit_kind";
const COL_PARENT_SEQ: &str = "parent_seq";
const COL_DURATION_MS: &str = "duration_ms";
const COL_TS_MICROS: &str = "ts_micros";
const COL_METADATA_JSON: &str = "metadata_json";
const COL_HAS_COMMIT_KIND: &str = "has_commit_kind";
const COL_HAS_PARENT_SEQ: &str = "has_parent_seq";
const COL_HAS_DURATION_MS: &str = "has_duration_ms";

fn arrow_schema() -> Arc<ArrowSchema> {
    // We avoid nullable Int64 columns for simplicity by carrying
    // explicit `has_*` boolean columns alongside; a `0` in `parent_seq`
    // when `has_parent_seq=false` is meaningless.
    Arc::new(ArrowSchema::new(vec![
        Field::new(COL_POND_ID, DataType::Utf8, false),
        Field::new(COL_RECORD_KIND, DataType::Utf8, false),
        Field::new(COL_TXN_SEQ, DataType::Int64, false),
        Field::new(COL_TXN_ID, DataType::Utf8, false),
        Field::new(COL_COMMIT_KIND, DataType::Utf8, false),
        Field::new(COL_HAS_COMMIT_KIND, DataType::Boolean, false),
        Field::new(COL_PARENT_SEQ, DataType::Int64, false),
        Field::new(COL_HAS_PARENT_SEQ, DataType::Boolean, false),
        Field::new(COL_DURATION_MS, DataType::Int64, false),
        Field::new(COL_HAS_DURATION_MS, DataType::Boolean, false),
        Field::new(COL_TS_MICROS, DataType::Int64, false),
        Field::new(COL_METADATA_JSON, DataType::Utf8, false),
    ]))
}

fn delta_columns() -> Vec<DeltaStructField> {
    vec![
        DeltaStructField::new(
            COL_POND_ID,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        DeltaStructField::new(
            COL_RECORD_KIND,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        DeltaStructField::new(
            COL_TXN_SEQ,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            COL_TXN_ID,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        DeltaStructField::new(
            COL_COMMIT_KIND,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        DeltaStructField::new(
            COL_HAS_COMMIT_KIND,
            DeltaDataType::Primitive(PrimitiveType::Boolean),
            false,
        ),
        DeltaStructField::new(
            COL_PARENT_SEQ,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            COL_HAS_PARENT_SEQ,
            DeltaDataType::Primitive(PrimitiveType::Boolean),
            false,
        ),
        DeltaStructField::new(
            COL_DURATION_MS,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            COL_HAS_DURATION_MS,
            DeltaDataType::Primitive(PrimitiveType::Boolean),
            false,
        ),
        DeltaStructField::new(
            COL_TS_MICROS,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            COL_METADATA_JSON,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
    ]
}

/// The control table.
pub struct ControlTable {
    path: PathBuf,
    table: DeltaTable,
    session_ctx: Arc<SessionContext>,
}

impl ControlTable {
    /// Create a new control table at `path` (errors if exists).
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;

        let url = url_from_path(&path)?;
        let table = DeltaTable::try_from_url(url)
            .await?
            .create()
            .with_columns(delta_columns())
            .with_save_mode(SaveMode::ErrorIfExists)
            .await?;

        let session_ctx = build_session_ctx(&table)?;
        Ok(Self {
            path,
            table,
            session_ctx,
        })
    }

    /// Open an existing control table.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let url = url_from_path(&path)?;
        let table = deltalake::open_table(url).await?;
        let session_ctx = build_session_ctx(&table)?;
        Ok(Self {
            path,
            table,
            session_ctx,
        })
    }

    /// On-disk path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Shared DataFusion session context with the control table registered
    /// under [`TABLE_NAME`].  External wrappers performing ad-hoc SQL
    /// reuse this so they see the same Delta version after every write.
    pub fn session_ctx(&self) -> &Arc<SessionContext> {
        &self.session_ctx
    }

    /// Borrow the underlying [`DeltaTable`] (e.g. to inspect commit info).
    pub fn delta_table(&self) -> &DeltaTable {
        &self.table
    }

    /// Replace the underlying [`DeltaTable`] handle and refresh the
    /// session context registration.  Used by external maintenance flows
    /// (compact/vacuum) that produced a new table handle through other
    /// means.
    pub fn set_delta_table(&mut self, table: DeltaTable) -> Result<()> {
        self.table = table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok(())
    }

    /// Run delta-rs `optimize(Compact)` on the control table to merge
    /// small parquets into larger ones.  Logical content is unchanged
    /// (control records are append-only audit history).  Returns
    /// `(num_files_added, num_files_removed)` so callers can detect
    /// no-ops.
    pub async fn compact(&mut self) -> Result<(u64, u64)> {
        use deltalake::operations::optimize::OptimizeType;
        let (new_table, metrics) = self
            .table
            .clone()
            .optimize()
            .with_type(OptimizeType::Compact)
            .await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok((metrics.num_files_added, metrics.num_files_removed))
    }

    /// Run delta-rs vacuum on the control table to physically reclaim
    /// parquets that are no longer referenced by any active commit
    /// (e.g., the small parquets tombstoned by `compact`).  Uses
    /// retention=0 / enforce=false, matching the prototype defaults
    /// used elsewhere.  Returns the count of files reclaimed.
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

    /// Delete all records belonging to `pond_id` from the control
    /// table.  Used by the per-pond_id `restart_from_compact`
    /// recovery pathway when a foreign import is being re-bootstrapped.
    ///
    /// Setting records (per-pond instance config) are NOT deleted by
    /// this call -- they are written under the local pond_id and live
    /// in the pond's own seq space, untouched by foreign-pond drops.
    pub async fn drop_pond_records(&mut self, pond_id: Uuid) -> Result<()> {
        let predicate = format!("{} = '{}'", COL_POND_ID, pond_id);
        let (new_table, _metrics) = self
            .table
            .clone()
            .delete()
            .with_predicate(predicate)
            .await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok(())
    }

    /// Delete the append-only lifecycle history for `pond_id` at or below
    /// `horizon_seq`, leaving `Setting` records (watermarks, store_id,
    /// mount, mode) intact.  This is the only way to actually SHRINK the
    /// control table: checkpoint/vacuum/compact merge or re-list files but
    /// never remove logical rows, so the log otherwise grows without bound.
    ///
    /// Callers are responsible for choosing a SAFE `horizon_seq`: every
    /// transaction at or below it must already be replicated to all push
    /// remotes and be old enough that no retained-history reader needs it.
    /// The deleted rows leave tombstoned parquet files behind until a
    /// subsequent vacuum reclaims them.  Returns the number of rows deleted.
    pub async fn prune_below(&mut self, pond_id: Uuid, horizon_seq: i64) -> Result<usize> {
        let predicate = format!(
            "{pid} = '{p}' AND {rk} != '{setting}' AND {ts} <= {h}",
            pid = COL_POND_ID,
            p = pond_id,
            rk = COL_RECORD_KIND,
            setting = RecordKind::Setting.as_str(),
            ts = COL_TXN_SEQ,
            h = horizon_seq,
        );
        let (new_table, metrics) = self
            .table
            .clone()
            .delete()
            .with_predicate(predicate)
            .await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok(metrics.num_deleted_rows)
    }

    /// Append a single record to the control table.
    pub async fn write_record(&mut self, record: ControlRecord) -> Result<()> {
        self.write_records(vec![record]).await
    }

    /// Append multiple records to the control table in a SINGLE Delta commit.
    ///
    /// All records become one parquet add-file and one table version, rather
    /// than one commit (and one tiny add-file) per record.  This is the
    /// append-only audit log's batching path: callers that emit several
    /// records back-to-back with no intervening data work (for example a
    /// transaction's terminal `DataCommitted` + `Completed`, or the fan-out
    /// of `PostPushPending` rows) should use this to avoid amplifying the
    /// control table's add-file count.  An empty slice is a no-op.
    pub async fn write_records(&mut self, records: Vec<ControlRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let schema = arrow_schema();

        let mut pond_ids = Vec::with_capacity(records.len());
        let mut record_kinds = Vec::with_capacity(records.len());
        let mut txn_seqs = Vec::with_capacity(records.len());
        let mut txn_ids = Vec::with_capacity(records.len());
        let mut commit_kinds = Vec::with_capacity(records.len());
        let mut has_commit = Vec::with_capacity(records.len());
        let mut parent_seqs = Vec::with_capacity(records.len());
        let mut has_parent = Vec::with_capacity(records.len());
        let mut durations = Vec::with_capacity(records.len());
        let mut has_duration = Vec::with_capacity(records.len());
        let mut timestamps = Vec::with_capacity(records.len());
        let mut metadatas = Vec::with_capacity(records.len());

        for record in &records {
            pond_ids.push(record.pond_id.to_string());
            record_kinds.push(record.record_kind.as_str().to_string());
            txn_seqs.push(record.txn_seq);
            txn_ids.push(record.txn_id.clone());
            commit_kinds.push(
                record
                    .commit_kind
                    .map(|k| k.as_str().to_string())
                    .unwrap_or_default(),
            );
            has_commit.push(record.commit_kind.is_some());
            parent_seqs.push(record.parent_seq.unwrap_or(0));
            has_parent.push(record.parent_seq.is_some());
            durations.push(record.duration_ms.unwrap_or(0));
            has_duration.push(record.duration_ms.is_some());
            timestamps.push(record.ts_micros);
            metadatas.push(record.metadata_json.clone());
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(pond_ids)),
                Arc::new(StringArray::from(record_kinds)),
                Arc::new(Int64Array::from(txn_seqs)),
                Arc::new(StringArray::from(txn_ids)),
                Arc::new(StringArray::from(commit_kinds)),
                Arc::new(BooleanArray::from(has_commit)),
                Arc::new(Int64Array::from(parent_seqs)),
                Arc::new(BooleanArray::from(has_parent)),
                Arc::new(Int64Array::from(durations)),
                Arc::new(BooleanArray::from(has_duration)),
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(StringArray::from(metadatas)),
            ],
        )?;

        let new_table = self.table.clone().write(vec![batch]).await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok(())
    }

    /// All records, ordered by `(txn_seq ASC, ts_micros ASC)`.  Returns
    /// records from ALL pond_ids (cross-pond inspection).  Use
    /// [`Self::all_records_for`] to scope to a single pond.
    pub async fn all_records(&self) -> Result<Vec<ControlRecord>> {
        self.all_records_inner(None).await
    }

    /// All records for `pond_id`, ordered by `(txn_seq ASC, ts_micros ASC)`.
    pub async fn all_records_for(&self, pond_id: Uuid) -> Result<Vec<ControlRecord>> {
        self.all_records_inner(Some(pond_id)).await
    }

    async fn all_records_inner(&self, pond_id: Option<Uuid>) -> Result<Vec<ControlRecord>> {
        let where_clause = pond_id
            .map(|p| format!("WHERE {pid} = '{p}' ", pid = COL_POND_ID, p = p))
            .unwrap_or_default();
        let sql = format!(
            "SELECT {pid}, {rk}, {ts}, {tid}, {ck}, {hck}, {ps}, {hps}, {dm}, {hdm}, {tsm}, {md} \
             FROM {table} \
             {where_clause}\
             ORDER BY {ts} ASC, {tsm} ASC",
            pid = COL_POND_ID,
            rk = COL_RECORD_KIND,
            ts = COL_TXN_SEQ,
            tid = COL_TXN_ID,
            ck = COL_COMMIT_KIND,
            hck = COL_HAS_COMMIT_KIND,
            ps = COL_PARENT_SEQ,
            hps = COL_HAS_PARENT_SEQ,
            dm = COL_DURATION_MS,
            hdm = COL_HAS_DURATION_MS,
            tsm = COL_TS_MICROS,
            md = COL_METADATA_JSON,
            table = TABLE_NAME,
            where_clause = where_clause,
        );
        let batches = self.session_ctx.sql(&sql).await?.collect().await?;
        let mut out = Vec::new();
        for batch in batches {
            decode_records(&batch, &mut out)?;
        }
        Ok(out)
    }

    /// Records of a specific kind for `pond_id`, oldest-first.
    pub async fn records_of_kind(
        &self,
        pond_id: Uuid,
        kind: RecordKind,
    ) -> Result<Vec<ControlRecord>> {
        let all = self.all_records_for(pond_id).await?;
        Ok(all.into_iter().filter(|r| r.record_kind == kind).collect())
    }

    /// Largest `txn_seq` recorded in any kind of record (Begin or
    /// later) for `pond_id`.  `0` if `pond_id` has no records.
    pub async fn last_txn_seq(&self, pond_id: Uuid) -> Result<i64> {
        let sql = format!(
            "SELECT MAX({ts}) AS m FROM {table} \
             WHERE {pid} = '{p}' AND {rk} != '{setting}'",
            ts = COL_TXN_SEQ,
            table = TABLE_NAME,
            pid = COL_POND_ID,
            p = pond_id,
            rk = COL_RECORD_KIND,
            setting = RecordKind::Setting.as_str(),
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
                .ok_or_else(|| StewardError::Invariant("last_txn_seq: column not Int64".into()))?;
            if !arr.is_null(0) {
                max = max.max(arr.value(0));
            }
        }
        Ok(max)
    }

    /// Largest `txn_seq` for which a `DataCommitted` record exists for
    /// `pond_id`.
    pub async fn last_committed_seq(&self, pond_id: Uuid) -> Result<i64> {
        let sql = format!(
            "SELECT MAX({ts}) AS m FROM {table} \
             WHERE {pid} = '{p}' AND {rk} = '{kind}'",
            ts = COL_TXN_SEQ,
            table = TABLE_NAME,
            pid = COL_POND_ID,
            p = pond_id,
            rk = COL_RECORD_KIND,
            kind = RecordKind::DataCommitted.as_str(),
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
                .ok_or_else(|| StewardError::Invariant("last_committed_seq: not Int64".into()))?;
            if !arr.is_null(0) {
                max = max.max(arr.value(0));
            }
        }
        Ok(max)
    }

    /// Records that look like they may belong to incomplete transactions
    /// for `pond_id`: `Begin` rows with no matching terminal record
    /// (`DataCommitted`, `Failed`, or `Completed`) for the same `txn_seq`.
    pub async fn incomplete_transactions(&self, pond_id: Uuid) -> Result<Vec<ControlRecord>> {
        let all = self.all_records_for(pond_id).await?;
        let mut by_seq: HashMap<i64, Vec<&ControlRecord>> = HashMap::new();
        for r in &all {
            if matches!(r.record_kind, RecordKind::Setting) {
                continue;
            }
            by_seq.entry(r.txn_seq).or_default().push(r);
        }
        let mut out = Vec::new();
        for (_seq, recs) in by_seq {
            let has_terminal = recs.iter().any(|r| {
                matches!(
                    r.record_kind,
                    RecordKind::DataCommitted | RecordKind::Failed | RecordKind::Completed
                )
            });
            if !has_terminal
                && let Some(begin) = recs.iter().find(|r| r.record_kind == RecordKind::Begin)
            {
                out.push((*begin).clone());
            }
        }
        out.sort_by_key(|r| r.txn_seq);
        Ok(out)
    }

    /// Resolve the data store's Delta Lake version at
    /// `(pond_id, txn_seq)`.  Returns the `metadata.data_delta_version`
    /// field of the `DataCommitted` record at that seq, or `None` if
    /// no such commit exists.
    pub async fn data_delta_version_at(&self, pond_id: Uuid, txn_seq: i64) -> Result<Option<i64>> {
        let all = self.all_records_for(pond_id).await?;
        let rec = all
            .iter()
            .find(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == txn_seq);
        let Some(rec) = rec else {
            return Ok(None);
        };
        let meta: DataCommittedMetadata = serde_json::from_str(&rec.metadata_json)?;
        Ok(Some(meta.data_delta_version))
    }

    /// Resolve the content-graph commit hash recorded at `(pond_id, txn_seq)`.
    /// Returns the `metadata.commit_hash` of the `DataCommitted` record at that
    /// seq, or `None` if no such commit exists or it did not stamp a spine.
    pub async fn commit_hash_at(&self, pond_id: Uuid, txn_seq: i64) -> Result<Option<String>> {
        let all = self.all_records_for(pond_id).await?;
        let rec = all
            .iter()
            .find(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == txn_seq);
        let Some(rec) = rec else {
            return Ok(None);
        };
        let meta: DataCommittedMetadata = serde_json::from_str(&rec.metadata_json)?;
        Ok(meta.commit_hash)
    }

    /// Resolve the content-graph root tree hash recorded at `(pond_id, txn_seq)`.
    /// Returns the `metadata.root_tree_hash` of the `DataCommitted` record at
    /// that seq, or `None` if no such commit exists or it did not stamp a spine.
    pub async fn root_tree_hash_at(&self, pond_id: Uuid, txn_seq: i64) -> Result<Option<String>> {
        let all = self.all_records_for(pond_id).await?;
        let rec = all
            .iter()
            .find(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == txn_seq);
        let Some(rec) = rec else {
            return Ok(None);
        };
        let meta: DataCommittedMetadata = serde_json::from_str(&rec.metadata_json)?;
        Ok(meta.root_tree_hash)
    }

    /// Resolve the content-graph parent commit hash recorded at
    /// `(pond_id, txn_seq)`.  Returns the `metadata.parent_commit_hash` of the
    /// `DataCommitted` record at that seq, or `None` if no such commit exists,
    /// it did not stamp a spine, or it is the genesis commit.
    pub async fn parent_commit_hash_at(
        &self,
        pond_id: Uuid,
        txn_seq: i64,
    ) -> Result<Option<String>> {
        let all = self.all_records_for(pond_id).await?;
        let rec = all
            .iter()
            .find(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == txn_seq);
        let Some(rec) = rec else {
            return Ok(None);
        };
        let meta: DataCommittedMetadata = serde_json::from_str(&rec.metadata_json)?;
        Ok(meta.parent_commit_hash)
    }

    /// The authoritative transparency-log leaf sequence for `pond_id`: the
    /// encoded commit-object bytes (hex) of every spine-bearing `DataCommitted`
    /// record, in commit order (`txn_seq` ascending).  Leaf `i` is the `i`-th
    /// spine-bearing commit; records that did not stamp a spine are skipped, so
    /// this is a dense sequence with no gaps.
    ///
    /// This is the source of truth the transparency-log tile export is
    /// reconciled against (design Decision D5): the export leaf count must never
    /// exceed this sequence's length, and any shortfall is replayed from it.
    pub async fn commit_objects_in_order(&self, pond_id: Uuid) -> Result<Vec<String>> {
        let all = self.all_records_for(pond_id).await?;
        let mut out = Vec::new();
        for rec in all {
            if rec.record_kind != RecordKind::DataCommitted {
                continue;
            }
            let meta: DataCommittedMetadata = serde_json::from_str(&rec.metadata_json)?;
            if let Some(obj) = meta.commit_object {
                out.push(obj);
            }
        }
        Ok(out)
    }

    /// Resolve the encoded commit object bytes recorded at `(pond_id, txn_seq)`.
    /// Returns the `metadata.commit_object` hex of the `DataCommitted` record at
    /// that seq, or `None` if no such commit exists or it did not stamp a spine.
    pub async fn commit_object_at(&self, pond_id: Uuid, txn_seq: i64) -> Result<Option<String>> {
        let all = self.all_records_for(pond_id).await?;
        let rec = all
            .iter()
            .find(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == txn_seq);
        let Some(rec) = rec else {
            return Ok(None);
        };
        let meta: DataCommittedMetadata = serde_json::from_str(&rec.metadata_json)?;
        Ok(meta.commit_object)
    }

    /// The highest `txn_seq` for `pond_id` that stamped a content-graph commit
    /// spine (its `DataCommitted` metadata carries a `commit_hash`).  This is
    /// the pond's content tip: the last content-changing commit.
    ///
    /// Content-preserving transactions such as compaction (`Ship::compact`)
    /// record a `DataCommitted` row with no spine, so their seq is skipped
    /// here.  Push and pull resolve the tip through this method precisely so a
    /// compaction seq never mis-resolves as the tip (design: "Compaction adds
    /// no commit").  Returns `None` if the pond has no spine-bearing commit.
    pub async fn latest_spine_seq(&self, pond_id: Uuid) -> Result<Option<i64>> {
        let all = self.all_records_for(pond_id).await?;
        let mut best: Option<i64> = None;
        for rec in &all {
            if rec.record_kind != RecordKind::DataCommitted {
                continue;
            }
            let meta: DataCommittedMetadata = serde_json::from_str(&rec.metadata_json)?;
            if meta.commit_hash.is_some() && best.is_none_or(|b| rec.txn_seq > b) {
                best = Some(rec.txn_seq);
            }
        }
        Ok(best)
    }

    /// Set a configuration key/value scoped to `pond_id`.  Records a
    /// [`RecordKind::Setting`] row.  Settings are pond-instance state
    /// (e.g., `last_pulled_seq:<remote>`); the Steward always uses its
    /// local pond_id when calling this.
    pub async fn config_set(&mut self, pond_id: Uuid, key: &str, value: &str) -> Result<()> {
        self.config_set_after(pond_id, key, value, i64::MIN).await
    }

    /// Set a configuration value with a timestamp strictly newer than
    /// `after_micros`, preserving deterministic latest-write-wins ordering for
    /// callers that serialize read/compare/write updates.
    pub async fn config_set_after(
        &mut self,
        pond_id: Uuid,
        key: &str,
        value: &str,
        after_micros: i64,
    ) -> Result<()> {
        let mut meta = HashMap::new();
        let _ = meta.insert("v".to_string(), value.to_string());
        let metadata_json = serde_json::to_string(&meta)?;
        let ts_micros = Utc::now()
            .timestamp_micros()
            .max(after_micros.saturating_add(1));
        let rec = ControlRecord {
            pond_id,
            record_kind: RecordKind::Setting,
            txn_seq: 0,
            txn_id: key.to_string(),
            commit_kind: None,
            parent_seq: None,
            duration_ms: None,
            ts_micros,
            metadata_json,
        };
        self.write_record(rec).await
    }

    /// Read the current value of `(pond_id, key)`, or `None` if never set.
    pub async fn config_get(&self, pond_id: Uuid, key: &str) -> Result<Option<String>> {
        Ok(self
            .config_get_with_timestamp(pond_id, key)
            .await?
            .map(|(_, value)| value))
    }

    /// Read the latest timestamped value of one setting.
    pub async fn config_get_with_timestamp(
        &self,
        pond_id: Uuid,
        key: &str,
    ) -> Result<Option<(i64, String)>> {
        let all = self.all_records_for(pond_id).await?;
        let mut latest = None;
        for rec in all
            .iter()
            .filter(|record| record.record_kind == RecordKind::Setting && record.txn_id == key)
        {
            let map: HashMap<String, String> = serde_json::from_str(&rec.metadata_json)?;
            if let Some(value) = map.get("v")
                && latest
                    .as_ref()
                    .is_none_or(|(timestamp, _)| rec.ts_micros >= *timestamp)
            {
                latest = Some((rec.ts_micros, value.clone()));
            }
        }
        Ok(latest)
    }

    /// Read all current settings for `pond_id` (latest-write-wins per key).
    pub async fn config_list(&self, pond_id: Uuid) -> Result<HashMap<String, String>> {
        // Latest-write-wins per key.  Walk all Setting records and
        // overlay in `ts_micros` order.
        let all = self.all_records_for(pond_id).await?;
        let mut settings: Vec<(i64, String, String)> = Vec::new();
        for rec in all.iter().filter(|r| r.record_kind == RecordKind::Setting) {
            let map: HashMap<String, String> = serde_json::from_str(&rec.metadata_json)?;
            if let Some(v) = map.get("v") {
                settings.push((rec.ts_micros, rec.txn_id.clone(), v.clone()));
            }
        }
        settings.sort_by_key(|(ts, _, _)| *ts);
        let mut out = HashMap::new();
        for (_ts, k, v) in settings {
            let _ = out.insert(k, v);
        }
        Ok(out)
    }
}

fn url_from_path(path: &Path) -> Result<Url> {
    Url::from_directory_path(path)
        .or_else(|_| Url::from_file_path(path))
        .map_err(|_| StewardError::InvalidPath(path.display().to_string()))
}

fn build_session_ctx(table: &DeltaTable) -> Result<Arc<SessionContext>> {
    let ctx = SessionContext::new();
    _ = ctx.register_table(TABLE_NAME, Arc::new(table.clone()))?;
    Ok(Arc::new(ctx))
}

fn read_string(col: &dyn Array, row: usize) -> Result<String> {
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(arr.value(row).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
        return Ok(arr.value(row).to_string());
    }
    Err(StewardError::Invariant(format!(
        "expected Utf8 or Utf8View, got {:?}",
        col.data_type()
    )))
}

fn read_i64(col: &dyn Array, row: usize) -> Result<i64> {
    let arr = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| StewardError::Invariant("expected Int64".into()))?;
    Ok(arr.value(row))
}

fn read_bool(col: &dyn Array, row: usize) -> Result<bool> {
    let arr = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| StewardError::Invariant("expected Boolean".into()))?;
    Ok(arr.value(row))
}

fn parse_record_kind(s: &str) -> Result<RecordKind> {
    Ok(match s {
        "begin" => RecordKind::Begin,
        "data_committed" => RecordKind::DataCommitted,
        "failed" => RecordKind::Failed,
        "completed" => RecordKind::Completed,
        "post_push_pending" => RecordKind::PostPushPending,
        "post_push_started" => RecordKind::PostPushStarted,
        "post_push_completed" => RecordKind::PostPushCompleted,
        "post_push_failed" => RecordKind::PostPushFailed,
        "setting" => RecordKind::Setting,
        other => {
            return Err(StewardError::Invariant(format!(
                "unknown record_kind {:?}",
                other
            )));
        }
    })
}

fn parse_commit_kind(s: &str) -> Result<CommitKind> {
    Ok(match s {
        "write" => CommitKind::Write,
        "compact" => CommitKind::Compact,
        other => {
            return Err(StewardError::Invariant(format!(
                "unknown commit_kind {:?}",
                other
            )));
        }
    })
}

fn decode_records(batch: &RecordBatch, out: &mut Vec<ControlRecord>) -> Result<()> {
    let cols: HashMap<String, usize> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().to_string(), i))
        .collect();

    let need = |name: &str| -> Result<usize> {
        cols.get(name)
            .copied()
            .ok_or_else(|| StewardError::Invariant(format!("missing column {}", name)))
    };

    let i_pid = need(COL_POND_ID)?;
    let i_rk = need(COL_RECORD_KIND)?;
    let i_ts = need(COL_TXN_SEQ)?;
    let i_tid = need(COL_TXN_ID)?;
    let i_ck = need(COL_COMMIT_KIND)?;
    let i_hck = need(COL_HAS_COMMIT_KIND)?;
    let i_ps = need(COL_PARENT_SEQ)?;
    let i_hps = need(COL_HAS_PARENT_SEQ)?;
    let i_dm = need(COL_DURATION_MS)?;
    let i_hdm = need(COL_HAS_DURATION_MS)?;
    let i_tsm = need(COL_TS_MICROS)?;
    let i_md = need(COL_METADATA_JSON)?;

    for row in 0..batch.num_rows() {
        let pond_id_str = read_string(batch.column(i_pid).as_ref(), row)?;
        let pond_id = Uuid::parse_str(&pond_id_str).map_err(|e| {
            StewardError::Invariant(format!(
                "control table pond_id `{}` is not a valid UUID: {}",
                pond_id_str, e
            ))
        })?;
        let rk = parse_record_kind(&read_string(batch.column(i_rk).as_ref(), row)?)?;
        let txn_seq = read_i64(batch.column(i_ts).as_ref(), row)?;
        let txn_id = read_string(batch.column(i_tid).as_ref(), row)?;
        let has_ck = read_bool(batch.column(i_hck).as_ref(), row)?;
        let commit_kind = if has_ck {
            Some(parse_commit_kind(&read_string(
                batch.column(i_ck).as_ref(),
                row,
            )?)?)
        } else {
            None
        };
        let has_ps = read_bool(batch.column(i_hps).as_ref(), row)?;
        let parent_seq = if has_ps {
            Some(read_i64(batch.column(i_ps).as_ref(), row)?)
        } else {
            None
        };
        let has_dm = read_bool(batch.column(i_hdm).as_ref(), row)?;
        let duration_ms = if has_dm {
            Some(read_i64(batch.column(i_dm).as_ref(), row)?)
        } else {
            None
        };
        let ts_micros = read_i64(batch.column(i_tsm).as_ref(), row)?;
        let metadata_json = read_string(batch.column(i_md).as_ref(), row)?;
        out.push(ControlRecord {
            pond_id,
            record_kind: rk,
            txn_seq,
            txn_id,
            commit_kind,
            parent_seq,
            duration_ms,
            ts_micros,
            metadata_json,
        });
    }
    Ok(())
}

/// Build a `txn_id` (UUID v4 hex) for a new transaction.
pub fn new_txn_id() -> String {
    Uuid::new_v4().simple().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_and_delta_schemas_agree() {
        let arrow = arrow_schema();
        let delta = delta_columns();
        assert_eq!(arrow.fields().len(), delta.len());
        for (af, df) in arrow.fields().iter().zip(delta.iter()) {
            assert_eq!(af.name(), df.name());
        }
    }

    #[test]
    fn record_kind_roundtrip() {
        for k in [
            RecordKind::Begin,
            RecordKind::DataCommitted,
            RecordKind::Failed,
            RecordKind::Completed,
            RecordKind::PostPushPending,
            RecordKind::PostPushStarted,
            RecordKind::PostPushCompleted,
            RecordKind::PostPushFailed,
            RecordKind::Setting,
        ] {
            assert_eq!(parse_record_kind(k.as_str()).unwrap(), k);
        }
    }
}
