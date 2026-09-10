// SPDX-License-Identifier: Apache-2.0

//! Dedicated bounded Delta table holding current native-v2 publication state.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, StringViewArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion::execution::context::SessionContext;
use deltalake::DeltaTable;
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use deltalake::protocol::SaveMode;
use url::Url;
use uuid::Uuid;

use crate::content::{NATIVE_FORMAT_V2, ObjectHash};
use crate::error::{Result, StoreError};

const TABLE_NAME: &str = "publication";
const PUBLICATION_PATH: &str = "_publication/";

mod column {
    pub const POND_ID: &str = "pond_id";
    pub const REF_NAME: &str = "ref_name";
    pub const FORMAT: &str = "format";
    pub const SNAPSHOT_TIP: &str = "snapshot_tip";
    pub const MANIFEST_ROOT: &str = "manifest_root";
    pub const PUBLICATION_RECORD: &str = "publication_record";
    pub const GENERATION: &str = "generation";
    pub const UPDATED_AT: &str = "updated_at";
}

/// One live active-row value for `(pond_id, ref_name)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationState {
    /// Source pond identity.
    pub pond_id: Uuid,
    /// Logical ref name.
    pub ref_name: String,
    /// Native format marker.
    pub format: String,
    /// Visible snapshot commit.
    pub snapshot_tip: ObjectHash,
    /// Persistent manifest-map root.
    pub manifest_root: ObjectHash,
    /// Immutable publication-record head.
    pub publication_record: ObjectHash,
    /// Monotonic ref generation, starting at one.
    pub generation: i64,
    /// Commit timestamp in microseconds since the Unix epoch.
    pub updated_at: i64,
}

impl PublicationState {
    /// Construct a native-v2 state row.
    pub fn new(
        pond_id: Uuid,
        ref_name: impl Into<String>,
        snapshot_tip: ObjectHash,
        manifest_root: ObjectHash,
        publication_record: ObjectHash,
        generation: i64,
        updated_at: i64,
    ) -> Result<Self> {
        let ref_name = ref_name.into();
        if ref_name.is_empty() {
            return Err(StoreError::Invariant(
                "publication ref name is empty".to_string(),
            ));
        }
        if generation <= 0 {
            return Err(StoreError::Invariant(format!(
                "publication generation must be positive, got {generation}"
            )));
        }
        Ok(Self {
            pond_id,
            ref_name,
            format: NATIVE_FORMAT_V2.to_string(),
            snapshot_tip,
            manifest_root,
            publication_record,
            generation,
            updated_at,
        })
    }
}

/// Expected prior active-row identity for compare-and-swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationExpectation {
    /// No row may exist.
    Missing,
    /// The current row must have exactly this generation and record head.
    Existing {
        /// Expected prior generation.
        generation: i64,
        /// Expected prior immutable record.
        publication_record: ObjectHash,
    },
}

/// Delta publication table rooted at `_publication/`.
pub struct PublicationTable {
    table: DeltaTable,
    context: Arc<SessionContext>,
}

impl PublicationTable {
    /// Create a new local publication table beneath `remote_root`.
    pub async fn create(remote_root: impl AsRef<Path>) -> Result<Self> {
        let root = remote_root.as_ref();
        std::fs::create_dir_all(root)?;
        let base = Url::from_directory_path(root)
            .or_else(|_| Url::from_file_path(root))
            .map_err(|_| StoreError::InvalidPath(root.display().to_string()))?;
        Self::create_at_url(base.as_str(), Default::default()).await
    }

    /// Open a local publication table beneath `remote_root`.
    pub async fn open(remote_root: impl AsRef<Path>) -> Result<Self> {
        let root = remote_root.as_ref();
        let base = Url::from_directory_path(root)
            .or_else(|_| Url::from_file_path(root))
            .map_err(|_| StoreError::InvalidPath(root.display().to_string()))?;
        Self::open_at_url(base.as_str(), Default::default()).await
    }

    /// Create the dedicated table beneath `remote_url`.
    pub async fn create_at_url(
        remote_url: &str,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let url = publication_url(remote_url)?;
        if url.scheme() == "file"
            && let Ok(path) = url.to_file_path()
        {
            std::fs::create_dir_all(path)?;
        }
        let table = DeltaTable::try_from_url_with_storage_options(url, storage_options)
            .await?
            .create()
            .with_columns(delta_columns())
            .with_partition_columns([column::POND_ID, column::REF_NAME])
            .with_configuration(std::collections::HashMap::from([
                (
                    "delta.deletedFileRetentionDuration".to_string(),
                    Some("interval 0 seconds".to_string()),
                ),
                (
                    "delta.logRetentionDuration".to_string(),
                    Some("interval 0 seconds".to_string()),
                ),
            ]))
            .with_save_mode(SaveMode::ErrorIfExists)
            .await?;
        let context = build_context(&table)?;
        Ok(Self { table, context })
    }

    /// Open the dedicated table beneath `remote_url`.
    pub async fn open_at_url(
        remote_url: &str,
        storage_options: std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let table = deltalake::open_table_with_storage_options(
            publication_url(remote_url)?,
            storage_options,
        )
        .await?;
        let context = build_context(&table)?;
        Ok(Self { table, context })
    }

    /// Read exactly one live active row.
    pub async fn get(&self, pond_id: Uuid, ref_name: &str) -> Result<Option<PublicationState>> {
        let sql = format!(
            "SELECT {format}, {tip}, {root}, {record}, {generation}, {updated} \
             FROM {table} WHERE {pond} = '{pond_id}' AND {reference} = '{ref_name}'",
            format = column::FORMAT,
            tip = column::SNAPSHOT_TIP,
            root = column::MANIFEST_ROOT,
            record = column::PUBLICATION_RECORD,
            generation = column::GENERATION,
            updated = column::UPDATED_AT,
            table = TABLE_NAME,
            pond = column::POND_ID,
            reference = column::REF_NAME,
            ref_name = sql_escape(ref_name),
        );
        let batches = self.context.sql(&sql).await?.collect().await?;
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        if total_rows == 0 {
            return Ok(None);
        }
        if total_rows != 1 {
            return Err(StoreError::Invariant(format!(
                "publication table has {total_rows} live rows for ({pond_id}, {ref_name:?})"
            )));
        }
        let batch = batches
            .iter()
            .find(|batch| batch.num_rows() == 1)
            .ok_or_else(|| StoreError::Invariant("publication query lost its row".to_string()))?;
        let format = string_value(batch.column(0).as_ref(), 0)?;
        if format != NATIVE_FORMAT_V2 {
            return Err(StoreError::Invariant(format!(
                "unsupported active publication format {format:?}"
            )));
        }
        let snapshot_tip = parse_hash(string_value(batch.column(1).as_ref(), 0)?, "snapshot_tip")?;
        let manifest_root =
            parse_hash(string_value(batch.column(2).as_ref(), 0)?, "manifest_root")?;
        let publication_record = parse_hash(
            string_value(batch.column(3).as_ref(), 0)?,
            "publication_record",
        )?;
        let generations = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| StoreError::Invariant("generation is not Int64".to_string()))?;
        let updated = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| StoreError::Invariant("updated_at is not Int64".to_string()))?;
        PublicationState::new(
            pond_id,
            ref_name,
            snapshot_tip,
            manifest_root,
            publication_record,
            generations.value(0),
            updated.value(0),
        )
        .map(Some)
    }

    /// Replace exactly one active row using optimistic concurrency and a
    /// generation/record-head compare-and-swap.
    ///
    /// A successful update is immediately checkpointed. A checkpoint failure
    /// is returned even though the row is already visible; retry recognizes
    /// the identical state and repairs the checkpoint without another write.
    pub async fn compare_and_swap(
        &mut self,
        expectation: PublicationExpectation,
        next: PublicationState,
    ) -> Result<PublicationState> {
        self.refresh().await?;
        let current = self.get(next.pond_id, &next.ref_name).await?;
        match (expectation, current.as_ref()) {
            (PublicationExpectation::Missing, None) => {
                if next.generation != 1 {
                    return Err(StoreError::Invariant(format!(
                        "initial publication generation is {}, expected 1",
                        next.generation
                    )));
                }
            }
            (
                PublicationExpectation::Existing {
                    generation,
                    publication_record,
                },
                Some(current),
            ) if current.generation == generation
                && current.publication_record == publication_record =>
            {
                if next.generation != generation + 1 {
                    return Err(StoreError::Invariant(format!(
                        "next publication generation is {}, expected {}",
                        next.generation,
                        generation + 1
                    )));
                }
            }
            (_, Some(current)) if current == &next => {
                self.checkpoint().await?;
                return Ok(current.clone());
            }
            (PublicationExpectation::Missing, Some(current)) => {
                return Err(StoreError::Invariant(format!(
                    "publication CAS expected no row, found generation {} record {}",
                    current.generation, current.publication_record
                )));
            }
            (PublicationExpectation::Existing { .. }, None) => {
                return Err(StoreError::Invariant(
                    "publication CAS expected an existing row, found none".to_string(),
                ));
            }
            (
                PublicationExpectation::Existing {
                    generation,
                    publication_record,
                },
                Some(current),
            ) => {
                return Err(StoreError::Invariant(format!(
                    "publication CAS expected generation {generation} record \
                     {publication_record}, found generation {} record {}",
                    current.generation, current.publication_record
                )));
            }
        }

        let batch = state_batch(&next)?;
        let predicate = format!(
            "{} = '{}' AND {} = '{}'",
            column::POND_ID,
            next.pond_id,
            column::REF_NAME,
            sql_escape(&next.ref_name)
        );
        let transaction_id = format!("{}:{}", next.pond_id, next.ref_name);
        let table = self
            .table
            .clone()
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where(predicate)
            .with_commit_properties(
                deltalake::kernel::transaction::CommitProperties::default()
                    .with_application_transaction(deltalake::kernel::Transaction::new(
                        transaction_id,
                        next.generation,
                    )),
            )
            .await?;
        self.table = table;
        self.context = build_context(&self.table)?;
        self.checkpoint().await?;
        Ok(next)
    }

    /// Current Delta version.
    #[must_use]
    pub fn version(&self) -> i64 {
        self.table.version().unwrap_or(0)
    }

    /// Number of active Parquet files, useful for isolation/cost tests.
    pub fn active_file_count(&self) -> Result<usize> {
        Ok(self.table.get_file_uris()?.count())
    }

    async fn refresh(&mut self) -> Result<()> {
        self.table.load().await?;
        self.context = build_context(&self.table)?;
        Ok(())
    }

    async fn checkpoint(&self) -> Result<()> {
        deltalake::checkpoints::create_checkpoint(&self.table, None)
            .await
            .map_err(StoreError::Delta)
    }
}

/// Child `_publication/` URL for a remote root.
pub fn publication_url(remote_url: &str) -> Result<Url> {
    let mut base =
        Url::parse(remote_url).map_err(|_| StoreError::InvalidPath(remote_url.to_string()))?;
    if !base.path().ends_with('/') {
        let path = format!("{}/", base.path());
        base.set_path(&path);
    }
    base.join(PUBLICATION_PATH)
        .map_err(|_| StoreError::InvalidPath(remote_url.to_string()))
}

fn delta_columns() -> Vec<DeltaStructField> {
    let string = || DeltaDataType::Primitive(PrimitiveType::String);
    vec![
        DeltaStructField::new(column::POND_ID, string(), false),
        DeltaStructField::new(column::REF_NAME, string(), false),
        DeltaStructField::new(column::FORMAT, string(), false),
        DeltaStructField::new(column::SNAPSHOT_TIP, string(), false),
        DeltaStructField::new(column::MANIFEST_ROOT, string(), false),
        DeltaStructField::new(column::PUBLICATION_RECORD, string(), false),
        DeltaStructField::new(
            column::GENERATION,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            column::UPDATED_AT,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
    ]
}

fn arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new(column::POND_ID, DataType::Utf8, false),
        Field::new(column::REF_NAME, DataType::Utf8, false),
        Field::new(column::FORMAT, DataType::Utf8, false),
        Field::new(column::SNAPSHOT_TIP, DataType::Utf8, false),
        Field::new(column::MANIFEST_ROOT, DataType::Utf8, false),
        Field::new(column::PUBLICATION_RECORD, DataType::Utf8, false),
        Field::new(column::GENERATION, DataType::Int64, false),
        Field::new(column::UPDATED_AT, DataType::Int64, false),
    ]))
}

fn state_batch(state: &PublicationState) -> Result<RecordBatch> {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(StringArray::from(vec![state.pond_id.to_string()])),
            Arc::new(StringArray::from(vec![state.ref_name.clone()])),
            Arc::new(StringArray::from(vec![state.format.clone()])),
            Arc::new(StringArray::from(vec![state.snapshot_tip.to_hex()])),
            Arc::new(StringArray::from(vec![state.manifest_root.to_hex()])),
            Arc::new(StringArray::from(vec![state.publication_record.to_hex()])),
            Arc::new(Int64Array::from(vec![state.generation])),
            Arc::new(Int64Array::from(vec![state.updated_at])),
        ],
    )
    .map_err(StoreError::Arrow)
}

fn build_context(table: &DeltaTable) -> Result<Arc<SessionContext>> {
    let context = SessionContext::new();
    let _ = context.register_table(TABLE_NAME, Arc::new(table.clone()))?;
    Ok(Arc::new(context))
}

fn string_value(array: &dyn Array, index: usize) -> Result<String> {
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(strings.value(index).to_string());
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(strings.value(index).to_string());
    }
    Err(StoreError::Invariant(format!(
        "publication string column has type {:?}",
        array.data_type()
    )))
}

fn parse_hash(value: String, field: &str) -> Result<ObjectHash> {
    ObjectHash::from_hex(&value)
        .map_err(|error| StoreError::Invariant(format!("invalid {field}: {error}")))
}

fn sql_escape(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn hash(value: &str) -> ObjectHash {
        ObjectHash::of_bytes(value.as_bytes())
    }

    #[tokio::test]
    async fn cas_replaces_one_live_row_and_isolates_refs() {
        let directory = tempdir().unwrap();
        let mut table = PublicationTable::create(directory.path()).await.unwrap();
        let pond = Uuid::new_v4();
        let first =
            PublicationState::new(pond, "main", hash("t1"), hash("m1"), hash("p1"), 1, 1).unwrap();
        table
            .compare_and_swap(PublicationExpectation::Missing, first.clone())
            .await
            .unwrap();
        let other =
            PublicationState::new(pond, "other", hash("x1"), hash("y1"), hash("z1"), 1, 1).unwrap();
        table
            .compare_and_swap(PublicationExpectation::Missing, other.clone())
            .await
            .unwrap();
        let foreign_pond = Uuid::new_v4();
        let foreign = PublicationState::new(
            foreign_pond,
            "main",
            hash("ft1"),
            hash("fm1"),
            hash("fp1"),
            1,
            1,
        )
        .unwrap();
        table
            .compare_and_swap(PublicationExpectation::Missing, foreign.clone())
            .await
            .unwrap();
        let second =
            PublicationState::new(pond, "main", hash("t2"), hash("m2"), hash("p2"), 2, 2).unwrap();
        table
            .compare_and_swap(
                PublicationExpectation::Existing {
                    generation: 1,
                    publication_record: first.publication_record,
                },
                second.clone(),
            )
            .await
            .unwrap();
        assert_eq!(table.get(pond, "main").await.unwrap(), Some(second));
        assert_eq!(table.get(pond, "other").await.unwrap(), Some(other));
        assert_eq!(
            table.get(foreign_pond, "main").await.unwrap(),
            Some(foreign)
        );
        assert_eq!(table.active_file_count().unwrap(), 3);
    }

    #[tokio::test]
    async fn stale_cas_fails_loudly() {
        let directory = tempdir().unwrap();
        let mut table = PublicationTable::create(directory.path()).await.unwrap();
        let pond = Uuid::new_v4();
        let first =
            PublicationState::new(pond, "main", hash("t1"), hash("m1"), hash("p1"), 1, 1).unwrap();
        table
            .compare_and_swap(PublicationExpectation::Missing, first.clone())
            .await
            .unwrap();
        let next =
            PublicationState::new(pond, "main", hash("t2"), hash("m2"), hash("p2"), 2, 2).unwrap();
        let error = table
            .compare_and_swap(
                PublicationExpectation::Existing {
                    generation: 0,
                    publication_record: hash("wrong"),
                },
                next,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("publication CAS expected"));
    }

    #[tokio::test]
    async fn concurrent_cas_allows_exactly_one_winner() {
        let directory = tempdir().unwrap();
        let pond = Uuid::new_v4();
        let mut creator = PublicationTable::create(directory.path()).await.unwrap();
        let first =
            PublicationState::new(pond, "main", hash("t1"), hash("m1"), hash("p1"), 1, 1).unwrap();
        creator
            .compare_and_swap(PublicationExpectation::Missing, first.clone())
            .await
            .unwrap();
        drop(creator);

        let mut left = PublicationTable::open(directory.path()).await.unwrap();
        let mut right = PublicationTable::open(directory.path()).await.unwrap();
        let expected = PublicationExpectation::Existing {
            generation: 1,
            publication_record: first.publication_record,
        };
        let left_next =
            PublicationState::new(pond, "main", hash("tl"), hash("ml"), hash("pl"), 2, 2).unwrap();
        let right_next =
            PublicationState::new(pond, "main", hash("tr"), hash("mr"), hash("pr"), 2, 2).unwrap();
        let (left_result, right_result) = tokio::join!(
            left.compare_and_swap(expected, left_next),
            right.compare_and_swap(expected, right_next)
        );
        assert_ne!(
            left_result.is_ok(),
            right_result.is_ok(),
            "exactly one concurrent CAS must commit"
        );
    }
}
