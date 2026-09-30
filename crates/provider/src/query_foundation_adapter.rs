// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! TinyFS immutable-version adapter for the query foundation.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};
use futures::FutureExt;
use futures::future::BoxFuture;
use parquet::arrow::async_reader::MetadataFetch;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::statistics::Statistics;
use query_foundation::materialize::{
    MaterializationCommit, MaterializationProgress, MaterializationPublication,
    TransactionalBatchWriter, TransactionalMaterializationSink,
};
use query_foundation::overlap::OverlapPolicy;
use query_foundation::snapshot::{
    ChunkDescriptor, DatasetSnapshot, EventTimeContract, ObjectDescriptor,
};
use query_foundation::statistics::TimeInterval;
use tinyfs::arrow::parquet::StreamingSeriesWriter;
use tinyfs::{
    CoherenceState, EntryType, FileID, FileVersionInfo, PersistenceLayer, ProviderContext, WD,
};
use tokio::io::AsyncWriteExt;

use crate::TinyFsPathBuilder;

/// Exact TinyFS membership captured in one persistence generation.
#[derive(Clone)]
pub struct TinyFsDatasetSnapshot {
    snapshot: DatasetSnapshot,
    coherence: Option<Arc<CoherenceState>>,
    generation: Option<u64>,
}

impl TinyFsDatasetSnapshot {
    /// Foundation snapshot and exact immutable chunk membership.
    #[must_use]
    pub fn snapshot(&self) -> &DatasetSnapshot {
        &self.snapshot
    }

    /// Build a provider that rejects execution after its persistence
    /// generation becomes stale or closes.
    pub fn table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        let provider = self.snapshot.table_provider()?;
        Ok(tinyfs::coherent_table_provider(
            provider,
            self.coherence.clone(),
            self.generation,
        ))
    }
}

/// Transactional append-only materialization sink backed by TinyFS versions.
pub struct TinyFsMaterializationSink {
    root: WD,
    coherence: Option<Arc<CoherenceState>>,
    event_time: Arc<str>,
}

impl TinyFsMaterializationSink {
    /// Bind materialization writes to one TinyFS working directory.
    #[must_use]
    pub fn new(root: WD, context: &ProviderContext, event_time: impl Into<Arc<str>>) -> Self {
        Self {
            root,
            coherence: context.persistence.coherence_state(),
            event_time: event_time.into(),
        }
    }
}

#[async_trait]
impl TransactionalMaterializationSink for TinyFsMaterializationSink {
    async fn begin(
        &self,
        output_id: &str,
        publication: &MaterializationPublication,
    ) -> Result<Box<dyn TransactionalBatchWriter>> {
        if matches!(publication, MaterializationPublication::Replace { .. }) {
            return Err(DataFusionError::Plan(
                "TinyFS native-v2 materialization is append-only and cannot publish replacement \
                 ranges"
                    .to_owned(),
            ));
        }
        if self.event_time.is_empty() {
            return Err(DataFusionError::Plan(
                "TinyFS materialization event-time column must not be empty".to_owned(),
            ));
        }
        let generation = if let Some(state) = &self.coherence {
            state.ensure_open().map_err(external_error)?;
            Some(state.generation())
        } else {
            None
        };
        Ok(Box::new(TinyFsMaterializationWriter {
            root: self.root.clone(),
            coherence: self.coherence.clone(),
            generation,
            output_id: output_id.to_owned(),
            event_time: Arc::clone(&self.event_time),
            writer: None,
        }))
    }
}

struct TinyFsMaterializationWriter {
    root: WD,
    coherence: Option<Arc<CoherenceState>>,
    generation: Option<u64>,
    output_id: String,
    event_time: Arc<str>,
    writer: Option<StreamingSeriesWriter>,
}

impl TinyFsMaterializationWriter {
    fn ensure_coherent(&self) -> Result<()> {
        if let (Some(coherence), Some(generation)) = (&self.coherence, self.generation) {
            coherence
                .ensure_generation(generation)
                .map_err(external_error)?;
        }
        Ok(())
    }

    fn refresh_generation(&mut self) {
        self.generation = self
            .coherence
            .as_ref()
            .map(|coherence| coherence.generation());
    }
}

#[async_trait]
impl TransactionalBatchWriter for TinyFsMaterializationWriter {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_coherent()?;
        if self.writer.is_none() {
            self.writer = Some(
                StreamingSeriesWriter::try_new(
                    &self.root,
                    &self.output_id,
                    batch.schema(),
                    self.event_time.as_ref(),
                )
                .await
                .map_err(external_error)?,
            );
            self.refresh_generation();
        }
        self.writer
            .as_mut()
            .expect("writer initialized")
            .write(batch)
            .await
            .map_err(external_error)
    }

    async fn close(&mut self) -> Result<()> {
        self.ensure_coherent()?;
        if let Some(writer) = &mut self.writer {
            writer.flush().await.map_err(external_error)?;
        }
        Ok(())
    }

    async fn commit(mut self: Box<Self>, commit: MaterializationCommit) -> Result<()> {
        self.ensure_coherent()?;
        match (commit.output.as_ref(), self.writer.as_mut()) {
            (Some(output), Some(writer)) => {
                if !matches!(
                    commit.publication,
                    MaterializationPublication::Append { .. }
                ) {
                    return Err(DataFusionError::Execution(
                        "TinyFS staged output requires append publication".to_owned(),
                    ));
                }
                if output.output_id() != self.output_id {
                    return Err(DataFusionError::Execution(format!(
                        "TinyFS staged output identity {:?} does not match commit identity {:?}",
                        self.output_id,
                        output.output_id()
                    )));
                }
                let expected = (
                    output.event_time_bounds().min(),
                    output.event_time_bounds().max(),
                    output.rows(),
                );
                let actual = writer
                    .output_metadata()
                    .map_err(external_error)?
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "TinyFS staged output has no rows at commit".to_owned(),
                        )
                    })?;
                if actual != expected {
                    return Err(DataFusionError::Execution(format!(
                        "TinyFS staged output metadata {actual:?} does not match commit metadata \
                         {expected:?}"
                    )));
                }
                writer
                    .set_exact_logical_attributes(materialization_attributes(
                        &commit.progress,
                        &self.event_time,
                    )?)
                    .map_err(external_error)?;
                self.ensure_coherent()?;
                let writer = self.writer.take().expect("matched staged writer");
                _ = writer.finish().await.map_err(external_error)?;
                Ok(())
            }
            (None, None) => {
                if !matches!(commit.publication, MaterializationPublication::NoOutput) {
                    return Err(DataFusionError::Execution(
                        "TinyFS empty materialization requires NoOutput publication".to_owned(),
                    ));
                }
                publish_progress_only(
                    &self.root,
                    &self.output_id,
                    materialization_attributes(&commit.progress, &self.event_time)?,
                )
                .await
            }
            (Some(_), None) => Err(DataFusionError::Execution(
                "TinyFS materialization commit names output but no rows were staged".to_owned(),
            )),
            (None, Some(_)) => Err(DataFusionError::Execution(
                "TinyFS materialization staged rows but commit names no output".to_owned(),
            )),
        }
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        _ = self.writer.take();
        Ok(())
    }
}

fn materialization_attributes(
    progress: &MaterializationProgress,
    event_time: &str,
) -> Result<Vec<u8>> {
    let mut attributes = BTreeMap::new();
    _ = attributes.insert(
        "watertown.materialization.observed_through",
        serde_json::json!(progress.settled().observed_through()),
    );
    _ = attributes.insert(
        "watertown.materialization.recipe_id",
        serde_json::json!(progress.recipe_id()),
    );
    _ = attributes.insert(
        "watertown.materialization.settled_through",
        serde_json::json!(progress.settled().settled_through()),
    );
    _ = attributes.insert(
        "watertown.materialization.source_state_id",
        serde_json::json!(progress.source_state_id()),
    );
    _ = attributes.insert("watertown.timestamp_column", serde_json::json!(event_time));
    serde_json::to_vec(&attributes).map_err(external_error)
}

async fn publish_progress_only(root: &WD, output_id: &str, progress: Vec<u8>) -> Result<()> {
    let path = format!("{output_id}.materialization-progress");
    let (_, mut writer) = root
        .create_file_path_streaming_with_type(path, EntryType::FilePhysicalVersion)
        .await
        .map_err(external_error)?;
    writer.write_all(&progress).await.map_err(external_error)?;
    writer.shutdown().await.map_err(external_error)
}

/// Capture exact live TinyFS Parquet versions using footer-only metadata
/// reads.
pub async fn capture_tinyfs_snapshot(
    context: &ProviderContext,
    file_id: FileID,
    snapshot_id: impl Into<Arc<str>>,
    schema: SchemaRef,
    event_time: Option<EventTimeContract>,
    overlap: OverlapPolicy,
) -> Result<TinyFsDatasetSnapshot> {
    let coherence = context.persistence.coherence_state();
    let generation = coherence.as_ref().map(|state| state.generation());
    if let Some(state) = &coherence {
        state.ensure_open().map_err(external_error)?;
    }
    let versions = context
        .persistence
        .list_file_versions(file_id)
        .await
        .map_err(external_error)?;
    let mut chunks = Vec::with_capacity(versions.len());
    for version in versions {
        chunks.push(
            version_chunk(
                Arc::clone(&context.persistence),
                file_id,
                version,
                event_time.as_ref(),
            )
            .await?,
        );
    }
    if let (Some(state), Some(generation)) = (&coherence, generation) {
        state
            .ensure_generation(generation)
            .map_err(external_error)?;
    }
    let snapshot = DatasetSnapshot::try_new(snapshot_id, schema, chunks, event_time, overlap)?;
    Ok(TinyFsDatasetSnapshot {
        snapshot,
        coherence,
        generation,
    })
}

async fn version_chunk(
    persistence: Arc<dyn PersistenceLayer>,
    file_id: FileID,
    version: FileVersionInfo,
    event_time: Option<&EventTimeContract>,
) -> Result<ChunkDescriptor> {
    if version.size == 0 {
        return Err(DataFusionError::Plan(format!(
            "TinyFS Parquet version {} for {file_id} is empty",
            version.version
        )));
    }
    let metadata = read_metadata(
        Arc::clone(&persistence),
        file_id,
        version.version,
        version.size,
    )
    .await?;
    let file_metadata = metadata.file_metadata();
    let arrow_schema = Arc::new(
        parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
        .map_err(external_error)?,
    );
    let logical_count = u64::try_from(file_metadata.num_rows()).map_err(|_| {
        DataFusionError::Plan(format!(
            "TinyFS Parquet version {} for {file_id} has a negative row count",
            version.version
        ))
    })?;
    let event_time_bounds = match event_time {
        Some(contract) if logical_count != 0 => {
            parquet_event_time_bounds(&metadata, &arrow_schema, contract.column())?
        }
        _ => None,
    };
    let chunk_id = version
        .extended_metadata
        .as_ref()
        .and_then(|metadata| metadata.get("logical_leaf_hash"))
        .cloned()
        .unwrap_or_else(|| format!("{file_id}@{}", version.version));
    let url = ListingTableUrl::parse(TinyFsPathBuilder::url_specific_version(
        &file_id,
        version.version,
    ))?;
    Ok(ChunkDescriptor::new(
        chunk_id,
        version.version,
        ObjectDescriptor::new(url),
        arrow_schema,
        logical_count,
        event_time_bounds,
    ))
}

async fn read_metadata(
    persistence: Arc<dyn PersistenceLayer>,
    file_id: FileID,
    version: u64,
    size: u64,
) -> Result<ParquetMetaData> {
    let fetch = TinyFsMetadataFetch {
        persistence,
        file_id,
        version,
    };
    let mut reader = ParquetMetaDataReader::new();
    reader.try_load(fetch, size).await.map_err(external_error)?;
    reader.finish().map_err(external_error)
}

struct TinyFsMetadataFetch {
    persistence: Arc<dyn PersistenceLayer>,
    file_id: FileID,
    version: u64,
}

impl MetadataFetch for TinyFsMetadataFetch {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let persistence = Arc::clone(&self.persistence);
        let file_id = self.file_id;
        let version = self.version;
        async move {
            persistence
                .read_file_version_range(file_id, version, range)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        }
        .boxed()
    }
}

fn parquet_event_time_bounds(
    metadata: &ParquetMetaData,
    schema: &SchemaRef,
    event_time: &str,
) -> Result<Option<TimeInterval>> {
    let field_index = schema.index_of(event_time).map_err(|_| {
        DataFusionError::Plan(format!(
            "TinyFS Parquet schema is missing event-time column '{event_time}'"
        ))
    })?;
    if !matches!(
        schema.field(field_index).data_type(),
        DataType::Int64 | DataType::Date64 | DataType::Timestamp(_, _)
    ) {
        return Err(DataFusionError::Plan(format!(
            "TinyFS Parquet event-time column '{event_time}' has unsupported type {}",
            schema.field(field_index).data_type()
        )));
    }
    let mut minimum = None;
    let mut maximum = None;
    for row_group in metadata.row_groups() {
        let Some(column) = row_group.columns().get(field_index) else {
            return Err(DataFusionError::Plan(format!(
                "TinyFS Parquet row group is missing event-time column '{event_time}'"
            )));
        };
        if let Some(Statistics::Int64(statistics)) = column.statistics()
            && let (Some(min), Some(max)) = (statistics.min_opt(), statistics.max_opt())
        {
            minimum = Some(minimum.map_or(*min, |current: i64| current.min(*min)));
            maximum = Some(maximum.map_or(*max, |current: i64| current.max(*max)));
        }
    }
    match (minimum, maximum) {
        (Some(minimum), Some(maximum)) => Ok(Some(TimeInterval::try_new(minimum, maximum)?)),
        (None, None) => Ok(None),
        _ => Err(DataFusionError::Plan(format!(
            "TinyFS Parquet event-time statistics for '{event_time}' are incomplete"
        ))),
    }
}

fn external_error(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
