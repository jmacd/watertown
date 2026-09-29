// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! TinyFS immutable-version adapter for the query foundation.

use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
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
use query_foundation::overlap::OverlapPolicy;
use query_foundation::snapshot::{
    ChunkDescriptor, DatasetSnapshot, EventTimeContract, ObjectDescriptor,
};
use query_foundation::statistics::TimeInterval;
use tinyfs::{CoherenceState, FileID, FileVersionInfo, PersistenceLayer, ProviderContext};

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
