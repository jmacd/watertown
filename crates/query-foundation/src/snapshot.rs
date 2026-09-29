// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Immutable query snapshot membership.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};

use crate::metrics::ChunkPruningMetrics;
use crate::provider::ChunkTableProvider;
use crate::statistics::TimeInterval;

/// Event-time column used for conservative chunk pruning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventTimeContract {
    column: Arc<str>,
}

impl EventTimeContract {
    /// Declare the event-time column.
    #[must_use]
    pub fn new(column: impl Into<Arc<str>>) -> Self {
        Self {
            column: column.into(),
        }
    }

    /// Event-time column name.
    #[must_use]
    pub fn column(&self) -> &str {
        &self.column
    }
}

/// Physical object referenced by an immutable chunk.
#[derive(Clone, Debug)]
pub struct ObjectDescriptor {
    url: ListingTableUrl,
}

impl ObjectDescriptor {
    /// Construct an object descriptor from an exact URL.
    #[must_use]
    pub fn new(url: ListingTableUrl) -> Self {
        Self { url }
    }

    /// Exact object URL.
    #[must_use]
    pub fn url(&self) -> &ListingTableUrl {
        &self.url
    }
}

/// Immutable logical chunk and its conservative metadata.
#[derive(Clone, Debug)]
pub struct ChunkDescriptor {
    chunk_id: Arc<str>,
    sequence: u64,
    object: ObjectDescriptor,
    schema: SchemaRef,
    logical_count: u64,
    event_time_bounds: Option<TimeInterval>,
}

impl ChunkDescriptor {
    /// Construct a chunk descriptor.
    #[must_use]
    pub fn new(
        chunk_id: impl Into<Arc<str>>,
        sequence: u64,
        object: ObjectDescriptor,
        schema: SchemaRef,
        logical_count: u64,
        event_time_bounds: Option<TimeInterval>,
    ) -> Self {
        Self {
            chunk_id: chunk_id.into(),
            sequence,
            object,
            schema,
            logical_count,
            event_time_bounds,
        }
    }

    /// Stable logical chunk identity.
    #[must_use]
    pub fn chunk_id(&self) -> &str {
        &self.chunk_id
    }

    /// Deterministic publication sequence.
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Physical object descriptor.
    #[must_use]
    pub fn object(&self) -> &ObjectDescriptor {
        &self.object
    }

    /// Schema stored in this physical chunk.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Logical rows represented by this chunk.
    #[must_use]
    pub fn logical_count(&self) -> u64 {
        self.logical_count
    }

    /// Inclusive event-time statistics, when known.
    #[must_use]
    pub fn event_time_bounds(&self) -> Option<TimeInterval> {
        self.event_time_bounds
    }
}

/// Exact immutable Parquet object membership captured for one query snapshot.
#[derive(Clone, Debug)]
pub struct DatasetSnapshot {
    snapshot_id: Arc<str>,
    schema: SchemaRef,
    chunks: Arc<[ChunkDescriptor]>,
    event_time: Option<EventTimeContract>,
}

impl DatasetSnapshot {
    /// Capture an exact ordered set of immutable chunks.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identity, membership order, event-time
    /// contracts, collection URLs, or incompatible chunk schemas.
    pub fn try_new(
        snapshot_id: impl Into<Arc<str>>,
        schema: SchemaRef,
        chunks: Vec<ChunkDescriptor>,
        event_time: Option<EventTimeContract>,
    ) -> Result<Self> {
        let snapshot_id = snapshot_id.into();
        if snapshot_id.is_empty() {
            return Err(DataFusionError::Plan(
                "snapshot identity must not be empty".to_owned(),
            ));
        }
        validate_event_time(&schema, event_time.as_ref())?;
        validate_chunks(&schema, &chunks, event_time.as_ref())?;
        Ok(Self {
            snapshot_id,
            schema,
            chunks: chunks.into(),
            event_time,
        })
    }

    /// Stable identity supplied by the snapshot publisher.
    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Dataset schema captured with the snapshot.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Exact ordered chunks visible to this snapshot.
    #[must_use]
    pub fn chunks(&self) -> &[ChunkDescriptor] {
        &self.chunks
    }

    /// Event-time contract, when this is a timeseries dataset.
    #[must_use]
    pub fn event_time(&self) -> Option<&EventTimeContract> {
        self.event_time.as_ref()
    }

    /// Build a chunk-aware DataFusion provider with fresh pruning counters.
    pub fn table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(ChunkTableProvider::new(
            self.clone(),
            Arc::new(ChunkPruningMetrics::default()),
        )))
    }

    /// Build a chunk-aware provider with caller-visible pruning counters.
    #[must_use]
    pub fn table_provider_with_metrics(
        &self,
        metrics: Arc<ChunkPruningMetrics>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(ChunkTableProvider::new(self.clone(), metrics))
    }
}

fn validate_event_time(schema: &SchemaRef, event_time: Option<&EventTimeContract>) -> Result<()> {
    let Some(event_time) = event_time else {
        return Ok(());
    };
    let field = schema.field_with_name(event_time.column()).map_err(|_| {
        DataFusionError::Plan(format!(
            "event-time column '{}' is absent from the snapshot schema",
            event_time.column()
        ))
    })?;
    if !matches!(
        field.data_type(),
        DataType::Int64 | DataType::Date64 | DataType::Timestamp(_, _)
    ) {
        return Err(DataFusionError::Plan(format!(
            "event-time column '{}' has unsupported type {}",
            event_time.column(),
            field.data_type()
        )));
    }
    Ok(())
}

fn validate_chunks(
    schema: &SchemaRef,
    chunks: &[ChunkDescriptor],
    event_time: Option<&EventTimeContract>,
) -> Result<()> {
    let mut chunk_ids = BTreeSet::new();
    let mut previous_sequence = None;
    for chunk in chunks {
        if chunk.chunk_id().is_empty() {
            return Err(DataFusionError::Plan(
                "chunk identity must not be empty".to_owned(),
            ));
        }
        if !chunk_ids.insert(chunk.chunk_id()) {
            return Err(DataFusionError::Plan(format!(
                "duplicate chunk identity '{}'",
                chunk.chunk_id()
            )));
        }
        if previous_sequence.is_some_and(|sequence| chunk.sequence() <= sequence) {
            return Err(DataFusionError::Plan(format!(
                "chunk sequence {} is not strictly greater than its predecessor",
                chunk.sequence()
            )));
        }
        previous_sequence = Some(chunk.sequence());
        if chunk.object().url().is_collection() {
            return Err(DataFusionError::Plan(format!(
                "chunk '{}' must reference an exact object, not collection URL {}",
                chunk.chunk_id(),
                chunk.object().url()
            )));
        }
        if chunk.logical_count() == 0 && chunk.event_time_bounds().is_some() {
            return Err(DataFusionError::Plan(format!(
                "empty chunk '{}' must not declare event-time bounds",
                chunk.chunk_id()
            )));
        }
        if event_time.is_none() && chunk.event_time_bounds().is_some() {
            return Err(DataFusionError::Plan(format!(
                "non-timeseries chunk '{}' must not declare event-time bounds",
                chunk.chunk_id()
            )));
        }
        validate_chunk_schema(schema, chunk, event_time)?;
    }
    Ok(())
}

fn validate_chunk_schema(
    schema: &SchemaRef,
    chunk: &ChunkDescriptor,
    event_time: Option<&EventTimeContract>,
) -> Result<()> {
    for field in chunk.schema().fields() {
        let snapshot_field = schema.field_with_name(field.name()).map_err(|_| {
            DataFusionError::Plan(format!(
                "chunk '{}' contains undeclared column '{}'",
                chunk.chunk_id(),
                field.name()
            ))
        })?;
        if snapshot_field.data_type() != field.data_type() {
            return Err(DataFusionError::Plan(format!(
                "chunk '{}' column '{}' has type {}, expected {}",
                chunk.chunk_id(),
                field.name(),
                field.data_type(),
                snapshot_field.data_type()
            )));
        }
        if field.is_nullable() && !snapshot_field.is_nullable() {
            return Err(DataFusionError::Plan(format!(
                "chunk '{}' column '{}' is nullable but the snapshot field is required",
                chunk.chunk_id(),
                field.name()
            )));
        }
    }
    for field in schema.fields() {
        if chunk.schema().field_with_name(field.name()).is_err() && !field.is_nullable() {
            return Err(DataFusionError::Plan(format!(
                "chunk '{}' is missing required column '{}'",
                chunk.chunk_id(),
                field.name()
            )));
        }
    }
    if let Some(event_time) = event_time
        && chunk.schema().field_with_name(event_time.column()).is_err()
    {
        return Err(DataFusionError::Plan(format!(
            "chunk '{}' is missing event-time column '{}'",
            chunk.chunk_id(),
            event_time.column()
        )));
    }
    Ok(())
}
