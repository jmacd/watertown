// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Immutable query snapshot membership.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::Result;

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
        logical_count: u64,
        event_time_bounds: Option<TimeInterval>,
    ) -> Self {
        Self {
            chunk_id: chunk_id.into(),
            sequence,
            object,
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
    #[must_use]
    pub fn new(
        snapshot_id: impl Into<Arc<str>>,
        schema: SchemaRef,
        chunks: Vec<ChunkDescriptor>,
        event_time: Option<EventTimeContract>,
    ) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            schema,
            chunks: chunks.into(),
            event_time,
        }
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
