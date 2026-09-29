// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Real-Parquet fixtures and an instrumented in-memory object store.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{SessionConfig, SessionContext};
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use url::Url;

use crate::metrics::ObjectStoreMetrics;
use crate::overlap::OverlapPolicy;
use crate::snapshot::{ChunkDescriptor, DatasetSnapshot, EventTimeContract, ObjectDescriptor};
use crate::statistics::TimeInterval;

const STORE_URL: &str = "memory://query-foundation";

/// Object store wrapper that measures successful read work.
#[derive(Debug)]
pub struct InstrumentedObjectStore {
    inner: Arc<dyn ObjectStore>,
    metrics: Arc<ObjectStoreMetrics>,
}

impl InstrumentedObjectStore {
    /// Wrap an object store with shared counters.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, metrics: Arc<ObjectStoreMetrics>) -> Self {
        Self { inner, metrics }
    }
}

impl fmt::Display for InstrumentedObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "InstrumentedObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for InstrumentedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let metadata_only = options.head;
        let ranged = options.range.is_some();
        let result = self.inner.get_opts(location, options).await?;
        if metadata_only {
            self.metrics.record_metadata_call();
        } else {
            self.metrics.record_read(
                location.as_ref(),
                result.range.end - result.range.start,
                ranged,
            );
        }
        Ok(result)
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.metrics.record_list_call();
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.metrics.record_list_call();
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// Isolated fixture for publishing Parquet chunks and building exact snapshots.
#[derive(Debug)]
pub struct FoundationFixture {
    inner: Arc<InMemory>,
    store: Arc<InstrumentedObjectStore>,
    schema: SchemaRef,
    metrics: Arc<ObjectStoreMetrics>,
}

impl FoundationFixture {
    /// Create an empty fixture with one dataset schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        let inner = Arc::new(InMemory::new());
        let metrics = Arc::new(ObjectStoreMetrics::default());
        let delegate: Arc<dyn ObjectStore> = inner.clone();
        let store = Arc::new(InstrumentedObjectStore::new(delegate, Arc::clone(&metrics)));
        Self {
            inner,
            store,
            schema,
            metrics,
        }
    }

    /// Shared physical-work counters.
    #[must_use]
    pub fn metrics(&self) -> &Arc<ObjectStoreMetrics> {
        &self.metrics
    }

    /// Create a DataFusion context registered with this fixture's store.
    pub fn context(&self) -> Result<SessionContext> {
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let url =
            Url::parse(STORE_URL).map_err(|error| DataFusionError::External(Box::new(error)))?;
        _ = context.register_object_store(&url, self.store.clone());
        Ok(context)
    }

    /// Publish batches as one real Parquet object and return its byte size.
    pub async fn put_parquet(
        &self,
        path: &str,
        batches: &[RecordBatch],
        max_row_group_size: usize,
    ) -> Result<u64> {
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| DataFusionError::Plan("Parquet fixture requires a batch".to_owned()))?;
        if batches.iter().any(|batch| batch.schema() != schema) {
            return Err(DataFusionError::Plan(
                "all batches in one Parquet fixture must share a schema".to_owned(),
            ));
        }
        let properties = WriterProperties::builder()
            .set_max_row_group_size(max_row_group_size)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        let mut bytes = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
            for batch in batches {
                writer.write(batch)?;
            }
            _ = writer.close()?;
        }
        let size = bytes.len() as u64;
        _ = self.inner.put(&Path::from(path), bytes.into()).await?;
        Ok(size)
    }

    /// Capture exact current membership without consulting later store listings.
    pub fn snapshot(&self, snapshot_id: &str, chunks: &[(&str, u64)]) -> Result<DatasetSnapshot> {
        let chunks = chunks
            .iter()
            .enumerate()
            .map(|(sequence, (path, logical_count))| {
                Ok(ChunkDescriptor::new(
                    *path,
                    sequence as u64,
                    ObjectDescriptor::new(self.object_url(path)?),
                    Arc::clone(&self.schema),
                    *logical_count,
                    None,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        DatasetSnapshot::try_new(
            snapshot_id,
            Arc::clone(&self.schema),
            chunks,
            None,
            OverlapPolicy::PreserveAll,
        )
    }

    /// Capture exact timeseries membership with per-chunk event-time statistics.
    pub fn timeseries_snapshot(
        &self,
        snapshot_id: &str,
        event_time_column: &str,
        chunks: &[(&str, u64, Option<TimeInterval>)],
    ) -> Result<DatasetSnapshot> {
        let chunks = chunks
            .iter()
            .enumerate()
            .map(|(sequence, (path, logical_count, bounds))| {
                Ok(ChunkDescriptor::new(
                    *path,
                    sequence as u64,
                    ObjectDescriptor::new(self.object_url(path)?),
                    Arc::clone(&self.schema),
                    *logical_count,
                    *bounds,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        DatasetSnapshot::try_new(
            snapshot_id,
            Arc::clone(&self.schema),
            chunks,
            Some(EventTimeContract::new(event_time_column)),
            OverlapPolicy::PreserveAll,
        )
    }

    /// Construct an exact descriptor for an object in this fixture.
    pub fn object_descriptor(&self, path: &str) -> Result<ObjectDescriptor> {
        Ok(ObjectDescriptor::new(self.object_url(path)?))
    }

    fn object_url(&self, path: &str) -> Result<datafusion::datasource::listing::ListingTableUrl> {
        datafusion::datasource::listing::ListingTableUrl::parse(format!("{STORE_URL}/{path}"))
    }
}
