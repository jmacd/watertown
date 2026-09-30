// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Parquet integration for TinyFS
//!
//! This module provides:
//! - High-level ForArrow integration for writing/reading `Vec<T>`
//! - Low-level RecordBatch operations  
//! - Temporal bounds extraction using Arrow compute kernels
//! - Parquet metadata parsing for existing files (used by copy command)

use super::schema::ForArrow;
use crate::error::ResultExt;
use crate::{EntryType, Result, WD};
use arrow_array::{Array, RecordBatch};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio_util::bytes::Bytes;

const STREAMING_MAX_ROW_GROUP_ROWS: usize = 8_192;

// ============================================================================
// Temporal Bounds Extraction
// ============================================================================

/// Extract min/max temporal bounds from a RecordBatch column using Arrow compute kernels.
/// Returns timestamps normalized to **microseconds** for consistent storage.
/// This is efficient - no serialization needed, just array aggregation.
pub fn extract_temporal_bounds_from_batch(
    batch: &RecordBatch,
    timestamp_column: &str,
) -> Result<(i64, i64)> {
    use arrow_array::types::{
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    };

    // Find the timestamp column
    let col_idx = batch.schema().index_of(timestamp_column).map_err(|_| {
        crate::Error::Other(format!(
            "Timestamp column '{}' not found in schema",
            timestamp_column
        ))
    })?;

    let column = batch.column(col_idx);

    // Try to get as Int64 array (raw i64 timestamps - assume microseconds for backward compat)
    if let Some(int64_array) = column.as_any().downcast_ref::<arrow_array::Int64Array>() {
        let min = arrow::compute::min(int64_array)
            .ok_or_else(|| crate::Error::Other("No min value in timestamp column".into()))?;
        let max = arrow::compute::max(int64_array)
            .ok_or_else(|| crate::Error::Other("No max value in timestamp column".into()))?;
        // Raw Int64 - assume already microseconds (legacy behavior)
        return Ok((min, max));
    }

    // Try TimestampSecondArray - convert seconds to microseconds
    if let Some(ts_array) = column
        .as_any()
        .downcast_ref::<arrow_array::TimestampSecondArray>()
    {
        let min = arrow::compute::min::<TimestampSecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No min value in timestamp column".into()))?;
        let max = arrow::compute::max::<TimestampSecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No max value in timestamp column".into()))?;
        // Convert seconds -> microseconds
        return Ok((min * 1_000_000, max * 1_000_000));
    }

    // Try TimestampMillisecondArray - convert milliseconds to microseconds
    if let Some(ts_array) = column
        .as_any()
        .downcast_ref::<arrow_array::TimestampMillisecondArray>()
    {
        let min = arrow::compute::min::<TimestampMillisecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No min value in timestamp column".into()))?;
        let max = arrow::compute::max::<TimestampMillisecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No max value in timestamp column".into()))?;
        // Convert milliseconds -> microseconds
        return Ok((min * 1_000, max * 1_000));
    }

    // Try TimestampMicrosecondArray - already in microseconds
    if let Some(ts_array) = column
        .as_any()
        .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
    {
        let min = arrow::compute::min::<TimestampMicrosecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No min value in timestamp column".into()))?;
        let max = arrow::compute::max::<TimestampMicrosecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No max value in timestamp column".into()))?;
        return Ok((min, max));
    }

    // Try TimestampNanosecondArray - convert nanoseconds to microseconds
    if let Some(ts_array) = column
        .as_any()
        .downcast_ref::<arrow_array::TimestampNanosecondArray>()
    {
        let min = arrow::compute::min::<TimestampNanosecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No min value in timestamp column".into()))?;
        let max = arrow::compute::max::<TimestampNanosecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No max value in timestamp column".into()))?;
        // Convert nanoseconds -> microseconds
        return Ok((min / 1_000, max / 1_000));
    }

    Err(crate::Error::Other(format!(
        "Timestamp column '{}' has unsupported type: {:?}",
        timestamp_column,
        column.data_type()
    )))
}

/// Extract temporal bounds from ParquetMetaData (high-level API).
/// Returns timestamps normalized to **microseconds** for consistent storage.
/// Used when parsing existing parquet files (e.g., copy command).
pub fn extract_temporal_bounds_from_parquet_metadata(
    parquet_meta: &parquet::file::metadata::ParquetMetaData,
    schema: &arrow_schema::SchemaRef,
    ts_column: &str,
) -> Result<(i64, i64)> {
    use arrow_schema::{DataType, TimeUnit};
    use parquet::file::statistics::Statistics;

    let ts_field = schema
        .fields()
        .iter()
        .find(|f| f.name() == ts_column)
        .ok_or_else(|| {
            crate::Error::Other(format!(
                "Timestamp column '{}' not found in schema",
                ts_column
            ))
        })?;

    let ts_col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == ts_column)
        .expect("already validated above");

    // Determine the multiplier to convert to microseconds based on Arrow schema
    let to_micros_multiplier: i64 = match ts_field.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => 1_000_000,
        DataType::Timestamp(TimeUnit::Millisecond, _) => 1_000,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => -1000, // will divide
        DataType::Int64 => 1, // Assume already microseconds for raw Int64
        _ => 1,               // Default: assume microseconds
    };

    let mut global_min: Option<i64> = None;
    let mut global_max: Option<i64> = None;

    for row_group in parquet_meta.row_groups() {
        if ts_col_idx >= row_group.columns().len() {
            continue;
        }

        let column_chunk = &row_group.columns()[ts_col_idx];
        if let Some(stats) = column_chunk.statistics()
            && let Statistics::Int64(int64_stats) = stats
            && let (Some(&min_val), Some(&max_val)) = (int64_stats.min_opt(), int64_stats.max_opt())
        {
            global_min = Some(global_min.map_or(min_val, |v| v.min(min_val)));
            global_max = Some(global_max.map_or(max_val, |v| v.max(max_val)));
        }
    }

    match (global_min, global_max) {
        (Some(min), Some(max)) => {
            // Normalize to microseconds
            let (min_micros, max_micros) = if to_micros_multiplier < 0 {
                // Nanoseconds: divide
                (min / (-to_micros_multiplier), max / (-to_micros_multiplier))
            } else {
                (min * to_micros_multiplier, max * to_micros_multiplier)
            };
            Ok((min_micros, max_micros))
        }
        _ => Err(crate::Error::Other(format!(
            "No statistics found for timestamp column '{}'",
            ts_column
        ))),
    }
}

// ============================================================================
// Write Options (Anti-Duplication Pattern)
// ============================================================================

/// Options for writing parquet files
#[derive(Default, Clone)]
pub struct WriteOptions<'a> {
    /// For series files: the timestamp column name (defaults to "timestamp")
    pub timestamp_column: Option<&'a str>,
}

// ============================================================================
// Core Write/Read Functions
// ============================================================================

/// Serialize a RecordBatch to parquet bytes in memory
fn serialize_batch_to_parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(cursor, batch.schema(), Some(props))
            .map_other_context("Arrow writer error")?;

        writer.write(batch).map_other_context("Write batch error")?;

        let _ = writer.close().map_other_context("Close writer error")?;
    }
    Ok(buffer)
}

/// Parse parquet bytes into concatenated RecordBatch
fn parse_parquet_to_batch(data: Vec<u8>) -> Result<RecordBatch> {
    let bytes = Bytes::from(data);
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_other_context("Parquet reader error")?
        .build()
        .map_other_context("Build reader error")?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_other_context("Read batch error")?;
        batches.push(batch);
    }

    if batches.is_empty() {
        Err(crate::Error::Other("No data in parquet file".to_string()))
    } else if batches.len() == 1 {
        Ok(batches.into_iter().next().expect("not empty"))
    } else {
        let schema = batches[0].schema();
        let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
        arrow::compute::concat_batches(&schema, batch_refs)
            .map_other_context("Concat batches error")
    }
}

type TemporalMetadata = (i64, i64, String);

#[derive(Default)]
struct WriterMetadata {
    temporal: Option<TemporalMetadata>,
    exact_attributes: Option<Vec<u8>>,
}

struct MetadataWriterProxy {
    inner: Pin<Box<dyn crate::file::FileMetadataWriter>>,
    metadata: Arc<Mutex<WriterMetadata>>,
    metadata_applied: bool,
}

impl MetadataWriterProxy {
    fn new(
        inner: Pin<Box<dyn crate::file::FileMetadataWriter>>,
        metadata: Arc<Mutex<WriterMetadata>>,
    ) -> Self {
        Self {
            inner,
            metadata,
            metadata_applied: false,
        }
    }

    fn apply_metadata(&mut self) -> std::io::Result<()> {
        if self.metadata_applied {
            return Ok(());
        }
        let metadata = self
            .metadata
            .lock()
            .map_err(|_| std::io::Error::other("stream writer metadata lock poisoned"))?;
        let (min, max, column) = metadata
            .temporal
            .clone()
            .ok_or_else(|| std::io::Error::other("stream has no temporal metadata"))?;
        self.inner
            .as_mut()
            .get_mut()
            .set_temporal_metadata(min, max, column);
        if let Some(attributes) = metadata.exact_attributes.clone() {
            self.inner
                .as_mut()
                .get_mut()
                .set_exact_logical_attributes(attributes);
        }
        self.metadata_applied = true;
        Ok(())
    }
}

impl AsyncWrite for MetadataWriterProxy {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.inner.as_mut().poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.inner.as_mut().poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.apply_metadata()?;
        self.inner.as_mut().poll_shutdown(context)
    }
}

/// Direct bounded-memory Parquet writer for one TinyFS series version.
pub struct StreamingSeriesWriter {
    writer: parquet::arrow::AsyncArrowWriter<MetadataWriterProxy>,
    metadata: Arc<Mutex<WriterMetadata>>,
    timestamp_column: String,
    rows: u64,
}

impl StreamingSeriesWriter {
    /// Open one hidden TinyFS series version for direct Parquet batches.
    pub async fn try_new<P>(
        root: &WD,
        path: P,
        schema: arrow_schema::SchemaRef,
        timestamp_column: impl Into<String>,
    ) -> Result<Self>
    where
        P: AsRef<Path> + Send + Sync,
    {
        let timestamp_column = timestamp_column.into();
        let (_, mut tinyfs_writer) = root
            .create_file_path_streaming_with_type(path, EntryType::TablePhysicalSeries)
            .await?;
        tinyfs_writer.require_temporal_metadata(timestamp_column.clone());
        let metadata = Arc::new(Mutex::new(WriterMetadata::default()));
        let proxy = MetadataWriterProxy::new(tinyfs_writer, Arc::clone(&metadata));
        let props = WriterProperties::builder()
            .set_max_row_group_size(STREAMING_MAX_ROW_GROUP_ROWS)
            .build();
        let writer = parquet::arrow::AsyncArrowWriter::try_new(proxy, schema, Some(props))
            .map_other_context("Arrow writer error")?;
        Ok(Self {
            writer,
            metadata,
            timestamp_column,
            rows: 0,
        })
    }

    /// Write one batch and update exact temporal metadata.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (batch_min, batch_max) =
            extract_temporal_bounds_from_batch(batch, &self.timestamp_column)?;
        {
            let mut metadata = self.metadata.lock().map_err(|_| {
                crate::Error::Other("stream writer metadata lock poisoned".to_string())
            })?;
            match &mut metadata.temporal {
                Some((min, max, _)) => {
                    *min = (*min).min(batch_min);
                    *max = (*max).max(batch_max);
                }
                None => {
                    metadata.temporal = Some((batch_min, batch_max, self.timestamp_column.clone()));
                }
            }
        }
        self.writer
            .write(batch)
            .await
            .map_other_context("Write batch error")?;
        self.rows += batch.num_rows() as u64;
        Ok(())
    }

    /// Flush complete row groups while keeping the TinyFS version hidden.
    pub async fn flush(&mut self) -> Result<()> {
        self.writer
            .flush()
            .await
            .map_other_context("Flush Arrow writer error")
    }

    /// Attach canonical logical attributes before finalization.
    pub fn set_exact_logical_attributes(&mut self, attributes: Vec<u8>) -> Result<()> {
        self.metadata
            .lock()
            .map_err(|_| crate::Error::Other("stream writer metadata lock poisoned".to_string()))?
            .exact_attributes = Some(attributes);
        Ok(())
    }

    /// Finalize and publish this TinyFS series version.
    pub async fn finish(self) -> Result<(i64, i64, u64)> {
        if self.rows == 0 {
            return Err(crate::Error::Other(
                "cannot finalize an empty series version".to_string(),
            ));
        }
        let rows = self.rows;
        _ = self
            .writer
            .close()
            .await
            .map_other_context("Close Arrow writer error")?;
        let (minimum, maximum, _) = self
            .metadata
            .lock()
            .map_err(|_| crate::Error::Other("stream writer metadata lock poisoned".to_string()))?
            .temporal
            .clone()
            .expect("non-empty stream has temporal metadata");
        Ok((minimum, maximum, rows))
    }
}

// ============================================================================
// ParquetExt Trait - Clean API
// ============================================================================

#[async_trait::async_trait]
pub trait ParquetExt {
    /// Write items to a parquet file (high-level ForArrow API)
    async fn create_table_from_items<T, P>(
        &self,
        path: P,
        items: &[T],
        entry_type: EntryType,
    ) -> Result<()>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync;

    /// Read a parquet file as items (high-level ForArrow API)
    async fn read_table_as_items<T, P>(&self, path: P) -> Result<Vec<T>>
    where
        T: for<'a> Deserialize<'a> + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync;

    /// Write a RecordBatch to a parquet file
    async fn create_table_from_batch<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        entry_type: EntryType,
    ) -> Result<()>
    where
        P: AsRef<Path> + Send + Sync;

    /// Read a parquet file as RecordBatch
    async fn read_table_as_batch<P>(&self, path: P) -> Result<RecordBatch>
    where
        P: AsRef<Path> + Send + Sync;

    /// Create a FileSeries from RecordBatch with automatic temporal extraction.
    /// Uses Arrow compute kernels to extract min/max from the timestamp column.
    async fn create_series_from_batch<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        P: AsRef<Path> + Send + Sync;

    /// Create a FileSeries from items with automatic temporal extraction
    async fn create_series_from_items<T, P>(
        &self,
        path: P,
        items: &[T],
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync;

    /// Write a new version to a FileSeries (create or append).
    /// If the file doesn't exist, creates it. If it exists, appends a new version.
    /// Uses Arrow compute kernels to extract min/max from the timestamp column.
    async fn write_series_from_batch<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        P: AsRef<Path> + Send + Sync;

    /// Write a new version to a FileSeries from items (create or append).
    async fn write_series_from_items<T, P>(
        &self,
        path: P,
        items: &[T],
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync;

    /// Create a new FileSeries by streaming RecordBatches through ArrowWriter.
    ///
    /// This is the preferred method for compacting multi-version series: read all
    /// versions through a DataFusion table provider (which merges them), then write
    /// as a single fresh version via this method.
    ///
    /// Temporal bounds are extracted from the completed parquet metadata (column
    /// statistics), so no manual min/max tracking is needed.
    async fn create_series_from_stream<P>(
        &self,
        path: P,
        stream: datafusion::physical_plan::SendableRecordBatchStream,
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        P: AsRef<Path> + Send + Sync;
}

#[async_trait::async_trait]
impl ParquetExt for WD {
    async fn create_table_from_items<T, P>(
        &self,
        path: P,
        items: &[T],
        entry_type: EntryType,
    ) -> Result<()>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync,
    {
        let fields = T::for_arrow();
        let batch = serde_arrow::to_record_batch(&fields, &items)
            .map_other_context("Failed to serialize to arrow")?;
        self.create_table_from_batch(path, &batch, entry_type).await
    }

    async fn read_table_as_items<T, P>(&self, path: P) -> Result<Vec<T>>
    where
        T: for<'a> Deserialize<'a> + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync,
    {
        let batch = self.read_table_as_batch(path).await?;
        let items = serde_arrow::from_record_batch(&batch)
            .map_other_context("Failed to deserialize from arrow")?;
        Ok(items)
    }

    async fn create_table_from_batch<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        entry_type: EntryType,
    ) -> Result<()>
    where
        P: AsRef<Path> + Send + Sync,
    {
        let buffer = serialize_batch_to_parquet(batch)?;

        let (_, mut writer) = self
            .create_file_path_streaming_with_type(&path, entry_type)
            .await?;
        writer
            .write_all(&buffer)
            .await
            .map_other_context("Write to TinyFS error")?;
        writer
            .shutdown()
            .await
            .map_other_context("Shutdown writer error")?;

        Ok(())
    }

    async fn read_table_as_batch<P>(&self, path: P) -> Result<RecordBatch>
    where
        P: AsRef<Path> + Send + Sync,
    {
        let data = self.read_file_path_to_vec(&path).await?;
        parse_parquet_to_batch(data)
    }

    async fn create_series_from_batch<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        P: AsRef<Path> + Send + Sync,
    {
        let ts_col = timestamp_column.unwrap_or("timestamp");

        // Extract temporal bounds directly from the RecordBatch using Arrow kernels
        let (min_time, max_time) = extract_temporal_bounds_from_batch(batch, ts_col)?;

        // Serialize batch to parquet
        let buffer = serialize_batch_to_parquet(batch)?;

        // Write to TinyFS with temporal metadata
        let (_, mut writer) = self
            .create_file_path_streaming_with_type(&path, EntryType::TablePhysicalSeries)
            .await?;

        writer
            .write_all(&buffer)
            .await
            .map_other_context("Write to TinyFS error")?;

        // Set temporal metadata from Arrow kernels (no parquet parsing needed!)
        writer.set_temporal_metadata(min_time, max_time, ts_col.to_string());

        writer
            .shutdown()
            .await
            .map_other_context("Shutdown writer error")?;

        Ok((min_time, max_time))
    }

    async fn create_series_from_items<T, P>(
        &self,
        path: P,
        items: &[T],
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync,
    {
        let fields = T::for_arrow();
        let batch = serde_arrow::to_record_batch::<&[T]>(&fields, &items)
            .map_other_context("Failed to serialize to arrow")?;
        self.create_series_from_batch(path, &batch, timestamp_column)
            .await
    }

    async fn write_series_from_batch<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        P: AsRef<Path> + Send + Sync,
    {
        let ts_col = timestamp_column.unwrap_or("timestamp");

        // Extract temporal bounds directly from the RecordBatch using Arrow kernels
        let (min_time, max_time) = extract_temporal_bounds_from_batch(batch, ts_col)?;

        // Serialize batch to parquet
        let buffer = serialize_batch_to_parquet(batch)?;

        // Use async_writer_path_with_type: handles both create AND append cases
        let mut writer = self
            .async_writer_path_with_type(&path, EntryType::TablePhysicalSeries)
            .await?;

        writer
            .write_all(&buffer)
            .await
            .map_other_context("Write to TinyFS error")?;

        // Set temporal metadata from Arrow kernels
        writer.set_temporal_metadata(min_time, max_time, ts_col.to_string());

        writer
            .shutdown()
            .await
            .map_other_context("Shutdown writer error")?;

        Ok((min_time, max_time))
    }

    async fn write_series_from_items<T, P>(
        &self,
        path: P,
        items: &[T],
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync,
    {
        let fields = T::for_arrow();
        let batch = serde_arrow::to_record_batch::<&[T]>(&fields, &items)
            .map_other_context("Failed to serialize to arrow")?;
        self.write_series_from_batch(path, &batch, timestamp_column)
            .await
    }

    async fn create_series_from_stream<P>(
        &self,
        path: P,
        mut stream: datafusion::physical_plan::SendableRecordBatchStream,
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        P: AsRef<Path> + Send + Sync,
    {
        use futures::stream::StreamExt;

        let ts_col = timestamp_column.unwrap_or("timestamp");

        // Get first non-empty batch to determine schema.
        let first_batch = loop {
            match stream.next().await {
                Some(Ok(batch)) if batch.num_rows() != 0 => break batch,
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    return Err(crate::Error::Other(format!(
                        "Failed to read first batch: {e}"
                    )));
                }
                None => {
                    return Err(crate::Error::Other(
                        "Empty stream -- no data to write".to_string(),
                    ));
                }
            }
        };

        let mut writer =
            StreamingSeriesWriter::try_new(self, path, first_batch.schema(), ts_col).await?;
        writer.write(&first_batch).await?;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_other_context("Read batch error")?;
            writer.write(&batch).await?;
        }

        let (minimum, maximum, _) = writer.finish().await?;
        Ok((minimum, maximum))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use crate::EntryType;
    use crate::arrow::parquet::extract_temporal_bounds_from_batch;
    use crate::arrow::{ForArrow, ParquetExt};
    use crate::memory::new_fs;
    use arrow::datatypes::{DataType, Field, FieldRef};
    use arrow_array::{RecordBatch, record_batch};
    use log::debug;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestRecord {
        id: i64,
        name: String,
        score: Option<f64>,
    }

    impl ForArrow for TestRecord {
        fn for_arrow() -> Vec<FieldRef> {
            vec![
                Arc::new(Field::new("id", DataType::Int64, false)),
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(Field::new("score", DataType::Float64, true)),
            ]
        }
    }

    #[tokio::test]
    async fn test_full_parquet_roundtrip_with_forarrow() -> TestResult {
        let fs = new_fs().await;
        let wd = fs.root().await?;

        let test_data = vec![
            TestRecord {
                id: 1,
                name: "Alice".to_string(),
                score: Some(95.5),
            },
            TestRecord {
                id: 2,
                name: "Bob".to_string(),
                score: None,
            },
            TestRecord {
                id: 3,
                name: "Charlie".to_string(),
                score: Some(87.3),
            },
        ];

        let test_path = "test_records.parquet";
        wd.create_table_from_items(test_path, &test_data, EntryType::TablePhysicalVersion)
            .await?;
        let read_data: Vec<TestRecord> = wd.read_table_as_items(test_path).await?;

        assert_eq!(test_data.len(), read_data.len());
        for (original, read) in test_data.iter().zip(read_data.iter()) {
            assert_eq!(original, read);
        }

        debug!("[OK] Full ParquetExt ForArrow roundtrip successful!");
        Ok(())
    }

    #[tokio::test]
    async fn test_low_level_recordbatch_operations() -> Result<(), Box<dyn std::error::Error>> {
        let fs = new_fs().await;
        let wd = fs.root().await?;

        let batch = record_batch!(
            ("product", Utf8, ["Widget A", "Widget B", "Widget C"]),
            ("quantity", Int64, [100_i64, 250_i64, 75_i64]),
            ("price", Float64, [19.99, 15.50, 8.25])
        )?;

        let test_path = "products.parquet";
        wd.create_table_from_batch(test_path, &batch, EntryType::TablePhysicalVersion)
            .await?;
        let read_batch = wd.read_table_as_batch(test_path).await?;

        assert_eq!(batch.schema(), read_batch.schema());
        assert_eq!(batch.num_rows(), read_batch.num_rows());
        assert_eq!(batch.num_columns(), read_batch.num_columns());

        debug!("[OK] Low-level RecordBatch operations successful!");
        Ok(())
    }

    #[tokio::test]
    async fn test_extract_temporal_bounds_from_batch() -> Result<(), Box<dyn std::error::Error>> {
        use arrow_array::{Float64Array, Int64Array};
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100_i64, 500_i64, 200_i64, 300_i64])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        )?;

        let (min, max) = extract_temporal_bounds_from_batch(&batch, "timestamp")?;
        assert_eq!(min, 100);
        assert_eq!(max, 500);

        debug!("[OK] Temporal bounds extraction using Arrow kernels works!");
        Ok(())
    }

    #[tokio::test]
    async fn test_large_dataset_batching() -> Result<(), Box<dyn std::error::Error>> {
        let fs = new_fs().await;
        let wd = fs.root().await?;

        let large_data: Vec<TestRecord> = (0..2500)
            .map(|i| TestRecord {
                id: i,
                name: format!("User_{}", i),
                score: if i % 3 == 0 {
                    None
                } else {
                    Some((i as f64) * 0.1)
                },
            })
            .collect();

        let test_path = "large_dataset.parquet";
        wd.create_table_from_items(test_path, &large_data, EntryType::TablePhysicalVersion)
            .await?;
        let read_data: Vec<TestRecord> = wd.read_table_as_items(test_path).await?;

        assert_eq!(large_data.len(), read_data.len());
        assert_eq!(large_data[0], read_data[0]);
        assert_eq!(large_data[1000], read_data[1000]);
        assert_eq!(large_data[2499], read_data[2499]);

        debug!("[OK] Large dataset batching successful!");
        Ok(())
    }

    #[tokio::test]
    async fn test_entry_type_integration() -> Result<(), Box<dyn std::error::Error>> {
        let fs = new_fs().await;
        let wd = fs.root().await?;

        let test_data = vec![
            TestRecord {
                id: 1,
                name: "Entry1".to_string(),
                score: Some(100.0),
            },
            TestRecord {
                id: 2,
                name: "Entry2".to_string(),
                score: Some(200.0),
            },
        ];

        // Test with FileTable entry type
        let table_path = "table_entries.parquet";
        wd.create_table_from_items(table_path, &test_data, EntryType::TablePhysicalVersion)
            .await?;

        // Test with FileData entry type
        let data_path = "data_entries.parquet";
        wd.create_table_from_items(data_path, &test_data, EntryType::FilePhysicalVersion)
            .await?;

        let table_data: Vec<TestRecord> = wd.read_table_as_items(table_path).await?;
        let data_data: Vec<TestRecord> = wd.read_table_as_items(data_path).await?;

        assert_eq!(test_data, table_data);
        assert_eq!(test_data, data_data);

        debug!("[OK] Entry type integration successful!");
        Ok(())
    }

    #[tokio::test]
    async fn test_write_series_create_and_append() -> Result<(), Box<dyn std::error::Error>> {
        use arrow_array::{Float64Array, Int64Array};
        use arrow_schema::{DataType, Field, Schema};

        let fs = new_fs().await;
        let wd = fs.root().await?;

        // Create parent directory first
        let _ = wd.create_dir_path("sensor").await?;

        let series_path = "sensor/data.series";

        // First write: creates the series file
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100_i64, 200_i64])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        )?;

        let (min1, max1) = wd
            .write_series_from_batch(series_path, &batch1, Some("timestamp"))
            .await?;
        assert_eq!(min1, 100);
        assert_eq!(max1, 200);
        debug!(
            "[OK] First write (create) successful: min={}, max={}",
            min1, max1
        );

        // Second write: appends to existing series (should NOT fail with AlreadyExists)
        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![300_i64, 400_i64])),
                Arc::new(Float64Array::from(vec![3.0, 4.0])),
            ],
        )?;

        let (min2, max2) = wd
            .write_series_from_batch(series_path, &batch2, Some("timestamp"))
            .await?;
        assert_eq!(min2, 300);
        assert_eq!(max2, 400);
        debug!(
            "[OK] Second write (append) successful: min={}, max={}",
            min2, max2
        );

        debug!("[OK] write_series_from_batch handles both create and append!");
        Ok(())
    }
}
