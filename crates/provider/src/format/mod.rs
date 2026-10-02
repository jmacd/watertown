// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Layer 2: Format Provider Traits
//!
//! Converts AsyncRead streams to Arrow RecordBatches for tabular formats.
//! For STREAMING formats only (CSV, JSON, etc.) - formats that don't require seek.
//! Parquet and other random-access formats use existing TinyFS ObjectStore path.
//!
//! Uses arrow-csv/arrow-json Decoder pattern for truly async streaming.

use crate::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::Stream;
use std::pin::Pin;
use tokio::io::AsyncRead;

pub(crate) mod batch;
pub mod compression;
pub mod csv;
pub mod excelhtml;
pub mod jsonlogs;
pub mod oteljson;
pub mod weblog;

/// Format provider trait - converts file data to Arrow RecordBatches
///
/// For STREAMING formats only (CSV, JSON, etc.) - formats that don't require seek.
/// Parquet and other random-access formats use existing TinyFS ObjectStore path.
///
/// Uses arrow's native async Decoder pattern with AsyncBufRead.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait FormatProvider: Send + Sync {
    /// Provider name (matches URL scheme)
    fn name(&self) -> &str;

    /// Open file and return schema + stream of RecordBatches
    ///
    /// Uses arrow's async Decoder pattern for true async streaming.
    /// The returned stream decodes data incrementally without blocking.
    ///
    /// # Arguments
    ///
    /// * `reader` - AsyncRead stream (possibly decompressed via Layer 1)
    /// * `url` - Provider URL with format-specific query parameters
    ///
    /// # Returns
    ///
    /// Tuple of (schema, stream) where stream yields RecordBatches asynchronously.
    async fn open_stream(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        url: &crate::Url,
    ) -> Result<(
        SchemaRef,
        Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    )>;

    /// Infer schema without creating a stream
    ///
    /// For formats with known schemas (like OtelJson), this is trivial.
    /// For formats needing inference (CSV), may need to peek at data.
    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        url: &crate::Url,
    ) -> Result<SchemaRef>;
}
