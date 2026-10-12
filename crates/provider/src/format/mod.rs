// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Layer 2: Format Provider Traits
//!
//! Converts AsyncRead streams to Arrow RecordBatches for tabular formats.
//! Streaming formats decode incrementally; buffered formats such as hledger
//! parse their entire input first. Parquet uses the TinyFS ObjectStore path.
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
pub mod hledger;
pub mod jsonlogs;
pub mod oteljson;
pub mod weblog;

/// Format provider trait - converts file data to Arrow RecordBatches
///
/// Providers may decode incrementally or buffer their input for parsing.
/// Parquet uses the existing TinyFS ObjectStore path.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait FormatProvider: Send + Sync {
    /// Provider name (matches URL scheme)
    fn name(&self) -> &str;

    /// Open file and return schema + stream of RecordBatches
    ///
    /// Streaming providers decode incrementally; buffered providers finish
    /// parsing before returning the batch stream.
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
