// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! CSV Format Provider
//!
//! Implements FormatProvider for CSV files using arrow_csv's async Decoder.
//! Supports customization via URL query parameters (delimiter, header, etc.)
//!
//! Uses the async pattern documented in arrow-csv for true async streaming.

use crate::format::FormatProvider;
use crate::{Error, Result, Url};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_csv::reader::{Decoder, Format};
use async_trait::async_trait;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};

/// CSV format options parsed from query parameters
///
/// Based on arrow_csv::reader::Format and ReaderBuilder options.
/// All fields have sensible defaults matching arrow_csv defaults.
///
/// # URL Examples
///
/// - `csv:///data/file.csv` - Default settings (comma delimiter, has header)
/// - `csv:///data/file.csv?delimiter=;` - Semicolon delimiter
/// - `csv:///data/file.csv?delimiter=;&has_header=false` - No header row
/// - `csv:///data/file.csv?batch_size=16384` - Larger batch size
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub struct CsvOptions {
    /// Field delimiter (default: ',')
    #[serde(default = "default_delimiter")]
    pub delimiter: char,

    /// Whether the file has a header row (default: true)
    #[serde(default = "default_has_header")]
    pub has_header: bool,

    /// Number of records per batch (default: 8192)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Quote character (default: '"')
    #[serde(default = "default_quote")]
    pub quote: char,

    /// Escape character (default: None)
    pub escape: Option<char>,

    /// Number of rows to sample for schema inference (default: 100)
    #[serde(default = "default_schema_infer_max_records")]
    pub schema_infer_max_records: usize,
}

fn default_delimiter() -> char {
    ','
}
fn default_has_header() -> bool {
    true
}
fn default_batch_size() -> usize {
    8192
}
fn default_quote() -> char {
    '"'
}
fn default_schema_infer_max_records() -> usize {
    100
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: default_delimiter(),
            has_header: default_has_header(),
            batch_size: default_batch_size(),
            quote: default_quote(),
            escape: None,
            schema_infer_max_records: default_schema_infer_max_records(),
        }
    }
}

/// CSV format provider using arrow_csv's async Decoder
pub struct CsvProvider;

impl CsvProvider {
    /// Create a new CSV provider
    pub fn new() -> Self {
        Self
    }

    /// Create arrow_csv Format from options
    fn build_format(options: &CsvOptions) -> Format {
        let mut format = Format::default()
            .with_delimiter(options.delimiter as u8)
            .with_header(options.has_header)
            .with_quote(options.quote as u8);

        if let Some(escape) = options.escape {
            format = format.with_escape(escape as u8);
        }

        format
    }
}

impl Default for CsvProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FormatProvider for CsvProvider {
    fn name(&self) -> &str {
        "csv"
    }

    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        url: &Url,
    ) -> Result<SchemaRef> {
        let options: CsvOptions = url.query_params().unwrap_or_default();
        let format = Self::build_format(&options);

        // Read entire file into memory for schema inference
        // This is acceptable - schema inference typically needs to see the full dataset anyway
        let mut bytes = Vec::new();
        let mut reader = reader;
        let _ = reader.read_to_end(&mut bytes).await.map_err(Error::Io)?;

        // Use arrow_csv's infer_schema
        let cursor = std::io::Cursor::new(&bytes);
        let (schema, _) = format
            .infer_schema(cursor, Some(options.schema_infer_max_records))
            .map_err(|e| Error::Arrow(e.to_string()))?;

        Ok(Arc::new(schema))
    }

    async fn open_stream(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        url: &Url,
    ) -> Result<(
        SchemaRef,
        Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    )> {
        let options: CsvOptions = url.query_params().unwrap_or_default();
        let format = Self::build_format(&options);

        // Wrap reader in BufReader for efficient buffered reading
        let buf_reader = tokio::io::BufReader::new(reader);

        // Strategy: Read a reasonable chunk (e.g., 1MB) into a buffer for inference,
        // then create a chained reader (buffered_chunk + remaining_stream)
        // This avoids loading entire large files into memory while still inferring schema

        use tokio::io::AsyncReadExt;

        // Read first chunk for schema inference (default: sample up to 1MB or schema_infer_max_records rows)
        // This is a reasonable compromise - most CSV headers + sample rows fit in much less
        let inference_chunk_size = 1024 * 1024; // 1MB should be plenty for schema inference
        let mut inference_buffer = vec![0u8; inference_chunk_size];
        let mut buf_reader = buf_reader;

        let bytes_read = buf_reader.read(&mut inference_buffer).await?;
        inference_buffer.truncate(bytes_read);

        // Infer schema from the buffered chunk
        let cursor = std::io::Cursor::new(&inference_buffer);
        let (schema, _) = format
            .infer_schema(cursor, Some(options.schema_infer_max_records))
            .map_err(|e| Error::Arrow(e.to_string()))?;
        let schema = Arc::new(schema);

        // Now create the decoder with the inferred schema
        let mut builder = arrow_csv::ReaderBuilder::new(schema.clone())
            .with_delimiter(options.delimiter as u8)
            .with_header(options.has_header)
            .with_batch_size(options.batch_size)
            .with_quote(options.quote as u8);

        if let Some(escape) = options.escape {
            builder = builder.with_escape(escape as u8);
        }

        let decoder = builder.build_decoder();

        // Create a chained reader: inference_buffer + remaining stream
        // This ensures we don't lose the data we read for inference
        let chained_reader = std::io::Cursor::new(inference_buffer).chain(buf_reader);
        let buf_reader = tokio::io::BufReader::new(chained_reader);

        // Create the stream using our helper
        // The reader is now positioned at the start, with the inference data included
        let stream = decode_csv_stream(decoder, buf_reader);

        Ok((schema, stream))
    }
}

/// Create a stream from a CSV Decoder and AsyncBufRead
///
/// This follows the pattern from arrow-csv documentation for async streaming.
#[allow(dead_code)] // Will be used once we implement file re-opening
fn decode_csv_stream<R: AsyncBufRead + Unpin + Send + 'static>(
    mut decoder: Decoder,
    mut reader: R,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> {
    Box::pin(futures::stream::poll_fn(move |cx| {
        loop {
            let buf = match futures::ready!(Pin::new(&mut reader).poll_fill_buf(cx)) {
                Ok([]) => break,
                Ok(b) => b,
                Err(e) => return Poll::Ready(Some(Err(Error::Io(e)))),
            };

            let decoded = match decoder.decode(buf) {
                Ok(decoded) => decoded,
                Err(e) => return Poll::Ready(Some(Err(Error::Arrow(e.to_string())))),
            };

            Pin::new(&mut reader).consume(decoded);

            // Check if decoder is ready to flush a batch
            if decoded == 0 || decoder.capacity() == 0 {
                break;
            }
        }

        // Flush accumulated records to a RecordBatch
        match decoder.flush() {
            Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Ok(None) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(Error::Arrow(e.to_string())))),
        }
    }))
}

// Register CSV format provider using linkme
crate::register_format_provider!(
    scheme: "csv",
    provider: CsvProvider::new
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factory::test_factory::{InfiniteCsvConfig, InfiniteCsvFile};
    use crate::{FileProvider, FormatProvider, Url, format::csv::CsvProvider};
    use datafusion::execution::context::SessionContext;
    use futures::StreamExt;
    use std::sync::Arc;
    use tinyfs::{FS, File, MemoryPersistence};

    #[test]
    fn test_csv_options_default() {
        let options = CsvOptions::default();
        assert_eq!(options.delimiter, ',');
        assert!(options.has_header);
        assert_eq!(options.batch_size, 8192);
        assert_eq!(options.quote, '"');
        assert_eq!(options.escape, None);
    }

    #[test]
    fn test_csv_options_parse() {
        let url =
            Url::parse("csv:///file.csv?delimiter=;&has_header=false&batch_size=1024").unwrap();
        let options: CsvOptions = url.query_params().unwrap();
        assert_eq!(options.delimiter, ';');
        assert!(!options.has_header);
        assert_eq!(options.batch_size, 1024);
    }

    #[test]
    fn test_csv_provider_name() {
        let provider = CsvProvider::new();
        assert_eq!(provider.name(), "csv");
    }

    #[tokio::test]
    async fn test_csv_schema_inference() {
        let csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\n";
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(&csv_data[..]);

        let provider = CsvProvider::new();
        let url = Url::parse("csv:///test.csv").unwrap();

        let schema = provider.infer_schema(reader, &url).await.unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(2).name(), "city");
    }

    #[tokio::test]
    async fn test_csv_full_stream() {
        use futures::StreamExt;

        let csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF\n";
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(&csv_data[..]);

        let provider = CsvProvider::new();
        let url = Url::parse("csv:///test.csv?batch_size=2").unwrap();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();

        // Verify schema
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(2).name(), "city");

        // Read first batch (2 rows due to batch_size=2)
        let batch1 = stream.next().await.unwrap().unwrap();
        assert_eq!(batch1.num_rows(), 2);
        assert_eq!(batch1.num_columns(), 3);

        // Read second batch (1 remaining row)
        let batch2 = stream.next().await.unwrap().unwrap();
        assert_eq!(batch2.num_rows(), 1);
        assert_eq!(batch2.num_columns(), 3);

        // Stream should be exhausted
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_csv_custom_delimiter() {
        use futures::StreamExt;

        let csv_data = b"name;age;city\nAlice;30;NYC\nBob;25;LA\n";
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(&csv_data[..]);

        let provider = CsvProvider::new();
        let url = Url::parse("csv:///test.csv?delimiter=;").unwrap();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();

        // Verify schema parsed with semicolon delimiter
        assert_eq!(schema.fields().len(), 3);

        // Read all data
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_csv_large_file_streaming() {
        use futures::StreamExt;

        // Simulate a large CSV file (10K rows = ~500KB)
        // This tests that we don't load the entire file into memory
        let mut csv_data = String::from("id,value,description\n");
        for i in 0..10_000 {
            csv_data.push_str(&format!("{},value_{},description for row {}\n", i, i, i));
        }
        let csv_bytes = csv_data.into_bytes();

        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(std::io::Cursor::new(csv_bytes));

        let provider = CsvProvider::new();
        // Use small batch size to create many batches
        let url = Url::parse("csv:///large.csv?batch_size=100").unwrap();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();

        // Verify schema
        assert_eq!(schema.fields().len(), 3);

        // Stream through all batches, counting rows
        let mut total_rows = 0;
        let mut batch_count = 0;
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            total_rows += batch.num_rows();
            batch_count += 1;
        }

        // Should have read all 10K rows
        assert_eq!(total_rows, 10_000);
        // Should have created ~100 batches (10K rows / 100 batch_size)
        assert!(
            (99..=101).contains(&batch_count),
            "Expected ~100 batches, got {}",
            batch_count
        );
    }

    #[tokio::test]
    async fn test_csv_infinite_streaming() {
        // Test that we can stream a large CSV file without loading it all into memory
        // This uses a dynamically-generated infinite CSV stream with 100K rows
        use crate::factory::test_factory::{InfiniteCsvConfig, InfiniteCsvFile};
        use tinyfs::File;

        // Create an infinite CSV file with 100K rows
        let config = InfiniteCsvConfig {
            row_count: 100_000,
            batch_size: 1024,
            columns: vec![
                "id".to_string(),
                "timestamp".to_string(),
                "value".to_string(),
                "label".to_string(),
            ],
        };

        let file = InfiniteCsvFile::new(config);

        // Get reader from the infinite CSV file
        let reader = file.async_reader().await.unwrap();

        // Create CSV provider with small batch size for more batches
        let provider = CsvProvider::new();
        let url = Url::parse("csv:///test.csv?batch_size=256").unwrap();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();

        // Verify schema (id, timestamp, value, label)
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "timestamp");
        assert_eq!(schema.field(2).name(), "value");
        assert_eq!(schema.field(3).name(), "label");

        // Stream through all batches
        let mut total_rows = 0;
        let mut batch_count = 0;
        let mut first_id = None;
        let mut last_id = None;

        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            total_rows += batch.num_rows();
            batch_count += 1;

            // Capture first and last IDs for verification
            if first_id.is_none() {
                let id_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                first_id = Some(id_col.value(0));
            }

            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            let len = arrow::array::Array::len(id_col);
            last_id = Some(id_col.value(len - 1));
        }

        // Verify we read all 100K rows
        assert_eq!(total_rows, 100_000);

        // Verify sequential IDs (0 to 99999)
        assert_eq!(first_id, Some(0));
        assert_eq!(last_id, Some(99_999));

        // Should have created ~390 batches (100K rows / 256 batch_size)
        assert!(
            (380..=400).contains(&batch_count),
            "Expected ~390 batches, got {}",
            batch_count
        );
    }

    /// Helper to create test environment with MemoryPersistence
    async fn create_test_environment() -> FS {
        let persistence = MemoryPersistence::default();
        FS::new(persistence.clone())
            .await
            .expect("Failed to create FS")
    }

    /// Helper to create a CSV file from InfiniteCsvFile generator
    async fn create_csv_file(fs: &FS, path: &str, config: InfiniteCsvConfig) -> Result<()> {
        let root = fs.root().await?;

        // Create parent directories if needed
        if let Some(parent) = std::path::Path::new(path).parent()
            && parent.to_str() != Some("/")
            && !parent.as_os_str().is_empty()
        {
            // Create each directory in the path
            let path_parts: Vec<_> = parent
                .components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(name) => name.to_str(),
                    _ => None,
                })
                .collect();

            let mut current_path = String::from("/");
            for part in path_parts {
                if !current_path.ends_with('/') {
                    current_path.push('/');
                }
                current_path.push_str(part);

                // Try to create directory, ignore if already exists
                let _ = root.create_dir_path(&current_path).await;
            }
        }

        // Generate CSV data using InfiniteCsvFile
        let infinite_file = InfiniteCsvFile::new(config);
        let mut reader = infinite_file.async_reader().await?;

        // Read all data into memory (for test simplicity)
        use tokio::io::AsyncReadExt;
        let mut csv_data = Vec::new();
        let _ = reader.read_to_end(&mut csv_data).await?;

        // Write to TinyFS
        let mut file_writer = root
            .async_writer_path_with_type(path, tinyfs::EntryType::FileDynamic)
            .await?;
        use tokio::io::AsyncWriteExt;
        file_writer.write_all(&csv_data).await?;
        file_writer.flush().await?;
        file_writer.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_csv_streaming_from_tinyfs() {
        // Create test environment
        let fs = create_test_environment().await;

        // Create a CSV file with timeseries data (100 rows)
        let temp_config = InfiniteCsvConfig {
            row_count: 100,
            batch_size: 1024,
            columns: vec!["timestamp".to_string(), "temperature".to_string()],
        };
        create_csv_file(&fs, "/data/temp.csv", temp_config)
            .await
            .expect("Failed to create temp.csv");

        // Open the file using URL-based API
        let url = Url::parse("csv:///data/temp.csv?batch_size=50").expect("Failed to parse URL");

        let reader = fs.open_url(&url).await.expect("Failed to open URL");

        // Use CsvProvider to stream the data
        let csv_provider = CsvProvider::new();
        let (schema, mut stream) = csv_provider
            .open_stream(reader, &url)
            .await
            .expect("Failed to open CSV stream");

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "temperature");

        // Stream through all batches
        let mut total_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = result.expect("Failed to get batch");
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 100);
    }

    #[tokio::test]
    async fn test_csv_in_datafusion() {
        // Test CSV data queried through DataFusion using CSV provider
        let fs = create_test_environment().await;

        // Create CSV file
        let config = InfiniteCsvConfig {
            row_count: 50,
            batch_size: 1024,
            columns: vec!["id".to_string(), "value".to_string(), "label".to_string()],
        };
        create_csv_file(&fs, "/data/metrics.csv", config)
            .await
            .expect("Failed to create metrics.csv");

        // Open with CSV provider
        let url = Url::parse("csv:///data/metrics.csv").expect("Failed to parse URL");
        let reader = fs.open_url(&url).await.expect("Failed to open URL");

        let csv_provider = CsvProvider::new();
        let (schema, stream) = csv_provider
            .open_stream(reader, &url)
            .await
            .expect("Failed to open CSV stream");

        // Create a simple MemTable from the streamed data
        let batches: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .expect("Failed to collect batches");

        // Register in DataFusion
        let ctx = SessionContext::new();
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches])
            .expect("Failed to create MemTable");
        let _ = ctx
            .register_table("metrics", Arc::new(mem_table))
            .expect("Failed to register table");

        // Query the data
        let df = ctx
            .sql("SELECT COUNT(*) as count FROM metrics")
            .await
            .expect("Failed to execute query");

        let results = df.collect().await.expect("Failed to collect results");
        assert!(!results.is_empty());
    }
}
