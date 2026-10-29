// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use std::io::{self, Write};

use crate::common::{ShipContext, TargetContext, classify_target};
use log::debug;

/// Resolve "auto" format to "table" or "raw" based on content.
///
/// When format is "auto":
///   - Queryable content uses a human-readable table, including through pipes
///   - Non-queryable content retains its raw bytes
fn resolve_auto_format(format: &str, is_queryable: bool) -> &str {
    if format != "auto" {
        return format;
    }
    if is_queryable { "table" } else { "raw" }
}

/// Handle output from a DataFusion stream based on display mode
/// Consolidates common output logic used by all cat operations
#[allow(clippy::print_stdout)]
async fn handle_stream_output(
    mut stream: datafusion::physical_plan::SendableRecordBatchStream,
    display: &str,
    _sql_query: Option<&str>,
    output: Option<&mut String>,
    source_desc: &str, // For debug messages: "url" or "path"
) -> Result<()> {
    if display == "table" {
        // Collect batches for table formatting
        let mut batches = Vec::new();
        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| anyhow::anyhow!("Failed to process batch from stream: {}", e))?;
            total_rows += batch.num_rows();
            batches.push(batch);
        }

        // Format and display the results as ASCII table
        let formatted = format_query_results(&batches).map_err(|e| {
            anyhow::anyhow!(
                "Failed to format query results for '{}': {}",
                source_desc,
                e
            )
        })?;

        if let Some(output_buffer) = output {
            output_buffer.push_str(&formatted);
        } else {
            print!("{}", formatted);
        }

        let batch_count = batches.len();
        debug!(
            "Successfully formatted {total_rows} rows in {batch_count} batches from: {source_desc}"
        );
    } else if display == "csv" {
        if let Some(output) = output {
            let mut buffer = Vec::new();
            write_stream_to_csv(stream, &mut buffer).await?;
            output.push_str(std::str::from_utf8(&buffer).map_err(|e| {
                anyhow::anyhow!("CSV output for '{source_desc}' is not valid UTF-8: {e}")
            })?);
        } else {
            write_stream_to_csv(stream, io::stdout()).await?;
        }
    } else if display == "raw" {
        if let Some(output) = output {
            // Testing mode - collect batches then write to buffer
            let mut batches = Vec::new();
            while let Some(batch_result) = stream.next().await {
                let batch =
                    batch_result.map_err(|e| anyhow::anyhow!("Failed to read batch: {}", e))?;
                batches.push(batch);
            }
            write_batches_to_buffer(&batches, output)?;
        } else {
            // Production mode - stream directly to stdout
            write_stream_to_stdout(stream).await?;
        }
    } else {
        return Err(anyhow::anyhow!(
            "unknown cat output format '{display}'; expected auto, raw, table, or csv"
        ));
    }

    Ok(())
}

async fn write_stream_to_csv<W: Write>(
    mut stream: datafusion::physical_plan::SendableRecordBatchStream,
    output: W,
) -> Result<()> {
    let schema = stream.schema();
    let mut writer = arrow_csv::WriterBuilder::new()
        .with_header(true)
        .build(output);
    let mut wrote_batch = false;
    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| anyhow::anyhow!("Failed to read batch for CSV: {e}"))?;
        writer
            .write(&batch)
            .map_err(|e| anyhow::anyhow!("Failed to write CSV batch: {e}"))?;
        wrote_batch = true;
    }
    if !wrote_batch {
        writer
            .write(&RecordBatch::new_empty(schema))
            .map_err(|e| anyhow::anyhow!("Failed to write empty CSV result: {e}"))?;
    }
    Ok(())
}

/// Write stream directly to stdout as parquet
async fn write_stream_to_stdout(
    mut stream: datafusion::physical_plan::SendableRecordBatchStream,
) -> Result<()> {
    // Get the first batch to determine schema
    let first_batch = match stream.next().await {
        Some(Ok(batch)) => batch,
        Some(Err(e)) => return Err(anyhow::anyhow!("Failed to read first batch: {}", e)),
        None => {
            debug!("Empty stream, no parquet output");
            return Ok(());
        }
    };

    let schema = first_batch.schema();
    let stdout = io::stdout();
    let mut writer = ArrowWriter::try_new(stdout, schema, None)
        .map_err(|e| anyhow::anyhow!("Failed to create parquet writer: {}", e))?;

    let mut total_rows = first_batch.num_rows();
    let mut batch_count = 1;

    writer
        .write(&first_batch)
        .map_err(|e| anyhow::anyhow!("Failed to write batch to parquet: {}", e))?;

    // Stream remaining batches
    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| anyhow::anyhow!("Failed to read batch from stream: {}", e))?;
        total_rows += batch.num_rows();
        batch_count += 1;
        writer
            .write(&batch)
            .map_err(|e| anyhow::anyhow!("Failed to write batch to parquet: {}", e))?;
    }

    let _metadata = writer
        .close()
        .map_err(|e| anyhow::anyhow!("Failed to close parquet writer: {}", e))?;

    debug!(
        "Streamed parquet data to stdout ({} rows in {} batches)",
        total_rows, batch_count
    );
    Ok(())
}

/// Write batches to in-memory buffer as parquet
fn write_batches_to_buffer(batches: &[RecordBatch], output: &mut String) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let mut buffer = Vec::new();
    let schema = batches[0].schema();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, None)
        .map_err(|e| anyhow::anyhow!("Failed to create parquet writer: {}", e))?;

    let mut total_rows = 0;
    for batch in batches {
        total_rows += batch.num_rows();
        writer
            .write(batch)
            .map_err(|e| anyhow::anyhow!("Failed to write batch to parquet: {}", e))?;
    }

    let _metadata = writer
        .close()
        .map_err(|e| anyhow::anyhow!("Failed to close parquet writer: {}", e))?;

    output.push_str(&String::from_utf8_lossy(&buffer));
    debug!(
        "Buffered {} bytes of parquet data for testing ({} rows in {} batches)",
        buffer.len(),
        total_rows,
        batches.len()
    );
    Ok(())
}

/// Cat file with optional SQL query
pub async fn cat_command(
    ship_context: &ShipContext,
    path: &str,
    display: &str, // Output format: "auto", "raw" (parquet bytes) or "table" (ASCII formatted)
    output: Option<&mut String>,
    _time_start: Option<i64>,
    _time_end: Option<i64>,
    sql_query: Option<&str>,
    explain: bool,
) -> Result<()> {
    debug!(
        "cat_command called with path: {}, sql_query: {:?}",
        path, sql_query
    );

    // Classify the path to determine pond vs host context
    let target = classify_target(path)?;

    let mut ship = match &target {
        TargetContext::Host(_) => ship_context.open_host()?,
        TargetContext::Pond(_) => ship_context.open_pond().await?,
    };

    crate::common::with_read_transaction(
        &mut ship,
        ship_context.original_args.clone(),
        async |tx| {
            if explain {
                explain_impl(tx, path, sql_query).await
            } else {
                cat_impl(tx, path, display, output, sql_query).await
            }
        },
    )
    .await
}

/// Internal implementation of cat command
#[allow(clippy::print_stdout)]
async fn cat_impl(
    tx: &mut steward::Transaction<'_>,
    path: &str,
    display: &str,
    output: Option<&mut String>,
    sql_query: Option<&str>,
) -> Result<()> {
    // Normalize bare paths to file:// URLs, keep URLs with schemes as-is
    let url = if path.contains("://") {
        path.to_string()
    } else {
        // Ensure path starts with / for absolute paths
        let normalized_path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{}", path)
        };
        format!("file://{}", normalized_path)
    };

    // Parse the URL for routing decisions (handles host+scheme:///path, scheme:///path, etc.)
    let parsed_url =
        provider::Url::parse(&url).map_err(|e| anyhow::anyhow!("Invalid URL '{}': {}", path, e))?;

    debug!(
        "cat_impl processing URL: {} (scheme={}, is_host={})",
        parsed_url,
        parsed_url.scheme(),
        parsed_url.is_host()
    );

    // For file:// scheme (raw data access), check if the file is queryable.
    // This handles both pond paths (file:///path) and host paths (host+file:///path).
    if parsed_url.scheme() == "file" && parsed_url.entry_type().is_none() {
        let fs = &**tx;
        let root = fs.root().await?;
        let file_path = parsed_url.path();

        let metadata = root
            .metadata_for_path(file_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get metadata for '{}': {}", path, e))?;

        let is_queryable =
            metadata.entry_type.is_table_file() || metadata.entry_type.is_series_file();

        let effective_display = resolve_auto_format(display, is_queryable);

        if !is_queryable {
            // Non-queryable file types (file:data) - use simple streaming
            if effective_display == "table" {
                return Err(anyhow::anyhow!(
                    "Table display mode only supports file:table and file:series types, but this file is {:?}",
                    metadata.entry_type
                ));
            }
            return stream_file_to_stdout(&root, file_path, output).await;
        }
    }

    // All queryable types go through Provider (unified path)
    let fs = &**tx;
    let fs_arc = std::sync::Arc::new(fs.clone());

    // Get ProviderContext from transaction (includes registered object stores)
    let provider_context = std::sync::Arc::new(tx.provider_context()?.clone());

    // Create Provider with context (supports all types including file://)
    let provider = provider::Provider::with_context(fs_arc, provider_context.clone());

    // Use DataFusion context from ProviderContext (has TinyFS object store registered)
    let ctx = provider_context.datafusion_session.as_ref().clone();

    // Create unified table provider (glob-cache for external formats, UNION fallback otherwise)
    let unified_table_provider = provider
        .create_provider_for_url(&url, &ctx)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create table provider for URL '{}': {}", url, e))?;

    let _ = ctx.register_table("source", unified_table_provider)?;

    let effective_sql_query = sql_query.unwrap_or("SELECT * FROM source");
    debug!("Executing SQL query: {}", effective_sql_query);

    let df = ctx.sql(effective_sql_query).await.map_err(|e| {
        anyhow::anyhow!(
            "Failed to execute SQL query '{}': {}",
            effective_sql_query,
            e
        )
    })?;

    let stream = df.execute_stream().await?;

    // Deregister "source" now that the execution plan holds its own
    // Arc to the table provider -- prevents stale bindings from
    // leaking into later queries within the same session
    let _ = ctx.deregister_table("source");

    // Use consolidated output handler
    // If we reach here, content is queryable (non-queryable returned early above)
    let effective_display = resolve_auto_format(display, true);
    handle_stream_output(stream, effective_display, sql_query, output, path).await?;

    Ok(())
}

/// Explain mode: show entry type, provider resolution, and schema without executing
#[allow(clippy::print_stdout)]
async fn explain_impl(
    tx: &mut steward::Transaction<'_>,
    path: &str,
    sql_query: Option<&str>,
) -> Result<()> {
    let url = if path.contains("://") {
        path.to_string()
    } else {
        let normalized_path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{}", path)
        };
        format!("file://{}", normalized_path)
    };

    let parsed_url =
        provider::Url::parse(&url).map_err(|e| anyhow::anyhow!("Invalid URL '{}': {}", path, e))?;

    println!("=== pond cat --explain ===\n");
    println!("Path:   {}", path);
    println!("URL:    {}", parsed_url);
    println!("Scheme: {}", parsed_url.scheme());
    println!("Host:   {}", parsed_url.is_host());

    // Try to get entry type for file:// paths
    if parsed_url.scheme() == "file" && parsed_url.entry_type().is_none() {
        let fs = &**tx;
        let root = fs.root().await?;
        let file_path = parsed_url.path();

        match root.metadata_for_path(file_path).await {
            Ok(metadata) => {
                let type_name = match metadata.entry_type {
                    tinyfs::EntryType::DirectoryPhysical => "directory",
                    tinyfs::EntryType::DirectoryDynamic => "dynamic-directory",
                    tinyfs::EntryType::Symlink => "symlink",
                    tinyfs::EntryType::FilePhysicalVersion => "data",
                    tinyfs::EntryType::FilePhysicalSeries => "data:series",
                    tinyfs::EntryType::FileDynamic => "data:dynamic",
                    tinyfs::EntryType::TablePhysicalVersion => "table",
                    tinyfs::EntryType::TablePhysicalSeries => "table:series",
                    tinyfs::EntryType::TableDynamic => "table:dynamic",
                };
                println!("Type:   {}", type_name);

                let is_queryable =
                    metadata.entry_type.is_table_file() || metadata.entry_type.is_series_file();
                println!("Query:  {}", if is_queryable { "yes" } else { "no" });

                if !is_queryable {
                    if let Some(size) = metadata.size {
                        println!("Size:   {} bytes", size);
                    }
                    println!("\nThis file outputs raw bytes (not queryable via SQL).");
                    return Ok(());
                }
            }
            Err(e) => {
                println!("Type:   (unknown: {})", e);
            }
        }
    } else if let Some(et) = parsed_url.entry_type() {
        println!("Type:   {:?} (from URL)", et);
    }

    // Create provider and get schema
    let fs = &**tx;
    let fs_arc = std::sync::Arc::new(fs.clone());
    let provider_context = std::sync::Arc::new(tx.provider_context()?.clone());
    let provider = provider::Provider::with_context(fs_arc, provider_context.clone());
    let ctx = provider_context.datafusion_session.as_ref().clone();

    let unified_table_provider = provider
        .create_provider_for_url(&url, &ctx)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create table provider for URL '{}': {}", url, e))?;

    let schema = unified_table_provider.schema();
    println!("\nSchema: {} fields", schema.fields().len());
    for field in schema.fields() {
        println!("  {:30} {}", field.name(), field.data_type());
    }

    let _ = ctx.register_table("source", unified_table_provider)?;

    let effective_sql = sql_query.unwrap_or("SELECT * FROM source");
    println!("\nSQL:    {}", effective_sql);

    // Show the logical plan
    let df = ctx
        .sql(effective_sql)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to parse SQL query '{}': {}", effective_sql, e))?;

    let output_schema = df.schema();
    println!("\nOutput: {} fields", output_schema.fields().len());
    for field in output_schema.fields() {
        println!("  {:30} {}", field.name(), field.data_type());
    }

    let _ = ctx.deregister_table("source");

    Ok(())
}

/// Write RecordBatch stream as parquet format to output (streaming, low memory)
async fn stream_file_to_stdout(
    root: &tinyfs::WD,
    path: &str,
    mut output: Option<&mut String>,
) -> Result<()> {
    use std::pin::Pin;
    use tokio::io::AsyncReadExt;

    let mut reader: Pin<Box<dyn tinyfs::AsyncReadSeek>> = root
        .async_reader_path(path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open file for reading: {}", e))?;

    // Stream in chunks rather than loading entire file
    let mut buffer = vec![0u8; 8192]; // 8KB chunks
    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read from file: {}", e))?;
        if bytes_read == 0 {
            break; // EOF
        }
        if let Some(out) = output.as_deref_mut() {
            out.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));
        } else {
            io::stdout()
                .write_all(&buffer[..bytes_read])
                .map_err(|e| anyhow::anyhow!("Failed to write to stdout: {}", e))?;
        }
    }
    Ok(())
}

/// Format RecordBatch results as a pretty-printed string with row summary
///
/// This is a presentation function specific to the cat command for displaying
/// query results in a human-readable format. Includes column types in headers
/// and a "Summary: X total rows" line at the end for compatibility with existing tests.
///
/// # Arguments
/// * `batches` - Vector of RecordBatch results to format
///
/// # Returns
/// String containing the formatted table output with row summary
fn format_query_results(batches: &[RecordBatch]) -> Result<String> {
    use arrow::util::pretty::pretty_format_batches_with_options;
    use arrow_cast::display::FormatOptions;

    if batches.is_empty() {
        return Ok("No data found\n".to_string());
    }

    // Use FormatOptions to show column types and handle errors gracefully
    let options = FormatOptions::default()
        .with_display_error(true)
        .with_types_info(true) // This shows column types in the headers
        .with_null("NULL"); // Show NULL values clearly

    let formatted = pretty_format_batches_with_options(batches, &options)
        .map_err(|e| anyhow::anyhow!("Failed to format query results: {}", e))?
        .to_string();

    // Calculate total row count across all batches
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Append row summary for compatibility with existing tests
    let result = if total_rows > 0 {
        format!(
            "{}\nSummary: {} total rows",
            formatted.trim_end(),
            total_rows
        )
    } else {
        "No data found\n".to_string()
    };

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use arrow_array::record_batch;
    use tempfile::TempDir;
    use tinyfs::ResultExt;
    use tinyfs::arrow::ParquetExt;

    #[test]
    fn auto_format_keeps_queryable_output_textual_for_pipes() {
        assert_eq!(resolve_auto_format("auto", true), "table");
        assert_eq!(resolve_auto_format("auto", false), "raw");
        assert_eq!(resolve_auto_format("raw", true), "raw");
        assert_eq!(resolve_auto_format("table", false), "table");
        assert_eq!(resolve_auto_format("csv", true), "csv");
    }

    /// Test setup helper - creates pond and returns context for testing
    struct TestSetup {
        pond_path: std::path::PathBuf,
        test_content: String,
        _temp_dir: TempDir,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            // Create a temporary directory for the test pond
            let temp_dir = TempDir::new()?;
            // Use a unique subdirectory to avoid any conflicts
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let pond_path = temp_dir.path().join(format!("test_pond_{}", timestamp));

            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::pond_only(Some(&pond_path), init_args.clone());

            // Initialize the pond
            init_command(&ship_context, "test-host").await?;

            let test_content =
                "timestamp,value\n2024-01-01T00:00:00Z,42.0\n2024-01-01T01:00:00Z,43.5\n";

            Ok(TestSetup {
                pond_path,
                test_content: test_content.to_string(),
                _temp_dir: temp_dir,
            })
        }

        /// Write test data as raw text file (file:data type)
        async fn write_text_file(&self, filename: &str) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

            let test_content = self.test_content.clone();
            let filename = filename.to_string();

            ship.write_transaction(
                &steward::PondUserMetadata::new(vec!["test".to_string(), "write".to_string()]),
                async |fs| {
                    let root = fs.root().await?;

                    root.write_file_path_from_slice(&filename, test_content.as_bytes())
                        .await?;

                    Ok(())
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write test data: {}", e))?;

            Ok(())
        }

        /// Write test data as parquet table (file:table type)
        async fn write_parquet_table(&self, filename: &str) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

            let filename = filename.to_string();

            ship.write_transaction(
                &steward::PondUserMetadata::new(vec![
                    "test".to_string(),
                    "write_table".to_string(),
                ]),
                async |fs| {
                    let root = fs.root().await?;

                    // Create Arrow RecordBatch from our test data
                    let batch = record_batch!(
                        (
                            "timestamp",
                            Utf8,
                            ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]
                        ),
                        ("value", Float64, [42.0_f64, 43.5_f64])
                    )
                    .map_other_context("Arrow error")?;

                    // Write as parquet table using ParquetExt
                    root.create_table_from_batch(
                        &filename,
                        &batch,
                        tinyfs::EntryType::TablePhysicalVersion,
                    )
                    .await?;

                    Ok(())
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write parquet table: {}", e))?;

            Ok(())
        }

        /// Write test data as parquet series (file:series type) - single version
        async fn write_parquet_series(&self, filename: &str) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

            let filename = filename.to_string();

            ship.write_transaction(
                &steward::PondUserMetadata::new(vec![
                    "test".to_string(),
                    "write_series".to_string(),
                ]),
                async |fs| {
                    let root = fs.root().await?;

                    // Create Arrow RecordBatch with temporal data for series using proper timestamp type
                    let batch = record_batch!(
                        ("timestamp", Int64, [1704067200000_i64, 1704070800000_i64]), // 2024-01-01 timestamps in milliseconds
                        ("value", Float64, [42.0_f64, 43.5_f64])
                    )
                    .map_other_context("Arrow error")?;

                    // Write as file:series using ParquetExt with temporal metadata extraction
                    let _ = root
                        .create_series_from_batch(&filename, &batch, Some("timestamp"))
                        .await?;

                    Ok(())
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write file series: {}", e))?;

            Ok(())
        }

        /// Create ShipContext for cat command
        fn cat_context(&self, filename: &str) -> ShipContext {
            let cat_args = vec!["pond".to_string(), "cat".to_string(), filename.to_string()];
            ShipContext::pond_only(Some(self.pond_path.clone()), cat_args)
        }

        /// Create expected DataFusion output for our test data using the same formatting path
        fn expected_datafusion_output(&self) -> Result<String> {
            // Create the same RecordBatch we use in write_parquet_table
            // Use Utf8View to match what parquet files actually produce
            let batch = record_batch!(
                (
                    "timestamp",
                    Utf8View,
                    ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]
                ),
                ("value", Float64, [42.0_f64, 43.5_f64])
            )
            .map_err(|e| anyhow::anyhow!("Arrow error: {}", e))?;

            // Format through the exact production code path so the expectation
            // cannot drift from cat's real table rendering.
            format_query_results(&[batch])
        }
    }

    #[tokio::test]
    async fn test_cat_command_text_file() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write text file
        setup.write_text_file("test_data.csv").await?;

        // Test cat command with output capture
        let cat_context = setup.cat_context("test_data.csv");
        let mut output_buffer = String::new();

        cat_command(
            &cat_context,
            "test_data.csv",
            "raw",
            Some(&mut output_buffer),
            None,
            None,
            None,
            false,
        )
        .await?;

        assert_eq!(
            output_buffer, setup.test_content,
            "Cat command output should exactly match file content"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_parquet_table() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write parquet table file
        setup.write_parquet_table("test_table.parquet").await?;

        // Get expected output from our baseline formatter
        let expected_output = setup.expected_datafusion_output()?;

        // Test cat command - this should use DataFusion SQL interface
        let cat_context = setup.cat_context("test_table.parquet");
        let mut output_buffer = String::new();

        cat_command(
            &cat_context,
            "test_table.parquet",
            "table", // Use table mode to get formatted output
            Some(&mut output_buffer),
            None,
            None,
            None,
            false,
        )
        .await?;

        // Test for exact equality with baseline
        assert_eq!(
            output_buffer.trim(),
            expected_output.trim(),
            "Cat command DataFusion output should exactly match baseline formatting"
        );

        debug!("[OK] DataFusion SQL output matches baseline!");

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_parquet_table_with_sql() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write parquet table file
        setup.write_parquet_table("test_table.parquet").await?;

        // Test cat command with custom SQL query
        let cat_args = vec![
            "pond".to_string(),
            "cat".to_string(),
            "test_table.parquet".to_string(),
            "--sql".to_string(),
            "SELECT timestamp, value * 2 as doubled_value FROM source WHERE value > 42".to_string(),
        ];
        let cat_context = ShipContext::pond_only(Some(setup.pond_path.clone()), cat_args);
        let mut output_buffer = String::new();

        cat_command(
            &cat_context,
            "test_table.parquet",
            "table",
            Some(&mut output_buffer),
            None,
            None,
            Some("SELECT timestamp, value * 2 as doubled_value FROM source WHERE value > 42"),
            false,
        )
        .await?;

        // Verify the output contains expected elements from the SQL query
        assert!(
            output_buffer.contains("doubled_value"),
            "Output should contain computed column name"
        );
        assert!(
            output_buffer.contains("87"),
            "Output should contain doubled value: 43.5 * 2 = 87"
        );
        assert!(
            !output_buffer.contains("42"),
            "Output should not contain filtered value (42)"
        );
        assert!(
            output_buffer.contains("Summary: 1 total rows"),
            "Output should contain filtered row count"
        );

        debug!("[OK] SQL query filtering and computation works!");

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_nonexistent_file() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Try to cat a non-existent file
        let cat_context = setup.cat_context("nonexistent.txt");
        let mut output_buffer = String::new();

        let result = cat_command(
            &cat_context,
            "nonexistent.txt",
            "raw",
            Some(&mut output_buffer),
            None,
            None,
            None,
            false,
        )
        .await;

        // Should fail with appropriate error
        assert!(
            result.is_err(),
            "Cat command should fail for non-existent file"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_text_file_raw_display() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a text file (file:data type) using the default test content
        setup.write_text_file("test_raw.txt").await?;

        let cat_context = setup.cat_context("test_raw.txt");
        let mut output_buffer = String::new();

        // Cat with raw display (should output the original text)
        cat_command(
            &cat_context,
            "test_raw.txt",
            "raw", // raw display mode
            Some(&mut output_buffer),
            None,
            None,
            None,
            false,
        )
        .await?;

        // Should get the exact same content as the test content
        assert_eq!(
            output_buffer, setup.test_content,
            "Raw display should output original text content"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_text_file_table_display_fails() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a text file (file:data type)
        setup.write_text_file("test_table_fail.txt").await?;

        let cat_context = setup.cat_context("test_table_fail.txt");
        let mut output_buffer = String::new();

        // Cat with table display should fail for file:data
        let result = cat_command(
            &cat_context,
            "test_table_fail.txt",
            "table", // table display mode on file:data
            Some(&mut output_buffer),
            None,
            None,
            None,
            false,
        )
        .await;

        // Should fail because text files can't be displayed as tables
        assert!(
            result.is_err(),
            "Table display should fail for file:data (text files)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_parquet_table_raw_display() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a parquet table (file:table type)
        setup.write_parquet_table("series_raw.parquet").await?;

        // Instead of testing the cat command output (which goes through String conversion),
        // let's verify the file was stored correctly as raw parquet data
        let mut ship = steward::Ship::open_pond(&setup.pond_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

        let tx = ship
            .begin_read(&steward::PondUserMetadata::new(vec![
                "test".to_string(),
                "read".to_string(),
            ]))
            .await?;
        let fs = &*tx;
        let root = fs.root().await?;

        // Read the raw file content as bytes
        let raw_content = root
            .read_file_path_to_vec("series_raw.parquet")
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read file: {}", e))?;

        _ = tx.commit().await?;

        // Verify it starts with PAR1 magic bytes
        assert!(
            raw_content.len() >= 4,
            "File should be at least 4 bytes long"
        );
        let magic_bytes = &raw_content[0..4];
        assert_eq!(
            magic_bytes, b"PAR1",
            "File should start with PAR1 magic bytes"
        );

        // Verify we can read it as parquet and get our test data back
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tokio_util::bytes::Bytes;

        let bytes = Bytes::from(raw_content);
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| anyhow::anyhow!("Failed to create arrow reader: {}", e))?
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build arrow reader: {}", e))?;

        let batch = arrow_reader
            .next()
            .ok_or_else(|| anyhow::anyhow!("Expected at least one record batch"))?
            .map_err(|e| anyhow::anyhow!("Failed to read record batch: {}", e))?;

        // Verify the schema matches our test data
        assert_eq!(batch.num_columns(), 2, "Should have 2 columns");
        assert_eq!(batch.num_rows(), 2, "Should have 2 rows");

        let schema = batch.schema();
        assert_eq!(
            schema.field(0).name(),
            "timestamp",
            "First column should be timestamp"
        );
        assert_eq!(
            schema.field(1).name(),
            "value",
            "Second column should be value"
        );

        // Verify the actual data values
        use arrow::array::{Float64Array, StringArray};
        let timestamp_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast timestamp column to StringArray"))?;
        let value_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast value column to Float64Array"))?;

        assert_eq!(
            timestamp_array.value(0),
            "2024-01-01T00:00:00Z",
            "First timestamp should match"
        );
        assert_eq!(
            timestamp_array.value(1),
            "2024-01-01T01:00:00Z",
            "Second timestamp should match"
        );
        assert_eq!(value_array.value(0), 42.0, "First value should match");
        assert_eq!(value_array.value(1), 43.5, "Second value should match");

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_parquet_table_table_display() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a parquet table (file:table type)
        setup.write_parquet_table("series_table.parquet").await?;

        let cat_context = setup.cat_context("series_table.parquet");
        let mut output_buffer = String::new();

        // Cat with table display (should format as text table)
        cat_command(
            &cat_context,
            "series_table.parquet",
            "table", // table display mode
            Some(&mut output_buffer),
            None,
            None,
            None,
            false,
        )
        .await?;

        // Should get formatted table output with column names and data
        assert!(
            output_buffer.contains("timestamp"),
            "Table display should contain timestamp column"
        );
        assert!(
            output_buffer.contains("value"),
            "Table display should contain value column"
        );
        assert!(
            output_buffer.contains("42"),
            "Table display should contain test data values"
        );
        assert!(
            output_buffer.contains("43.5"),
            "Table display should contain test data values"
        );
        assert!(
            output_buffer.contains("Summary: 2 total rows"),
            "Table display should contain row summary"
        );

        Ok(())
    }

    // ========== FILE:SERIES TESTS - Single Version ==========

    #[tokio::test]
    async fn test_cat_command_series_single_version_raw_display() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a single-version file:series
        setup.write_parquet_series("test_series.parquet").await?;

        // Test raw display mode - should show raw parquet bytes like file:table
        // Same approach as file:table test - read directly from storage to verify PAR1 magic bytes
        let mut ship = steward::Ship::open_pond(&setup.pond_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open pond: {}", e))?;

        let tx = ship
            .begin_read(&steward::PondUserMetadata::new(vec![
                "test".to_string(),
                "verify".to_string(),
            ]))
            .await?;
        let fs = &*tx;
        let root = fs.root().await?;

        // Read the raw file bytes
        let mut file_content = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            let mut reader = root
                .async_reader_path("test_series.parquet")
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open file for reading: {}", e))?;
            _ = reader
                .read_to_end(&mut file_content)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read file: {}", e))?;
        }

        _ = tx.commit().await?;

        // Verify PAR1 magic bytes
        assert!(file_content.len() > 4, "File should be larger than 4 bytes");
        assert_eq!(
            &file_content[0..4],
            b"PAR1",
            "File should start with PAR1 magic bytes"
        );

        // Verify we can read it as parquet and get the expected data
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tokio_util::bytes::Bytes;

        let bytes = Bytes::from(file_content);
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| anyhow::anyhow!("Failed to create arrow reader: {}", e))?
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build arrow reader: {}", e))?;

        let batch = arrow_reader
            .next()
            .ok_or_else(|| anyhow::anyhow!("Expected at least one record batch"))?
            .map_err(|e| anyhow::anyhow!("Failed to read record batch: {}", e))?;

        // Verify the schema matches our test data
        assert_eq!(batch.num_columns(), 2, "Should have 2 columns");
        assert_eq!(batch.num_rows(), 2, "Should have 2 rows");

        let schema = batch.schema();
        assert_eq!(
            schema.field(0).name(),
            "timestamp",
            "First column should be timestamp"
        );
        assert_eq!(
            schema.field(1).name(),
            "value",
            "Second column should be value"
        );

        // Verify the actual data values - single version series data
        use arrow::array::{Float64Array, Int64Array};
        let timestamp_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast timestamp column to Int64Array"))?;
        let value_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast value column to Float64Array"))?;

        assert_eq!(
            timestamp_array.value(0),
            1704067200000_i64,
            "First timestamp should match"
        );
        assert_eq!(
            timestamp_array.value(1),
            1704070800000_i64,
            "Second timestamp should match"
        );
        assert_eq!(value_array.value(0), 42.0, "First value should match");
        assert_eq!(value_array.value(1), 43.5, "Second value should match");

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_series_single_version_table_display() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a single-version file:series
        setup
            .write_parquet_series("test_series_table.parquet")
            .await?;

        let cat_context = setup.cat_context("test_series_table.parquet");
        let mut output_buffer = String::new();

        // Cat with table display (should format as text table)
        cat_command(
            &cat_context,
            "test_series_table.parquet",
            "table", // table display mode
            Some(&mut output_buffer),
            None,
            None,
            None,
            false,
        )
        .await?;

        // Should get formatted table output with column names and data
        assert!(
            output_buffer.contains("timestamp"),
            "Table display should contain timestamp column"
        );
        assert!(
            output_buffer.contains("value"),
            "Table display should contain value column"
        );
        assert!(
            output_buffer.contains("42"),
            "Table display should contain test data values"
        );
        assert!(
            output_buffer.contains("43.5"),
            "Table display should contain test data values"
        );
        assert!(
            output_buffer.contains("Summary: 2 total rows"),
            "Table display should contain row summary"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_command_series_single_version_with_sql() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Write a single-version file:series with test data
        setup
            .write_parquet_series("test_series_sql.parquet")
            .await?;

        let cat_context = setup.cat_context("test_series_sql.parquet");
        let mut output_buffer = String::new();

        // Cat with custom SQL query
        cat_command(
            &cat_context,
            "test_series_sql.parquet",
            "table",
            Some(&mut output_buffer),
            None,
            None,
            Some("SELECT timestamp, value * 2 as doubled_value FROM source WHERE value > 40"),
            false,
        )
        .await?;

        // Verify the output contains expected elements from the SQL query
        assert!(
            output_buffer.contains("doubled_value"),
            "Output should contain computed column name"
        );
        assert!(
            output_buffer.contains("84"),
            "Output should contain doubled value: 42.0 * 2 = 84"
        );
        assert!(
            output_buffer.contains("87"),
            "Output should contain doubled value: 43.5 * 2 = 87"
        );
        assert!(
            !output_buffer.contains(" 40 ") && !output_buffer.contains(" 40.0"),
            "Output should not contain value 40 or 40.0 in data columns\nActual output:\n{}",
            output_buffer
        );
        assert!(
            output_buffer.contains("Summary: 2 total rows"),
            "Output should contain row count"
        );

        Ok(())
    }

    // --- Host filesystem cat tests (via hostmount) ---

    /// Test cat of a raw text file on the host filesystem via host+file:// URL
    #[tokio::test]
    async fn test_cat_host_raw_file() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("hello.txt");
        std::fs::write(&file_path, "Hello from the host filesystem!\n")?;

        // Create a ShipContext with no pond, just host root
        let ship_context = ShipContext::new(
            None::<&std::path::Path>,
            Some(temp_dir.path()),
            Vec::new(),
            vec!["pond".to_string(), "cat".to_string()],
        );

        let url = format!("host+file:///{}", "hello.txt");
        let mut output_buffer = String::new();

        cat_command(
            &ship_context,
            &url,
            "raw",
            Some(&mut output_buffer),
            None,
            None,
            None,
            false,
        )
        .await?;

        assert_eq!(
            output_buffer, "Hello from the host filesystem!\n",
            "Host cat should stream raw bytes for data files"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cat_invalid_host_xls_url_fails_before_opening_pond() {
        let ship_context = ShipContext::pond_only(Some("/does/not/exist"), Vec::new());
        for url in [
            "hostfs+xls://../../caspar_water_bank/sbmc_register.xls",
            "host+xls://../../caspar_water_bank/sbmc_register.xls",
        ] {
            let error = cat_command(&ship_context, url, "table", None, None, None, None, false)
                .await
                .expect_err("invalid URL")
                .to_string();
            assert!(error.contains("Invalid target URL"), "{error}");
            assert!(!error.contains("POND"), "{error}");
            assert!(!error.contains("Failed to initialize ship"), "{error}");
        }
    }

    #[tokio::test]
    async fn test_cat_host_xls_table_and_sql() -> Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(
            temp_dir.path().join("book.xls"),
            include_bytes!("../../../provider/tests/fixtures/multiple.xls"),
        )?;
        let ship_context = ShipContext::new(
            None::<&std::path::Path>,
            Some(temp_dir.path()),
            Vec::new(),
            vec!["pond".to_string(), "cat".to_string()],
        );
        for (sql, expected_rows) in [
            (None, 3),
            (Some("SELECT name, value FROM source WHERE value > 15"), 2),
        ] {
            let mut output = String::new();
            cat_command(
                &ship_context,
                "host+xls:///book.xls?sheet=Readings&batch_size=2",
                "table",
                Some(&mut output),
                None,
                None,
                sql,
                false,
            )
            .await?;
            assert!(output.contains("beta"));
            assert!(output.contains("gamma"));
            assert_eq!(output.contains("alpha"), sql.is_none());
            assert!(output.contains(&format!("Summary: {expected_rows} total rows")));
        }
        let mut output = String::new();
        cat_command(
            &ship_context,
            "host+xls:///book.xls?sheet=Readings",
            "csv",
            Some(&mut output),
            None,
            None,
            None,
            false,
        )
        .await?;
        assert_eq!(
            output.lines().next(),
            Some("name,value,active,when,mixed,late_mixed")
        );
        assert!(output.contains("alpha,10"));
        Ok(())
    }

    /// Test cat of a CSV file on the host filesystem via host+csv:// URL
    #[tokio::test]
    async fn test_cat_host_csv_file() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let csv_content = "name,value\nalpha,1\nbeta,2\ngamma,3\n";
        let file_path = temp_dir.path().join("data.csv");
        std::fs::write(&file_path, csv_content)?;

        let ship_context = ShipContext::new(
            None::<&std::path::Path>,
            Some(temp_dir.path()),
            Vec::new(),
            vec!["pond".to_string(), "cat".to_string()],
        );

        let url = format!("host+csv:///{}", "data.csv");
        let mut output_buffer = String::new();

        cat_command(
            &ship_context,
            &url,
            "table",
            Some(&mut output_buffer),
            None,
            None,
            None,
            false,
        )
        .await?;

        // Verify the table output contains expected data
        assert!(
            output_buffer.contains("alpha"),
            "Output should contain 'alpha'. Got:\n{}",
            output_buffer
        );
        assert!(
            output_buffer.contains("beta"),
            "Output should contain 'beta'"
        );
        assert!(
            output_buffer.contains("gamma"),
            "Output should contain 'gamma'"
        );
        assert!(
            output_buffer.contains("Summary: 3 total rows"),
            "Output should show 3 rows. Got:\n{}",
            output_buffer
        );

        Ok(())
    }

    /// Test cat of a CSV file on the host filesystem with SQL query
    #[tokio::test]
    async fn test_cat_host_csv_file_with_sql() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let csv_content = "name,value\nalpha,10\nbeta,20\ngamma,30\n";
        let file_path = temp_dir.path().join("data.csv");
        std::fs::write(&file_path, csv_content)?;

        let ship_context = ShipContext::new(
            None::<&std::path::Path>,
            Some(temp_dir.path()),
            Vec::new(),
            vec!["pond".to_string(), "cat".to_string()],
        );

        let url = format!("host+csv:///{}", "data.csv");
        let mut output_buffer = String::new();

        cat_command(
            &ship_context,
            &url,
            "table",
            Some(&mut output_buffer),
            None,
            None,
            Some("SELECT name, value FROM source WHERE value > 15"),
            false,
        )
        .await?;

        // Should only contain beta and gamma (value > 15)
        assert!(
            !output_buffer.contains("alpha"),
            "Output should NOT contain 'alpha' (filtered out). Got:\n{}",
            output_buffer
        );
        assert!(
            output_buffer.contains("beta"),
            "Output should contain 'beta'"
        );
        assert!(
            output_buffer.contains("gamma"),
            "Output should contain 'gamma'"
        );
        assert!(
            output_buffer.contains("Summary: 2 total rows"),
            "Output should show 2 rows. Got:\n{}",
            output_buffer
        );

        Ok(())
    }

    /// Test cat of a host file with absolute path (no -d flag)
    #[tokio::test]
    async fn test_cat_host_absolute_path() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("abs_test.txt");
        std::fs::write(&file_path, "absolute path test\n")?;

        // No host_root -- host URLs use absolute paths
        let ship_context = ShipContext::new(
            None::<&std::path::Path>,
            None::<&std::path::Path>,
            Vec::new(),
            vec!["pond".to_string(), "cat".to_string()],
        );

        let url = format!("host+file://{}", file_path.display());
        let mut output_buffer = String::new();

        cat_command(
            &ship_context,
            &url,
            "raw",
            Some(&mut output_buffer),
            None,
            None,
            None,
            false,
        )
        .await?;

        assert_eq!(
            output_buffer, "absolute path test\n",
            "Host cat with absolute path should work"
        );

        Ok(())
    }

    /// Test cat of a nonexistent host file gives a clear error
    #[tokio::test]
    async fn test_cat_host_nonexistent_file() -> Result<()> {
        let temp_dir = TempDir::new()?;

        let ship_context = ShipContext::new(
            None::<&std::path::Path>,
            Some(temp_dir.path()),
            Vec::new(),
            vec!["pond".to_string(), "cat".to_string()],
        );

        let url = "host+file:///does_not_exist.txt";
        let mut output_buffer = String::new();

        let result = cat_command(
            &ship_context,
            url,
            "raw",
            Some(&mut output_buffer),
            None,
            None,
            None,
            false,
        )
        .await;

        assert!(result.is_err(), "Cat of nonexistent host file should error");

        Ok(())
    }
}
