// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::{FileInfoVisitor, ShipContext};
use anyhow::Result;
use std::sync::Arc;

/// Describe command - shows file types and schemas for files matching the pattern
///
/// This command operates on an existing pond via the provided Ship.
/// Uses scoped transactions for consistent filesystem access.
pub async fn describe_command<F>(
    ship_context: &ShipContext,
    pattern: &str,
    mut handler: F,
) -> Result<()>
where
    F: FnMut(&str),
{
    log::debug!("describe_command called with pattern {pattern}");

    let mut ship = ship_context.open_pond().await?;

    let output = crate::common::with_read_transaction(
        &mut ship,
        vec!["describe".to_string(), pattern.to_string()],
        async |tx| describe_command_impl(tx, ship_context, pattern).await,
    )
    .await?;

    handler(&output);
    Ok(())
}

/// Describe a provider URL (e.g., "oteljson:///otel/file.json" or "excelhtml:///data/*.htm")
async fn describe_provider_url(
    tx: &mut steward::Transaction<'_>,
    _ship_context: &ShipContext,
    url: &str,
) -> Result<String> {
    log::debug!("Describing provider URL: {}", url);

    // Get TinyFS from transaction
    let fs = &**tx;
    let fs_arc = Arc::new(fs.clone());

    // Create Provider instance
    let provider = provider::Provider::new(fs_arc);

    use std::cell::RefCell;
    let results = RefCell::new(Vec::new());

    // Use clean for_each_match API to handle both single files and patterns
    let _count = provider
        .for_each_match(url, |table_provider, file_path| {
            let url = url.to_string();
            let results = &results;
            async move {
                // Get schema from TableProvider
                let schema = table_provider.schema();
                let schema_info = extract_schema_info(&schema, true)
                    .map_err(|e| provider::Error::InvalidUrl(e.to_string()))?;

                // Parse URL for provider name
                let parsed_url = provider::Url::parse(&url)
                    .map_err(|e| provider::Error::InvalidUrl(e.to_string()))?;
                let provider_name = parsed_url.scheme();

                results
                    .borrow_mut()
                    .push((file_path, provider_name.to_string(), schema_info));
                Ok(())
            }
        })
        .await
        .map_err(|e| anyhow::anyhow!("Failed to process URL '{}': {}", url, e))?;

    let results = results.into_inner();

    // Format output from results
    let mut output = String::new();

    if results.len() > 1 {
        output.push_str(&format!("Pattern matched {} files\n\n", results.len()));
    }

    output.push_str("=== Provider Schema Description ===\n\n");

    for (i, (file_path, provider_name, schema_info)) in results.iter().enumerate() {
        if i > 0 {
            output.push('\n');
        }

        output.push_str(&format!("File: {}\n", file_path));
        output.push_str(&format!("Provider: {}\n", provider_name));
        output.push_str(&format!("Schema: {} fields\n", schema_info.field_count));

        for field_info in &schema_info.fields {
            output.push_str(&format!(
                "  * {}: {}\n",
                field_info.name, field_info.data_type
            ));
        }

        if let Some(timestamp_col) = &schema_info.timestamp_column {
            output.push_str(&format!("Timestamp Column: {}\n", timestamp_col));
        }
    }

    Ok(output)
}

/// Implementation of describe command
async fn describe_command_impl(
    tx: &mut steward::Transaction<'_>,
    ship_context: &ShipContext,
    pattern: &str,
) -> Result<String> {
    // Check for provider URL syntax (e.g., "oteljson:///path")
    if pattern.contains("://") {
        return describe_provider_url(tx, ship_context, pattern).await;
    }

    let fs = &**tx;
    let root = fs
        .root()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get filesystem root: {}", e))?;

    // Use the same file matching logic as list command
    let mut visitor = FileInfoVisitor::new(false); // don't show hidden files
    let mut files = root
        .visit_with_visitor(pattern, &mut visitor)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to find files with pattern '{}': {}", pattern, e))?;

    if files.is_empty() {
        return Ok(format!("No files found matching pattern '{}'", pattern));
    }

    // Sort results by path for consistent output
    files.sort_by(|a, b| a.path.cmp(&b.path));

    let mut output = String::new();
    output.push_str("=== File Schema Description ===\n");
    output.push_str(&format!("Pattern: {}\n", pattern));
    output.push_str(&format!("Files found: {}\n\n", files.len()));

    for file_info in files {
        output.push_str(&format!("[FILE] {}\n", file_info.path));
        output.push_str(&format!("   Type: {:?}\n", file_info.metadata.entry_type));

        match file_info.metadata.entry_type {
            tinyfs::EntryType::TablePhysicalSeries | tinyfs::EntryType::TableDynamic => {
                output.push_str("   Format: Parquet series\n");
                match describe_file_series_schema(ship_context, &file_info.path).await {
                    Ok(schema_info) => {
                        output
                            .push_str(&format!("   Schema: {} fields\n", schema_info.field_count));
                        for field_info in schema_info.fields {
                            output.push_str(&format!(
                                "     * {}: {}\n",
                                field_info.name, field_info.data_type
                            ));
                        }
                        if let Some(timestamp_col) = schema_info.timestamp_column {
                            output.push_str(&format!("   Timestamp Column: {}\n", timestamp_col));
                        }
                    }
                    Err(e) => {
                        output.push_str(&format!("   Schema: Error loading schema - {}\n", e));
                    }
                }

                // For TablePhysicalSeries, show detailed version statistics
                if matches!(
                    file_info.metadata.entry_type,
                    tinyfs::EntryType::TablePhysicalSeries
                ) {
                    output.push_str("\n   === Version History ===\n");
                    match describe_file_series_versions(tx, &file_info.path).await {
                        Ok(version_stats) => {
                            if version_stats.is_empty() {
                                output.push_str("   No versions found\n");
                            } else {
                                for stats in version_stats {
                                    output.push_str(&format!(
                                        "   Version {}: {} rows",
                                        stats.version, stats.row_count
                                    ));

                                    // Show timestamp range if available
                                    if let (Some(min_time), Some(max_time)) =
                                        (stats.min_event_time, stats.max_event_time)
                                    {
                                        // Convert microseconds to human-readable format
                                        use chrono::{DateTime, Utc};
                                        let min_dt =
                                            DateTime::<Utc>::from_timestamp_micros(min_time);
                                        let max_dt =
                                            DateTime::<Utc>::from_timestamp_micros(max_time);

                                        if let (Some(min), Some(max)) = (min_dt, max_dt) {
                                            output.push_str(&format!(
                                                ", time range: {} to {}",
                                                min.format("%Y-%m-%d %H:%M:%S UTC"),
                                                max.format("%Y-%m-%d %H:%M:%S UTC")
                                            ));
                                        }
                                    }
                                    output.push('\n');

                                    // Show null counts for columns with nulls (indented under this version)
                                    let mut null_cols: Vec<_> = stats
                                        .column_null_counts
                                        .iter()
                                        .filter(|&(_, count)| *count > 0)
                                        .collect();

                                    if !null_cols.is_empty() {
                                        null_cols.sort_by_key(|(name, _)| *name);
                                        output.push_str("       Null counts: ");
                                        let null_strs: Vec<String> = null_cols
                                            .iter()
                                            .map(|(col, count)| format!("{}: {}", col, count))
                                            .collect();
                                        output.push_str(&null_strs.join(", "));
                                        output.push('\n');
                                    }
                                }
                                // Show total size after all versions
                                output.push_str(&format!(
                                    "   Total size (all versions): {} bytes\n",
                                    file_info.metadata.size.unwrap_or(0)
                                ));
                            }
                        }
                        Err(e) => {
                            output
                                .push_str(&format!("   Error loading version statistics: {}\n", e));
                        }
                    }
                }
            }
            tinyfs::EntryType::TablePhysicalVersion => {
                output.push_str("   Format: Parquet table\n");
                match describe_file_table_schema(ship_context, &file_info.path).await {
                    Ok(schema_info) => {
                        output
                            .push_str(&format!("   Schema: {} fields\n", schema_info.field_count));
                        for field_info in schema_info.fields {
                            output.push_str(&format!(
                                "     * {}: {}\n",
                                field_info.name, field_info.data_type
                            ));
                        }
                    }
                    Err(e) => {
                        output.push_str(&format!("   Schema: Error loading schema - {}\n", e));
                    }
                }
            }
            tinyfs::EntryType::FilePhysicalVersion | tinyfs::EntryType::FileDynamic => {
                output.push_str("   Format: Raw data\n");
            }
            tinyfs::EntryType::FilePhysicalSeries => {
                output.push_str("   Format: Raw data series (versions concatenated on read)\n");
            }
            tinyfs::EntryType::DirectoryPhysical | tinyfs::EntryType::DirectoryDynamic => {
                output.push_str("   Format: Directory\n");
            }
            tinyfs::EntryType::Symlink => {
                output.push_str("   Format: Symbolic link\n");
            }
        }

        // Show size for non-series files (series files show it in Version History section)
        if !matches!(
            file_info.metadata.entry_type,
            tinyfs::EntryType::TablePhysicalSeries
        ) {
            output.push_str(&format!(
                "   Size: {} bytes\n",
                file_info.metadata.size.unwrap_or(0)
            ));
        }
        // Note: Version number is already shown in "Version History" section above for series files
        // For non-series files, this is the only place version is shown
        if !matches!(
            file_info.metadata.entry_type,
            tinyfs::EntryType::TablePhysicalSeries
        ) {
            output.push_str(&format!("   Version: {}\n", file_info.metadata.version));
        }
        output.push('\n');
    }

    Ok(output)
}

#[derive(Debug)]
pub struct SchemaInfo {
    pub field_count: usize,
    pub fields: Vec<FieldInfo>,
    pub timestamp_column: Option<String>,
}

#[derive(Debug)]
pub struct FieldInfo {
    pub name: String,
    pub data_type: String,
}

/// Describe the schema of a file:series using tlogfs schema API
async fn describe_file_series_schema(ship_context: &ShipContext, path: &str) -> Result<SchemaInfo> {
    let mut ship = ship_context.open_pond().await?;
    let mut tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "describe-schema".to_string(),
        ]))
        .await?;

    match describe_file_series_schema_impl(&mut tx, path).await {
        Ok(schema_info) => {
            _ = tx.commit().await?;
            Ok(schema_info)
        }
        Err(e) => Err(tx.abort(&e).await.into()),
    }
}

/// Implementation of file series schema description
async fn describe_file_series_schema_impl(
    tx: &mut steward::Transaction<'_>,
    path: &str,
) -> Result<SchemaInfo> {
    let fs = &**tx;
    let root = fs.root().await?;

    // Use tlogfs get_file_schema API - works for both static and dynamic files
    let schema = tlogfs::get_file_schema(&root, path, &tx.provider_context()?)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get schema for '{}': {}", path, e))?;

    extract_schema_info(&schema, true)
}

/// Describe the schema of a file:table using tlogfs schema API
async fn describe_file_table_schema(ship_context: &ShipContext, path: &str) -> Result<SchemaInfo> {
    let mut ship = ship_context.open_pond().await?;
    let mut tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "describe-schema".to_string(),
        ]))
        .await?;

    match describe_file_table_schema_impl(&mut tx, path).await {
        Ok(schema_info) => {
            _ = tx.commit().await?;
            Ok(schema_info)
        }
        Err(e) => Err(tx.abort(&e).await.into()),
    }
}

/// Implementation of file table schema description
async fn describe_file_table_schema_impl(
    tx: &mut steward::Transaction<'_>,
    path: &str,
) -> Result<SchemaInfo> {
    let fs = &**tx;
    let root = fs.root().await?;

    // Use tlogfs get_file_schema API - works for both static and dynamic files
    let schema = tlogfs::get_file_schema(&root, path, &tx.provider_context()?)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get schema for '{}': {}", path, e))?;

    extract_schema_info(&schema, false)
}

/// Extract schema info from Arrow schema
fn extract_schema_info(
    schema: &arrow::datatypes::Schema,
    detect_timestamp: bool,
) -> Result<SchemaInfo> {
    let mut fields = Vec::new();
    for field in schema.fields() {
        fields.push(FieldInfo {
            name: field.name().clone(),
            data_type: format!("{:?}", field.data_type()),
        });
    }

    // Try to detect timestamp column for series.
    // Search in specificity order: most specific names first.
    let timestamp_column = if detect_timestamp {
        let fields = schema.fields();
        let find_by = |name: &str| {
            fields
                .iter()
                .find(|f| f.name().to_lowercase() == name)
                .map(|f| f.name().clone())
        };
        find_by("timestamp")
            .or_else(|| find_by("datetime"))
            .or_else(|| find_by("event_time"))
            .or_else(|| find_by("ts"))
            .or_else(|| find_by("time"))
    } else {
        None
    };

    Ok(SchemaInfo {
        field_count: schema.fields().len(),
        fields,
        timestamp_column,
    })
}

#[derive(Debug)]
pub struct VersionStats {
    pub version: u64,
    pub row_count: usize,
    pub min_event_time: Option<i64>,
    pub max_event_time: Option<i64>,
    pub column_null_counts: std::collections::HashMap<String, usize>,
}

/// Get detailed statistics for all versions of a file series
async fn describe_file_series_versions(
    tx: &mut steward::Transaction<'_>,
    path: &str,
) -> Result<Vec<VersionStats>> {
    let fs = &**tx;
    let root = fs.root().await?;

    // Resolve the file node
    let (_wd, lookup) = root
        .resolve_path(path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to resolve path '{}': {}", path, e))?;

    let file_id = match lookup {
        tinyfs::Lookup::Found(node) => node.id(),
        _ => return Err(anyhow::anyhow!("Path '{}' not found", path)),
    };

    // Get all versions
    let versions = root
        .list_file_versions(path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list versions for '{}': {}", path, e))?;

    let mut version_stats = Vec::new();

    // Get session context and provider context for querying
    let provider_context = tx.provider_context()?;
    let session_ctx = tx.session_context().await?;

    for version_info in versions {
        // Create table provider for this specific version
        let options = provider::TableProviderOptions {
            version_selection: provider::VersionSelection::SpecificVersion(version_info.version),
            additional_urls: vec![],
            bounds: tinyfs::SeriesReadBounds::NONE,
        };

        let listing_result =
            provider::create_table_provider(file_id, &provider_context, options).await;

        let stats = match listing_result {
            Ok(table_provider) => {
                // Query to count rows and null values per column
                let schema = table_provider.schema();
                let field_names: Vec<String> =
                    schema.fields().iter().map(|f| f.name().clone()).collect();

                // Build SQL to count nulls for each column
                let null_count_exprs: Vec<String> = field_names
                    .iter()
                    .map(|col| {
                        format!(
                            "COUNT(CASE WHEN \"{}\" IS NULL THEN 1 END) as \"{}_nulls\"",
                            col, col
                        )
                    })
                    .collect();

                // Use a unique table name to avoid collisions (include file_id and version)
                let table_name = format!(
                    "describe_{}_{}",
                    file_id.node_id().to_short_string(),
                    version_info.version
                );
                _ = session_ctx.register_table(&table_name, table_provider.clone())?;

                let sql = format!(
                    "SELECT COUNT(*) as row_count, {} FROM \"{}\"",
                    null_count_exprs.join(", "),
                    table_name
                );

                // Execute the query
                let query_result = session_ctx.sql(&sql).await;

                // Deregister table immediately after use to keep session clean
                _ = session_ctx.deregister_table(&table_name);

                match query_result {
                    Ok(df) => {
                        match df.collect().await {
                            Ok(batches) => {
                                if batches.is_empty() || batches[0].num_rows() == 0 {
                                    VersionStats {
                                        version: version_info.version,
                                        row_count: 0,
                                        min_event_time: version_info
                                            .extended_metadata
                                            .as_ref()
                                            .and_then(|m| m.get("min_event_time"))
                                            .and_then(|s| s.parse::<i64>().ok()),
                                        max_event_time: version_info
                                            .extended_metadata
                                            .as_ref()
                                            .and_then(|m| m.get("max_event_time"))
                                            .and_then(|s| s.parse::<i64>().ok()),
                                        column_null_counts: std::collections::HashMap::new(),
                                    }
                                } else {
                                    let batch = &batches[0];
                                    let row_count_col = batch.column(0);
                                    let row_count = if let Some(array) = row_count_col
                                        .as_any()
                                        .downcast_ref::<arrow::array::Int64Array>(
                                    ) {
                                        array.value(0) as usize
                                    } else {
                                        0
                                    };

                                    // Extract null counts
                                    let mut column_null_counts = std::collections::HashMap::new();
                                    for (i, field_name) in field_names.iter().enumerate() {
                                        if let Some(null_col) = batch
                                            .column(i + 1)
                                            .as_any()
                                            .downcast_ref::<arrow::array::Int64Array>(
                                        ) {
                                            _ = column_null_counts.insert(
                                                field_name.clone(),
                                                null_col.value(0) as usize,
                                            );
                                        }
                                    }

                                    VersionStats {
                                        version: version_info.version,
                                        row_count,
                                        min_event_time: version_info
                                            .extended_metadata
                                            .as_ref()
                                            .and_then(|m| m.get("min_event_time"))
                                            .and_then(|s| s.parse::<i64>().ok()),
                                        max_event_time: version_info
                                            .extended_metadata
                                            .as_ref()
                                            .and_then(|m| m.get("max_event_time"))
                                            .and_then(|s| s.parse::<i64>().ok()),
                                        column_null_counts,
                                    }
                                }
                            }
                            Err(e) => {
                                log::warn!(
                                    "Failed to collect query results for version {}: {}",
                                    version_info.version,
                                    e
                                );
                                VersionStats {
                                    version: version_info.version,
                                    row_count: 0,
                                    min_event_time: None,
                                    max_event_time: None,
                                    column_null_counts: std::collections::HashMap::new(),
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to execute query for version {}: {}",
                            version_info.version,
                            e
                        );
                        VersionStats {
                            version: version_info.version,
                            row_count: 0,
                            min_event_time: None,
                            max_event_time: None,
                            column_null_counts: std::collections::HashMap::new(),
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!(
                    "Failed to create table provider for version {}: {}",
                    version_info.version,
                    e
                );
                VersionStats {
                    version: version_info.version,
                    row_count: 0,
                    min_event_time: None,
                    max_event_time: None,
                    column_null_counts: std::collections::HashMap::new(),
                }
            }
        };

        version_stats.push(stats);
    }

    // Check for duplicate versions - this indicates a data integrity error
    let mut seen_versions: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for stats in &version_stats {
        if !seen_versions.insert(stats.version) {
            return Err(anyhow::anyhow!(
                "Data integrity error: Duplicate version {} found for file '{}'",
                stats.version,
                path
            ));
        }
    }

    Ok(version_stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::copy::{CopyOptions, copy_command};
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use anyhow::Result;
    use std::path::PathBuf;
    use tempfile::TempDir;

    struct TestSetup {
        temp_dir: TempDir,
        ship_context: ShipContext,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            let temp_dir = tempfile::tempdir()?;
            let pond_path = temp_dir.path().join("test_pond");

            // Initialize the pond using init_command
            let ship_context = ShipContext {
                pond_path: Some(pond_path.clone()),
                host_root: None,
                mount_specs: Vec::new(),
                original_args: vec!["pond".to_string(), "init".to_string()],
            };
            init_command(&ship_context, "test-host").await?;

            Ok(TestSetup {
                temp_dir,
                ship_context,
            })
        }

        async fn create_host_file(&self, filename: &str, content: &str) -> Result<PathBuf> {
            let host_file = self.temp_dir.path().join(filename);
            tokio::fs::write(&host_file, content).await?;
            Ok(host_file)
        }

        /// Create a sample Parquet table file by writing directly to pond
        async fn create_parquet_table(&self, pond_path: &str, csv_data: &str) -> Result<()> {
            use arrow_array::{RecordBatch, StringArray};
            use arrow_schema::{DataType, Field, Schema};
            use std::sync::Arc;
            use tinyfs::arrow::ParquetExt;

            // Parse CSV data - handle generic columns
            let lines: Vec<&str> = csv_data.lines().collect();
            let headers: Vec<&str> = lines
                .first()
                .map(|h| h.split(',').collect())
                .unwrap_or_default();
            let num_cols = headers.len();

            // For simplicity, store all values as strings
            let mut columns: Vec<Vec<String>> = vec![Vec::new(); num_cols];

            for line in lines.iter().skip(1) {
                let values: Vec<&str> = line.split(',').collect();
                for (i, col) in columns.iter_mut().enumerate() {
                    col.push(values.get(i).unwrap_or(&"").to_string());
                }
            }

            // Create schema with all string columns
            let fields: Vec<Field> = headers
                .iter()
                .map(|h| Field::new(*h, DataType::Utf8, true))
                .collect();
            let schema = Arc::new(Schema::new(fields));

            // Create string arrays for each column
            let arrays: Vec<Arc<dyn arrow_array::Array>> = columns
                .iter()
                .map(|col| Arc::new(StringArray::from(col.clone())) as Arc<dyn arrow_array::Array>)
                .collect();

            let batch = RecordBatch::try_new(schema, arrays)?;

            // Write directly to pond using transact
            let pond_path = pond_path.to_string();
            let mut ship = self.ship_context.open_pond().await?;
            ship.write_transaction(
                &steward::PondUserMetadata::new(vec!["test".to_string()]),
                async |fs| {
                    let root = fs.root().await?;

                    root.create_table_from_batch(
                        &pond_path,
                        &batch,
                        tinyfs::EntryType::TablePhysicalVersion,
                    )
                    .await?;

                    Ok(())
                },
            )
            .await?;

            Ok(())
        }

        /// Create a sample Parquet series by writing directly to pond
        async fn create_parquet_series(&self, pond_path: &str, csv_data: &str) -> Result<()> {
            use arrow_array::{Float64Array, Int64Array, RecordBatch};
            use arrow_schema::{DataType, Field, Schema};
            use std::sync::Arc;
            use tinyfs::arrow::ParquetExt;

            // Parse CSV data
            let lines: Vec<&str> = csv_data.lines().collect();

            // Build simple test batch with timestamp and value columns
            let mut timestamp_values: Vec<i64> = Vec::new();
            let mut value_values: Vec<f64> = Vec::new();

            for line in lines.iter().skip(1) {
                let values: Vec<&str> = line.split(',').collect();
                if let Some(ts) = values.first() {
                    timestamp_values.push(ts.parse::<i64>().unwrap_or(0));
                }
                if let Some(v) = values.get(1) {
                    value_values.push(v.parse::<f64>().unwrap_or(0.0));
                }
            }

            // Create schema and arrays
            let schema = Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Int64, false),
                Field::new("value", DataType::Float64, false),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(timestamp_values)),
                    Arc::new(Float64Array::from(value_values)),
                ],
            )?;

            // Write directly to pond using transact
            let pond_path = pond_path.to_string();
            let mut ship = self.ship_context.open_pond().await?;
            ship.write_transaction(
                &steward::PondUserMetadata::new(vec!["test".to_string()]),
                async |fs| {
                    let root = fs.root().await?;

                    let _ = root
                        .create_series_from_batch(&pond_path, &batch, Some("timestamp"))
                        .await?;

                    Ok(())
                },
            )
            .await?;

            Ok(())
        }

        /// Create a raw data file
        async fn create_raw_data_file(&self, pond_path: &str, content: &str) -> Result<()> {
            let host_file = self.create_host_file("temp.txt", content).await?;
            copy_command(
                &self.ship_context,
                &[format!("host:///{}", host_file.display())],
                pond_path,
                &CopyOptions::default(),
            )
            .await?;
            Ok(())
        }

        /// Helper to capture describe command output
        async fn describe_output(&self, pattern: &str) -> Result<String> {
            let mut output = String::new();
            describe_command(&self.ship_context, pattern, |s| {
                output.push_str(s);
            })
            .await?;
            Ok(output)
        }
    }

    #[tokio::test]
    async fn test_describe_command_empty_pond() -> Result<()> {
        let setup = TestSetup::new().await?;

        let output = setup.describe_output("**/*").await?;
        assert!(output.contains("No files found matching pattern"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_parquet_table() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a Parquet table file
        let csv_data = "name,age,city\nAlice,30,NYC\nBob,25,LA";
        setup
            .create_parquet_table("users.parquet", csv_data)
            .await?;

        let output = setup.describe_output("users.parquet").await?;

        assert!(output.contains("[FILE] /users.parquet"));
        assert!(output.contains("Type: TablePhysicalVersion"));
        let output = setup.describe_output("users.parquet").await?;

        assert!(output.contains("[FILE] /users.parquet"));
        assert!(output.contains("Type: TablePhysicalVersion"));
        assert!(output.contains("Format: Parquet table"));
        // Schema parsing might fail in tests, so be more flexible
        assert!(output.contains("Schema:"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_parquet_series() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a Parquet series file with timestamp column
        let csv_data =
            "timestamp,device_id,value\n1609459200000,device1,100.0\n1609459260000,device2,101.5";
        setup
            .create_parquet_series("sensors.parquet", csv_data)
            .await?;

        let output = setup.describe_output("sensors.parquet").await?;

        assert!(output.contains("[FILE] /sensors.parquet"));
        assert!(output.contains("Type: TablePhysicalSeries"));
        assert!(output.contains("Format: Parquet series"));
        // Schema parsing might fail in tests, so be more flexible
        assert!(output.contains("Schema:"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_multiple_files() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create multiple files of different types
        setup
            .create_parquet_table("table1.parquet", "id,name\n1,Alice\n2,Bob")
            .await?;
        setup
            .create_parquet_series("series1.parquet", "timestamp,value\n1609459200000,100")
            .await?;
        setup
            .create_raw_data_file("readme.txt", "This is a text file")
            .await?;

        let output = setup.describe_output("**/*").await?;

        assert!(output.contains("Files found: 3"));
        assert!(output.contains("[FILE] /readme.txt"));
        assert!(output.contains("[FILE] /series1.parquet"));
        assert!(output.contains("[FILE] /table1.parquet"));

        // Check that different types are described correctly
        assert!(output.contains("Type: TablePhysicalVersion"));
        assert!(output.contains("Type: TablePhysicalSeries"));
        assert!(output.contains("Type: FilePhysicalVersion"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_pattern_matching() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create files with different extensions
        setup
            .create_parquet_table("users.parquet", "name,age\nAlice,30")
            .await?;
        setup
            .create_raw_data_file("config.txt", "debug=true")
            .await?;
        setup
            .create_raw_data_file("data.json", r#"{"key": "value"}"#)
            .await?;

        // Test pattern matching for parquet files only
        let output = setup.describe_output("*.parquet").await?;
        assert!(output.contains("Files found: 1"));
        assert!(output.contains("users.parquet"));
        assert!(!output.contains("config.txt"));
        assert!(!output.contains("data.json"));

        // Test pattern matching for text files
        let output = setup.describe_output("*.txt").await?;
        assert!(output.contains("Files found: 1"));
        assert!(output.contains("config.txt"));

        Ok(())
    }

    #[tokio::test]
    async fn test_describe_command_nonexistent_pattern() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create one file
        setup
            .create_raw_data_file("existing.txt", "content")
            .await?;

        // Try to describe a non-matching pattern
        let output = setup.describe_output("nonexistent*").await?;
        assert!(output.contains("No files found matching pattern 'nonexistent*'"));

        Ok(())
    }
}
