// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Test factories for unit testing the factory system
//!
//! Simple factories that don't require external dependencies,
//! used to test the factory infrastructure.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::Result as TinyFSResult;
use tinyfs::ResultExt;

/// Test factory configuration
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestConfig {
    /// Message to print during execution
    pub message: String,
    /// Number of times to repeat the message
    #[serde(default = "default_repeat")]
    pub repeat_count: usize,
    /// Whether the test should intentionally fail
    #[serde(default)]
    pub fail: bool,
    /// Optional: Path to file to read and verify
    #[serde(default)]
    pub file_to_read: Option<String>,
    /// Optional: Expected content of file (for verification)
    #[serde(default)]
    pub expected_content: Option<String>,
}

fn default_repeat() -> usize {
    1
}

/// Validate test configuration from YAML bytes
fn validate_test_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config: TestConfig =
        serde_yaml::from_slice(config_bytes).map_other_context("Invalid test config")?;

    serde_json::to_value(config).map_other_context("Failed to convert config")
}

/// Initialize test factory - creates a simple output directory
async fn initialize_test(
    config: Value,
    _context: crate::FactoryContext,
) -> Result<(), tinyfs::Error> {
    let test_config: TestConfig =
        serde_json::from_value(config).map_other_context("Invalid config")?;

    log::info!(
        "Initializing test factory with message: {}",
        test_config.message
    );

    // The parent_node_id in context is the parent directory (e.g., /configs)
    // We don't have the config file's node ID here, so we can't create subdirectories
    // This is a limitation of the initialize hook - it runs after creation but doesn't get the new node ID
    // For now, skip directory creation in initialize and do it in execute
    log::info!("Test factory initialized (directory creation deferred to execution)");
    Ok(())
}

/// Execute test factory - prints message multiple times
async fn execute_test(
    config: Value,
    context: crate::FactoryContext,
    ctx: crate::ExecutionContext,
) -> Result<(), tinyfs::Error> {
    let parsed_config: TestConfig =
        serde_json::from_value(config).map_other_context("Invalid config")?;

    // Check if this execution should fail
    if parsed_config.fail {
        return Err(tinyfs::Error::Other(format!(
            "Test factory intentionally failed: {}",
            parsed_config.message
        )));
    }

    log::debug!("=== Test Factory Execution ===");
    log::debug!("Message: {}", parsed_config.message);
    log::debug!("Repeat count: {}", parsed_config.repeat_count);
    log::debug!("Context: {:?}", ctx);
    log::debug!("==============================");

    for i in 1..=parsed_config.repeat_count {
        log::debug!("[{}] {}", i, parsed_config.message);
    }

    if let Some(path) = &parsed_config.file_to_read {
        let actual = context.root().await?.read_file_path_to_vec(path).await?;
        if let Some(expected) = &parsed_config.expected_content
            && actual != expected.as_bytes()
        {
            return Err(tinyfs::Error::Other(format!(
                "test factory read '{path}' as {:?}, expected {:?}",
                String::from_utf8_lossy(&actual),
                expected
            )));
        }
    }

    // For testing purposes, create a result file on the host filesystem
    let result_path = format!(
        "/tmp/test-executor-result-{}.txt",
        context.file_id.node_id()
    );
    let result_content = format!(
        "Executed {} times\nMessage: {}\nContext: {:?}\n",
        parsed_config.repeat_count, parsed_config.message, ctx
    );

    std::fs::write(&result_path, result_content).map_other_context("Failed to write result")?;

    log::info!(
        "Test factory execution completed, result written to {}",
        result_path
    );
    Ok(())
}

// Register the test executable factory
crate::register_executable_factory!(
    name: "test-executor",
    description: "Test executable factory for unit testing",
    post_commit: read_only,
    validate: validate_test_config,
    initialize: |config, context| async move {
        initialize_test(config, context).await
    },
    execute: |config, context, ctx| async move {
        execute_test(config, context, ctx).await
    }
);

// ============================================================================
// Test Directory Factory
// ============================================================================

/// Validate test directory configuration from YAML bytes
fn validate_test_dir_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config = tinyfs::testing::TestDirectoryConfig::from_yaml_bytes(config_bytes)
        .map_other_context("Invalid test-dir config")?;

    serde_json::to_value(config).map_other_context("Failed to convert config")
}

/// Create a test directory handle
fn create_test_dir(
    config: Value,
    context: crate::FactoryContext,
) -> TinyFSResult<tinyfs::DirHandle> {
    let _test_config: tinyfs::testing::TestDirectoryConfig =
        serde_json::from_value(config).map_other_context("Invalid config")?;

    // Create an empty dynamic directory for testing
    // TestDirectoryConfig only has name/metadata, not actual file specs
    let dynamic_config = crate::DynamicDirConfig { entries: vec![] };
    let dynamic_dir = crate::DynamicDirDirectory::new(dynamic_config, context);
    Ok(dynamic_dir.create_handle())
}

crate::register_dynamic_factory!(
    name: "test-dir",
    description: "Test directory factory with required YAML config for unit testing",
    directory: create_test_dir,
    validate: validate_test_dir_config
);

// ============================================================================
// Test File Factory
// ============================================================================

/// Validate test file configuration from YAML bytes
fn validate_test_file_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config = tinyfs::testing::TestFileConfig::from_yaml_bytes(config_bytes)
        .map_other_context("Invalid test-file config")?;

    serde_json::to_value(config).map_other_context("Failed to convert config")
}

/// Create a test file handle
fn create_test_file(
    config: Value,
    _context: crate::FactoryContext,
) -> TinyFSResult<tinyfs::FileHandle> {
    let parsed: tinyfs::testing::TestFileConfig =
        serde_json::from_value(config).map_other_context("Invalid config")?;

    // Return file handle with the configured content
    Ok(crate::ConfigFile::new(parsed.content.into_bytes()).create_handle())
}

crate::register_dynamic_factory!(
    name: "test-file",
    description: "Test file factory with required YAML config for unit testing",
    file: create_test_file,
    validate: validate_test_file_config
);

// ============================================================================
// Test Infinite CSV Stream Factory
// ============================================================================

/// Configuration for infinite CSV stream
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InfiniteCsvConfig {
    /// Number of rows to generate (for testing, we use finite but large number)
    #[serde(default = "default_row_count")]
    pub row_count: usize,
    /// Batch size for generating rows
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Column names
    #[serde(default = "default_columns")]
    pub columns: Vec<String>,
}

fn default_row_count() -> usize {
    100_000 // Large enough to test streaming, small enough for tests
}

fn default_batch_size() -> usize {
    1024 // Generate 1KB chunks at a time
}

fn default_columns() -> Vec<String> {
    vec![
        "id".to_string(),
        "timestamp".to_string(),
        "value".to_string(),
        "label".to_string(),
    ]
}

impl Default for InfiniteCsvConfig {
    fn default() -> Self {
        Self {
            row_count: default_row_count(),
            batch_size: default_batch_size(),
            columns: default_columns(),
        }
    }
}

/// Custom File implementation that generates CSV data on-demand
pub struct InfiniteCsvFile {
    config: InfiniteCsvConfig,
}

impl InfiniteCsvFile {
    #[must_use]
    pub fn new(config: InfiniteCsvConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl tinyfs::File for InfiniteCsvFile {
    async fn async_reader(&self) -> TinyFSResult<Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
        // Create a streaming reader that generates CSV data on demand
        let reader = InfiniteCsvReader::new(self.config.clone());
        Ok(Box::pin(reader))
    }

    async fn async_writer(&self) -> TinyFSResult<Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
        Err(tinyfs::Error::Other(
            "Infinite CSV files are read-only".to_string(),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl tinyfs::Metadata for InfiniteCsvFile {
    async fn metadata(&self) -> TinyFSResult<tinyfs::NodeMetadata> {
        // Estimate size based on row count
        let header_size = self.config.columns.join(",").len() + 1;
        let estimated_row_size = 50; // rough estimate
        let total_size = header_size + (self.config.row_count * estimated_row_size);

        Ok(tinyfs::NodeMetadata {
            version: 1,
            size: Some(total_size as u64),
            blake3: None,
            bao_outboard: None,
            entry_type: tinyfs::EntryType::FileDynamic,
            timestamp: 0,
        })
    }
}

/// Streaming reader that generates CSV data on-demand
struct InfiniteCsvReader {
    config: InfiniteCsvConfig,
    position: usize,
    current_row: usize,
    buffer: Vec<u8>,
    buffer_pos: usize,
}

impl InfiniteCsvReader {
    fn new(config: InfiniteCsvConfig) -> Self {
        // Pre-generate header and some initial rows for schema inference
        let mut initial_data = format!("{}\n", config.columns.join(","));

        // Generate first 100 rows for schema inference (if available)
        let initial_rows = config.row_count.min(100);
        for i in 0..initial_rows {
            initial_data.push_str(&Self::generate_row(i, &config.columns));
        }

        Self {
            config,
            position: 0,
            current_row: initial_rows,
            buffer: initial_data.into_bytes(),
            buffer_pos: 0,
        }
    }

    /// Generate a single CSV row based on column configuration
    fn generate_row(index: usize, columns: &[String]) -> String {
        let mut values = Vec::new();
        for col in columns {
            let value = match col.as_str() {
                "id" => index.to_string(),
                "timestamp" => (1700000000 + index as i64).to_string(),
                "value" => format!("{:.6}", (index as f64 * std::f64::consts::PI).sin()),
                "label" => format!("label_{}", index % 10),
                "temperature" => format!("{:.2}", 20.0 + (index as f64 * 0.1).sin() * 5.0),
                "humidity" => format!("{:.2}", 50.0 + (index as f64 * 0.2).cos() * 20.0),
                "pressure" => format!("{:.2}", 1013.25 + (index as f64 * 0.15).sin() * 10.0),
                _ => index.to_string(), // Default: use row index
            };
            values.push(value);
        }
        format!("{}\n", values.join(","))
    }

    /// Fill buffer with next batch of CSV rows
    fn fill_buffer(&mut self) {
        if self.current_row >= self.config.row_count {
            // Reached end
            return;
        }

        let mut batch = String::new();
        let end_row = (self.current_row + self.config.batch_size).min(self.config.row_count);

        for i in self.current_row..end_row {
            batch.push_str(&Self::generate_row(i, &self.config.columns));
        }

        self.current_row = end_row;
        self.buffer = batch.into_bytes();
        self.buffer_pos = 0;
    }
}

impl tokio::io::AsyncRead for InfiniteCsvReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Check if we need more data
        if self.buffer_pos >= self.buffer.len() {
            if self.current_row >= self.config.row_count {
                // EOF
                return std::task::Poll::Ready(Ok(()));
            }
            self.fill_buffer();
        }

        // Copy from buffer to output
        let available = &self.buffer[self.buffer_pos..];
        let to_copy = available.len().min(buf.remaining());
        buf.put_slice(&available[..to_copy]);

        self.buffer_pos += to_copy;
        self.position += to_copy;

        std::task::Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncSeek for InfiniteCsvReader {
    fn start_seek(self: Pin<&mut Self>, _position: std::io::SeekFrom) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "InfiniteCsvReader does not support seeking",
        ))
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "InfiniteCsvReader does not support seeking",
        )))
    }
}

/// Validate infinite CSV configuration
fn validate_infinite_csv_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config: InfiniteCsvConfig =
        serde_yaml::from_slice(config_bytes).map_other_context("Invalid infinite-csv config")?;

    serde_json::to_value(config).map_other_context("Failed to convert config")
}

/// Create infinite CSV file handle
fn create_infinite_csv_file(
    config: Value,
    _context: crate::FactoryContext,
) -> TinyFSResult<tinyfs::FileHandle> {
    let parsed: InfiniteCsvConfig =
        serde_json::from_value(config).map_other_context("Invalid config")?;

    let file = InfiniteCsvFile::new(parsed);
    Ok(tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(
        Box::new(file) as Box<dyn tinyfs::File>,
    ))))
}

crate::register_dynamic_factory!(
    name: "infinite-csv",
    description: "Generates infinite CSV stream for testing streaming readers",
    file: create_infinite_csv_file,
    validate: validate_infinite_csv_config
);
