// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Synthetic Timeseries Factory
//!
//! Generates dynamic FileSeries containing configurable timeseries with known
//! distributions. Each series contains multiple named "points", where each point
//! is a sum of configurable waveform components (sine, triangle, square, line).
//!
//! The factory streams Arrow RecordBatches on demand via a DataFusion
//! StreamingTable -- no Parquet serialization or bulk in-memory materialization
//! involved.  Batches are generated lazily in chunks of 8192 rows, keeping
//! memory usage O(batch_size) regardless of the total time range.
//!
//! ## Example config
//!
//! ```yaml
//! start: "2024-01-01T00:00:00Z"
//! end: "2024-01-02T00:00:00Z"
//! interval: "15m"
//! time_column: "timestamp"
//! points:
//!   - name: "temperature"
//!     components:
//!       - type: sine
//!         amplitude: 10.0
//!         period: "24h"
//!         offset: 20.0
//!       - type: line
//!         slope: 0.001
//!   - name: "pressure"
//!     components:
//!       - type: sine
//!         amplitude: 5.0
//!         period: "12h"
//!         offset: 1013.0
//!       - type: square
//!         amplitude: 2.0
//!         period: "6h"
//! ```

use crate::register_dynamic_factory;
use arrow::array::{Float64Array, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tinyfs::ResultExt;
use tinyfs::{EntryType, FileHandle, FileID, NodeMetadata, Result as TinyFSResult};
use tokio::sync::Mutex;

// ============================================================================
// Configuration Types
// ============================================================================

/// Waveform component type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub enum WaveformComponent {
    /// Sine wave: offset + amplitude * sin(2pi * t / period + phase)
    Sine {
        #[serde(default)]
        amplitude: f64,
        /// Period as a human-readable duration (e.g., "24h", "1h30m", "90s")
        period: String,
        #[serde(default)]
        offset: f64,
        /// Phase offset in radians
        #[serde(default)]
        phase: f64,
    },
    /// Triangle wave: offset + amplitude * triangle(t / period + phase)
    Triangle {
        #[serde(default)]
        amplitude: f64,
        period: String,
        #[serde(default)]
        offset: f64,
        #[serde(default)]
        phase: f64,
    },
    /// Square wave: offset + amplitude * sign(sin(2pi * t / period + phase))
    Square {
        #[serde(default)]
        amplitude: f64,
        period: String,
        #[serde(default)]
        offset: f64,
        #[serde(default)]
        phase: f64,
    },
    /// Linear ramp: offset + slope * t_seconds
    Line {
        #[serde(default)]
        slope: f64,
        #[serde(default)]
        offset: f64,
    },
}

impl WaveformComponent {
    /// Evaluate the waveform at time `t_seconds` (seconds since series start)
    fn evaluate(&self, t_seconds: f64) -> f64 {
        match self {
            WaveformComponent::Sine {
                amplitude,
                period,
                offset,
                phase,
            } => {
                let period_secs = parse_duration_secs(period);
                offset
                    + amplitude
                        * (2.0 * std::f64::consts::PI * t_seconds / period_secs + phase).sin()
            }
            WaveformComponent::Triangle {
                amplitude,
                period,
                offset,
                phase,
            } => {
                let period_secs = parse_duration_secs(period);
                let t_norm = (t_seconds / period_secs + phase / (2.0 * std::f64::consts::PI))
                    .rem_euclid(1.0);
                // Triangle wave: rises from -1 to 1 in first half, falls from 1 to -1 in second half
                let tri = if t_norm < 0.5 {
                    4.0 * t_norm - 1.0
                } else {
                    3.0 - 4.0 * t_norm
                };
                offset + amplitude * tri
            }
            WaveformComponent::Square {
                amplitude,
                period,
                offset,
                phase,
            } => {
                let period_secs = parse_duration_secs(period);
                let sin_val = (2.0 * std::f64::consts::PI * t_seconds / period_secs + phase).sin();
                offset + amplitude * sin_val.signum()
            }
            WaveformComponent::Line { slope, offset } => offset + slope * t_seconds,
        }
    }
}

/// A named timeseries point -- its value at each timestamp is the sum of its components
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub struct PointConfig {
    /// Column name for this point
    pub name: String,
    /// Waveform components whose values are summed
    pub components: Vec<WaveformComponent>,
}

/// Top-level configuration for the synthetic-timeseries factory
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "documentation", derive(schemars::JsonSchema))]
pub struct SyntheticTimeseriesConfig {
    /// Start time (RFC 3339 / ISO 8601)
    pub start: String,
    /// End time (RFC 3339 / ISO 8601)
    pub end: String,
    /// Sampling interval as a human-readable duration (e.g., "15m", "1h", "30s")
    pub interval: String,
    /// Name of the timestamp column (default: "timestamp")
    #[serde(default = "default_time_column")]
    pub time_column: String,
    /// Named timeseries points (each becomes a Float64 column)
    pub points: Vec<PointConfig>,
}

fn default_time_column() -> String {
    "timestamp".to_string()
}

// ============================================================================
// Duration Parsing
// ============================================================================

/// Parse a human-readable duration string into seconds.
///
/// Supports: `30s`, `15m`, `1h`, `1h30m`, `2d`, `1d12h`, combinations thereof.
/// Falls back to `humantime::parse_duration` for the heavy lifting.
fn parse_duration_secs(s: &str) -> f64 {
    match humantime::parse_duration(s) {
        Ok(d) => d.as_secs_f64(),
        Err(_) => {
            log::warn!("Failed to parse duration '{}', defaulting to 3600s (1h)", s);
            3600.0
        }
    }
}

// ============================================================================
// Schema & Streaming Batch Generation
// ============================================================================

/// Build the Arrow schema for a synthetic timeseries config.
fn build_schema(config: &SyntheticTimeseriesConfig) -> SchemaRef {
    let mut fields = vec![Field::new(
        &config.time_column,
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    )];
    for point in &config.points {
        fields.push(Field::new(&point.name, DataType::Float64, false));
    }
    Arc::new(Schema::new(fields))
}

/// Batch size for streaming generation -- each poll yields at most this many rows.
const STREAMING_BATCH_SIZE: usize = 8192;

/// A synchronous, chunk-at-a-time Stream of RecordBatches.
///
/// Each `poll_next` generates up to `STREAMING_BATCH_SIZE` rows from the
/// synthetic config, advancing `current_ts` forward.  Memory usage is
/// O(STREAMING_BATCH_SIZE) regardless of the total time range.
struct SyntheticBatchStream {
    config: SyntheticTimeseriesConfig,
    schema: SchemaRef,
    start_ms: i64,
    end_ms: i64,
    interval_ms: i64,
    current_ts: i64,
}

impl Stream for SyntheticBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.current_ts > self.end_ms {
            return Poll::Ready(None);
        }

        let start_ms = self.start_ms;
        let end_ms = self.end_ms;
        let interval_ms = self.interval_ms;

        // Collect timestamps for this chunk
        let mut timestamps = Vec::with_capacity(STREAMING_BATCH_SIZE);
        let mut ts = self.current_ts;
        while ts <= end_ms && timestamps.len() < STREAMING_BATCH_SIZE {
            timestamps.push(ts);
            ts += interval_ms;
        }
        self.current_ts = ts;

        let num_rows = timestamps.len();
        let mut columns: Vec<Arc<dyn arrow::array::Array>> =
            Vec::with_capacity(1 + self.config.points.len());

        // Timestamp column
        columns.push(Arc::new(
            TimestampMillisecondArray::from(timestamps.clone()).with_timezone("UTC"),
        ));

        // Value columns
        for point in &self.config.points {
            let mut values = Vec::with_capacity(num_rows);
            for &ts_ms in &timestamps {
                let t_seconds = (ts_ms - start_ms) as f64 / 1000.0;
                let value: f64 = point.components.iter().map(|c| c.evaluate(t_seconds)).sum();
                values.push(value);
            }
            columns.push(Arc::new(Float64Array::from(values)));
        }

        match RecordBatch::try_new(self.schema.clone(), columns) {
            Ok(batch) => Poll::Ready(Some(Ok(batch))),
            Err(e) => {
                // Stop the stream after an error
                self.current_ts = self.end_ms + 1;
                Poll::Ready(Some(Err(DataFusionError::ArrowError(Box::new(e), None))))
            }
        }
    }
}

/// PartitionStream adapter for the synthetic timeseries generator.
#[derive(Debug)]
struct SyntheticPartitionStream {
    config: SyntheticTimeseriesConfig,
    schema: SchemaRef,
    start_ms: i64,
    end_ms: i64,
    interval_ms: i64,
}

impl PartitionStream for SyntheticPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let stream = SyntheticBatchStream {
            config: self.config.clone(),
            schema: self.schema.clone(),
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            interval_ms: self.interval_ms,
            current_ts: self.start_ms,
        };
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// Generate Arrow RecordBatches from the synthetic config.
///
/// Returns `(schema, batches)` -- used only in unit tests.
/// Production code uses `SyntheticPartitionStream` for streaming.
#[cfg(test)]
fn generate_batches(
    config: &SyntheticTimeseriesConfig,
) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    let start_ms = parse_rfc3339_to_millis(&config.start)?;
    let end_ms = parse_rfc3339_to_millis(&config.end)?;

    if end_ms <= start_ms {
        return Err(format!(
            "end ({}) must be after start ({})",
            config.end, config.start
        ));
    }

    let interval_ms = (parse_duration_secs(&config.interval) * 1000.0) as i64;
    if interval_ms <= 0 {
        return Err(format!(
            "interval must be positive, got '{}'",
            config.interval
        ));
    }

    if config.points.is_empty() {
        return Err("at least one point must be configured".to_string());
    }

    let schema = build_schema(config);

    // Generate timestamps
    let mut timestamps: Vec<i64> = Vec::new();
    let mut ts = start_ms;
    while ts <= end_ms {
        timestamps.push(ts);
        ts += interval_ms;
    }

    let num_rows = timestamps.len();
    if num_rows == 0 {
        return Err(
            "interval is larger than the time range -- no data points generated".to_string(),
        );
    }

    let mut columns: Vec<Arc<dyn arrow::array::Array>> =
        Vec::with_capacity(1 + config.points.len());

    columns.push(Arc::new(
        TimestampMillisecondArray::from(timestamps.clone()).with_timezone("UTC"),
    ));

    for point in &config.points {
        let mut values = Vec::with_capacity(num_rows);
        for &ts_ms in &timestamps {
            let t_seconds = (ts_ms - start_ms) as f64 / 1000.0;
            let value: f64 = point.components.iter().map(|c| c.evaluate(t_seconds)).sum();
            values.push(value);
        }
        columns.push(Arc::new(Float64Array::from(values)));
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;

    Ok((schema, vec![batch]))
}

/// Parse an RFC 3339 timestamp string to epoch milliseconds
fn parse_rfc3339_to_millis(s: &str) -> Result<i64, String> {
    let dt = chrono::DateTime::parse_from_rfc3339(s)
        .map_err(|e| format!("Invalid timestamp '{}': {}", s, e))?;
    Ok(dt.timestamp_millis())
}

// ============================================================================
// File Implementation
// ============================================================================

/// Dynamic file that generates synthetic timeseries data on query
pub struct SyntheticTimeseriesFile {
    config: SyntheticTimeseriesConfig,
}

impl SyntheticTimeseriesFile {
    #[must_use]
    pub fn new(config: SyntheticTimeseriesConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl tinyfs::File for SyntheticTimeseriesFile {
    async fn async_reader(&self) -> TinyFSResult<Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
        Err(tinyfs::Error::Other(
            "SyntheticTimeseriesFile does not support direct byte reading. \
             Use QueryableFile::as_table_provider for SQL queries."
                .to_string(),
        ))
    }

    async fn async_writer(&self) -> TinyFSResult<Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
        Err(tinyfs::Error::Other(
            "SyntheticTimeseriesFile is read-only".to_string(),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_queryable(&self) -> Option<&dyn tinyfs::QueryableFile> {
        Some(self)
    }
}

#[async_trait]
impl tinyfs::Metadata for SyntheticTimeseriesFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::TableDynamic,
            timestamp: 0,
        })
    }
}

#[async_trait]
impl tinyfs::QueryableFile for SyntheticTimeseriesFile {
    async fn as_table_provider(
        &self,
        _id: FileID,
        _context: &tinyfs::ProviderContext,
    ) -> tinyfs::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let start_ms =
            parse_rfc3339_to_millis(&self.config.start).map_other_context("Invalid start time")?;
        let end_ms =
            parse_rfc3339_to_millis(&self.config.end).map_other_context("Invalid end time")?;

        if end_ms <= start_ms {
            return Err(tinyfs::Error::Other(format!(
                "end ({}) must be after start ({})",
                self.config.end, self.config.start
            )));
        }

        let interval_ms = (parse_duration_secs(&self.config.interval) * 1000.0) as i64;
        if interval_ms <= 0 {
            return Err(tinyfs::Error::Other(format!(
                "interval must be positive, got '{}'",
                self.config.interval
            )));
        }

        let schema = build_schema(&self.config);

        let partition = SyntheticPartitionStream {
            config: self.config.clone(),
            schema: schema.clone(),
            start_ms,
            end_ms,
            interval_ms,
        };

        let table = StreamingTable::try_new(schema, vec![Arc::new(partition)])
            .map_other_context("Failed to create StreamingTable")?;

        Ok(Arc::new(table))
    }
}

impl std::fmt::Debug for SyntheticTimeseriesFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyntheticTimeseriesFile")
            .field("start", &self.config.start)
            .field("end", &self.config.end)
            .field("interval", &self.config.interval)
            .field(
                "points",
                &self
                    .config
                    .points
                    .iter()
                    .map(|p| &p.name)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

// ============================================================================
// Factory Registration
// ============================================================================

fn create_synthetic_timeseries_handle(
    config: Value,
    _context: crate::FactoryContext,
) -> TinyFSResult<FileHandle> {
    let cfg: SyntheticTimeseriesConfig =
        serde_json::from_value(config).map_other_context("Invalid synthetic-timeseries config")?;

    let file = SyntheticTimeseriesFile::new(cfg);
    Ok(file.create_handle())
}

fn validate_synthetic_timeseries_config(config: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config).map_other_context("Invalid UTF-8")?;

    let cfg: SyntheticTimeseriesConfig = serde_yaml::from_str(config_str)
        .map_other_context("Invalid synthetic-timeseries config")?;

    // Validate time range
    let start_ms = parse_rfc3339_to_millis(&cfg.start).map_err(tinyfs::Error::Other)?;
    let end_ms = parse_rfc3339_to_millis(&cfg.end).map_err(tinyfs::Error::Other)?;
    if end_ms <= start_ms {
        return Err(tinyfs::Error::Other(format!(
            "end ({}) must be after start ({})",
            cfg.end, cfg.start
        )));
    }

    // Validate interval
    let interval_secs = parse_duration_secs(&cfg.interval);
    if interval_secs <= 0.0 {
        return Err(tinyfs::Error::Other(
            "interval must be a positive duration".to_string(),
        ));
    }

    // Validate points
    if cfg.points.is_empty() {
        return Err(tinyfs::Error::Other(
            "at least one point must be configured".to_string(),
        ));
    }

    for point in &cfg.points {
        if point.name.is_empty() {
            return Err(tinyfs::Error::Other(
                "point name cannot be empty".to_string(),
            ));
        }
        if point.components.is_empty() {
            return Err(tinyfs::Error::Other(format!(
                "point '{}' must have at least one component",
                point.name
            )));
        }
    }

    // Check for duplicate point names
    let mut seen_names = std::collections::HashSet::new();
    for point in &cfg.points {
        if !seen_names.insert(&point.name) {
            return Err(tinyfs::Error::Other(format!(
                "duplicate point name: '{}'",
                point.name
            )));
        }
    }

    serde_json::to_value(&cfg).map_other_context("Failed to convert config")
}

/// Downcast function for SyntheticTimeseriesFile
fn try_as_synthetic_queryable(file: &dyn tinyfs::File) -> Option<&dyn tinyfs::QueryableFile> {
    file.as_any()
        .downcast_ref::<SyntheticTimeseriesFile>()
        .map(|f| f as &dyn tinyfs::QueryableFile)
}

register_dynamic_factory!(
    name: "synthetic-timeseries",
    description: "Generate synthetic timeseries with configurable waveforms (sine, triangle, square, line)",
    file: create_synthetic_timeseries_handle,
    validate: validate_synthetic_timeseries_config,
    try_as_queryable: try_as_synthetic_queryable
);

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert!((parse_duration_secs("30s") - 30.0).abs() < 0.001);
        assert!((parse_duration_secs("15m") - 900.0).abs() < 0.001);
        assert!((parse_duration_secs("1h") - 3600.0).abs() < 0.001);
        assert!((parse_duration_secs("24h") - 86400.0).abs() < 0.001);
        assert!((parse_duration_secs("1h30m") - 5400.0).abs() < 0.001);
    }

    #[test]
    fn test_sine_wave() {
        let comp = WaveformComponent::Sine {
            amplitude: 10.0,
            period: "24h".to_string(),
            offset: 20.0,
            phase: 0.0,
        };
        // At t=0, sin(0) = 0
        assert!((comp.evaluate(0.0) - 20.0).abs() < 0.001);
        // At t=quarter-period, sin(pi/2) = 1
        let quarter = 6.0 * 3600.0; // 6 hours
        assert!((comp.evaluate(quarter) - 30.0).abs() < 0.001);
    }

    #[test]
    fn test_triangle_wave() {
        let comp = WaveformComponent::Triangle {
            amplitude: 10.0,
            period: "4h".to_string(),
            offset: 0.0,
            phase: 0.0,
        };
        // At t=0 -> -1 * 10 = -10
        assert!((comp.evaluate(0.0) - (-10.0)).abs() < 0.001);
        // At t=quarter-period -> 0 * 10 = 0
        let quarter = 3600.0; // 1 hour
        assert!((comp.evaluate(quarter) - 0.0).abs() < 0.001);
        // At t=half-period -> 1 * 10 = 10
        let half = 2.0 * 3600.0; // 2 hours
        assert!((comp.evaluate(half) - 10.0).abs() < 0.001);
    }

    #[test]
    fn test_square_wave() {
        let comp = WaveformComponent::Square {
            amplitude: 5.0,
            period: "2h".to_string(),
            offset: 0.0,
            phase: 0.0,
        };
        // Just after t=0, sin is positive -> +5
        assert!((comp.evaluate(1.0) - 5.0).abs() < 0.001);
        // At t=half-period + epsilon, sin is negative -> -5
        assert!((comp.evaluate(3601.0) - (-5.0)).abs() < 0.001);
    }

    #[test]
    fn test_line() {
        let comp = WaveformComponent::Line {
            slope: 0.001,
            offset: 100.0,
        };
        assert!((comp.evaluate(0.0) - 100.0).abs() < 0.001);
        assert!((comp.evaluate(1000.0) - 101.0).abs() < 0.001);
    }

    #[test]
    fn test_sum_of_components() {
        let point = PointConfig {
            name: "test".to_string(),
            components: vec![
                WaveformComponent::Line {
                    slope: 0.0,
                    offset: 100.0,
                },
                WaveformComponent::Line {
                    slope: 0.0,
                    offset: 50.0,
                },
            ],
        };
        let value: f64 = point.components.iter().map(|c| c.evaluate(0.0)).sum();
        assert!((value - 150.0).abs() < 0.001);
    }

    #[test]
    fn test_generate_batches_basic() {
        let config = SyntheticTimeseriesConfig {
            start: "2024-01-01T00:00:00Z".to_string(),
            end: "2024-01-01T01:00:00Z".to_string(),
            interval: "15m".to_string(),
            time_column: "timestamp".to_string(),
            points: vec![PointConfig {
                name: "temperature".to_string(),
                components: vec![WaveformComponent::Sine {
                    amplitude: 10.0,
                    period: "1h".to_string(),
                    offset: 20.0,
                    phase: 0.0,
                }],
            }],
        };

        let (schema, batches) = generate_batches(&config).unwrap();
        assert_eq!(schema.fields().len(), 2); // timestamp + temperature
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        // 0m, 15m, 30m, 45m, 60m = 5 rows
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_generate_batches_multiple_points() {
        let config = SyntheticTimeseriesConfig {
            start: "2024-01-01T00:00:00Z".to_string(),
            end: "2024-01-01T00:30:00Z".to_string(),
            interval: "10m".to_string(),
            time_column: "ts".to_string(),
            points: vec![
                PointConfig {
                    name: "temp".to_string(),
                    components: vec![WaveformComponent::Line {
                        slope: 0.0,
                        offset: 20.0,
                    }],
                },
                PointConfig {
                    name: "pressure".to_string(),
                    components: vec![WaveformComponent::Line {
                        slope: 0.0,
                        offset: 1013.0,
                    }],
                },
            ],
        };

        let (schema, batches) = generate_batches(&config).unwrap();
        assert_eq!(schema.fields().len(), 3); // ts + temp + pressure
        assert_eq!(batches[0].num_rows(), 4); // 0m, 10m, 20m, 30m
    }

    #[test]
    fn test_validate_config_valid() {
        let yaml = r#"
start: "2024-01-01T00:00:00Z"
end: "2024-01-02T00:00:00Z"
interval: "15m"
points:
  - name: "temperature"
    components:
      - type: sine
        amplitude: 10.0
        period: "24h"
        offset: 20.0
"#;
        let result = validate_synthetic_timeseries_config(yaml.as_bytes());
        assert!(result.is_ok(), "Valid config should pass: {:?}", result);
    }

    #[test]
    fn test_validate_config_end_before_start() {
        let yaml = r#"
start: "2024-01-02T00:00:00Z"
end: "2024-01-01T00:00:00Z"
interval: "15m"
points:
  - name: "temp"
    components:
      - type: line
        offset: 20.0
"#;
        let result = validate_synthetic_timeseries_config(yaml.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_config_no_points() {
        let yaml = r#"
start: "2024-01-01T00:00:00Z"
end: "2024-01-02T00:00:00Z"
interval: "15m"
points: []
"#;
        let result = validate_synthetic_timeseries_config(yaml.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_config_duplicate_names() {
        let yaml = r#"
start: "2024-01-01T00:00:00Z"
end: "2024-01-02T00:00:00Z"
interval: "15m"
points:
  - name: "temp"
    components:
      - type: line
        offset: 20.0
  - name: "temp"
    components:
      - type: line
        offset: 30.0
"#;
        let result = validate_synthetic_timeseries_config(yaml.as_bytes());
        assert!(result.is_err());
    }
}
