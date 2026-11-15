// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! OpenTelemetry JSON Format Provider
//!
//! Reads OpenTelemetry JSON Lines files containing metrics data.
//! This implementation:
//! - Focuses ONLY on gauge data points from the metrics signal
//! - Performs two-pass reading: first pass discovers schema (all metric names + timestamps)
//! - Creates a wide schema with timestamp column + one nullable Float64 column per metric
//! - Discards metric attributes (only interested in metric names and values)
//! - Uses official opentelemetry-proto crate for type-safe parsing
//!
//! Data format: JSON Lines, each line is an OTLP ExportMetricsServiceRequest with structure
//! matching the OpenTelemetry Protocol specification.

use crate::format::FormatProvider;
use crate::{Error, Result, Url};
use arrow::array::{Float64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::Stream;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncRead;

/// OpenTelemetry JSON format provider
pub struct OtelJsonProvider;

impl OtelJsonProvider {
    /// Create a new OtelJson provider
    pub fn new() -> Self {
        Self
    }
}

impl Default for OtelJsonProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a single observation: timestamp + metric name + value
#[derive(Debug, Clone)]
struct Observation {
    timestamp_ns: i64,
    metric_name: String,
    value: f64,
}

/// Parse a single JSON line using official OTLP types and extract gauge observations
fn parse_line(line: &str) -> Result<Vec<Observation>> {
    let request: ExportMetricsServiceRequest = serde_json::from_str(line)
        .map_err(|e| Error::Arrow(format!("Failed to parse OTLP JSON: {}", e)))?;

    let mut observations = Vec::new();

    for resource_metric in request.resource_metrics {
        for scope_metric in resource_metric.scope_metrics {
            for metric in scope_metric.metrics {
                // Only process gauge metrics - check the data field
                if let Some(Data::Gauge(gauge)) = metric.data {
                    for data_point in gauge.data_points {
                        // Extract the value - gauge data points have various value types
                        let value = match data_point.value {
                            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v)) => v,
                            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(v)) => v as f64,
                            None => continue, // Skip data points without values
                        };

                        observations.push(Observation {
                            timestamp_ns: data_point.time_unix_nano as i64,
                            metric_name: metric.name.clone(),
                            value,
                        });
                    }
                }
            }
        }
    }

    Ok(observations)
}

/// Read+parse the entire OTLP JSON Lines input.
///
/// Returns every gauge observation together with the sorted set of unique
/// metric names discovered (used to build the wide schema). Shared by
/// `infer_schema` and `open_stream` so the input is read and parsed once per
/// call instead of via two separate copies of the read loop.
async fn read_all(
    reader: Pin<Box<dyn AsyncRead + Send>>,
) -> Result<(Vec<Observation>, Vec<String>)> {
    let mut metric_names = BTreeSet::new();
    let mut observations: Vec<Observation> = Vec::new();

    let skipped =
        crate::format::batch::read_clean_lines(reader, |trimmed| match parse_line(trimmed) {
            Ok(obs) => {
                for o in &obs {
                    let _ = metric_names.insert(o.metric_name.clone());
                }
                observations.extend(obs);
                Ok(true)
            }
            Err(_) => Ok(false),
        })
        .await?;

    if skipped > 0 {
        log::warn!("oteljson: skipped {skipped} corrupt line(s) during read");
    }

    Ok((observations, metric_names.into_iter().collect()))
}

/// Create Arrow schema from discovered metric names
fn create_arrow_schema(metric_names: &[String]) -> SchemaRef {
    let mut fields = vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )];

    for metric_name in metric_names {
        fields.push(Field::new(metric_name, DataType::Float64, true));
    }

    Arc::new(Schema::new(fields))
}

/// Build a single RecordBatch from observations
/// Each row represents one unique timestamp, with columns for each metric
fn build_record_batch(
    schema: SchemaRef,
    observations: &[Observation],
    metric_names: &[String],
) -> Result<RecordBatch> {
    // Group observations by timestamp
    let mut timestamp_map: BTreeMap<i64, BTreeMap<String, f64>> = BTreeMap::new();

    for obs in observations {
        let _ = timestamp_map
            .entry(obs.timestamp_ns)
            .or_default()
            .insert(obs.metric_name.clone(), obs.value);
    }

    // Build timestamp array
    let timestamps: Vec<i64> = timestamp_map.keys().copied().collect();
    let timestamp_array = TimestampNanosecondArray::from(timestamps.clone());

    // Build metric value arrays
    let mut metric_arrays = Vec::new();
    for metric_name in metric_names {
        let values: Vec<Option<f64>> = timestamps
            .iter()
            .map(|ts| {
                timestamp_map
                    .get(ts)
                    .and_then(|m| m.get(metric_name))
                    .copied()
            })
            .collect();
        metric_arrays.push(Arc::new(Float64Array::from(values)) as arrow::array::ArrayRef);
    }

    // Combine into columns
    let mut columns = vec![Arc::new(timestamp_array) as arrow::array::ArrayRef];
    columns.extend(metric_arrays);

    RecordBatch::try_new(schema, columns).map_err(|e| Error::Arrow(e.to_string()))
}

#[async_trait]
impl FormatProvider for OtelJsonProvider {
    fn name(&self) -> &str {
        "oteljson"
    }

    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        _url: &Url,
    ) -> Result<SchemaRef> {
        let (_observations, metric_names) = read_all(reader).await?;
        Ok(create_arrow_schema(&metric_names))
    }

    async fn open_stream(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        _url: &Url,
    ) -> Result<(
        SchemaRef,
        Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    )> {
        let (observations, metric_names) = read_all(reader).await?;
        let schema = create_arrow_schema(&metric_names);
        let batch = build_record_batch(schema.clone(), &observations, &metric_names)?;
        let stream = crate::format::batch::single_batch_stream(batch);
        Ok((schema, stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_parse_gauge_data() {
        // JSON structure using OTLP format with proper field names
        let json = r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"test_metric","gauge":{"dataPoints":[{"timeUnixNano":"1750424338211000000","asDouble":123.45}]}}]}]}]}"#;

        let observations = parse_line(json).unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].metric_name, "test_metric");
        assert_eq!(observations[0].value, 123.45);
        assert_eq!(observations[0].timestamp_ns, 1750424338211000000);
    }

    #[tokio::test]
    async fn test_schema_discovery() {
        let data = r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"metric_a","gauge":{"dataPoints":[{"timeUnixNano":"1000000000","asDouble":1.0}]}}]}]}]}
{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"metric_b","gauge":{"dataPoints":[{"timeUnixNano":"2000000000","asDouble":2.0}]}}]}]}]}"#;

        let cursor = Cursor::new(data);
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(cursor);

        let (_observations, metric_names) = read_all(reader).await.unwrap();
        assert_eq!(metric_names.len(), 2);
        assert!(metric_names.contains(&"metric_a".to_string()));
        assert!(metric_names.contains(&"metric_b".to_string()));
    }

    #[tokio::test]
    async fn test_oteljson_provider() {
        let data = r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"temp","gauge":{"dataPoints":[{"timeUnixNano":"1000000000","asDouble":20.5}]}}]}]}]}
{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"humidity","gauge":{"dataPoints":[{"timeUnixNano":"1000000000","asDouble":65.0}]}},{"name":"temp","gauge":{"dataPoints":[{"timeUnixNano":"2000000000","asDouble":21.0}]}}]}]}]}"#;

        let cursor = Cursor::new(data);
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(cursor);
        let url = Url::parse("oteljson:///test.json").unwrap();

        let provider = OtelJsonProvider::new();
        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();

        // Verify schema: timestamp + humidity + temp (alphabetically sorted)
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "humidity");
        assert_eq!(schema.field(2).name(), "temp");

        // Verify we get one batch
        use futures::StreamExt;
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2); // Two unique timestamps
        assert_eq!(batch.num_columns(), 3);
    }
}

// Register the OtelJson provider
crate::register_format_provider!(scheme: "oteljson", provider: OtelJsonProvider::new);
