// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! JSON Lines (Logs) Format Provider
//!
//! Reads newline-delimited JSON files (JSON Lines / NDJSON) and presents them
//! as Arrow record batches.  Every JSON key becomes a Utf8 column; values are
//! stored as their JSON string representation.
//!
//! Two-pass reader:
//!   1. Discover all unique keys across all lines (sorted, for stable schema).
//!   2. Build record batches with one Utf8 column per key.
//!
//! Designed for systemd journal JSON output (`journalctl --output=json`) but
//! works with any JSON Lines source.

use crate::format::FormatProvider;
use crate::{Result, Url};
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::Stream;
use serde_json::Value;
use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncRead;

/// JSON Lines format provider
pub struct JsonLogsProvider;

impl JsonLogsProvider {
    /// Create a new JSON Lines provider
    pub fn new() -> Self {
        Self
    }
}

impl Default for JsonLogsProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Read all lines from the async reader, stripping null bytes and blank lines.
/// Returns the parsed JSON values and the set of all keys discovered.
async fn read_all_lines(
    reader: Pin<Box<dyn AsyncRead + Send>>,
) -> Result<(Vec<Value>, Vec<String>)> {
    let mut keys = BTreeSet::new();
    let mut rows: Vec<Value> = Vec::new();

    let skipped = crate::format::batch::read_clean_lines(reader, |trimmed| {
        match serde_json::from_str::<Value>(trimmed) {
            Ok(Value::Object(map)) => {
                for key in map.keys() {
                    let _ = keys.insert(key.clone());
                }
                rows.push(Value::Object(map));
                Ok(true)
            }
            // Non-object JSON values and parse errors are skipped.
            Ok(_) | Err(_) => Ok(false),
        }
    })
    .await?;

    if skipped > 0 {
        log::warn!("jsonlogs: skipped {skipped} unparseable or non-object line(s)");
    }

    let sorted_keys: Vec<String> = keys.into_iter().collect();
    Ok((rows, sorted_keys))
}

/// Create an Arrow schema where every key is a Utf8 column.
fn create_schema(keys: &[String]) -> SchemaRef {
    let fields: Vec<Field> = keys
        .iter()
        .map(|k| Field::new(k, DataType::Utf8, true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Build a single RecordBatch from parsed rows.
///
/// Each JSON value is converted to its string representation:
/// - Strings are stored directly (without surrounding quotes)
/// - Numbers, booleans, nulls are stored as their JSON text
/// - Nested objects/arrays are stored as compact JSON strings
fn build_record_batch(schema: SchemaRef, rows: &[Value], keys: &[String]) -> Result<RecordBatch> {
    let columns: Vec<Arc<dyn arrow::array::Array>> = keys
        .iter()
        .map(|key| {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| {
                    row.get(key).and_then(|v| match v {
                        Value::Null => None,
                        Value::String(s) => Some(s.clone()),
                        Value::Number(n) => Some(n.to_string()),
                        Value::Bool(b) => Some(b.to_string()),
                        // Nested objects/arrays: store as compact JSON
                        other => Some(other.to_string()),
                    })
                })
                .collect();
            Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>
        })
        .collect();

    crate::format::batch::finish_batch(schema, columns)
}

#[async_trait]
impl FormatProvider for JsonLogsProvider {
    fn name(&self) -> &str {
        "jsonlogs"
    }

    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        _url: &Url,
    ) -> Result<SchemaRef> {
        let (_rows, keys) = read_all_lines(reader).await?;
        Ok(create_schema(&keys))
    }

    async fn open_stream(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        _url: &Url,
    ) -> Result<(
        SchemaRef,
        Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    )> {
        let (rows, keys) = read_all_lines(reader).await?;
        let schema = create_schema(&keys);
        let batch = build_record_batch(schema.clone(), &rows, &keys)?;

        let stream = crate::format::batch::single_batch_stream(batch);
        Ok((schema, stream))
    }
}

// Register the JSON Lines provider
crate::register_format_provider!(scheme: "jsonlogs", provider: JsonLogsProvider::new);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use futures::StreamExt;
    use std::io::Cursor;

    fn make_reader(data: &str) -> Pin<Box<dyn AsyncRead + Send>> {
        Box::pin(Cursor::new(data.to_string().into_bytes()))
    }

    #[tokio::test]
    async fn test_empty_input() {
        let reader = make_reader("");
        let url = Url::parse("jsonlogs:///test.jsonl").unwrap();
        let provider = JsonLogsProvider::new();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        assert_eq!(schema.fields().len(), 0);

        let next = stream.next().await;
        assert!(next.unwrap().unwrap().num_rows() == 0);
    }

    #[tokio::test]
    async fn test_single_line() {
        let data = r#"{"name":"Alice","age":"30"}"#;
        let reader = make_reader(data);
        let url = Url::parse("jsonlogs:///test.jsonl").unwrap();
        let provider = JsonLogsProvider::new();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        assert_eq!(schema.fields().len(), 2);
        // BTreeSet sorts keys alphabetically
        assert_eq!(schema.field(0).name(), "age");
        assert_eq!(schema.field(1).name(), "name");

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);

        let age_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(age_col.value(0), "30");

        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
    }

    #[tokio::test]
    async fn test_heterogeneous_keys() {
        let data = r#"{"a":"1","b":"2"}
{"b":"3","c":"4"}"#;
        let reader = make_reader(data);
        let url = Url::parse("jsonlogs:///test.jsonl").unwrap();
        let provider = JsonLogsProvider::new();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        // Schema should have all 3 keys: a, b, c
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "a");
        assert_eq!(schema.field(1).name(), "b");
        assert_eq!(schema.field(2).name(), "c");

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Row 0: a=1, b=2, c=null
        let a_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a_col.value(0), "1");
        assert!(a_col.is_null(1));

        let c_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(c_col.is_null(0));
        assert_eq!(c_col.value(1), "4");
    }

    #[tokio::test]
    async fn test_numeric_and_bool_values() {
        let data = r#"{"count":42,"active":true,"label":"test"}"#;
        let reader = make_reader(data);
        let url = Url::parse("jsonlogs:///test.jsonl").unwrap();
        let provider = JsonLogsProvider::new();

        let (_schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();

        // All stored as strings
        let active = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(active.value(0), "true");

        let count = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(count.value(0), "42");
    }

    #[tokio::test]
    async fn test_skips_corrupt_lines() {
        let data = "not valid json\n{\"ok\":\"yes\"}\n{malformed}\n";
        let reader = make_reader(data);
        let url = Url::parse("jsonlogs:///test.jsonl").unwrap();
        let provider = JsonLogsProvider::new();

        let (_schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        // Only 1 valid line
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_null_byte_stripping() {
        let data = "{\"msg\":\"hello\"\0}\n";
        let reader = make_reader(data);
        let url = Url::parse("jsonlogs:///test.jsonl").unwrap();
        let provider = JsonLogsProvider::new();

        let (_schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_journal_like_data() {
        // Mimics journalctl --output=json
        let data = r#"{"__REALTIME_TIMESTAMP":"1772431543477823","PRIORITY":"3","MESSAGE":"Connection refused","_SYSTEMD_UNIT":"ssh.service","_HOSTNAME":"watershop","_PID":"19269","_BOOT_ID":"abc123"}
{"__REALTIME_TIMESTAMP":"1772431543501605","PRIORITY":"6","MESSAGE":"Session opened","_SYSTEMD_UNIT":"ssh.service","_HOSTNAME":"watershop","_PID":"19272","_BOOT_ID":"abc123"}"#;
        let reader = make_reader(data);
        let url = Url::parse("jsonlogs:///test.jsonl").unwrap();
        let provider = JsonLogsProvider::new();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();

        // Should have all 7 unique keys
        assert_eq!(schema.fields().len(), 7);

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Find MESSAGE column
        let msg_idx = schema.index_of("MESSAGE").unwrap();
        let messages = batch
            .column(msg_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(messages.value(0), "Connection refused");
        assert_eq!(messages.value(1), "Session opened");
    }

    #[tokio::test]
    async fn test_schema_inference() {
        let data = r#"{"x":"1","y":"2"}
{"y":"3","z":"4"}"#;
        let reader = make_reader(data);
        let url = Url::parse("jsonlogs:///test.jsonl").unwrap();
        let provider = JsonLogsProvider::new();

        let schema = provider.infer_schema(reader, &url).await.unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "x");
        assert_eq!(schema.field(1).name(), "y");
        assert_eq!(schema.field(2).name(), "z");
        // All Utf8
        for field in schema.fields() {
            assert_eq!(*field.data_type(), DataType::Utf8);
        }
    }
}
