// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Web Log (CLF/Combined) Format Provider
//!
//! Parses Combined Log Format (nginx default, Apache `LogFormat combined`) into
//! typed Arrow record batches.  Unlike `jsonlogs://` (all-Utf8), this provider
//! uses native Arrow types for timestamp, status code, and body size so that
//! DataFusion queries work without CAST.
//!
//! Combined Log Format example:
//! ```text
//! 93.184.216.34 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 2326 "http://www.example.com/" "Mozilla/5.0 ..."
//! ```
//!
//! Falls back to Common Log Format (7 fields, no referer/user-agent).

use crate::format::FormatProvider;
use crate::{Error, Result, Url};
use arrow::array::{StringArray, TimestampMicrosecondArray, UInt16Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::DateTime;
use futures::stream::Stream;
use regex::Regex;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

/// Web Log format provider -- parses CLF/Combined log lines.
pub struct WeblogProvider;

impl WeblogProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for WeblogProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Arrow schema for parsed web log entries.
fn weblog_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("remote_addr", DataType::Utf8, false),
        Field::new("remote_user", DataType::Utf8, true),
        Field::new("method", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("protocol", DataType::Utf8, true),
        Field::new("status", DataType::UInt16, false),
        Field::new("body_bytes_sent", DataType::UInt64, false),
        Field::new("http_referer", DataType::Utf8, true),
        Field::new("http_user_agent", DataType::Utf8, true),
    ]))
}

/// A single parsed log line.
struct LogEntry {
    timestamp_us: i64,
    remote_addr: String,
    remote_user: Option<String>,
    method: Option<String>,
    path: Option<String>,
    protocol: Option<String>,
    status: u16,
    body_bytes_sent: u64,
    http_referer: Option<String>,
    http_user_agent: Option<String>,
}

/// Parse a CLF timestamp like `10/Oct/2000:13:55:36 -0700` into microseconds
/// since Unix epoch (UTC).
fn parse_clf_timestamp(s: &str) -> Option<i64> {
    // CLF format: DD/Mon/YYYY:HH:MM:SS +ZZZZ
    // chrono parse with explicit format
    let fmt = "%d/%b/%Y:%H:%M:%S %z";
    DateTime::parse_from_str(s, fmt)
        .ok()
        .map(|dt| dt.timestamp_micros())
}

/// Parse the request field like `GET /index.html HTTP/1.1`.
fn parse_request(req: &str) -> (Option<String>, Option<String>, Option<String>) {
    let parts: Vec<&str> = req.splitn(3, ' ').collect();
    match parts.len() {
        3 => (
            Some(parts[0].to_string()),
            Some(parts[1].to_string()),
            Some(parts[2].to_string()),
        ),
        2 => (Some(parts[0].to_string()), Some(parts[1].to_string()), None),
        1 if !parts[0].is_empty() => (Some(parts[0].to_string()), None, None),
        _ => (None, None, None),
    }
}

/// Normalize a CLF field: `-` becomes None.
fn dash_to_none(s: &str) -> Option<String> {
    if s == "-" { None } else { Some(s.to_string()) }
}

/// Read all lines, parse each, skip malformed lines with a warning.
async fn read_all_lines(reader: Pin<Box<dyn AsyncRead + Send>>) -> Result<Vec<LogEntry>> {
    let mut reader = BufReader::new(reader);
    let mut entries = Vec::new();
    let mut line = String::new();
    let mut skipped = 0u64;

    // Combined Log Format regex:
    //   (\S+)                   remote_addr
    //   \S+                     remote_ident (ignored)
    //   (\S+)                   remote_user
    //   \[([^\]]+)\]            timestamp
    //   "([^"]*)"               request
    //   (\d{3})                 status
    //   (\d+|-)                 body_bytes_sent
    //   (?:\s+"([^"]*)")?       http_referer  (optional, Combined only)
    //   (?:\s+"([^"]*)")?       http_user_agent (optional, Combined only)
    let re = Regex::new(
        r#"^(\S+) \S+ (\S+) \[([^\]]+)\] "([^"]*)" (\d{3}) (\d+|-)(?:\s+"([^"]*)")?(?:\s+"([^"]*)")?$"#,
    )
    .expect("weblog regex must compile");

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await.map_err(Error::Io)?;
        if bytes_read == 0 {
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(caps) = re.captures(trimmed) {
            let remote_addr = caps[1].to_string();
            let remote_user = dash_to_none(&caps[2]);
            let ts_str = &caps[3];
            let request = &caps[4];
            let status_str = &caps[5];
            let bytes_str = &caps[6];
            let referer = caps.get(7).and_then(|m| dash_to_none(m.as_str()));
            let user_agent = caps.get(8).and_then(|m| dash_to_none(m.as_str()));

            let timestamp_us = match parse_clf_timestamp(ts_str) {
                Some(ts) => ts,
                None => {
                    skipped += 1;
                    continue;
                }
            };

            let status: u16 = match status_str.parse() {
                Ok(s) => s,
                Err(_) => {
                    skipped += 1;
                    continue;
                }
            };

            let body_bytes_sent: u64 = if bytes_str == "-" {
                0
            } else {
                match bytes_str.parse() {
                    Ok(b) => b,
                    Err(_) => {
                        skipped += 1;
                        continue;
                    }
                }
            };

            let (method, path, protocol) = parse_request(request);

            entries.push(LogEntry {
                timestamp_us,
                remote_addr,
                remote_user,
                method,
                path,
                protocol,
                status,
                body_bytes_sent,
                http_referer: referer,
                http_user_agent: user_agent,
            });
        } else {
            skipped += 1;
        }
    }

    if skipped > 0 {
        log::warn!("weblog: skipped {skipped} unparseable line(s)");
    }

    Ok(entries)
}

/// Build a RecordBatch from parsed log entries.
fn build_record_batch(schema: SchemaRef, entries: &[LogEntry]) -> Result<RecordBatch> {
    let timestamps: Vec<i64> = entries.iter().map(|e| e.timestamp_us).collect();
    let remote_addrs: Vec<&str> = entries.iter().map(|e| e.remote_addr.as_str()).collect();
    let remote_users: Vec<Option<&str>> =
        entries.iter().map(|e| e.remote_user.as_deref()).collect();
    let methods: Vec<Option<&str>> = entries.iter().map(|e| e.method.as_deref()).collect();
    let paths: Vec<Option<&str>> = entries.iter().map(|e| e.path.as_deref()).collect();
    let protocols: Vec<Option<&str>> = entries.iter().map(|e| e.protocol.as_deref()).collect();
    let statuses: Vec<u16> = entries.iter().map(|e| e.status).collect();
    let body_bytes: Vec<u64> = entries.iter().map(|e| e.body_bytes_sent).collect();
    let referers: Vec<Option<&str>> = entries.iter().map(|e| e.http_referer.as_deref()).collect();
    let user_agents: Vec<Option<&str>> = entries
        .iter()
        .map(|e| e.http_user_agent.as_deref())
        .collect();

    let columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC")),
        Arc::new(StringArray::from(remote_addrs)),
        Arc::new(StringArray::from(remote_users)),
        Arc::new(StringArray::from(methods)),
        Arc::new(StringArray::from(paths)),
        Arc::new(StringArray::from(protocols)),
        Arc::new(UInt16Array::from(statuses)),
        Arc::new(UInt64Array::from(body_bytes)),
        Arc::new(StringArray::from(referers)),
        Arc::new(StringArray::from(user_agents)),
    ];

    if entries.is_empty() {
        crate::format::batch::finish_batch(schema, columns)
    } else {
        RecordBatch::try_new(schema, columns).map_err(|e| Error::Arrow(e.to_string()))
    }
}

#[async_trait]
impl FormatProvider for WeblogProvider {
    fn name(&self) -> &str {
        "weblog"
    }

    async fn infer_schema(
        &self,
        _reader: Pin<Box<dyn AsyncRead + Send>>,
        _url: &Url,
    ) -> Result<SchemaRef> {
        // Schema is fixed -- no need to read data.
        Ok(weblog_schema())
    }

    async fn open_stream(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        _url: &Url,
    ) -> Result<(
        SchemaRef,
        Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    )> {
        let entries = read_all_lines(reader).await?;
        let schema = weblog_schema();
        let batch = build_record_batch(schema.clone(), &entries)?;
        let stream = crate::format::batch::single_batch_stream(batch);
        Ok((schema, stream))
    }
}

// Register the weblog provider
crate::register_format_provider!(scheme: "weblog", provider: WeblogProvider::new);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use futures::StreamExt;
    use std::io::Cursor;

    fn make_reader(data: &str) -> Pin<Box<dyn AsyncRead + Send>> {
        Box::pin(Cursor::new(data.to_string().into_bytes()))
    }

    // Sample Combined Log Format line
    const COMBINED_LINE: &str = r#"93.184.216.34 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 2326 "http://www.example.com/start.html" "Mozilla/5.0 (X11; Linux x86_64)""#;

    // Common Log Format (no referer/user-agent)
    const COMMON_LINE: &str =
        r#"127.0.0.1 - - [01/Jan/2025:00:00:00 +0000] "POST /api/data HTTP/2.0" 201 512"#;

    #[tokio::test]
    async fn test_empty_input() {
        let reader = make_reader("");
        let url = Url::parse("weblog:///test.log").unwrap();
        let provider = WeblogProvider::new();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        assert_eq!(schema.fields().len(), 10);

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_combined_format() {
        let reader = make_reader(COMBINED_LINE);
        let url = Url::parse("weblog:///test.log").unwrap();
        let provider = WeblogProvider::new();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        assert_eq!(schema.fields().len(), 10);

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Check remote_addr
        let addr = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(addr.value(0), "93.184.216.34");

        // Check remote_user
        let user = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(user.value(0), "frank");

        // Check method
        let method = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(method.value(0), "GET");

        // Check path
        let path = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path.value(0), "/index.html");

        // Check protocol
        let proto = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(proto.value(0), "HTTP/1.1");

        // Check status
        let status = batch
            .column(6)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert_eq!(status.value(0), 200);

        // Check body_bytes_sent
        let bytes = batch
            .column(7)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(bytes.value(0), 2326);

        // Check referer
        let referer = batch
            .column(8)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(referer.value(0), "http://www.example.com/start.html");

        // Check user_agent
        let ua = batch
            .column(9)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ua.value(0), "Mozilla/5.0 (X11; Linux x86_64)");
    }

    #[tokio::test]
    async fn test_common_log_format() {
        let reader = make_reader(COMMON_LINE);
        let url = Url::parse("weblog:///test.log").unwrap();
        let provider = WeblogProvider::new();

        let (_schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        // remote_user should be null (was "-")
        let user = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(user.is_null(0));

        // method = POST
        let method = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(method.value(0), "POST");

        // status = 201
        let status = batch
            .column(6)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert_eq!(status.value(0), 201);

        // referer and user_agent should be null (not present in CLF)
        let referer = batch
            .column(8)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(referer.is_null(0));

        let ua = batch
            .column(9)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(ua.is_null(0));
    }

    #[tokio::test]
    async fn test_multiple_lines() {
        let data = format!("{}\n{}\n", COMBINED_LINE, COMMON_LINE);
        let reader = make_reader(&data);
        let url = Url::parse("weblog:///test.log").unwrap();
        let provider = WeblogProvider::new();

        let (_schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_skips_malformed_lines() {
        let data = "this is not a log line\n\n{\"json\":true}\n".to_string()
            + COMBINED_LINE
            + "\nmore garbage\n";
        let reader = make_reader(&data);
        let url = Url::parse("weblog:///test.log").unwrap();
        let provider = WeblogProvider::new();

        let (_schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        // Only 1 valid line
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_dash_body_bytes() {
        // Some servers log `-` for body_bytes_sent when response has no body
        let line = r#"10.0.0.1 - - [15/Mar/2025:12:00:00 +0000] "HEAD / HTTP/1.1" 204 -"#;
        let reader = make_reader(line);
        let url = Url::parse("weblog:///test.log").unwrap();
        let provider = WeblogProvider::new();

        let (_schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let bytes = batch
            .column(7)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(bytes.value(0), 0);
    }

    #[tokio::test]
    async fn test_ipv6_remote_addr() {
        let line = r#"::1 - - [15/Mar/2025:12:00:00 +0000] "GET /health HTTP/1.1" 200 0"#;
        let reader = make_reader(line);
        let url = Url::parse("weblog:///test.log").unwrap();
        let provider = WeblogProvider::new();

        let (_schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let addr = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(addr.value(0), "::1");
    }

    #[tokio::test]
    async fn test_timestamp_utc_conversion() {
        // Line with -0700 offset: 13:55:36 -0700 = 20:55:36 UTC
        let reader = make_reader(COMBINED_LINE);
        let url = Url::parse("weblog:///test.log").unwrap();
        let provider = WeblogProvider::new();

        let (_schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();

        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let val = ts.value(0);
        // 2000-10-10T20:55:36 UTC in microseconds
        let expected = DateTime::parse_from_rfc3339("2000-10-10T20:55:36+00:00")
            .unwrap()
            .timestamp_micros();
        assert_eq!(val, expected);
    }

    #[tokio::test]
    async fn test_schema_inference() {
        let reader = make_reader("");
        let url = Url::parse("weblog:///test.log").unwrap();
        let provider = WeblogProvider::new();

        let schema = provider.infer_schema(reader, &url).await.unwrap();
        assert_eq!(schema.fields().len(), 10);
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(
            *schema.field(0).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(6).name(), "status");
        assert_eq!(*schema.field(6).data_type(), DataType::UInt16);
        assert_eq!(schema.field(7).name(), "body_bytes_sent");
        assert_eq!(*schema.field(7).data_type(), DataType::UInt64);
    }

    #[tokio::test]
    async fn test_malformed_request() {
        // Protocol scanners sometimes send garbage in the request field
        let line = r#"192.168.1.1 - - [15/Mar/2025:03:14:15 +0000] "" 400 0"#;
        let reader = make_reader(line);
        let url = Url::parse("weblog:///test.log").unwrap();
        let provider = WeblogProvider::new();

        let (_schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Empty request: all request fields should be null
        let method = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(method.is_null(0));
    }
}
