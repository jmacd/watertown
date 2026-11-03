// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::{
    FormatProvider, Result, Url,
    site_analytics::{MAX_RECORD_BYTES, Record, invalid, parse_record},
};
use arrow::{
    array::{StringArray, TimestampMicrosecondArray, UInt32Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::Stream;
use std::{pin::Pin, sync::Arc};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

pub struct SiteAnalyticsProvider;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("schema_version", DataType::UInt32, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("site_id", DataType::Utf8, false),
        Field::new("catalog_id", DataType::Utf8, false),
        Field::new("page_uri", DataType::Utf8, false),
        Field::new("segment_id", DataType::Utf8, false),
        Field::new("record_seq", DataType::UInt64, false),
    ]))
}

fn batch(rows: &[Record]) -> Result<RecordBatch> {
    crate::format::batch::finish_batch(
        schema(),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|r| r.schema_version),
            )),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.received_at_us))
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| &r.site_id),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| &r.catalog_id),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| &r.page_uri),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.segment_id.to_string()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|r| r.record_seq),
            )),
        ],
    )
}

#[async_trait]
impl FormatProvider for SiteAnalyticsProvider {
    fn name(&self) -> &str {
        "site-analytics"
    }

    async fn infer_schema(
        &self,
        _reader: Pin<Box<dyn AsyncRead + Send>>,
        _url: &Url,
    ) -> Result<SchemaRef> {
        Ok(schema())
    }

    async fn open_stream(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        _url: &Url,
    ) -> Result<(
        SchemaRef,
        Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    )> {
        let stream = async_stream::try_stream! {
            let mut reader = BufReader::new(reader);
            let mut rows = Vec::with_capacity(1024);
            let mut previous: Option<(String, uuid::Uuid, u64)> = None;
            loop {
                let mut line = Vec::new();
                (&mut reader).take(MAX_RECORD_BYTES + 1).read_until(b'\n', &mut line).await?;
                if line.is_empty() { break; }
                let record = parse_record(&line)?;
                // A physical series version may start after sequence 1.
                // Ingestion establishes the complete segment's membership.
                if let Some((site, segment, seq)) = &previous
                    && (site != &record.site_id || segment != &record.segment_id
                        || seq.checked_add(1) != Some(record.record_seq))
                {
                    Err(invalid("mixed identities or nonconsecutive records in a series chunk"))?;
                }
                previous = Some((record.site_id.clone(), record.segment_id, record.record_seq));
                rows.push(record);
                if rows.len() == 1024 {
                    yield batch(&rows)?;
                    rows.clear();
                }
            }
            if !rows.is_empty() { yield batch(&rows)?; }
        };
        Ok((schema(), Box::pin(stream)))
    }
}

crate::register_format_provider!(scheme: "site-analytics", provider: || SiteAnalyticsProvider);
