// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! ExcelHTML Format Provider
//!
//! Parses HydroVu HTML export files that wrap tabular data in HTML <table> elements.
//! These files are Excel-compatible HTML with a specific structure:
//! - Metadata in <meta> tags
//! - Main data in <table id="isi-report">
//! - Section headers for location properties
//! - Data headers with column names and attributes
//! - Data rows with timestamp attributes

#![allow(elided_lifetimes_in_paths)]

use crate::{Error, FormatProvider, Result, Url};
use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncRead;

/// ExcelHTML format provider for HydroVu exports
pub struct ExcelHtmlProvider;

impl ExcelHtmlProvider {
    /// Create a new ExcelHTML provider
    pub fn new() -> Self {
        Self
    }
}

impl Default for ExcelHtmlProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FormatProvider for ExcelHtmlProvider {
    fn name(&self) -> &str {
        "excelhtml"
    }

    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        url: &Url,
    ) -> Result<SchemaRef> {
        // For ExcelHTML, we need to parse the file to infer schema
        // Reuse open_stream logic
        let (schema, _stream) = self.open_stream(reader, url).await?;
        Ok(schema)
    }

    async fn open_stream(
        &self,
        mut reader: Pin<Box<dyn AsyncRead + Send>>,
        _url: &Url,
    ) -> Result<(
        SchemaRef,
        Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    )> {
        // Read entire HTML file into memory
        // (These files are typically small, < 10MB)
        use tokio::io::AsyncReadExt;
        let mut html_bytes = Vec::new();
        let _ = reader.read_to_end(&mut html_bytes).await?;

        let html_str = String::from_utf8(html_bytes)
            .map_err(|e| Error::InvalidUrl(format!("Invalid UTF-8 in HTML: {}", e)))?;

        // Parse HTML
        let dom = tl::parse(&html_str, tl::ParserOptions::default())
            .map_err(|e| Error::InvalidUrl(format!("HTML parse error: {}", e)))?;

        // Find the data table
        let parser = dom.parser();
        let table = dom
            .get_element_by_id("isi-report")
            .ok_or_else(|| Error::InvalidUrl("No table with id='isi-report' found".to_string()))?;

        let table_node = table
            .get(parser)
            .ok_or_else(|| Error::InvalidUrl("Could not resolve table node".to_string()))?;

        // Extract column headers and data rows
        let (schema, rows) = parse_table_data(table_node, parser)?;

        // Create stream from parsed rows
        let batch_size = 1000;
        let stream = create_batch_stream(schema.clone(), rows, batch_size);

        Ok((schema, Box::pin(stream)))
    }
}

/// Parse table data: extract schema from headers and data from rows
fn parse_table_data(
    table_node: &tl::Node,
    parser: &tl::Parser,
) -> Result<(SchemaRef, Vec<Vec<String>>)> {
    let table_html = table_node.inner_html(parser);

    // Re-parse just the table content
    let table_dom = tl::parse(&table_html, tl::ParserOptions::default())
        .map_err(|e| Error::InvalidUrl(format!("Table parse error: {}", e)))?;

    let table_parser = table_dom.parser();

    // Find all <tr> elements
    let rows: Vec<_> = table_dom
        .nodes()
        .iter()
        .filter_map(|node| {
            node.as_tag()
                .filter(|&tag| tag.name().as_utf8_str() == "tr")
        })
        .collect();

    // Find the data header row (has class="dataHeader" or isi-data-table attribute)
    let header_row = rows
        .iter()
        .find(|row| {
            row.attributes()
                .class()
                .map(|c| c.as_utf8_str().contains("dataHeader"))
                .unwrap_or(false)
                || row.attributes().get("isi-data-table").flatten().is_some()
        })
        .ok_or_else(|| Error::InvalidUrl("No dataHeader row found".to_string()))?;

    // Extract column names from header
    let column_names = extract_column_names(header_row, table_parser)?;

    // Build schema (DateTime as String, all parameters as Float64)
    let mut fields = Vec::new();
    for col_name in &column_names {
        let data_type = if col_name == "Date Time" {
            DataType::Utf8
        } else {
            DataType::Float64
        };
        fields.push(Field::new(col_name, data_type, true));
    }
    let schema = Arc::new(Schema::new(fields));

    // Extract data rows (have class="data" or isi-data-row attribute)
    let mut data_rows = Vec::new();
    for row in rows.iter() {
        let is_data_row = row
            .attributes()
            .class()
            .map(|c| c.as_utf8_str().split_whitespace().any(|cls| cls == "data"))
            .unwrap_or(false)
            || row.attributes().get("isi-data-row").flatten().is_some();

        if is_data_row {
            let row_data = extract_row_data(row, table_parser, column_names.len())?;
            data_rows.push(row_data);
        }
    }

    Ok((schema, data_rows))
}

/// Extract column names from header row
fn extract_column_names(header_row: &tl::HTMLTag, parser: &tl::Parser) -> Result<Vec<String>> {
    let mut columns = Vec::new();

    // Find all <td> elements in this row
    let header_html = header_row.inner_html(parser);
    let header_dom = tl::parse(&header_html, tl::ParserOptions::default())
        .map_err(|e| Error::InvalidUrl(format!("Header parse error: {}", e)))?;

    for node in header_dom.nodes().iter() {
        if let Some(tag) = node.as_tag()
            && tag.name().as_utf8_str() == "td"
        {
            let text = tag.inner_text(header_dom.parser()).to_string();
            columns.push(text.trim().to_string());
        }
    }

    if columns.is_empty() {
        return Err(Error::InvalidUrl("No columns found in header".to_string()));
    }

    Ok(columns)
}

/// Extract data from a single row
fn extract_row_data(
    row: &tl::HTMLTag,
    parser: &tl::Parser,
    expected_cols: usize,
) -> Result<Vec<String>> {
    let mut values = Vec::new();

    let row_html = row.inner_html(parser);
    let row_dom = tl::parse(&row_html, tl::ParserOptions::default())
        .map_err(|e| Error::InvalidUrl(format!("Row parse error: {}", e)))?;

    for node in row_dom.nodes().iter() {
        if let Some(tag) = node.as_tag()
            && tag.name().as_utf8_str() == "td"
        {
            let text = tag.inner_text(row_dom.parser()).to_string();
            values.push(text.trim().to_string());
        }
    }

    // Pad with empty strings if needed
    while values.len() < expected_cols {
        values.push(String::new());
    }

    Ok(values)
}

/// Create a stream of RecordBatches from parsed rows
fn create_batch_stream(
    schema: SchemaRef,
    rows: Vec<Vec<String>>,
    batch_size: usize,
) -> impl Stream<Item = Result<RecordBatch>> {
    async_stream::stream! {
        for chunk in rows.chunks(batch_size) {
            let batch = rows_to_batch(schema.clone(), chunk)?;
            yield Ok(batch);
        }
    }
}

/// Convert a chunk of rows to a RecordBatch
fn rows_to_batch(schema: SchemaRef, rows: &[Vec<String>]) -> Result<RecordBatch> {
    let num_cols = schema.fields().len();
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

    for col_idx in 0..num_cols {
        let field = schema.field(col_idx);

        match field.data_type() {
            DataType::Utf8 => {
                let values: Vec<Option<&str>> = rows
                    .iter()
                    .map(|row| {
                        let val = row.get(col_idx).map(|s| s.as_str()).unwrap_or("");
                        if val.is_empty() { None } else { Some(val) }
                    })
                    .collect();
                columns.push(Arc::new(StringArray::from(values)));
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|row| {
                        row.get(col_idx).and_then(|s| {
                            let trimmed = s.trim();
                            if trimmed.is_empty() {
                                None
                            } else {
                                trimmed.parse::<f64>().ok()
                            }
                        })
                    })
                    .collect();
                columns.push(Arc::new(Float64Array::from(values)));
            }
            _ => {
                return Err(Error::InvalidUrl(format!(
                    "Unsupported data type: {:?}",
                    field.data_type()
                )));
            }
        }
    }

    RecordBatch::try_new(schema, columns).map_err(|e| Error::Arrow(e.to_string()))
}

// Register the format provider
crate::register_format_provider!(scheme: "excelhtml", provider: ExcelHtmlProvider::new);

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_excelhtml_provider_registration() {
        let provider = crate::FormatRegistry::get_provider("excelhtml");
        assert!(provider.is_some());
        assert_eq!(provider.unwrap().name(), "excelhtml");
    }
}
