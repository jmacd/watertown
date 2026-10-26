// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Excel (.xls and .xlsx) tables, buffered for Calamine's seekable reader.

use super::batch::{BatchStream, finish_batch};
use crate::{Error, FormatProvider, Result, Url};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use calamine::{Data, Range, Reader, Sheets, open_workbook_auto_from_rs};
use serde::Deserialize;
use std::collections::HashSet;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct XlsOptions {
    sheet: Option<String>,
    table: Option<String>,
    header: bool,
    header_row: Option<usize>,
    batch_size: usize,
}

impl Default for XlsOptions {
    fn default() -> Self {
        Self {
            sheet: None,
            table: None,
            header: true,
            header_row: None,
            batch_size: 8192,
        }
    }
}

#[derive(Default)]
pub struct XlsProvider;

impl XlsProvider {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FormatProvider for XlsProvider {
    fn name(&self) -> &str {
        "xls"
    }

    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        url: &Url,
    ) -> Result<SchemaRef> {
        let (schema, _) = self.open_stream(reader, url).await?;
        Ok(schema)
    }

    async fn open_stream(
        &self,
        mut reader: Pin<Box<dyn AsyncRead + Send>>,
        url: &Url,
    ) -> Result<(SchemaRef, BatchStream)> {
        let options: XlsOptions = url.query_params()?;
        if options.batch_size == 0 {
            return Err(Error::InvalidUrl("XLS batch_size must be positive".into()));
        }
        if options.table.is_some() && (!options.header || options.header_row.is_some()) {
            return Err(Error::InvalidUrl(
                "XLS table cannot be combined with header=false or header_row".into(),
            ));
        }
        let mut bytes = Vec::new();
        _ = reader.read_to_end(&mut bytes).await?;
        let batch_size = options.batch_size;
        let (range, schema, data_start, source_columns) =
            tokio::task::spawn_blocking(move || read_sheet(bytes, &options))
                .await
                .map_err(|e| Error::Xls(format!("workbook parsing task failed: {e}")))??;

        // Parsing and inference run off the async executor. Arrow conversion is
        // bounded by batch_size; the retained worksheet supplies rows lazily.
        let stream_schema = schema.clone();
        let stream = async_stream::try_stream! {
            let mut rows = range.rows().skip(data_start);
            loop {
                let chunk: Vec<_> = rows.by_ref().take(batch_size).collect();
                if chunk.is_empty() {
                    break;
                }
                yield rows_to_batch(stream_schema.clone(), &chunk, &source_columns)?;
            }
        };
        Ok((schema, Box::pin(stream)))
    }
}

fn read_sheet(
    bytes: Vec<u8>,
    options: &XlsOptions,
) -> Result<(Range<Data>, SchemaRef, usize, Vec<usize>)> {
    let mut workbook = open_workbook_auto_from_rs(Cursor::new(bytes))
        .map_err(|e| Error::Xls(format!("cannot open Excel workbook: {e}")))?;
    if !matches!(workbook, Sheets::Xls(_) | Sheets::Xlsx(_)) {
        return Err(Error::Xls(
            "cannot open Excel workbook: only .xls and .xlsx formats are supported".into(),
        ));
    }
    let names = workbook.sheet_names();
    let sheet = match options.sheet.as_deref() {
        Some(name) if names.iter().any(|n| n == name) => name.to_owned(),
        Some(name) => {
            return Err(Error::Xls(format!(
                "worksheet '{name}' not found; available worksheets: {names:?}"
            )));
        }
        None if names.len() == 1 => names[0].clone(),
        None => {
            return Err(Error::Xls(format!(
                "specify ?sheet=Name when the workbook does not have exactly one worksheet; available worksheets: {names:?}"
            )));
        }
    };
    if options.header && options.header_row.is_none() {
        match &mut workbook {
            Sheets::Xlsx(workbook) => {
                workbook.load_tables().map_err(|e| {
                    Error::Xls(format!(
                        "cannot read Excel tables in worksheet '{sheet}': {e}"
                    ))
                })?;
                let table_names = workbook
                    .table_names_in_sheet(&sheet)
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                if let Some(table_name) =
                    select_table(&table_names, options.table.as_deref(), &sheet)?
                {
                    let table = workbook.table_by_name(&table_name).map_err(|e| {
                        Error::Xls(format!("cannot read Excel table '{table_name}': {e}"))
                    })?;
                    let columns = table.columns().to_vec();
                    let range = table.data().clone();
                    let schema = table_schema(&range, &columns, &sheet, &table_name)?;
                    let source_columns = (0..columns.len()).collect();
                    return Ok((range, schema, 0, source_columns));
                }
            }
            Sheets::Xls(_) if options.table.is_some() => {
                return Err(Error::InvalidUrl(
                    "XLS table selection is only supported for .xlsx workbooks".into(),
                ));
            }
            _ => {}
        }
    }
    let range = workbook
        .worksheet_range(&sheet)
        .map_err(|e| Error::Xls(format!("cannot read worksheet '{sheet}': {e}")))?;
    if range.is_empty() {
        return Err(Error::Xls(format!("worksheet '{sheet}' is empty")));
    }
    let header_index = header_index(&range, options, &sheet)?;
    let data_start = header_index.map_or(0, |index| index + 1);
    let (schema, source_columns) = sheet_schema(&range, header_index, &sheet)?;
    Ok((range, schema, data_start, source_columns))
}

fn select_table(
    table_names: &[String],
    requested: Option<&str>,
    sheet: &str,
) -> Result<Option<String>> {
    match requested {
        Some(name) if table_names.iter().any(|candidate| candidate == name) => {
            Ok(Some(name.to_owned()))
        }
        Some(name) => Err(Error::Xls(format!(
            "Excel table '{name}' not found in worksheet '{sheet}'; available tables: \
             {table_names:?}"
        ))),
        None if table_names.len() == 1 => Ok(Some(table_names[0].clone())),
        None if table_names.len() > 1 => Err(Error::Xls(format!(
            "worksheet '{sheet}' contains multiple Excel tables; specify ?table=Name from \
             {table_names:?}"
        ))),
        None => Ok(None),
    }
}

fn header_index(range: &Range<Data>, options: &XlsOptions, sheet: &str) -> Result<Option<usize>> {
    match (options.header, options.header_row) {
        (false, None) => Ok(None),
        (false, Some(_)) => Err(Error::InvalidUrl(
            "XLS header_row requires header=true".into(),
        )),
        (true, None) => Ok(Some(detect_header_index(range.rows()))),
        (true, Some(0)) => Err(Error::InvalidUrl(
            "XLS header_row must be a positive worksheet row number".into(),
        )),
        (true, Some(row)) => {
            let (start_row, _) = range
                .start()
                .ok_or_else(|| Error::Xls(format!("worksheet '{sheet}' is empty")))?;
            let first_row = start_row as usize + 1;
            let last_row = first_row + range.height() - 1;
            if !(first_row..=last_row).contains(&row) {
                return Err(Error::Xls(format!(
                    "worksheet '{sheet}' header_row {row} is outside populated rows \
                     {first_row}..={last_row}"
                )));
            }
            Ok(Some(row - first_row))
        }
    }
}

fn detect_header_index<'a>(rows: impl IntoIterator<Item = &'a [Data]>) -> usize {
    let mut saw_sparse_preamble = false;
    let mut saw_blank_separator = false;
    for (index, row) in rows.into_iter().enumerate() {
        let populated = row
            .iter()
            .filter(|cell| !matches!(cell, Data::Empty))
            .count();
        if populated == 0 {
            saw_blank_separator |= saw_sparse_preamble;
            continue;
        }
        let text_only = row
            .iter()
            .filter(|cell| !matches!(cell, Data::Empty))
            .all(|cell| matches!(cell, Data::String(_)));
        if saw_blank_separator && populated > 1 && text_only {
            return index;
        }
        if populated > 1 {
            return 0;
        }
        saw_sparse_preamble = true;
    }
    0
}

fn sheet_schema(
    range: &Range<Data>,
    header_index: Option<usize>,
    sheet: &str,
) -> Result<(SchemaRef, Vec<usize>)> {
    let data_start = header_index.map_or(0, |index| index + 1);
    let types = infer_types(range, data_start, range.width(), sheet)?;
    let header_row = header_index
        .map(|index| {
            range
                .rows()
                .nth(index)
                .ok_or_else(|| Error::Xls(format!("worksheet '{sheet}' header row is missing")))
        })
        .transpose()?;
    let column_names = (0..types.len())
        .map(|index| match header_row {
            Some(row) if !matches!(row[index], Data::Empty) => cell_text(&row[index]),
            _ => Ok(String::new()),
        })
        .collect::<Result<Vec<_>>>()?;
    let source_columns = (0..types.len())
        .filter(|index| {
            header_row.is_none()
                || !column_names[*index].trim().is_empty()
                || types[*index] != DataType::Null
        })
        .collect::<Vec<_>>();
    if source_columns.is_empty() {
        return Err(Error::Xls(format!(
            "worksheet '{sheet}' has no named or populated columns after its header"
        )));
    }
    let selected_types = source_columns
        .iter()
        .map(|index| types[*index].clone())
        .collect();
    let selected_names = source_columns
        .iter()
        .map(|index| column_names[*index].clone())
        .collect();
    let schema = schema_from_names(selected_types, selected_names, &source_columns, sheet)?;
    Ok((schema, source_columns))
}

fn table_schema(
    range: &Range<Data>,
    column_names: &[String],
    sheet: &str,
    table: &str,
) -> Result<SchemaRef> {
    if !range.is_empty() && range.width() != column_names.len() {
        return Err(Error::Xls(format!(
            "Excel table '{table}' in worksheet '{sheet}' declares {} columns but contains {}",
            column_names.len(),
            range.width()
        )));
    }
    let types = infer_types(range, 0, column_names.len(), sheet)?;
    let source_columns = (0..column_names.len()).collect::<Vec<_>>();
    schema_from_names(types, column_names.to_vec(), &source_columns, sheet)
}

fn infer_types(
    range: &Range<Data>,
    data_start: usize,
    width: usize,
    sheet: &str,
) -> Result<Vec<DataType>> {
    let (start_row, start_col) = range.start().unwrap_or((0, 0));
    let mut types = vec![DataType::Null; width];
    for (row_index, row) in range.rows().enumerate().skip(data_start) {
        for (col_index, cell) in row.iter().enumerate() {
            let kind = cell_type(cell).map_err(|e| {
                Error::Xls(format!(
                    "worksheet '{sheet}', row {}, column {}: {e}",
                    u64::from(start_row) + row_index as u64 + 1,
                    u64::from(start_col) + col_index as u64 + 1
                ))
            })?;
            types[col_index] = merge_types(&types[col_index], kind);
        }
    }
    Ok(types)
}

fn schema_from_names(
    types: Vec<DataType>,
    column_names: Vec<String>,
    source_columns: &[usize],
    sheet: &str,
) -> Result<SchemaRef> {
    let mut names = HashSet::new();
    let mut fields = Vec::with_capacity(types.len());
    for ((kind, name), source_index) in types
        .into_iter()
        .zip(column_names)
        .zip(source_columns.iter().copied())
    {
        let name = if name.trim().is_empty() {
            format!("column_{}", source_index + 1)
        } else {
            name
        };
        if !names.insert(name.clone()) {
            return Err(Error::Xls(format!(
                "worksheet '{sheet}' has duplicate column name '{name}'; use ?header=false"
            )));
        }
        fields.push(Field::new(
            name,
            if kind == DataType::Null {
                DataType::Utf8
            } else {
                kind
            },
            true,
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn cell_type(cell: &Data) -> Result<DataType> {
    Ok(match cell {
        Data::Empty => DataType::Null,
        Data::Int(_) => DataType::Int64,
        Data::Float(_) => DataType::Float64,
        Data::Bool(_) => DataType::Boolean,
        Data::DateTime(date) if date.is_datetime() => {
            _ = timestamp(cell)?;
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        Data::Error(error) => return Err(Error::Xls(format!("spreadsheet cell error: {error}"))),
        _ => DataType::Utf8,
    })
}

fn merge_types(current: &DataType, next: DataType) -> DataType {
    match (current, &next) {
        (DataType::Null, _) => next,
        (_, DataType::Null) => current.clone(),
        _ if current == &next => next,
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
            DataType::Float64
        }
        _ => DataType::Utf8,
    }
}

fn timestamp(cell: &Data) -> Result<i64> {
    use calamine::DataType as _;
    cell.as_datetime()
        .map(|date| date.and_utc().timestamp_micros())
        .ok_or_else(|| Error::Xls(format!("invalid Excel datetime: {cell}")))
}

fn cell_text(cell: &Data) -> Result<String> {
    match cell {
        Data::DateTime(date) if date.is_datetime() => {
            use calamine::DataType as _;
            cell.as_datetime()
                .map(|date| date.format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
                .ok_or_else(|| Error::Xls(format!("invalid Excel datetime: {cell}")))
        }
        Data::Error(error) => Err(Error::Xls(format!("spreadsheet cell error: {error}"))),
        _ => Ok(cell.to_string()),
    }
}

fn rows_to_batch(
    schema: SchemaRef,
    rows: &[&[Data]],
    source_columns: &[usize],
) -> Result<arrow::record_batch::RecordBatch> {
    if schema.fields().len() != source_columns.len() {
        return Err(Error::Xls(
            "Excel schema does not match its source-column mapping".into(),
        ));
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (field, source_index) in schema.fields().iter().zip(source_columns) {
        let cells = || rows.iter().map(|row| &row[*source_index]);
        let unexpected = |cell: &Data| {
            Error::Xls(format!(
                "cell '{cell}' does not match inferred type for column '{}'",
                field.name()
            ))
        };
        let column: ArrayRef = match field.data_type() {
            DataType::Int64 => Arc::new(Int64Array::from(
                cells()
                    .map(|cell| match cell {
                        Data::Empty => Ok(None),
                        Data::Int(value) => Ok(Some(*value)),
                        _ => Err(unexpected(cell)),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )),
            DataType::Float64 => Arc::new(Float64Array::from(
                cells()
                    .map(|cell| match cell {
                        Data::Empty => Ok(None),
                        Data::Int(value) => Ok(Some(*value as f64)),
                        Data::Float(value) => Ok(Some(*value)),
                        _ => Err(unexpected(cell)),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )),
            DataType::Boolean => Arc::new(BooleanArray::from(
                cells()
                    .map(|cell| match cell {
                        Data::Empty => Ok(None),
                        Data::Bool(value) => Ok(Some(*value)),
                        _ => Err(unexpected(cell)),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Arc::new(TimestampMicrosecondArray::from(
                    cells()
                        .map(|cell| match cell {
                            Data::Empty => Ok(None),
                            _ => timestamp(cell).map(Some),
                        })
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            DataType::Utf8 => Arc::new(StringArray::from(
                cells()
                    .map(|cell| match cell {
                        Data::Empty => Ok(None),
                        _ => cell_text(cell).map(Some),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )),
            _ => return Err(Error::Xls(format!("unsupported inferred type: {field:?}"))),
        };
        columns.push(column);
    }
    finish_batch(schema, columns)
}

crate::register_format_provider!(scheme: "xls", provider: XlsProvider::new);

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> Data {
        Data::String(value.to_owned())
    }

    #[test]
    fn detects_report_header_after_sparse_preamble_and_blank_row() {
        let rows = [
            vec![
                text("Joshua MacDonald and Ariana Bayer"),
                Data::Empty,
                Data::Empty,
            ],
            vec![text("Account QuickReport"), Data::Empty, Data::Empty],
            vec![text("All Dates"), Data::Empty, Data::Empty],
            vec![Data::Empty, Data::Empty, Data::Empty],
            vec![
                Data::Empty,
                text("Distribution account"),
                text("Transaction date"),
            ],
            vec![text("Water Company Yearly"), Data::Empty, Data::Empty],
        ];

        assert_eq!(detect_header_index(rows.iter().map(Vec::as_slice)), 4);
    }

    #[test]
    fn keeps_first_row_without_report_separator_pattern() {
        let ordinary_table = [
            vec![text("name"), text("value")],
            vec![text("alpha"), Data::Int(10)],
        ];
        let sparse_header = [
            vec![text("name"), Data::Empty],
            vec![text("alpha"), text("value")],
        ];

        assert_eq!(
            detect_header_index(ordinary_table.iter().map(Vec::as_slice)),
            0
        );
        assert_eq!(
            detect_header_index(sparse_header.iter().map(Vec::as_slice)),
            0
        );
    }

    #[test]
    fn selects_unambiguous_excel_table() {
        let one = vec!["Transactions".to_owned()];
        let many = vec!["Transactions".to_owned(), "Summary".to_owned()];

        assert_eq!(
            select_table(&one, None, "Report").expect("one table"),
            Some("Transactions".to_owned())
        );
        assert_eq!(
            select_table(&many, Some("Summary"), "Report").expect("named table"),
            Some("Summary".to_owned())
        );
        assert!(select_table(&many, None, "Report").is_err());
        assert!(select_table(&one, Some("Missing"), "Report").is_err());
        assert_eq!(select_table(&[], None, "Report").expect("no table"), None);
    }
}
