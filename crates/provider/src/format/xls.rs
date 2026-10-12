// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Binary Excel (.xls) tables, buffered for Calamine's seekable reader.

use super::batch::{BatchStream, finish_batch};
use crate::{Error, FormatProvider, Result, Url};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use calamine::{Data, Range, Reader, Xls};
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
    header: bool,
    batch_size: usize,
}

impl Default for XlsOptions {
    fn default() -> Self {
        Self {
            sheet: None,
            header: true,
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
        let mut bytes = Vec::new();
        _ = reader.read_to_end(&mut bytes).await?;
        let header = options.header;
        let batch_size = options.batch_size;
        let (range, schema) = tokio::task::spawn_blocking(move || read_sheet(bytes, &options))
            .await
            .map_err(|e| Error::Xls(format!("workbook parsing task failed: {e}")))??;

        // Parsing and inference run off the async executor. Arrow conversion is
        // bounded by batch_size; the retained worksheet supplies rows lazily.
        let stream_schema = schema.clone();
        let stream = async_stream::try_stream! {
            let mut rows = range.rows().skip(usize::from(header));
            loop {
                let chunk: Vec<_> = rows.by_ref().take(batch_size).collect();
                if chunk.is_empty() {
                    break;
                }
                yield rows_to_batch(stream_schema.clone(), &chunk)?;
            }
        };
        Ok((schema, Box::pin(stream)))
    }
}

fn read_sheet(bytes: Vec<u8>, options: &XlsOptions) -> Result<(Range<Data>, SchemaRef)> {
    let mut workbook = Xls::new(Cursor::new(bytes))
        .map_err(|e| Error::Xls(format!("cannot open binary Excel workbook: {e}")))?;
    let names = workbook.sheet_names();
    let sheet = match options.sheet.as_deref() {
        Some(name) if names.iter().any(|n| n == name) => name,
        Some(name) => {
            return Err(Error::Xls(format!(
                "worksheet '{name}' not found; available worksheets: {names:?}"
            )));
        }
        None if names.len() == 1 => &names[0],
        None => {
            return Err(Error::Xls(format!(
                "specify ?sheet=Name when the workbook does not have exactly one worksheet; available worksheets: {names:?}"
            )));
        }
    };
    let range = workbook
        .worksheet_range(sheet)
        .map_err(|e| Error::Xls(format!("cannot read worksheet '{sheet}': {e}")))?;
    if range.is_empty() {
        return Err(Error::Xls(format!("worksheet '{sheet}' is empty")));
    }
    let schema = sheet_schema(&range, options.header, sheet)?;
    Ok((range, schema))
}

fn sheet_schema(range: &Range<Data>, header: bool, sheet: &str) -> Result<SchemaRef> {
    let (start_row, start_col) = range
        .start()
        .ok_or_else(|| Error::Xls(format!("worksheet '{sheet}' is empty")))?;
    let mut types = vec![DataType::Null; range.width()];
    for (row_index, row) in range.rows().enumerate() {
        for (col_index, cell) in row.iter().enumerate() {
            let kind = cell_type(cell).map_err(|e| {
                Error::Xls(format!(
                    "worksheet '{sheet}', row {}, column {}: {e}",
                    u64::from(start_row) + row_index as u64 + 1,
                    u64::from(start_col) + col_index as u64 + 1
                ))
            })?;
            if header && row_index == 0 {
                continue;
            }
            types[col_index] = merge_types(&types[col_index], kind);
        }
    }

    let first_row = range
        .rows()
        .next()
        .ok_or_else(|| Error::Xls(format!("worksheet '{sheet}' is empty")))?;
    let mut names = HashSet::new();
    let mut fields = Vec::with_capacity(types.len());
    for (index, kind) in types.into_iter().enumerate() {
        let name = if header && !matches!(first_row[index], Data::Empty) {
            cell_text(&first_row[index])?
        } else {
            String::new()
        };
        let name = if name.trim().is_empty() {
            format!("column_{}", index + 1)
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

fn rows_to_batch(schema: SchemaRef, rows: &[&[Data]]) -> Result<arrow::record_batch::RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (index, field) in schema.fields().iter().enumerate() {
        let cells = || rows.iter().map(|row| &row[index]);
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
