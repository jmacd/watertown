// SPDX-License-Identifier: Apache-2.0

//! Opt-in physical encoding contract carried by logical Arrow field metadata.

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Schema};
use parquet::{
    basic::{Encoding, PageType},
    errors::{ParquetError, Result},
    file::{
        metadata::ParquetMetaData,
        properties::{WriterProperties, WriterPropertiesBuilder},
    },
    schema::types::ColumnPath,
};

pub const POLICY_KEY: &str = "watertown.parquet.encoding";
pub const DICTIONARY_V1: &str = "bounded-utf8-dictionary.1";
pub const MAX_VALUE_BYTES: usize = 1024;
pub const MAX_ROW_GROUP_ROWS: usize = 8192;
pub const DICTIONARY_BYTES: usize = 16 * 1024 * 1024;

fn columns(schema: &Schema) -> Result<Vec<usize>> {
    let mut selected = Vec::new();
    for (index, field) in schema.fields().iter().enumerate() {
        if let Some(policy) = field.metadata().get(POLICY_KEY) {
            if policy != DICTIONARY_V1 || field.data_type() != &DataType::Utf8 {
                return Err(ParquetError::General(format!(
                    "unsupported Parquet policy {policy:?} on field {:?} ({:?})",
                    field.name(),
                    field.data_type()
                )));
            }
            selected.push(index);
        }
    }
    Ok(selected)
}

/// Leave unmarked schemas unchanged; marked columns have a bounded dictionary.
pub fn writer_properties(
    schema: &Schema,
    builder: WriterPropertiesBuilder,
) -> Result<WriterProperties> {
    let selected = columns(schema)?;
    if selected.is_empty() {
        return Ok(builder.build());
    }
    let existing = builder.build();
    let rows = existing
        .max_row_group_row_count()
        .map_or(MAX_ROW_GROUP_ROWS, |rows| rows.min(MAX_ROW_GROUP_ROWS));
    let mut builder = existing
        .into_builder()
        .set_max_row_group_row_count(Some(rows))
        .set_dictionary_page_size_limit(DICTIONARY_BYTES);
    for index in selected {
        builder = builder.set_column_dictionary_enabled(
            ColumnPath::new(vec![schema.field(index).name().clone()]),
            true,
        );
    }
    Ok(builder.build())
}

/// The row-group cap times the maximum encoded value fits below the dictionary cap.
pub fn validate_batch(batch: &RecordBatch) -> Result<()> {
    for index in columns(batch.schema().as_ref())? {
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ParquetError::General("dictionary policy requires Utf8 values".into())
            })?;
        if values
            .iter()
            .flatten()
            .any(|value| value.len() > MAX_VALUE_BYTES)
        {
            return Err(ParquetError::General(format!(
                "dictionary policy field {:?} exceeds {MAX_VALUE_BYTES} UTF-8 bytes",
                batch.schema().field(index).name()
            )));
        }
    }
    Ok(())
}

/// Check data-page encodings, not PLAIN dictionary-page entries in the encoding list.
pub fn validate_metadata(schema: &Schema, metadata: &ParquetMetaData) -> Result<()> {
    for index in columns(schema)? {
        let name = schema.field(index).name();
        for group in metadata.row_groups() {
            let column = group
                .columns()
                .iter()
                .find(|column| column.column_path().parts() == [name.as_str()])
                .ok_or_else(|| ParquetError::General(format!("missing policy column {name:?}")))?;
            let dictionary_only = if let Some(mask) = column.page_encoding_stats_mask() {
                mask.is_only(Encoding::RLE_DICTIONARY) || mask.is_only(Encoding::PLAIN_DICTIONARY)
            } else if let Some(stats) = column.page_encoding_stats() {
                let mut data = stats
                    .iter()
                    .filter(|stat| {
                        matches!(stat.page_type, PageType::DATA_PAGE | PageType::DATA_PAGE_V2)
                            && stat.count > 0
                    })
                    .peekable();
                data.peek().is_some()
                    && data.all(|stat| {
                        matches!(
                            stat.encoding,
                            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY
                        )
                    })
            } else {
                false
            };
            if column.num_values() > 0
                && (column.dictionary_page_offset().is_none() || !dictionary_only)
            {
                return Err(ParquetError::General(format!(
                    "dictionary encoding requirement not met for {name:?}"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use parquet::arrow::ArrowWriter;
    use std::{collections::HashMap, sync::Arc};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("page_uri", DataType::Utf8, false)
                .with_metadata(HashMap::from([(POLICY_KEY.into(), DICTIONARY_V1.into())])),
        ]))
    }

    fn encode(batch: &RecordBatch, props: WriterProperties) -> (Vec<u8>, ParquetMetaData) {
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props)).expect("writer");
        writer.write(batch).expect("write");
        let metadata = writer.close().expect("close");
        (bytes, metadata)
    }

    #[test]
    fn bounded_dictionary_survives_row_groups_and_rejects_fallback() {
        let schema = schema();
        let values = (0..MAX_ROW_GROUP_ROWS + 1)
            .map(|i| format!("/{i:08}{}", "x".repeat(MAX_VALUE_BYTES - 9)))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))])
            .expect("batch");
        validate_batch(&batch).expect("maximum supported values");
        let props = writer_properties(
            &schema,
            WriterProperties::builder().set_dictionary_enabled(false),
        )
        .expect("policy");
        let (bytes, metadata) = encode(&batch, props.clone());
        assert_eq!(metadata.num_row_groups(), 2);
        validate_metadata(&schema, &metadata).expect("no dictionary fallback");
        assert_eq!(encode(&batch, props).0, bytes, "deterministic encoding");

        let (_, plain) = encode(
            &batch,
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .build(),
        );
        assert!(validate_metadata(&schema, &plain).is_err());
        let too_long = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                "x".repeat(MAX_VALUE_BYTES + 1),
            ]))],
        )
        .expect("batch");
        assert!(validate_batch(&too_long).is_err());
        let unknown = Schema::new(vec![
            Field::new("page_uri", DataType::Utf8, false)
                .with_metadata(HashMap::from([(POLICY_KEY.into(), "future-policy".into())])),
        ]);
        assert!(writer_properties(&unknown, WriterProperties::builder()).is_err());
        let plain_schema = Schema::new(vec![Field::new("page_uri", DataType::Utf8, false)]);
        let plain_props = writer_properties(
            &plain_schema,
            WriterProperties::builder().set_dictionary_enabled(false),
        )
        .expect("unmarked policy");
        assert_eq!(
            encode(&batch, plain_props)
                .1
                .row_group(0)
                .column(0)
                .encodings()
                .collect::<Vec<_>>(),
            plain.row_group(0).column(0).encodings().collect::<Vec<_>>()
        );
    }

    #[test]
    #[allow(clippy::print_stderr)]
    fn representative_uri_column_savings() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            schema().field(0).clone(),
            Field::new("page_views", DataType::UInt64, false),
        ]));
        for pages in [10, 100, 1000] {
            let values = (0..24)
                .flat_map(|_| {
                    (0..pages).map(|page| {
                        format!("/noyo-harbor/observations/{page:04}/daily-water-quality")
                    })
                })
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(
                        arrow_array::TimestampMicrosecondArray::from(
                            (0..24)
                                .flat_map(|hour| vec![hour * 3_600_000_000; pages])
                                .collect::<Vec<_>>(),
                        )
                        .with_timezone("UTC"),
                    ),
                    Arc::new(StringArray::from(values)),
                    Arc::new(arrow_array::UInt64Array::from(vec![7; 24 * pages])),
                ],
            )
            .expect("batch");
            let (plain, plain_meta) = encode(
                &batch,
                WriterProperties::builder()
                    .set_dictionary_enabled(false)
                    .build(),
            );
            let (encoded, encoded_meta) = encode(
                &batch,
                writer_properties(
                    &schema,
                    WriterProperties::builder().set_dictionary_enabled(false),
                )
                .expect("policy"),
            );
            validate_metadata(&schema, &encoded_meta).expect("dictionary");
            let size = |meta: &ParquetMetaData| {
                meta.row_groups()
                    .iter()
                    .map(|group| group.column(1).compressed_size())
                    .sum::<i64>()
            };
            eprintln!(
                "pages={pages}: URI bytes {} -> {}, file bytes {} -> {}",
                size(&plain_meta),
                size(&encoded_meta),
                plain.len(),
                encoded.len()
            );
            assert!(size(&encoded_meta) < size(&plain_meta));
            assert!(encoded.len() < plain.len());
        }
    }
}
