// SPDX-License-Identifier: Apache-2.0

//! Canonical logical-series encoding: `docs/logical-series-identity-design.md`
//! delivery gate 1 (schema encoding freeze).
//!
//! This module is pure content hashing, exactly like its siblings [`super::tree`]
//! and [`super::manifest`]: it turns project-owned logical content -- a
//! schema, an ordered run of [`RecordBatch`] rows, or an appended file byte
//! range -- into stable bytes and a `blake3` leaf hash. It knows nothing about
//! Delta Lake, Parquet, Bao outboards, packs, or the `watertown.series.v1` root object
//! that will later chain these leaves together; those are later delivery
//! gates. Physical encoding never contributes: chunking a `RecordBatch`
//! differently, or storing a string column dictionary-encoded instead of
//! plain, must not change the hash produced here.
//!
//! # Accepted logical types
//!
//! [`canonical_data_type`] is the single source of truth for which Arrow
//! logical types this module accepts, shared by schema encoding
//! ([`encode_canonical_schema`]) and row encoding ([`encode_canonical_rows`])
//! so the two can never silently drift apart. Accepted today: `Boolean`,
//! signed and unsigned integers of every width, `Float32`/`Float64`,
//! `Utf8`/`LargeUtf8`, `Binary`/`LargeBinary`, `Date32`, `Timestamp` (any unit
//! and timezone), and `Decimal128`. A `Dictionary` column is normalized to its
//! value type -- dictionary encoding is a physical detail, not logical
//! content. Every other Arrow type (nested types such as `List`/`Struct`/
//! `Map`/`Union`, `Decimal256`, `FixedSizeBinary`, `Time32`/`Time64`,
//! `Duration`, `Interval`, view types, run-end encoding, `Null`, `Float16`,
//! ...) is rejected with a precise error rather than stringified or silently
//! dropped. Enabling v2 writing for a series that needs one of these types
//! requires extending this module (and its golden vectors) first.
//!
//! # Wire framing
//!
//! Three independently-tagged encodings compose:
//!
//! - [`encode_canonical_schema`] -- `SCHEMA_MAGIC`, field count, then each
//!   field's name, nullability, logical type tag, and sorted metadata; then
//!   the schema's own sorted metadata. [`schema_fingerprint`] is `blake3` of
//!   these bytes -- a fixed-size stand-in for the schema so the leaf preimage
//!   below does not need to repeat it.
//! - [`encode_canonical_rows`] -- `ROWS_MAGIC`, a `u64` row count, then every
//!   row (batch order, then original row order within a batch) with one
//!   scalar per schema column. Each scalar starts with a present/absent
//!   marker byte; present fixed-width scalars are little-endian in their
//!   declared width, and present variable-width scalars are a `u64` byte
//!   length followed by bytes, per the RFC's canonical-scalar rules.
//! - The leaf preimage itself (built by [`table_leaf_hash`] and
//!   [`file_leaf_hash`]) follows `docs/logical-series-identity-design.md`'s
//!   `logical_leaf` framing exactly: `LEAF_MAGIC`, payload kind, the
//!   length-prefixed schema fingerprint (empty for a file leaf), the `u64`
//!   logical count, the length-prefixed canonical payload, explicit
//!   min/max-event-time presence flags, and length-prefixed canonical logical
//!   attributes.
//!
//! Decoding (and therefore the `watertown.series.v1` root object, packs, readers, and
//! migration) is out of scope for this gate; only the pure, one-directional
//! encode-and-hash path is implemented and tested here.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use super::{ObjectHash, push_len_prefixed, push_len_prefixed_u64};

/// Magic header for canonical schema bytes (the input to [`schema_fingerprint`]).
const SCHEMA_MAGIC: &[u8] = b"watertown.series-schema.v1\n";
const LEGACY_SCHEMA_MAGIC: &[u8] = b"dp.series-schema.1\n";

/// Magic header for the canonical table-row payload.
const ROWS_MAGIC: &[u8] = b"watertown.series-rows.v1\n";
const LEGACY_ROWS_MAGIC: &[u8] = b"dp.series-rows.1\n";

/// Domain-separation tag for a leaf preimage, exactly as specified in
/// `docs/logical-series-identity-design.md`.
const LEAF_MAGIC: &[u8] = b"watertown.series-leaf.v1\n";
const LEGACY_LEAF_MAGIC: &[u8] = b"dp.series-leaf.1\n";

pub(crate) const fn legacy_leaf_magic() -> &'static [u8] {
    LEGACY_LEAF_MAGIC
}

pub(crate) const fn legacy_schema_magic() -> &'static [u8] {
    LEGACY_SCHEMA_MAGIC
}

pub(crate) const fn legacy_rows_magic() -> &'static [u8] {
    LEGACY_ROWS_MAGIC
}

/// `payload_kind` byte for a table leaf.
///
/// Deliberately *not* `tinyfs::EntryType::TablePhysicalSeries as u8`: this
/// value is part of a hashed wire format and must never move just because an
/// unrelated enum gains or reorders variants, so it is minted fresh here.
///
/// `pub(crate)`: [`super::series_manifest`]'s `watertown.series.v1` root object
/// records the same payload kind at the series level, and must use this exact
/// wire value rather than mint a second, potentially-divergent encoding of
/// "table or file".
pub(crate) const LEAF_KIND_TABLE: u8 = 0;
/// `payload_kind` byte for a file leaf. See [`LEAF_KIND_TABLE`].
pub(crate) const LEAF_KIND_FILE: u8 = 1;

/// `bounds_flags` bit: `min_event_time` is present.
///
/// `pub(crate)`: [`super::series_manifest`]'s aggregate event-time bounds use
/// the identical flag bits, so a bit's meaning can never drift between the
/// per-leaf and per-series encodings.
pub(crate) const LEAF_HAS_MIN: u8 = 0b0000_0001;
/// `bounds_flags` bit: `max_event_time` is present. See [`LEAF_HAS_MIN`].
pub(crate) const LEAF_HAS_MAX: u8 = 0b0000_0010;

/// Marker byte for an absent scalar.
const SCALAR_ABSENT: u8 = 0;
/// Marker byte for a present scalar.
const SCALAR_PRESENT: u8 = 1;

/// Canonical quiet-NaN bits. Rust's `f32::NAN` and `f64::NAN` bit patterns are
/// explicitly not stable across compiler versions or target platforms.
const CANONICAL_F32_NAN: u32 = 0x7fc0_0000;
const CANONICAL_F64_NAN: u64 = 0x7ff8_0000_0000_0000;

/// Wire tags for each accepted canonical logical type. Stable and part of the
/// schema fingerprint: a value's meaning must never change or be reused.
const TAG_BOOLEAN: u8 = 0;
const TAG_INT8: u8 = 1;
const TAG_INT16: u8 = 2;
const TAG_INT32: u8 = 3;
const TAG_INT64: u8 = 4;
const TAG_UINT8: u8 = 5;
const TAG_UINT16: u8 = 6;
const TAG_UINT32: u8 = 7;
const TAG_UINT64: u8 = 8;
const TAG_FLOAT32: u8 = 9;
const TAG_FLOAT64: u8 = 10;
const TAG_UTF8: u8 = 11;
const TAG_LARGE_UTF8: u8 = 12;
const TAG_BINARY: u8 = 13;
const TAG_LARGE_BINARY: u8 = 14;
const TAG_DATE32: u8 = 15;
const TAG_TIMESTAMP: u8 = 16;
const TAG_DECIMAL128: u8 = 17;

/// Resolve `dt` to the canonical logical type it must encode as.
///
/// A `Dictionary` is unwrapped to its value type -- the key width and
/// dictionary-ness are a physical encoding choice, not logical content, so a
/// plain `Utf8` column and a `Dictionary<UInt16, Utf8>` column holding the
/// same logical values must resolve identically here. Every other type is
/// returned unchanged if accepted, or rejected with a precise error.
///
/// This is the single source of truth for accepted types: both
/// [`encode_canonical_schema`] and [`encode_canonical_rows`] call it, so they
/// can never silently accept different type sets.
///
/// # Errors
///
/// Returns an error naming the exact unsupported Arrow `DataType` for any
/// nested type (`List`, `LargeList`, `FixedSizeList`, `Struct`, `Map`,
/// `Union`, `RunEndEncoded`, ...) or any type Watertown does not yet produce
/// (`Decimal256`, `FixedSizeBinary`, `Time32`, `Time64`, `Duration`,
/// `Interval`, view types, `Null`, `Float16`, ...).
fn canonical_data_type(dt: &DataType) -> Result<DataType, String> {
    match dt {
        DataType::Dictionary(_, value) => canonical_data_type(value),
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Date32
        | DataType::Timestamp(_, _)
        | DataType::Decimal128(_, _) => Ok(dt.clone()),
        other => Err(format!(
            "unsupported canonical series type {other:?}: only boolean, \
             signed/unsigned integers, float32/float64, utf8/large_utf8, \
             binary/large_binary, date32, timestamp, and decimal128 are \
             accepted (a dictionary column is normalized to its value type)"
        )),
    }
}

/// Encode an already-[`canonical_data_type`]-resolved type's wire tag.
///
/// # Panics
///
/// Panics if `dt` is not a value [`canonical_data_type`] can return (in
/// particular, never `Dictionary`); every caller in this module upholds that
/// precondition.
fn encode_resolved_data_type(dt: &DataType, buf: &mut Vec<u8>) {
    match dt {
        DataType::Boolean => buf.push(TAG_BOOLEAN),
        DataType::Int8 => buf.push(TAG_INT8),
        DataType::Int16 => buf.push(TAG_INT16),
        DataType::Int32 => buf.push(TAG_INT32),
        DataType::Int64 => buf.push(TAG_INT64),
        DataType::UInt8 => buf.push(TAG_UINT8),
        DataType::UInt16 => buf.push(TAG_UINT16),
        DataType::UInt32 => buf.push(TAG_UINT32),
        DataType::UInt64 => buf.push(TAG_UINT64),
        DataType::Float32 => buf.push(TAG_FLOAT32),
        DataType::Float64 => buf.push(TAG_FLOAT64),
        DataType::Utf8 => buf.push(TAG_UTF8),
        DataType::LargeUtf8 => buf.push(TAG_LARGE_UTF8),
        DataType::Binary => buf.push(TAG_BINARY),
        DataType::LargeBinary => buf.push(TAG_LARGE_BINARY),
        DataType::Date32 => buf.push(TAG_DATE32),
        DataType::Timestamp(unit, tz) => {
            buf.push(TAG_TIMESTAMP);
            buf.push(match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 1,
                TimeUnit::Microsecond => 2,
                TimeUnit::Nanosecond => 3,
            });
            match tz {
                Some(tz) => {
                    buf.push(1);
                    push_len_prefixed(buf, tz.as_bytes());
                }
                None => buf.push(0),
            }
        }
        DataType::Decimal128(precision, scale) => {
            buf.push(TAG_DECIMAL128);
            buf.push(*precision);
            buf.push(scale.to_le_bytes()[0]);
        }
        other => unreachable!("canonical_data_type never returns {other:?}"),
    }
}

/// Append a field or schema metadata map, sorted by UTF-8 key bytes so
/// producer-side `HashMap` iteration order never affects the hash.
fn encode_metadata(metadata: &std::collections::HashMap<String, String>, buf: &mut Vec<u8>) {
    let mut pairs: Vec<(&String, &String)> = metadata.iter().collect();
    pairs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let count = u32::try_from(pairs.len()).expect("metadata entry count exceeds u32::MAX");
    buf.extend_from_slice(&count.to_le_bytes());
    for (k, v) in pairs {
        push_len_prefixed(buf, k.as_bytes());
        push_len_prefixed(buf, v.as_bytes());
    }
}

/// Encode a schema into its canonical wire bytes.
///
/// Column order, field names, nullability, and logical types are part of the
/// result; physical layout (buffer alignment, dictionary key width, chunking)
/// is not, because [`canonical_data_type`] resolves each field's `DataType`
/// first. Field and schema metadata maps are sorted by key so their producer
/// iteration order does not matter.
///
/// # Errors
///
/// Returns an error if any field's type is not accepted by
/// [`canonical_data_type`].
pub fn encode_canonical_schema(schema: &Schema) -> Result<Vec<u8>, String> {
    encode_canonical_schema_with_magic(schema, SCHEMA_MAGIC)
}

pub(crate) fn encode_canonical_schema_with_magic(
    schema: &Schema,
    magic: &[u8],
) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    buf.extend_from_slice(magic);
    let field_count =
        u32::try_from(schema.fields().len()).expect("schema field count exceeds u32::MAX");
    buf.extend_from_slice(&field_count.to_le_bytes());
    for field in schema.fields() {
        push_len_prefixed(&mut buf, field.name().as_bytes());
        buf.push(u8::from(field.is_nullable()));
        let resolved = canonical_data_type(field.data_type())
            .map_err(|e| format!("field {:?}: {e}", field.name()))?;
        encode_resolved_data_type(&resolved, &mut buf);
        encode_metadata(field.metadata(), &mut buf);
    }
    encode_metadata(schema.metadata(), &mut buf);
    Ok(buf)
}

/// Compute a schema's fingerprint: `blake3` of [`encode_canonical_schema`].
///
/// This is the fixed-size value the `logical_leaf` preimage in
/// `docs/logical-series-identity-design.md` calls `schema_fingerprint` for a
/// table leaf.
///
/// # Errors
///
/// Propagates [`encode_canonical_schema`]'s error for an unsupported field
/// type.
pub fn schema_fingerprint(schema: &Schema) -> Result<ObjectHash, String> {
    schema_fingerprint_with_magic(schema, SCHEMA_MAGIC)
}

pub(crate) fn schema_fingerprint_with_magic(
    schema: &Schema,
    magic: &[u8],
) -> Result<ObjectHash, String> {
    Ok(ObjectHash::of_bytes(&encode_canonical_schema_with_magic(
        schema, magic,
    )?))
}

/// Resolve `schema` to its canonical logical schema: every field's
/// `DataType` replaced by [`canonical_data_type`] (a `Dictionary<K, V>`
/// column becomes plain `V`; every other accepted type is unchanged), with
/// each field's name, nullability, and metadata preserved exactly, and the
/// schema's own metadata preserved exactly.
///
/// This is the schema every leaf's decoded batches must actually be cast
/// to (with `arrow-cast`, not merely reinterpreted) before they can be
/// safely concatenated or accumulated together: two leaves whose Parquet
/// happened to physically encode the same logical column differently (one
/// dictionary-encoded, one plain) share this one [`schema_fingerprint`]
/// but are not the same physical Arrow `DataType`, so naively rewrapping
/// one leaf's arrays under another leaf's raw (possibly
/// differently-encoded) schema can fail outright or -- worse -- silently
/// misinterpret bytes. Casting every leaf to this single canonical schema
/// makes every accumulated batch's physical type agree by construction.
///
/// # Errors
///
/// Returns an error if any field's type is not accepted by
/// [`canonical_data_type`] (the exact same rejection [`schema_fingerprint`]
/// itself would give for that field).
pub fn canonicalize_schema(schema: &Schema) -> Result<Arc<Schema>, String> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let resolved = canonical_data_type(field.data_type())
            .map_err(|e| format!("field {:?}: {e}", field.name()))?;
        fields.push(Arc::new(
            Field::new(field.name(), resolved, field.is_nullable())
                .with_metadata(field.metadata().clone()),
        ));
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// Encode one scalar (a single column's value in a single row) into `buf`.
///
/// Dictionary arrays are materialized once per column before this function is
/// called, so the logical value -- not its physical key -- is what gets
/// encoded. Every accepted type writes a little-endian fixed-width
/// representation (with floating point NaNs normalized to one quiet-NaN bit
/// pattern per width, and signed zero preserved) or, for variable-width
/// values, a `u64` byte length followed by bytes.
///
/// # Errors
///
/// Returns an error if `array`'s data type is not accepted by
/// [`canonical_data_type`]. Callers in this module validate column types
/// against the schema before calling this, so it fires only as a defense in
/// depth.
fn encode_scalar(array: &dyn Array, row: usize, buf: &mut Vec<u8>) -> Result<(), String> {
    if array.is_null(row) {
        buf.push(SCALAR_ABSENT);
        return Ok(());
    }
    buf.push(SCALAR_PRESENT);
    match array.data_type() {
        DataType::Boolean => buf.push(u8::from(array.as_boolean().value(row))),
        DataType::Int8 => buf.push(array.as_primitive::<Int8Type>().value(row).to_le_bytes()[0]),
        DataType::Int16 => {
            buf.extend_from_slice(&array.as_primitive::<Int16Type>().value(row).to_le_bytes());
        }
        DataType::Int32 => {
            buf.extend_from_slice(&array.as_primitive::<Int32Type>().value(row).to_le_bytes());
        }
        DataType::Int64 => {
            buf.extend_from_slice(&array.as_primitive::<Int64Type>().value(row).to_le_bytes());
        }
        DataType::UInt8 => buf.push(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => {
            buf.extend_from_slice(&array.as_primitive::<UInt16Type>().value(row).to_le_bytes());
        }
        DataType::UInt32 => {
            buf.extend_from_slice(&array.as_primitive::<UInt32Type>().value(row).to_le_bytes());
        }
        DataType::UInt64 => {
            buf.extend_from_slice(&array.as_primitive::<UInt64Type>().value(row).to_le_bytes());
        }
        DataType::Float32 => {
            let v = array.as_primitive::<Float32Type>().value(row);
            let bits = if v.is_nan() {
                CANONICAL_F32_NAN
            } else {
                v.to_bits()
            };
            buf.extend_from_slice(&bits.to_le_bytes());
        }
        DataType::Float64 => {
            let v = array.as_primitive::<Float64Type>().value(row);
            let bits = if v.is_nan() {
                CANONICAL_F64_NAN
            } else {
                v.to_bits()
            };
            buf.extend_from_slice(&bits.to_le_bytes());
        }
        DataType::Utf8 => {
            push_len_prefixed_u64(buf, array.as_string::<i32>().value(row).as_bytes());
        }
        DataType::LargeUtf8 => {
            push_len_prefixed_u64(buf, array.as_string::<i64>().value(row).as_bytes());
        }
        DataType::Binary => push_len_prefixed_u64(buf, array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => push_len_prefixed_u64(buf, array.as_binary::<i64>().value(row)),
        DataType::Date32 => {
            buf.extend_from_slice(&array.as_primitive::<Date32Type>().value(row).to_le_bytes());
        }
        DataType::Timestamp(unit, _) => {
            let v = match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row),
                TimeUnit::Millisecond => {
                    array.as_primitive::<TimestampMillisecondType>().value(row)
                }
                TimeUnit::Microsecond => {
                    array.as_primitive::<TimestampMicrosecondType>().value(row)
                }
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
            };
            buf.extend_from_slice(&v.to_le_bytes());
        }
        DataType::Decimal128(_, _) => {
            buf.extend_from_slice(
                &array
                    .as_primitive::<Decimal128Type>()
                    .value(row)
                    .to_le_bytes(),
            );
        }
        other => {
            return Err(format!(
                "unsupported canonical series type at encode time: {other:?}"
            ));
        }
    }
    Ok(())
}

/// Encode an ordered run of `RecordBatch`es into the canonical table-row
/// payload: `ROWS_MAGIC`, a `u64` row count, then every row in batch order
/// (then original row order within a batch), one scalar per schema column.
///
/// Row order is the caller's append order; this function neither sorts nor
/// deduplicates. How the rows are split across `batches` -- one giant batch
/// or many small ones -- does not affect the result, since only logical
/// values are read from each array via its public accessors.
///
/// # Errors
///
/// Returns an error if `schema` has a field type [`canonical_data_type`]
/// rejects, if a batch's column count does not match `schema`'s field count,
/// or if a batch column's resolved type does not match its schema field's
/// resolved type (a dictionary column is compatible with its plain value
/// type; anything else is a caller bug this must not hash over silently).
#[allow(dead_code)] // exercised by this module's canonical-row-encoding unit tests
pub(crate) fn encode_canonical_rows(
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, String> {
    encode_canonical_rows_with_magic(schema, batches, ROWS_MAGIC)
}

pub(crate) fn encode_canonical_rows_with_magic(
    schema: &Schema,
    batches: &[RecordBatch],
    magic: &[u8],
) -> Result<Vec<u8>, String> {
    let row_count = batches.iter().try_fold(0u64, |total, batch| {
        total
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| "table row count exceeds u64::MAX".to_string())
    })?;

    let mut buf = Vec::new();
    buf.extend_from_slice(magic);
    buf.extend_from_slice(&row_count.to_le_bytes());

    for (batch_idx, batch) in batches.iter().enumerate() {
        buf.extend_from_slice(&encode_canonical_batch_rows_at(schema, batch, batch_idx)?);
    }
    Ok(buf)
}

/// Encode one record batch's canonical scalar rows without the table payload
/// magic or total-row-count prefix.
///
/// Concatenating this output for batches in order after
/// `watertown.series-rows.v1\n` and the total `u64` row count produces exactly the
/// payload encoded by the logical table identity protocol.
///
/// # Errors
///
/// Returns an error for unsupported or schema-incompatible arrays.
pub fn encode_canonical_batch_rows(
    schema: &Schema,
    batch: &RecordBatch,
) -> Result<Vec<u8>, String> {
    encode_canonical_batch_rows_at(schema, batch, 0)
}

fn encode_canonical_batch_rows_at(
    schema: &Schema,
    batch: &RecordBatch,
    batch_idx: usize,
) -> Result<Vec<u8>, String> {
    let resolved_fields: Vec<DataType> = schema
        .fields()
        .iter()
        .map(|field| {
            canonical_data_type(field.data_type())
                .map_err(|error| format!("field {:?}: {error}", field.name()))
        })
        .collect::<Result<_, _>>()?;
    if batch.num_columns() != resolved_fields.len() {
        return Err(format!(
            "batch {batch_idx}: {} column(s) does not match schema's {} field(s)",
            batch.num_columns(),
            resolved_fields.len()
        ));
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (col_idx, resolved) in resolved_fields.iter().enumerate() {
        let array = batch.column(col_idx).as_ref();
        let actual = canonical_data_type(array.data_type()).map_err(|error| {
            format!(
                "batch {batch_idx} column {col_idx} ({:?}): {error}",
                schema.field(col_idx).name()
            )
        })?;
        if &actual != resolved {
            return Err(format!(
                "batch {batch_idx} column {col_idx} ({:?}): array type {:?} does not match \
                 schema field type {:?}",
                schema.field(col_idx).name(),
                array.data_type(),
                schema.field(col_idx).data_type(),
            ));
        }
        let canonical = if matches!(array.data_type(), DataType::Dictionary(_, _)) {
            arrow::compute::cast(array, resolved).map_err(|error| {
                format!(
                    "batch {batch_idx} column {col_idx} ({:?}): normalize dictionary: {error}",
                    schema.field(col_idx).name()
                )
            })?
        } else {
            batch.column(col_idx).clone()
        };
        columns.push(canonical);
    }
    let mut rows = Vec::new();
    for row in 0..batch.num_rows() {
        for array in &columns {
            encode_scalar(array.as_ref(), row, &mut rows)?;
        }
    }
    Ok(rows)
}

/// Emit one JSON string with a project-owned, stable escape policy.
fn encode_json_string(value: &str, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{09}' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{0c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            '\u{00}'..='\u{1f}' => {
                let byte = ch as u8;
                out.push_str("\\u00");
                out.push(HEX[usize::from(byte >> 4)] as char);
                out.push(HEX[usize::from(byte & 0x0f)] as char);
            }
            _ => out.push(ch),
        }
    }
    out.push('"');
}

/// Canonicalize a JSON value: recursively sort object keys by UTF-8 bytes and
/// emit no insignificant whitespace.
fn canonicalize_json(value: &serde_json::Value, out: &mut String) -> Result<(), String> {
    match value {
        serde_json::Value::Null => out.push_str("null"),
        serde_json::Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        serde_json::Value::Number(n) => {
            if let Some(value) = n.as_i64() {
                out.push_str(&value.to_string());
            } else if let Some(value) = n.as_u64() {
                out.push_str(&value.to_string());
            } else {
                return Err(format!(
                    "logical attribute number {n} is not an i64 or u64 integer"
                ));
            }
        }
        serde_json::Value::String(s) => encode_json_string(s, out),
        serde_json::Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonicalize_json(item, out)?;
            }
            out.push(']');
        }
        serde_json::Value::Object(map) => {
            out.push('{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                encode_json_string(key, out);
                out.push(':');
                canonicalize_json(&map[*key], out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

/// Encode logical attributes as canonical JSON bytes: recursively sorted
/// object keys, no insignificant whitespace, project-owned string escaping,
/// and numbers restricted to signed or unsigned 64-bit integers.
///
/// # Errors
///
/// Returns an error if `json` does not parse, or if its top-level value is
/// not a JSON object (an array, string, number, boolean, or `null` at the top
/// level is rejected, matching the tree-entry `extended_attributes`
/// convention of always being an object).
pub fn encode_canonical_attributes(json: &str) -> Result<Vec<u8>, String> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("invalid logical attributes JSON: {e}"))?;
    if !value.is_object() {
        return Err(format!(
            "logical attributes must be a JSON object, got {value}"
        ));
    }
    let mut out = String::new();
    canonicalize_json(&value, &mut out)?;
    Ok(out.into_bytes())
}

/// Verify that `bytes` are already exactly what [`encode_canonical_attributes`]
/// would produce for some logical-attributes JSON object: valid UTF-8, a
/// parseable JSON object, with no insignificant whitespace and object keys
/// already sorted by UTF-8 bytes.
///
/// Used by [`super::series_manifest`]'s decoder, which -- unlike an encoder
/// starting from caller-provided JSON -- receives already-serialized bytes
/// off the wire and must reject any that are merely valid JSON without being
/// *canonical* JSON (for example, insignificant whitespace or out-of-order
/// keys), since two byte strings that mean the same JSON but hash differently
/// would silently fracture series identity.
///
/// # Errors
///
/// Returns an error if `bytes` is not valid UTF-8, does not parse as JSON, is
/// not a JSON object, or does not re-canonicalize to the exact same bytes.
pub(crate) fn validate_canonical_attributes(bytes: &[u8]) -> Result<(), String> {
    let text =
        std::str::from_utf8(bytes).map_err(|e| format!("logical attributes not UTF-8: {e}"))?;
    let canonical = encode_canonical_attributes(text)?;
    if canonical != bytes {
        return Err("logical attributes bytes are not canonical JSON".to_string());
    }
    Ok(())
}

/// Build the `logical_leaf` preimage exactly as specified in
/// `docs/logical-series-identity-design.md`, and return its `blake3` hash.
#[allow(clippy::too_many_arguments)]
fn leaf_hash_with_magic(
    magic: &[u8],
    payload_kind: u8,
    schema_fingerprint: &[u8],
    logical_count: u64,
    canonical_payload: &[u8],
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    canonical_attributes: &[u8],
) -> ObjectHash {
    let mut buf = Vec::new();
    buf.extend_from_slice(magic);
    buf.push(payload_kind);
    push_len_prefixed(&mut buf, schema_fingerprint);
    buf.extend_from_slice(&logical_count.to_le_bytes());
    push_len_prefixed_u64(&mut buf, canonical_payload);
    let mut flags = 0u8;
    if min_event_time.is_some() {
        flags |= LEAF_HAS_MIN;
    }
    if max_event_time.is_some() {
        flags |= LEAF_HAS_MAX;
    }
    buf.push(flags);
    if let Some(v) = min_event_time {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    if let Some(v) = max_event_time {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    push_len_prefixed(&mut buf, canonical_attributes);
    ObjectHash::of_bytes(&buf)
}

/// Compute a `TablePhysicalSeries` leaf's identity hash from its schema,
/// ordered `RecordBatch` rows, aggregate event-time bounds, and logical
/// attributes.
///
/// `min_event_time` and `max_event_time` are independently optional so an
/// absent bound is distinguishable from every other value, including zero.
/// `logical_attributes`, if given, must be a JSON object; pass `None` -- not
/// `Some("{}")`  -- when a leaf has no logical attributes at all, since an
/// absent value and an empty object hash differently.
///
/// # Errors
///
/// Propagates [`encode_canonical_schema`]'s, [`encode_canonical_rows`]'s, and
/// [`encode_canonical_attributes`]'s errors.
pub fn table_leaf_hash(
    schema: &Schema,
    batches: &[RecordBatch],
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    logical_attributes: Option<&str>,
) -> Result<ObjectHash, String> {
    let attrs = match logical_attributes {
        Some(json) => Some(encode_canonical_attributes(json)?),
        None => None,
    };
    table_leaf_hash_canonical(
        schema,
        batches,
        min_event_time,
        max_event_time,
        attrs.as_deref(),
    )
}

/// Compute a `TablePhysicalSeries` leaf's identity hash exactly as
/// [`table_leaf_hash`] does, except `canonical_logical_attributes` is already
/// canonical logical-attribute bytes (as produced by
/// [`encode_canonical_attributes`], or as carried verbatim by a
/// [`super::series_pack::PackLeafDescriptor`]) rather than raw JSON text.
///
/// This is the entry point a pack-verifying reader uses (delivery gate 4):
/// a descriptor's attributes are stored pre-canonicalized (they were already
/// validated once when the descriptor was constructed), so re-parsing them
/// as JSON here would be redundant work and a second, potentially divergent,
/// canonicalization path. [`table_leaf_hash`] is defined in terms of this
/// function so the two can never drift apart.
///
/// # Errors
///
/// Propagates [`encode_canonical_schema`]'s and [`encode_canonical_rows`]'s
/// errors, or an error if a logical table leaf would be empty (zero rows).
pub fn table_leaf_hash_canonical(
    schema: &Schema,
    batches: &[RecordBatch],
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    canonical_logical_attributes: Option<&[u8]>,
) -> Result<ObjectHash, String> {
    table_leaf_hash_canonical_with_magic(
        schema,
        batches,
        min_event_time,
        max_event_time,
        canonical_logical_attributes,
        LEAF_MAGIC,
        SCHEMA_MAGIC,
        ROWS_MAGIC,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn table_leaf_hash_canonical_with_magic(
    schema: &Schema,
    batches: &[RecordBatch],
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    canonical_logical_attributes: Option<&[u8]>,
    leaf_magic: &[u8],
    schema_magic: &[u8],
    rows_magic: &[u8],
) -> Result<ObjectHash, String> {
    let fingerprint = schema_fingerprint_with_magic(schema, schema_magic)?;
    let payload = encode_canonical_rows_with_magic(schema, batches, rows_magic)?;
    let logical_count = batches.iter().try_fold(0u64, |total, batch| {
        total
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| "table leaf row count exceeds u64::MAX".to_string())
    })?;
    if logical_count == 0 {
        return Err("a logical table leaf must contain at least one row".to_string());
    }
    Ok(leaf_hash_with_magic(
        leaf_magic,
        LEAF_KIND_TABLE,
        fingerprint.as_bytes(),
        logical_count,
        &payload,
        min_event_time,
        max_event_time,
        canonical_logical_attributes.unwrap_or(&[]),
    ))
}

/// Compute a `FilePhysicalSeries` leaf's identity hash from its exact
/// appended byte range, aggregate event-time bounds, and logical attributes.
///
/// The schema fingerprint is empty and `logical_count` is `bytes.len()`, per
/// `docs/logical-series-identity-design.md`. See [`table_leaf_hash`] for the
/// bounds/attributes absent-vs-empty distinction.
///
/// # Errors
///
/// Propagates [`encode_canonical_attributes`]'s error.
pub fn file_leaf_hash(
    bytes: &[u8],
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    logical_attributes: Option<&str>,
) -> Result<ObjectHash, String> {
    let attrs = match logical_attributes {
        Some(json) => Some(encode_canonical_attributes(json)?),
        None => None,
    };
    file_leaf_hash_canonical(bytes, min_event_time, max_event_time, attrs.as_deref())
}

/// Compute a `FilePhysicalSeries` leaf's identity hash exactly as
/// [`file_leaf_hash`] does, except `canonical_logical_attributes` is already
/// canonical logical-attribute bytes rather than raw JSON text. See
/// [`table_leaf_hash_canonical`]'s docs for why a pack-verifying reader wants
/// this entry point specifically; [`IncrementalFileLeafHasher`] produces the
/// identical hash from a byte stream rather than a single buffer.
///
/// # Errors
///
/// Returns an error if `bytes` is empty (an empty logical leaf is not a
/// supported model).
pub fn file_leaf_hash_canonical(
    bytes: &[u8],
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    canonical_logical_attributes: Option<&[u8]>,
) -> Result<ObjectHash, String> {
    file_leaf_hash_canonical_with_magic(
        bytes,
        min_event_time,
        max_event_time,
        canonical_logical_attributes,
        LEAF_MAGIC,
    )
}

pub(crate) fn file_leaf_hash_canonical_with_magic(
    bytes: &[u8],
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    canonical_logical_attributes: Option<&[u8]>,
    magic: &[u8],
) -> Result<ObjectHash, String> {
    if bytes.is_empty() {
        return Err("a logical file leaf must contain at least one byte".to_string());
    }
    let logical_count = bytes.len() as u64;
    Ok(leaf_hash_with_magic(
        magic,
        LEAF_KIND_FILE,
        &[],
        logical_count,
        bytes,
        min_event_time,
        max_event_time,
        canonical_logical_attributes.unwrap_or(&[]),
    ))
}

/// Incremental equivalent of [`table_leaf_hash_canonical`].
///
/// Callers first scan record batches to total their canonical row-byte length,
/// then construct this hasher and feed the same batches again. Only one
/// canonicalized record batch is retained at a time.
pub struct IncrementalTableLeafHasher {
    hasher: blake3::Hasher,
    schema: Schema,
    logical_count: u64,
    rows_written: u64,
    canonical_rows_len: u64,
    canonical_rows_written: u64,
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    canonical_attributes: Vec<u8>,
}

impl IncrementalTableLeafHasher {
    /// Start hashing a table leaf with known row and canonical-byte counts.
    ///
    /// `canonical_rows_len` excludes the fixed row-payload magic and row-count
    /// prefix. `canonical_logical_attributes` must already be canonical JSON
    /// bytes when present.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty leaf, unsupported schema, or length
    /// overflow.
    pub fn new(
        schema: &Schema,
        logical_count: u64,
        canonical_rows_len: u64,
        min_event_time: Option<i64>,
        max_event_time: Option<i64>,
        canonical_logical_attributes: Option<&[u8]>,
    ) -> Result<Self, String> {
        Self::new_with_magic(
            LEAF_MAGIC,
            schema,
            logical_count,
            canonical_rows_len,
            min_event_time,
            max_event_time,
            canonical_logical_attributes,
        )
    }

    pub(crate) fn new_with_magic(
        magic: &[u8],
        schema: &Schema,
        logical_count: u64,
        canonical_rows_len: u64,
        min_event_time: Option<i64>,
        max_event_time: Option<i64>,
        canonical_logical_attributes: Option<&[u8]>,
    ) -> Result<Self, String> {
        Self::new_with_magic_and_rows_domain(
            magic,
            schema,
            logical_count,
            canonical_rows_len,
            min_event_time,
            max_event_time,
            canonical_logical_attributes,
            ROWS_MAGIC,
            SCHEMA_MAGIC,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_magic_and_rows_domain(
        magic: &[u8],
        schema: &Schema,
        logical_count: u64,
        canonical_rows_len: u64,
        min_event_time: Option<i64>,
        max_event_time: Option<i64>,
        canonical_logical_attributes: Option<&[u8]>,
        rows_magic: &[u8],
        schema_magic: &[u8],
    ) -> Result<Self, String> {
        if logical_count == 0 {
            return Err("a logical table leaf must contain at least one row".to_string());
        }
        let fingerprint = schema_fingerprint_with_magic(schema, schema_magic)?;
        let payload_len = u64::try_from(rows_magic.len() + std::mem::size_of::<u64>())
            .expect("fixed row payload prefix fits u64")
            .checked_add(canonical_rows_len)
            .ok_or_else(|| "canonical table payload length exceeds u64::MAX".to_string())?;
        let mut hasher = blake3::Hasher::new();
        let _ = hasher.update(magic);
        let _ = hasher.update(&[LEAF_KIND_TABLE]);
        let fingerprint_len = u32::try_from(fingerprint.as_bytes().len())
            .expect("BLAKE3 fingerprint length fits u32");
        let _ = hasher.update(&fingerprint_len.to_le_bytes());
        let _ = hasher.update(fingerprint.as_bytes());
        let _ = hasher.update(&logical_count.to_le_bytes());
        let _ = hasher.update(&payload_len.to_le_bytes());
        let _ = hasher.update(rows_magic);
        let _ = hasher.update(&logical_count.to_le_bytes());
        Ok(Self {
            hasher,
            schema: schema.clone(),
            logical_count,
            rows_written: 0,
            canonical_rows_len,
            canonical_rows_written: 0,
            min_event_time,
            max_event_time,
            canonical_attributes: canonical_logical_attributes.unwrap_or(&[]).to_vec(),
        })
    }

    /// Feed the next record batch in logical row order.
    ///
    /// # Errors
    ///
    /// Returns an error for an incompatible batch or if declared row/byte
    /// counts would be exceeded.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), String> {
        let rows = encode_canonical_batch_rows(&self.schema, batch)?;
        let new_row_count = self
            .rows_written
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| "table leaf row count exceeds u64::MAX".to_string())?;
        let new_byte_count = self
            .canonical_rows_written
            .checked_add(rows.len() as u64)
            .ok_or_else(|| "canonical table row bytes exceed u64::MAX".to_string())?;
        if new_row_count > self.logical_count || new_byte_count > self.canonical_rows_len {
            return Err("table leaf received more rows or bytes than declared".to_string());
        }
        let _ = self.hasher.update(&rows);
        self.rows_written = new_row_count;
        self.canonical_rows_written = new_byte_count;
        Ok(())
    }

    /// Finish this leaf and produce its identity hash.
    ///
    /// # Errors
    ///
    /// Returns an error if the supplied batches were truncated.
    pub fn finish(mut self) -> Result<ObjectHash, String> {
        if self.rows_written != self.logical_count
            || self.canonical_rows_written != self.canonical_rows_len
        {
            return Err(format!(
                "table leaf received {} row(s)/{} canonical byte(s), expected {}/{}",
                self.rows_written,
                self.canonical_rows_written,
                self.logical_count,
                self.canonical_rows_len
            ));
        }
        let mut flags = 0u8;
        if self.min_event_time.is_some() {
            flags |= LEAF_HAS_MIN;
        }
        if self.max_event_time.is_some() {
            flags |= LEAF_HAS_MAX;
        }
        let _ = self.hasher.update(&[flags]);
        if let Some(value) = self.min_event_time {
            let _ = self.hasher.update(&value.to_le_bytes());
        }
        if let Some(value) = self.max_event_time {
            let _ = self.hasher.update(&value.to_le_bytes());
        }
        let attributes_len = u32::try_from(self.canonical_attributes.len())
            .map_err(|_| "table leaf canonical attributes length exceeds u32::MAX".to_string())?;
        let _ = self.hasher.update(&attributes_len.to_le_bytes());
        let _ = self.hasher.update(&self.canonical_attributes);
        Ok(ObjectHash::from_bytes(*self.hasher.finalize().as_bytes()))
    }
}

/// Incremental, streaming equivalent of [`file_leaf_hash_canonical`]: computes
/// the identical `blake3` leaf hash from a byte stream fed in arbitrary-sized
/// chunks, rather than requiring the whole leaf's bytes in one buffer.
///
/// `docs/logical-series-identity-design.md` delivery gate 4: a file pack's
/// physical objects stream in from a remote blob store and a single logical
/// leaf may cross a physical-object boundary, so a reader must be able to
/// hash a leaf's bytes as they arrive rather than buffering the whole leaf
/// (let alone the whole pack) first. This type shares [`file_leaf_hash`]'s
/// and [`file_leaf_hash_canonical`]'s exact preimage framing -- the header
/// (magic, payload kind, absent schema fingerprint, `logical_count` twice:
/// once as the leaf's own count and once as the canonical payload's
/// length-prefix, since for a file leaf they are the same value) is hashed
/// eagerly in [`Self::new`], each [`Self::write`] call feeds the next slice
/// of the framed payload, and [`Self::finish`] appends the trailer (bounds
/// flags/values, length-prefixed attributes) and finalizes -- so the
/// resulting hash is bit-for-bit identical to calling
/// [`file_leaf_hash_canonical`] on the fully assembled buffer.
///
/// The declared byte count (`logical_count`) is enforced as bytes arrive:
/// [`Self::write`] rejects a chunk that would push the running total past
/// it, and [`Self::finish`] rejects finishing short. Neither "too many
/// bytes" nor "too few bytes" can silently produce a hash for the wrong
/// content.
pub struct IncrementalFileLeafHasher {
    hasher: blake3::Hasher,
    logical_count: u64,
    written: u64,
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    canonical_attributes: Vec<u8>,
}

impl IncrementalFileLeafHasher {
    /// Start hashing one file leaf declaring exactly `logical_count` bytes.
    ///
    /// `canonical_logical_attributes`, if given, must already be canonical
    /// logical-attribute bytes (see [`file_leaf_hash_canonical`]); this
    /// constructor does not itself re-validate them, matching the trust a
    /// caller already established when it decoded the
    /// [`super::series_pack::PackLeafDescriptor`] they came from.
    ///
    /// # Errors
    ///
    /// Returns an error if `logical_count` is `0` (an empty logical leaf is
    /// not a supported model).
    pub fn new(
        logical_count: u64,
        min_event_time: Option<i64>,
        max_event_time: Option<i64>,
        canonical_logical_attributes: Option<&[u8]>,
    ) -> Result<Self, String> {
        Self::new_with_magic(
            LEAF_MAGIC,
            logical_count,
            min_event_time,
            max_event_time,
            canonical_logical_attributes,
        )
    }

    pub(crate) fn new_with_magic(
        magic: &[u8],
        logical_count: u64,
        min_event_time: Option<i64>,
        max_event_time: Option<i64>,
        canonical_logical_attributes: Option<&[u8]>,
    ) -> Result<Self, String> {
        if logical_count == 0 {
            return Err("a logical file leaf must contain at least one byte".to_string());
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(magic);
        hasher.update(&[LEAF_KIND_FILE]);
        // File leaves never carry a schema fingerprint.
        hasher.update(&0u32.to_le_bytes());
        hasher.update(&logical_count.to_le_bytes());
        // The canonical payload for a file leaf *is* the exact byte range, so
        // its u64 length prefix equals `logical_count` itself.
        hasher.update(&logical_count.to_le_bytes());
        Ok(Self {
            hasher,
            logical_count,
            written: 0,
            min_event_time,
            max_event_time,
            canonical_attributes: canonical_logical_attributes.unwrap_or(&[]).to_vec(),
        })
    }

    /// This leaf's still-outstanding byte count: `logical_count` minus the
    /// bytes fed so far. Reaches `0` exactly when the leaf is complete.
    #[must_use]
    pub fn remaining(&self) -> u64 {
        self.logical_count - self.written
    }

    /// Feed the next `chunk` of this leaf's bytes, in order.
    ///
    /// `chunk` need not align to any particular boundary -- a caller may feed
    /// one physical object's entire content, or an arbitrary read-buffer's
    /// worth, as convenient. Callers that must partition a physical object's
    /// bytes across leaf boundaries should feed only the slice belonging to
    /// this leaf at the moment; see the leaf-partitioning contract in
    /// `docs/logical-series-identity-design.md`'s pack section.
    ///
    /// # Errors
    ///
    /// Returns an error if `chunk` would push the total bytes fed past this
    /// leaf's declared `logical_count` (extra/trailing bytes).
    pub fn write(&mut self, chunk: &[u8]) -> Result<(), String> {
        let new_written = self
            .written
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| "file leaf byte count exceeds u64::MAX".to_string())?;
        if new_written > self.logical_count {
            return Err(format!(
                "file leaf received more bytes than its declared logical_count {} (got at least {new_written})",
                self.logical_count
            ));
        }
        self.hasher.update(chunk);
        self.written = new_written;
        Ok(())
    }

    /// Finish this leaf and produce its identity hash.
    ///
    /// # Errors
    ///
    /// Returns an error if fewer than `logical_count` bytes were fed via
    /// [`Self::write`] (a truncated leaf).
    pub fn finish(mut self) -> Result<ObjectHash, String> {
        if self.written != self.logical_count {
            return Err(format!(
                "file leaf received only {} of its declared {} byte(s) (truncated)",
                self.written, self.logical_count
            ));
        }
        let mut flags = 0u8;
        if self.min_event_time.is_some() {
            flags |= LEAF_HAS_MIN;
        }
        if self.max_event_time.is_some() {
            flags |= LEAF_HAS_MAX;
        }
        self.hasher.update(&[flags]);
        if let Some(v) = self.min_event_time {
            self.hasher.update(&v.to_le_bytes());
        }
        if let Some(v) = self.max_event_time {
            self.hasher.update(&v.to_le_bytes());
        }
        let attrs_len = u32::try_from(self.canonical_attributes.len())
            .map_err(|_| "file leaf canonical attributes length exceeds u32::MAX".to_string())?;
        self.hasher.update(&attrs_len.to_le_bytes());
        self.hasher.update(&self.canonical_attributes);
        Ok(ObjectHash::from_bytes(*self.hasher.finalize().as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray, Float32Array,
        Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray,
        TimestampMicrosecondArray, UInt16Array,
    };
    use arrow_schema::{Field, TimeUnit};

    use super::*;

    fn schema_one_utf8(name: &str) -> Schema {
        Schema::new(vec![Field::new(name, DataType::Utf8, true)])
    }

    fn batch_one_utf8(schema: &Schema, values: &[Option<&str>]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(values.to_vec()))],
        )
        .unwrap()
    }

    // -- schema hashing --------------------------------------------------

    #[test]
    fn schema_change_changes_hash() {
        let a = schema_one_utf8("value");
        let b = schema_one_utf8("other_name");
        assert_ne!(
            schema_fingerprint(&a).unwrap(),
            schema_fingerprint(&b).unwrap()
        );
    }

    #[test]
    fn schema_nullability_changes_hash() {
        let a = Schema::new(vec![Field::new("v", DataType::Int32, true)]);
        let b = Schema::new(vec![Field::new("v", DataType::Int32, false)]);
        assert_ne!(
            schema_fingerprint(&a).unwrap(),
            schema_fingerprint(&b).unwrap()
        );
    }

    #[test]
    fn schema_field_order_changes_hash() {
        let a = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let b = Schema::new(vec![
            Field::new("b", DataType::Int32, true),
            Field::new("a", DataType::Int32, true),
        ]);
        assert_ne!(
            schema_fingerprint(&a).unwrap(),
            schema_fingerprint(&b).unwrap()
        );
    }

    #[test]
    fn schema_metadata_key_order_does_not_change_hash() {
        let mut m1 = std::collections::HashMap::new();
        m1.insert("z".to_string(), "1".to_string());
        m1.insert("a".to_string(), "2".to_string());
        let mut m2 = std::collections::HashMap::new();
        m2.insert("a".to_string(), "2".to_string());
        m2.insert("z".to_string(), "1".to_string());
        let a = Schema::new(vec![Field::new("v", DataType::Int32, true)]).with_metadata(m1);
        let b = Schema::new(vec![Field::new("v", DataType::Int32, true)]).with_metadata(m2);
        assert_eq!(
            schema_fingerprint(&a).unwrap(),
            schema_fingerprint(&b).unwrap()
        );
    }

    #[test]
    fn timestamp_timezone_change_is_a_schema_change() {
        let a = Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        let b = Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]);
        assert_ne!(
            schema_fingerprint(&a).unwrap(),
            schema_fingerprint(&b).unwrap()
        );
    }

    #[test]
    fn utf8_and_large_utf8_are_distinct_schema_types() {
        let a = Schema::new(vec![Field::new("v", DataType::Utf8, true)]);
        let b = Schema::new(vec![Field::new("v", DataType::LargeUtf8, true)]);
        assert_ne!(
            schema_fingerprint(&a).unwrap(),
            schema_fingerprint(&b).unwrap()
        );
    }

    #[test]
    fn logical_type_tags_are_frozen() {
        let cases = [
            (DataType::Boolean, "00"),
            (DataType::Int8, "01"),
            (DataType::Int16, "02"),
            (DataType::Int32, "03"),
            (DataType::Int64, "04"),
            (DataType::UInt8, "05"),
            (DataType::UInt16, "06"),
            (DataType::UInt32, "07"),
            (DataType::UInt64, "08"),
            (DataType::Float32, "09"),
            (DataType::Float64, "0a"),
            (DataType::Utf8, "0b"),
            (DataType::LargeUtf8, "0c"),
            (DataType::Binary, "0d"),
            (DataType::LargeBinary, "0e"),
            (DataType::Date32, "0f"),
            (DataType::Timestamp(TimeUnit::Second, None), "100000"),
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                "10010103000000555443",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "10020103000000555443",
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                "10030103000000555443",
            ),
            (DataType::Decimal128(38, -10), "1126f6"),
        ];
        for (data_type, expected) in cases {
            let mut encoded = Vec::new();
            encode_resolved_data_type(&data_type, &mut encoded);
            assert_eq!(hex::encode(encoded), expected, "{data_type:?}");
        }
    }

    #[test]
    fn metadata_encoding_is_frozen_and_sorted() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("z".to_string(), "last".to_string());
        metadata.insert("a".to_string(), "first".to_string());
        let mut encoded = Vec::new();
        encode_metadata(&metadata, &mut encoded);
        assert_eq!(
            hex::encode(encoded),
            "020000000100000061050000006669727374010000007a040000006c617374"
        );
    }

    // -- row hashing -------------------------------------------------------

    #[test]
    fn batch_chunking_does_not_change_hash() {
        let schema = schema_one_utf8("value");
        let one_batch = batch_one_utf8(&schema, &[Some("a"), Some("b"), Some("c")]);
        let three_batches = vec![
            batch_one_utf8(&schema, &[Some("a")]),
            batch_one_utf8(&schema, &[Some("b")]),
            batch_one_utf8(&schema, &[Some("c")]),
        ];
        let whole = encode_canonical_rows(&schema, &[one_batch]).unwrap();
        let chunked = encode_canonical_rows(&schema, &three_batches).unwrap();
        assert_eq!(whole, chunked);
    }

    #[test]
    fn row_order_changes_hash() {
        let schema = schema_one_utf8("value");
        let forward = batch_one_utf8(&schema, &[Some("a"), Some("b")]);
        let backward = batch_one_utf8(&schema, &[Some("b"), Some("a")]);
        let a = encode_canonical_rows(&schema, &[forward]).unwrap();
        let b = encode_canonical_rows(&schema, &[backward]).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn append_order_across_batches_is_preserved_not_batch_identity() {
        // Two batches appended in a different order produce different rows,
        // confirming batch order (not just per-batch content) contributes.
        let schema = schema_one_utf8("value");
        let ab = vec![
            batch_one_utf8(&schema, &[Some("a")]),
            batch_one_utf8(&schema, &[Some("b")]),
        ];
        let ba = vec![
            batch_one_utf8(&schema, &[Some("b")]),
            batch_one_utf8(&schema, &[Some("a")]),
        ];
        assert_ne!(
            encode_canonical_rows(&schema, &ab).unwrap(),
            encode_canonical_rows(&schema, &ba).unwrap()
        );
    }

    #[test]
    fn null_and_empty_string_are_distinct() {
        let schema = schema_one_utf8("value");
        let null_batch = batch_one_utf8(&schema, &[None]);
        let empty_batch = batch_one_utf8(&schema, &[Some("")]);
        assert_ne!(
            encode_canonical_rows(&schema, &[null_batch]).unwrap(),
            encode_canonical_rows(&schema, &[empty_batch]).unwrap()
        );
    }

    /// A physical schema whose one field is dictionary-encoded, used only to
    /// build a batch whose arrays match what `RecordBatch::try_new` expects.
    /// The *declared* logical schema passed to the encoder is always the
    /// plain (non-dictionary) schema -- dictionary-ness never appears there.
    fn dictionary_schema(name: &str) -> Schema {
        Schema::new(vec![Field::new(
            name,
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            true,
        )])
    }

    #[test]
    fn dictionary_column_matches_its_logical_values() {
        let plain_schema = schema_one_utf8("value");
        let plain_batch = batch_one_utf8(&plain_schema, &[Some("x"), None, Some("y")]);

        // The declared schema's field type is the plain logical type: the
        // dictionary is a physical detail of one particular batch's arrays.
        let dict_array: DictionaryArray<arrow_array::types::UInt16Type> =
            vec![Some("x"), None, Some("y")].into_iter().collect();
        let dict_batch = RecordBatch::try_new(
            Arc::new(dictionary_schema("value")),
            vec![Arc::new(dict_array)],
        )
        .unwrap();

        assert_eq!(
            encode_canonical_rows(&plain_schema, &[plain_batch]).unwrap(),
            encode_canonical_rows(&plain_schema, &[dict_batch]).unwrap()
        );
    }

    #[test]
    fn dictionary_and_plain_leaf_hashes_match() {
        let plain_schema = schema_one_utf8("value");
        let plain_batch = batch_one_utf8(&plain_schema, &[Some("x"), Some("y")]);
        let dict_array: DictionaryArray<arrow_array::types::UInt16Type> =
            vec![Some("x"), Some("y")].into_iter().collect();
        let dict_batch = RecordBatch::try_new(
            Arc::new(dictionary_schema("value")),
            vec![Arc::new(dict_array)],
        )
        .unwrap();

        let plain_hash = table_leaf_hash(&plain_schema, &[plain_batch], None, None, None).unwrap();
        let dict_hash = table_leaf_hash(&plain_schema, &[dict_batch], None, None, None).unwrap();
        assert_eq!(plain_hash, dict_hash);
    }

    #[test]
    fn incremental_table_leaf_matches_buffered_hash_across_physical_encodings() {
        let schema = schema_one_utf8("value");
        let plain_batch = batch_one_utf8(&schema, &[Some("x"), None]);
        let dictionary: DictionaryArray<arrow_array::types::UInt16Type> =
            vec![Some("y"), Some("z")].into_iter().collect();
        let dictionary_batch = RecordBatch::try_new(
            Arc::new(dictionary_schema("value")),
            vec![Arc::new(dictionary)],
        )
        .unwrap();
        let batches = vec![plain_batch, dictionary_batch];
        let canonical_rows_len = batches
            .iter()
            .try_fold(0u64, |total, batch| -> Result<u64, String> {
                let len = u64::try_from(encode_canonical_batch_rows(&schema, batch)?.len())
                    .map_err(|_| "test batch length exceeds u64::MAX".to_string())?;
                total
                    .checked_add(len)
                    .ok_or_else(|| "test row bytes exceed u64::MAX".to_string())
            })
            .unwrap();
        let mut incremental =
            IncrementalTableLeafHasher::new(&schema, 4, canonical_rows_len, Some(1), Some(2), None)
                .unwrap();
        for batch in &batches {
            incremental.write_batch(batch).unwrap();
        }
        assert_eq!(
            incremental.finish().unwrap(),
            table_leaf_hash(&schema, &batches, Some(1), Some(2), None).unwrap()
        );
    }

    #[test]
    fn nan_payloads_normalize_to_one_pattern() {
        let schema = Schema::new(vec![Field::new("v", DataType::Float64, true)]);
        // Two distinct NaN bit patterns (differing payload / sign bit).
        let a = f64::from_bits(0x7ff8_0000_0000_0001);
        let b = f64::from_bits(0xfff8_0000_0000_0002);
        assert!(a.is_nan() && b.is_nan());
        let batch_a = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Float64Array::from(vec![Some(a)]))],
        )
        .unwrap();
        let batch_b = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Float64Array::from(vec![Some(b)]))],
        )
        .unwrap();
        assert_eq!(
            encode_canonical_rows(&schema, &[batch_a]).unwrap(),
            encode_canonical_rows(&schema, &[batch_b]).unwrap()
        );
    }

    #[test]
    fn nan_bytes_are_frozen() {
        let f32_schema = Schema::new(vec![Field::new("v", DataType::Float32, false)]);
        let f32_batch = RecordBatch::try_new(
            Arc::new(f32_schema.clone()),
            vec![Arc::new(Float32Array::from(vec![f32::NAN]))],
        )
        .unwrap();
        assert_eq!(
            hex::encode(encode_canonical_rows(&f32_schema, &[f32_batch]).unwrap()),
            "7761746572746f776e2e7365726965732d726f77732e76310a0100000000000000010000c07f"
        );

        let f64_schema = Schema::new(vec![Field::new("v", DataType::Float64, false)]);
        let f64_batch = RecordBatch::try_new(
            Arc::new(f64_schema.clone()),
            vec![Arc::new(Float64Array::from(vec![f64::NAN]))],
        )
        .unwrap();
        assert_eq!(
            hex::encode(encode_canonical_rows(&f64_schema, &[f64_batch]).unwrap()),
            "7761746572746f776e2e7365726965732d726f77732e76310a010000000000000001000000000000f87f"
        );
    }

    #[test]
    fn signed_zero_is_preserved() {
        let schema = Schema::new(vec![Field::new("v", DataType::Float64, true)]);
        let pos = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Float64Array::from(vec![Some(0.0_f64)]))],
        )
        .unwrap();
        let neg = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Float64Array::from(vec![Some(-0.0_f64)]))],
        )
        .unwrap();
        assert_ne!(
            encode_canonical_rows(&schema, &[pos]).unwrap(),
            encode_canonical_rows(&schema, &[neg]).unwrap()
        );
    }

    #[test]
    fn mixed_type_batch_round_trips_every_supported_type() {
        let schema = Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i", DataType::Int64, true),
            Field::new("u", DataType::UInt16, true),
            Field::new("f", DataType::Float32, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("ls", DataType::LargeUtf8, true),
            Field::new("bin", DataType::Binary, true),
            Field::new("d", DataType::Date32, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("dec", DataType::Decimal128(10, 2), true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(Int64Array::from(vec![Some(-7_i64)])),
                Arc::new(UInt16Array::from(vec![Some(42_u16)])),
                Arc::new(Float32Array::from(vec![Some(1.5_f32)])),
                Arc::new(StringArray::from(vec![Some("hello")])),
                Arc::new(LargeStringArray::from(vec![Some("world")])),
                Arc::new(BinaryArray::from(vec![Some(&b"\x00\x01"[..])])),
                Arc::new(Date32Array::from(vec![Some(19_000)])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_000_i64)])
                        .with_timezone("UTC"),
                ),
                Arc::new(
                    Decimal128Array::from(vec![Some(12345_i128)])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        // Just confirm every accepted type encodes without error and is
        // deterministic; exact bytes for a fixed subset are frozen below.
        let a = encode_canonical_rows(&schema, std::slice::from_ref(&batch)).unwrap();
        let b = encode_canonical_rows(&schema, &[batch]).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn unsupported_nested_type_is_rejected_by_schema_and_rows() {
        let field = Field::new(
            "v",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        );
        let schema = Schema::new(vec![field]);
        assert!(encode_canonical_schema(&schema).is_err());

        // Row encoding must reject the same type -- never fall back to
        // stringifying or silently skipping it.
        let list_array =
            arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(
                vec![Some(vec![Some(1)])],
            );
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(list_array)]).unwrap();
        assert!(encode_canonical_rows(&schema, &[batch]).is_err());
    }

    #[test]
    fn unsupported_decimal256_is_rejected() {
        let schema = Schema::new(vec![Field::new("v", DataType::Decimal256(10, 2), true)]);
        assert!(encode_canonical_schema(&schema).is_err());
    }

    // -- leaf hashing --------------------------------------------------

    #[test]
    fn file_leaf_bytes_change_hash() {
        let a = file_leaf_hash(b"hello", None, None, None).unwrap();
        let b = file_leaf_hash(b"hellp", None, None, None).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn empty_logical_leaves_are_rejected() {
        assert!(file_leaf_hash(b"", None, None, None).is_err());
        let schema = schema_one_utf8("value");
        let empty = batch_one_utf8(&schema, &[]);
        assert!(table_leaf_hash(&schema, &[empty], None, None, None).is_err());
    }

    #[test]
    fn file_leaf_is_deterministic() {
        let a = file_leaf_hash(b"same bytes", Some(1), Some(2), Some(r#"{"a":1}"#)).unwrap();
        let b = file_leaf_hash(b"same bytes", Some(1), Some(2), Some(r#"{"a":1}"#)).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn table_and_file_leaves_never_collide() {
        // Same logical_count, same (empty) attributes, same bounds -- only
        // payload_kind and schema_fingerprint framing should distinguish them.
        let schema = Schema::new(vec![Field::new("v", DataType::Utf8, true)]);
        let batch = batch_one_utf8(&schema, &[Some("x")]);
        let table = table_leaf_hash(&schema, &[batch], None, None, None).unwrap();
        let file = file_leaf_hash(b"x", None, None, None).unwrap();
        assert_ne!(table, file);
    }

    #[test]
    fn bounds_absent_differs_from_bounds_zero() {
        let a = file_leaf_hash(b"x", None, None, None).unwrap();
        let b = file_leaf_hash(b"x", Some(0), Some(0), None).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn bounds_min_only_differs_from_both_present() {
        let a = file_leaf_hash(b"x", Some(5), None, None).unwrap();
        let b = file_leaf_hash(b"x", Some(5), Some(5), None).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn attributes_absent_differs_from_empty_object() {
        let a = file_leaf_hash(b"x", None, None, None).unwrap();
        let b = file_leaf_hash(b"x", None, None, Some("{}")).unwrap();
        assert_ne!(a, b);
    }

    // -- canonical attribute JSON --------------------------------------

    #[test]
    fn attribute_object_keys_are_recursively_sorted() {
        let a = encode_canonical_attributes(r#"{"b":1,"a":{"z":1,"y":2}}"#).unwrap();
        let b = encode_canonical_attributes(r#"{"a":{"y":2,"z":1},"b":1}"#).unwrap();
        assert_eq!(a, b);
        assert_eq!(
            std::str::from_utf8(&a).unwrap(),
            r#"{"a":{"y":2,"z":1},"b":1}"#
        );
    }

    #[test]
    fn attribute_string_escaping_is_frozen() {
        let encoded =
            encode_canonical_attributes(r#"{"z":"é","a":"quote\" slash\\ line\n ctrl\u0001"}"#)
                .unwrap();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "{\"a\":\"quote\\\" slash\\\\ line\\n ctrl\\u0001\",\"z\":\"é\"}"
        );
    }

    #[test]
    fn attributes_reject_non_integer_numbers() {
        assert!(encode_canonical_attributes(r#"{"fraction":1.5}"#).is_err());
        assert!(encode_canonical_attributes(r#"{"exponent":1e20}"#).is_err());
        assert!(encode_canonical_attributes(r#"{"too_large":18446744073709551616}"#).is_err());
    }

    #[test]
    fn attributes_reject_non_object_top_level() {
        assert!(encode_canonical_attributes("[1,2,3]").is_err());
        assert!(encode_canonical_attributes("\"a string\"").is_err());
        assert!(encode_canonical_attributes("42").is_err());
        assert!(encode_canonical_attributes("null").is_err());
        assert!(encode_canonical_attributes("true").is_err());
    }

    #[test]
    fn attributes_reject_malformed_json() {
        assert!(encode_canonical_attributes("{not json}").is_err());
        assert!(encode_canonical_attributes("{\"a\":}").is_err());
        assert!(encode_canonical_attributes("").is_err());
    }

    // -- golden vectors --------------------------------------------------
    //
    // These freeze exact encoded bytes and hashes so an Arrow upgrade or an
    // accidental refactor is caught immediately. They were computed once by
    // this implementation and are pinned here as independent literals, not
    // derived at test time from the function under test.

    #[test]
    fn golden_mixed_table_leaf() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("reading", DataType::Float64, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alpha"), None])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        1_700_000_000_000_000_i64,
                        1_700_000_001_000_000_i64,
                    ])
                    .with_timezone("UTC"),
                ),
                Arc::new(Float64Array::from(vec![Some(1.5_f64), Some(-0.0_f64)])),
            ],
        )
        .unwrap();

        let schema_bytes = encode_canonical_schema(&schema).unwrap();
        assert_eq!(
            hex::encode(&schema_bytes),
            "7761746572746f776e2e7365726965732d736368656d612e76310a04000000020000006964000300000000040000006e\
616d65010b000000000200000074730010020103000000555443000000000700000072656164696e67010a0000\
000000000000"
        );

        let row_bytes = encode_canonical_rows(&schema, std::slice::from_ref(&batch)).unwrap();
        assert_eq!(
            hex::encode(&row_bytes),
            "7761746572746f776e2e7365726965732d726f77732e76310a02000000000000000101000000010500000000000000\
616c7068610100401e18240a060001000000000000f83f0102000000000140822d18240a06000100000000\
00000080"
        );

        let hash = table_leaf_hash(
            &schema,
            &[batch],
            Some(1_700_000_000_000_000),
            Some(1_700_000_001_000_000),
            Some(r#"{"unit":"C"}"#),
        )
        .unwrap();
        assert_eq!(
            hash.to_hex(),
            "325a3ad2b15552adf5335d49bd283425d35d9380fe8833e8e333221f4bfbdc41"
        );
    }

    #[test]
    fn golden_file_leaf() {
        let hash = file_leaf_hash(
            b"pressure,depth\n1.0,2.0\n",
            Some(1_700_000_000_000_000),
            Some(1_700_000_001_000_000),
            Some(r#"{"source":"csv"}"#),
        )
        .unwrap();
        assert_eq!(
            hash.to_hex(),
            "316b52fa1dc942471704cb2498faf9e73903d46ec7a4d03b778ac02125648ad6"
        );
    }

    // -- canonical-attrs variants and incremental file hashing (gate 4) -----

    #[test]
    fn file_leaf_hash_canonical_matches_json_variant() {
        let attrs = encode_canonical_attributes(r#"{"source":"csv"}"#).unwrap();
        let via_json = file_leaf_hash(
            b"hello world",
            Some(1),
            Some(2),
            Some(r#"{"source":"csv"}"#),
        )
        .unwrap();
        let via_canonical =
            file_leaf_hash_canonical(b"hello world", Some(1), Some(2), Some(&attrs)).unwrap();
        assert_eq!(via_json, via_canonical);
    }

    #[test]
    fn table_leaf_hash_canonical_matches_json_variant() {
        let schema = schema_one_utf8("value");
        let batch = batch_one_utf8(&schema, &[Some("a"), Some("b")]);
        let attrs = encode_canonical_attributes(r#"{"unit":"C"}"#).unwrap();
        let via_json = table_leaf_hash(
            &schema,
            std::slice::from_ref(&batch),
            Some(1),
            Some(2),
            Some(r#"{"unit":"C"}"#),
        )
        .unwrap();
        let via_canonical = table_leaf_hash_canonical(
            &schema,
            std::slice::from_ref(&batch),
            Some(1),
            Some(2),
            Some(&attrs),
        )
        .unwrap();
        assert_eq!(via_json, via_canonical);
    }

    #[test]
    fn incremental_file_hasher_matches_whole_buffer_hash() {
        let bytes = b"pressure,depth\n1.0,2.0\n";
        let whole = file_leaf_hash_canonical(bytes, Some(10), Some(20), None).unwrap();

        let mut incremental =
            IncrementalFileLeafHasher::new(bytes.len() as u64, Some(10), Some(20), None).unwrap();
        incremental.write(bytes).unwrap();
        let via_incremental = incremental.finish().unwrap();
        assert_eq!(whole, via_incremental);
    }

    #[test]
    fn incremental_file_hasher_is_chunk_size_independent() {
        let bytes = b"the quick brown fox jumps over the lazy dog";
        let whole = file_leaf_hash_canonical(bytes, None, None, None).unwrap();

        for chunk_size in [1usize, 3, 7, bytes.len()] {
            let mut hasher =
                IncrementalFileLeafHasher::new(bytes.len() as u64, None, None, None).unwrap();
            for chunk in bytes.chunks(chunk_size) {
                hasher.write(chunk).unwrap();
            }
            assert_eq!(hasher.finish().unwrap(), whole, "chunk_size={chunk_size}");
        }
    }

    #[test]
    fn incremental_file_hasher_matches_json_attrs_variant() {
        let bytes = b"attrs matter too";
        let canonical_attrs = encode_canonical_attributes(r#"{"k":1}"#).unwrap();
        let whole = file_leaf_hash(bytes, Some(5), None, Some(r#"{"k":1}"#)).unwrap();

        let mut hasher = IncrementalFileLeafHasher::new(
            bytes.len() as u64,
            Some(5),
            None,
            Some(&canonical_attrs),
        )
        .unwrap();
        hasher.write(bytes).unwrap();
        assert_eq!(hasher.finish().unwrap(), whole);
    }

    #[test]
    fn incremental_file_hasher_rejects_extra_bytes() {
        let mut hasher = IncrementalFileLeafHasher::new(4, None, None, None).unwrap();
        hasher.write(b"abcd").unwrap();
        let err = hasher
            .write(b"e")
            .expect_err("must reject bytes beyond logical_count");
        assert!(err.contains("logical_count"), "unexpected error: {err}");
    }

    #[test]
    fn incremental_file_hasher_rejects_truncated_finish() {
        let mut hasher = IncrementalFileLeafHasher::new(4, None, None, None).unwrap();
        hasher.write(b"ab").unwrap();
        let err = hasher.finish().expect_err("must reject finishing short");
        assert!(err.contains("truncated"), "unexpected error: {err}");
    }

    #[test]
    fn incremental_file_hasher_rejects_zero_logical_count() {
        assert!(IncrementalFileLeafHasher::new(0, None, None, None).is_err());
    }

    #[test]
    fn incremental_file_hasher_remaining_reaches_zero_exactly() {
        let mut hasher = IncrementalFileLeafHasher::new(5, None, None, None).unwrap();
        assert_eq!(hasher.remaining(), 5);
        hasher.write(b"ab").unwrap();
        assert_eq!(hasher.remaining(), 3);
        hasher.write(b"cde").unwrap();
        assert_eq!(hasher.remaining(), 0);
        hasher.finish().unwrap();
    }
}
