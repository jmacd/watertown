// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Write-time stamping of the v2 logical-series identity
//! (`docs/logical-series-identity-design.md`) onto `OplogEntry` rows.
//!
//! Every nonempty `FilePhysicalSeries`/`TablePhysicalSeries` append is a
//! logical leaf. Its identity -- `blake3` over project-defined canonical
//! logical bytes -- is computed exactly once, here, at write time, and
//! persisted directly on the Oplog row (`logical_leaf_hash`,
//! `logical_count`, and, for tables, `series_schema_fingerprint`). Steward's
//! fold (`steward/src/content_tree.rs`) later just reads these columns to
//! build the ordered `watertown.series.v1` manifest; it never re-derives identity
//! from physical (Parquet/Bao) encoding. This module is the single place
//! that computes the hash, so the fold and the writer can never disagree
//! about what a leaf's identity is.
//!
//! An **empty** append (the existing "extended-attributes-only" write,
//! e.g. touching only temporal metadata with zero appended bytes/rows)
//! creates no logical leaf: `docs/logical-series-identity-design.md`'s
//! nonempty-leaf invariant disallows an empty logical leaf, so such a row
//! is stamped with `logical_leaf_hash`/`logical_count` left `None` rather
//! than being rejected outright, and the fold simply skips rows with no
//! leaf hash. For `TablePhysicalSeries`, `series_schema_fingerprint` is a
//! narrower exception: a schema fingerprint is not itself a logical leaf,
//! so it is stamped whenever the Parquet content is decodable at all --
//! even a schema-but-zero-rows file -- independent of whether the row also
//! got a `logical_leaf_hash`.

use crate::error::TLogFSError;
use crate::schema::OplogEntry;
use std::path::Path;
use tinyfs::EntryType;

/// Load the physical bytes of `entry`'s content, resolving an externalized
/// (`Large`) reference against `_large_files/` under `pond_path` if needed.
///
/// Returns an empty `Vec` for a row with neither inline content nor an
/// external reference (there is no such row today, but this keeps stamping
/// total rather than requiring a panic/unwrap on an unexpected shape).
///
/// Used only for `TablePhysicalSeries`: decoding Parquet requires the whole
/// buffer (`ParquetRecordBatchReaderBuilder` needs a `ChunkReader`/`Bytes`),
/// so there is no streaming alternative for tables here. `FilePhysicalSeries`
/// hashing never calls this -- see [`hash_file_series_content`], which
/// streams external content through [`sync_store::content::IncrementalFileLeafHasher`]
/// instead of buffering it whole (item 5,
/// `docs/logical-series-identity-design.md`).
async fn load_content_bytes(pond_path: &Path, entry: &OplogEntry) -> Result<Vec<u8>, TLogFSError> {
    if let Some(content) = &entry.content {
        return Ok(content.clone());
    }
    if let Some(blake3) = &entry.blake3 {
        return crate::large_files::read_external_bytes(pond_path, blake3).await;
    }
    Ok(Vec::new())
}

/// Compute a `FilePhysicalSeries` append's logical leaf hash without ever
/// buffering an externalized blob whole.
///
/// An inline append's bytes are already resident in `entry.content`, so
/// [`sync_store::content::file_leaf_hash`] is used directly. An externalized
/// (`Large`) append is instead streamed from `_large_files/` through
/// [`crate::large_files::ParquetFileReader`] in bounded chunks, feeding each
/// chunk to an [`sync_store::content::IncrementalFileLeafHasher`] -- so a
/// large external file series never requires a second whole-file `Vec`
/// buffer purely to compute its hash.
async fn hash_file_series_content(
    pond_path: &Path,
    entry: &OplogEntry,
) -> Result<sync_store::content::ObjectHash, TLogFSError> {
    if let Some(content) = &entry.content {
        if content.is_empty() {
            // Caller (`stamp_logical_leaf`) never reaches this for an empty
            // append, but stay total rather than assume.
            return Err(TLogFSError::ArrowMessage(
                "hash_file_series_content: called with empty inline content".to_string(),
            ));
        }
        return sync_store::content::file_leaf_hash(
            content,
            entry.min_event_time,
            entry.max_event_time,
            entry.extended_attributes.as_deref(),
        )
        .map_err(|e| {
            TLogFSError::ArrowMessage(format!(
                "logical leaf stamping: failed to compute file leaf hash: {e}"
            ))
        });
    }
    let Some(blake3) = &entry.blake3 else {
        return Err(TLogFSError::ArrowMessage(
            "hash_file_series_content: entry has neither inline content nor an external \
             reference"
                .to_string(),
        ));
    };
    let logical_count = entry.size.unwrap_or(0);
    if logical_count <= 0 {
        return Err(TLogFSError::ArrowMessage(format!(
            "hash_file_series_content: externalized append for node {} declares size {:?}, \
             expected a positive byte count",
            entry.node_id, entry.size
        )));
    }
    let attrs = match entry.extended_attributes.as_deref() {
        Some(json) => Some(
            sync_store::content::encode_canonical_attributes(json).map_err(|e| {
                TLogFSError::ArrowMessage(format!(
                    "logical leaf stamping: failed to canonicalize logical attributes: {e}"
                ))
            })?,
        ),
        None => None,
    };
    let mut hasher = sync_store::content::IncrementalFileLeafHasher::new(
        logical_count as u64,
        entry.min_event_time,
        entry.max_event_time,
        attrs.as_deref(),
    )
    .map_err(|e| {
        TLogFSError::ArrowMessage(format!(
            "logical leaf stamping: failed to start incremental file leaf hasher: {e}"
        ))
    })?;

    let path = crate::large_files::find_large_file_path(pond_path, blake3)
        .await
        .map_err(|e| TLogFSError::ArrowMessage(format!("locate large file {blake3}: {e}")))?
        .ok_or_else(|| TLogFSError::LargeFileNotFound {
            blake3: blake3.clone(),
            path: format!("_large_files/blake3={blake3}"),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "large file not found"),
        })?;
    let mut reader = crate::large_files::ParquetFileReader::new(path.clone())
        .await
        .map_err(|e| TLogFSError::LargeFileNotFound {
            blake3: blake3.clone(),
            path: path.display().to_string(),
            source: e,
        })?;
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = reader.read(&mut buf).await.map_err(|e| {
            TLogFSError::ArrowMessage(format!("stream external file series {blake3}: {e}"))
        })?;
        if n == 0 {
            break;
        }
        hasher.write(&buf[..n]).map_err(|e| {
            TLogFSError::ArrowMessage(format!(
                "logical leaf stamping: incremental hasher rejected external content: {e}"
            ))
        })?;
    }
    hasher.finish().map_err(|e| {
        TLogFSError::ArrowMessage(format!(
            "logical leaf stamping: failed to finish incremental file leaf hash: {e}"
        ))
    })
}

/// Decode a Parquet buffer into its schema and `RecordBatch`es, exactly as
/// `TableProcessor`/`SeriesProcessor` do in `file_writer.rs`. An empty buffer
/// decodes to `(None, [])` (matching the existing extended-attributes-only
/// write allowance) rather than erroring.
///
/// The schema is read from the Parquet file's own metadata (via the reader
/// builder), independent of how many row groups follow: a Parquet file can
/// carry a schema with zero encoded rows, and `series_schema_fingerprint`
/// must still be available in that case even though `logical_leaf_hash`/
/// `logical_count` are not (the nonempty-leaf invariant governs the latter
/// two only -- a schema fingerprint is not itself a logical leaf).
#[allow(clippy::type_complexity)]
fn decode_parquet_batches(
    bytes: &[u8],
) -> Result<
    (
        Option<arrow::datatypes::SchemaRef>,
        Vec<arrow::record_batch::RecordBatch>,
    ),
    TLogFSError,
> {
    if bytes.is_empty() {
        return Ok((None, Vec::new()));
    }
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tokio_util::bytes::Bytes;

    let buf = Bytes::copy_from_slice(bytes);
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(buf).map_err(|e| {
        TLogFSError::ArrowMessage(format!(
            "logical leaf stamping: failed to open Parquet content: {e}"
        ))
    })?;
    let schema = reader_builder.schema().clone();
    let reader = reader_builder.build().map_err(|e| {
        TLogFSError::ArrowMessage(format!(
            "logical leaf stamping: failed to build Parquet reader: {e}"
        ))
    })?;
    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| {
            TLogFSError::ArrowMessage(format!(
                "logical leaf stamping: failed to read Parquet batch: {e}"
            ))
        })?;
        batches.push(batch);
    }
    Ok((Some(schema), batches))
}

/// Public wrapper around [`decode_parquet_batches`] for callers outside this
/// crate that need to decode a `TablePhysicalSeries` version's Parquet bytes
/// back into a schema and `RecordBatch`es using the exact same decoder this
/// module stamps leaf identity from -- currently, steward's initial series
/// pack publication (`docs/logical-series-identity-design.md`), which must
/// reconstruct the same logical content this module already hashed at write
/// time, not a second, possibly-divergent decode path.
///
/// See [`decode_parquet_batches`] for the exact empty-buffer and
/// zero-row-with-schema behavior.
pub fn decode_table_series_leaf(
    bytes: &[u8],
) -> Result<
    (
        Option<arrow::datatypes::SchemaRef>,
        Vec<arrow::record_batch::RecordBatch>,
    ),
    TLogFSError,
> {
    decode_parquet_batches(bytes)
}

/// Stamp `entry` in place with its v2 logical-series leaf identity, if it is
/// a nonempty `FilePhysicalSeries` or `TablePhysicalSeries` append.
///
/// No-op (all three fields stay `None`) for every other `EntryType`, and
/// for an empty append of either series kind -- the nonempty-leaf invariant
/// means there is no leaf to identify, not an error.
///
/// # Errors
///
/// Returns an error if externalized content cannot be read back, if Parquet
/// content cannot be decoded, or if the canonical hashing primitives in
/// `sync_store::content` reject the content (this is deliberately loud: a
/// silently-skipped stamp on a nonempty append would be silent identity
/// loss).
pub async fn stamp_logical_leaf(
    pond_path: &Path,
    entry: &mut OplogEntry,
) -> Result<(), TLogFSError> {
    match entry.file_type {
        EntryType::FilePhysicalSeries => {
            // An append is empty either as an inline zero-length write, or
            // (for an externalized append) a declared zero/absent `size` --
            // check the count only, never the bytes, so an external append's
            // content is never buffered whole merely to test emptiness.
            let is_empty = match (&entry.content, entry.blake3.is_some()) {
                (Some(content), _) => content.is_empty(),
                (None, true) => entry.size.unwrap_or(0) <= 0,
                (None, false) => true,
            };
            if is_empty {
                return Ok(());
            }
            let hash = hash_file_series_content(pond_path, entry).await?;
            let logical_count = match &entry.content {
                Some(content) => content.len() as i64,
                None => entry.size.unwrap_or(0),
            };
            entry.logical_leaf_hash = Some(hash.to_hex());
            entry.logical_count = Some(logical_count);
        }
        EntryType::TablePhysicalSeries => {
            let bytes = load_content_bytes(pond_path, entry).await?;
            let (schema, batches) = decode_parquet_batches(&bytes)?;
            let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

            // The schema fingerprint is not itself a logical leaf, so it is
            // available whenever the Parquet content is decodable at all --
            // even a schema-but-zero-rows file -- independent of the
            // nonempty-leaf invariant governing `logical_leaf_hash`/
            // `logical_count` below. Without this, a `TablePhysicalSeries`
            // series whose every version happened to carry zero rows would
            // have no schema fingerprint to validate against
            // `SeriesManifest::new`'s `PayloadKind::Table` requirement.
            if let Some(schema) = &schema {
                let fingerprint = sync_store::content::schema_fingerprint(schema).map_err(|e| {
                    TLogFSError::ArrowMessage(format!(
                        "logical leaf stamping: failed to compute schema fingerprint: {e}"
                    ))
                })?;
                entry.series_schema_fingerprint = Some(fingerprint.to_hex());
            }

            if total_rows == 0 {
                // Either no bytes at all, or a Parquet file with schema but no
                // rows (also an empty logical leaf, per the nonempty-leaf
                // invariant): leave the leaf hash/count unset.
                return Ok(());
            }
            // `decode_parquet_batches` only ever returns `schema: None` when
            // `bytes` is empty, in which case `batches` is also empty and
            // `total_rows` above is 0 -- so this point is only reached with
            // `schema: Some`. Checked explicitly rather than `.expect()`ed so
            // a future change to that invariant surfaces as a diagnosable
            // error on this row instead of a panic.
            let schema = schema.ok_or_else(|| {
                TLogFSError::ArrowMessage(
                    "logical leaf stamping: TablePhysicalSeries row has nonzero rows but no \
                     decoded schema"
                        .to_string(),
                )
            })?;
            let attrs = entry.extended_attributes.as_deref();
            let hash = sync_store::content::table_leaf_hash(
                &schema,
                &batches,
                entry.min_event_time,
                entry.max_event_time,
                attrs,
            )
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!(
                    "logical leaf stamping: failed to compute table leaf hash: {e}"
                ))
            })?;
            entry.logical_leaf_hash = Some(hash.to_hex());
            entry.logical_count = Some(total_rows as i64);
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ExtendedAttributes;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int64Array, RecordBatch};
    use tinyfs::{EntryType, FileID, local_pond_uuid};

    fn series_id(entry_type: EntryType) -> FileID {
        FileID::new_in_partition(
            FileID::root_for(local_pond_uuid()).part_id(),
            entry_type,
            local_pond_uuid(),
        )
    }

    fn parquet_bytes(values: &[i64]) -> Vec<u8> {
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![std::sync::Arc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("record batch");
        let mut buf = Vec::new();
        {
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
            writer.write(&batch).expect("write batch");
            _ = writer.close().expect("close writer");
        }
        buf
    }

    /// A schema-only (zero-row) parquet buffer: same schema, no batches
    /// written before `close()`.
    fn zero_row_parquet_bytes() -> Vec<u8> {
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let mut buf = Vec::new();
        {
            let writer =
                parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
            _ = writer.close().expect("close writer");
        }
        buf
    }

    #[tokio::test]
    async fn file_series_append_stamps_expected_leaf_hash_and_count() {
        let content = b"alice,1\nbob,2\n".to_vec();
        let mut entry = OplogEntry::new_file_series(
            series_id(EntryType::FilePhysicalSeries),
            0,
            1,
            content.clone(),
            10,
            20,
            ExtendedAttributes::default(),
            1,
        );

        stamp_logical_leaf(Path::new("/unused"), &mut entry)
            .await
            .expect("stamp");

        let expected = sync_store::content::file_leaf_hash(
            &content,
            Some(10),
            Some(20),
            entry.extended_attributes.as_deref(),
        )
        .expect("compute expected hash");
        assert_eq!(
            entry.logical_leaf_hash,
            Some(expected.to_hex()),
            "stamped leaf hash must match the canonical implementation directly"
        );
        assert_eq!(entry.logical_count, Some(content.len() as i64));
        assert!(
            entry.series_schema_fingerprint.is_none(),
            "a FilePhysicalSeries row has no schema fingerprint"
        );
    }

    #[tokio::test]
    async fn empty_file_series_append_is_stamped_leafless_not_errored() {
        let mut entry = OplogEntry::new_file_series(
            series_id(EntryType::FilePhysicalSeries),
            0,
            1,
            Vec::new(),
            10,
            20,
            ExtendedAttributes::default(),
            1,
        );

        stamp_logical_leaf(Path::new("/unused"), &mut entry)
            .await
            .expect("stamp of empty append must not error");
        assert!(entry.logical_leaf_hash.is_none());
        assert!(entry.logical_count.is_none());
    }

    #[tokio::test]
    async fn table_series_append_stamps_leaf_hash_count_and_schema_fingerprint() {
        let bytes = parquet_bytes(&[1, 2, 3]);
        let mut entry = OplogEntry::new_file_series(
            series_id(EntryType::TablePhysicalSeries),
            0,
            1,
            bytes.clone(),
            10,
            20,
            ExtendedAttributes::default(),
            1,
        );

        stamp_logical_leaf(Path::new("/unused"), &mut entry)
            .await
            .expect("stamp");

        assert_eq!(
            entry.logical_count,
            Some(3),
            "row count must reflect the decoded RecordBatch"
        );
        assert!(entry.logical_leaf_hash.is_some());
        assert!(
            entry.series_schema_fingerprint.is_some(),
            "a nonempty TablePhysicalSeries row must carry a schema fingerprint"
        );
    }

    #[tokio::test]
    async fn zero_row_table_series_append_gets_schema_fingerprint_but_no_leaf() {
        // A schema-only (zero-row) Parquet buffer is a decodable, empty
        // logical leaf: `logical_leaf_hash`/`logical_count` must stay `None`
        // per the nonempty-leaf invariant, but `series_schema_fingerprint`
        // must still be stamped since it is not itself a logical leaf and a
        // later `SeriesManifest::new` unconditionally requires it for
        // `PayloadKind::Table`.
        let bytes = zero_row_parquet_bytes();
        let mut entry = OplogEntry::new_file_series(
            series_id(EntryType::TablePhysicalSeries),
            0,
            1,
            bytes,
            10,
            20,
            ExtendedAttributes::default(),
            1,
        );

        stamp_logical_leaf(Path::new("/unused"), &mut entry)
            .await
            .expect("stamp");

        assert!(entry.logical_leaf_hash.is_none());
        assert!(entry.logical_count.is_none());
        assert!(
            entry.series_schema_fingerprint.is_some(),
            "schema fingerprint must be available even for a zero-row leaf"
        );
    }

    #[tokio::test]
    async fn non_series_entry_type_is_left_untouched() {
        let mut entry = OplogEntry::new_small_file(
            FileID::new_in_partition(
                FileID::root_for(local_pond_uuid()).part_id(),
                EntryType::FilePhysicalVersion,
                local_pond_uuid(),
            ),
            0,
            1,
            b"plain file content".to_vec(),
            1,
        );
        stamp_logical_leaf(Path::new("/unused"), &mut entry)
            .await
            .expect("stamp");
        assert!(entry.logical_leaf_hash.is_none());
        assert!(entry.logical_count.is_none());
        assert!(entry.series_schema_fingerprint.is_none());
    }
}
