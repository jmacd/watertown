// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Focused coverage for `steward::pack_maintenance` (`Ship::collapse_versions`,
//! `docs/logical-series-identity-design.md`) not already exercised by
//! `crates/steward/src/ship.rs`'s own unit tests (file-series repack,
//! unchanged root/version/manifest, repeat settlement) or
//! `content_source_pack_test.rs` (fetching a maintenance-published pack's
//! physical objects through `LocalPondSource`):
//!
//! - a `TablePhysicalSeries` repack (the file-series path is covered
//!   elsewhere; the table path has its own accumulator and had no coverage
//!   through `Ship::collapse_versions` at all),
//! - a physical-object boundary genuinely crossing the middle of one
//!   logical leaf (not merely one leaf per object, or one object for the
//!   whole series), and
//! - a repack that fails after durably writing physical objects but before
//!   publishing the pack index that would name them, proving the crash-safe
//!   ordering the module's own docs describe: no dangling/orphaned
//!   advertisement, only harmless unreferenced objects, and the series
//!   remains fully readable and re-repackable afterward.

use std::path::Path;
use steward::{
    ContentSource, LocalPondSource, Ship, compute_content_tree, fetch_object_graph, rebuild_pond,
};
use sync_store::content::PackIndex;
use tempfile::tempdir;
use tinyfs::EntryType;
use tinyfs::ResultExt;
use tinyfs::arrow::parquet::ParquetExt;
use tlogfs::PondUserMetadata;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

fn table_batch(ts_micros: i64, label: &str) -> arrow_array::RecordBatch {
    use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![ts_micros])),
            Arc::new(Int64Array::from(vec![ts_micros])),
            Arc::new(StringArray::from(vec![label])),
        ],
    )
    .expect("build table batch")
}

fn evolved_table_batch(ts_micros: i64, label: &str) -> arrow_array::RecordBatch {
    use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("quality", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![ts_micros])),
            Arc::new(Int64Array::from(vec![ts_micros])),
            Arc::new(StringArray::from(vec![label])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )
    .expect("build evolved table batch")
}

/// Same logical shape as [`table_batch`] (one `timestamp`/`value`/`label`
/// row), but `label` is physically `Dictionary<Int32, Utf8>` rather than
/// plain `Utf8` -- the exact same logical column encoded a different way,
/// which `sync_store::content::canonicalize_schema` (finding 3) normalizes
/// to plain `Utf8` when computing `schema_fingerprint`, so this leaf is
/// accepted into the same series as a plain-`Utf8`-labeled leaf, and
/// `repack_table_series` must be able to cast it to the run's shared
/// canonical schema alongside plain leaves rather than requiring every
/// leaf's raw physical schema to already match the first leaf's.
fn table_batch_with_dictionary_label(ts_micros: i64, label: &str) -> arrow_array::RecordBatch {
    use arrow_array::{
        DictionaryArray, Int64Array, RecordBatch, TimestampMicrosecondArray, types::Int32Type,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "label",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
    ]));
    let label_dict: DictionaryArray<Int32Type> = vec![Some(label)].into_iter().collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![ts_micros])),
            Arc::new(Int64Array::from(vec![ts_micros])),
            Arc::new(label_dict),
        ],
    )
    .expect("build dictionary-labeled table batch")
}

/// Decode every row of one physical Parquet object's raw bytes into a
/// single concatenated `RecordBatch`, mirroring how the real v2
/// materializer decodes a table pack's physical objects
/// (`content_pull.rs`'s use of `ParquetRecordBatchReaderBuilder`).
fn decode_parquet_rows(bytes: &[u8]) -> arrow_array::RecordBatch {
    use arrow::compute::concat_batches;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .expect("open parquet reader")
        .build()
        .expect("build parquet reader");
    let batches: Vec<arrow_array::RecordBatch> =
        reader.collect::<Result<Vec<_>, _>>().expect("read batches");
    let schema = batches[0].schema();
    concat_batches(&schema, &batches).expect("concat batches")
}

async fn maintained_pack_for_series(
    source: &LocalPondSource,
    pond_path: &Path,
    series_name: &str,
    entry_type: EntryType,
) -> (sync_store::content::ObjectHash, PackIndex) {
    let tip = source
        .get_tip("")
        .await
        .expect("get tip")
        .expect("a content-changing commit exists");
    let commit_bytes = source
        .get_object(tip)
        .await
        .expect("get tip object")
        .expect("tip commit object is served");
    let commit = sync_store::content::Commit::decode(&commit_bytes).expect("decode tip commit");
    let manifest_bytes = source
        .get_object(commit.node_manifest_hash)
        .await
        .expect("get node manifest object")
        .expect("node manifest object is served");
    let manifest_entries =
        sync_store::content::decode_manifest(&manifest_bytes).expect("decode node manifest");
    let series_hash = manifest_entries
        .iter()
        .find(|entry| entry.name == series_name && entry.entry_type == entry_type)
        .unwrap_or_else(|| panic!("manifest contains {series_name} as {entry_type:?}"))
        .child_hash;

    let series_dir = steward::get_data_path(pond_path)
        .join(sync_store::pack_keys::PACKS_ROOT)
        .join(sync_store::pack_keys::series_dir_name(series_hash));
    let published: Vec<sync_store::content::ObjectHash> = std::fs::read_dir(&series_dir)
        .expect("read maintained series advertisement directory")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            sync_store::pack_keys::parse_pack_file_name(entry.file_name().to_str()?).ok()
        })
        .collect();
    assert_eq!(
        published.len(),
        1,
        "the current series hash must have exactly one maintained advertisement"
    );
    let pack_bytes = source
        .get_pack_index(series_hash, published[0])
        .await
        .expect("fetch maintained pack index")
        .expect("maintained pack index is present");
    (
        series_hash,
        PackIndex::decode(&pack_bytes).expect("decode maintained pack index"),
    )
}

/// A `TablePhysicalSeries` with several live versions must be repacked
/// through `Ship::collapse_versions` exactly as a `FilePhysicalSeries` is:
/// candidate discovery, a real repack that reduces physical object count,
/// unchanged root/Delta version, every row surviving afterward (verified
/// through the maintenance-published pack's own physical objects), and
/// settlement on a repeated run.
#[tokio::test]
async fn collapse_versions_repacks_a_table_series() {
    let temp_dir = tempdir().expect("tempdir");
    let pond_path = temp_dir.path().join("table_repack_pond");
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");

    let versions = [
        (1_000_000i64, "a"),
        (2_000_000i64, "b"),
        (3_000_000i64, "c"),
        (4_000_000i64, "d"),
    ];
    ship.write_transaction(&meta("table-series"), async move |fs| {
        let root = fs.root().await?;
        let _ = root.create_dir_path("data").await?;
        for (ts, label) in versions {
            let batch = table_batch(ts, label);
            let _ = root
                .write_series_from_batch("/data/events.table", &batch, Some("timestamp"))
                .await?;
        }
        Ok(())
    })
    .await
    .expect("table series transaction");

    let root_before = compute_content_tree(&ship)
        .await
        .expect("root hash before repack")
        .root_tree_hash;
    let delta_version_before = ship.data_persistence().table().version();

    let report = ship
        .collapse_versions(1)
        .await
        .expect("pack maintenance must succeed for a table series");
    assert_eq!(report.candidates, 1, "the one table series is a candidate");
    assert_eq!(
        report.series_repacked, 1,
        "the over-threshold table series must be repacked"
    );
    assert_eq!(report.unsupported_legacy, 0);
    assert!(
        report.pack_objects_written > 0,
        "repacking a table series must publish at least one new physical pack object"
    );

    let root_after = compute_content_tree(&ship)
        .await
        .expect("root hash after repack")
        .root_tree_hash;
    assert_eq!(
        root_after, root_before,
        "pack-only maintenance must never change the root tree hash, table series included"
    );
    assert_eq!(
        ship.data_persistence().table().version(),
        delta_version_before,
        "pack-only maintenance must never advance the Delta version"
    );

    // Rows survive byte-identical: fetch the maintenance-published pack's
    // physical objects through `LocalPondSource` (the same production
    // surface a `pond://`/clean-reader reconstruction uses) and decode
    // every row, confirming every original label is present.
    drop(ship);
    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source after table repack");
    let tip = source
        .get_tip("")
        .await
        .expect("get tip")
        .expect("a content-changing commit exists");
    let commit_bytes = source
        .get_object(tip)
        .await
        .expect("get tip object")
        .expect("tip commit object is served");
    let commit = sync_store::content::Commit::decode(&commit_bytes).expect("decode tip commit");
    let manifest_bytes = source
        .get_object(commit.node_manifest_hash)
        .await
        .expect("get node manifest object")
        .expect("node manifest object is served");
    let manifest_entries =
        sync_store::content::decode_manifest(&manifest_bytes).expect("decode node manifest");
    let series_entry = manifest_entries
        .iter()
        .find(|e| e.name == "events.table" && e.entry_type == EntryType::TablePhysicalSeries)
        .expect("the table series has a manifest entry");
    let series_hash = series_entry.child_hash;

    let series_dir = steward::get_data_path(&pond_path)
        .join(sync_store::pack_keys::PACKS_ROOT)
        .join(sync_store::pack_keys::series_dir_name(series_hash));
    let published: Vec<sync_store::content::ObjectHash> = std::fs::read_dir(&series_dir)
        .expect("read published pack advertisement directory")
        .filter_map(|e| e.ok())
        .filter_map(|e| sync_store::pack_keys::parse_pack_file_name(e.file_name().to_str()?).ok())
        .collect();
    assert_eq!(published.len(), 1, "one pack advertisement was published");
    let pack_hash = published[0];
    let pack_bytes = source
        .get_pack_index(series_hash, pack_hash)
        .await
        .expect("fetch published pack index")
        .expect("the published table pack must be fetchable");
    let pack = PackIndex::decode(&pack_bytes).expect("decode published table pack index");
    assert!(
        pack.physical_object_hashes().len() < versions.len(),
        "the published table pack must be more bounded than one physical object per append: \
         {} objects for {} appends",
        pack.physical_object_hashes().len(),
        versions.len()
    );

    let mut labels: Vec<String> = Vec::new();
    for &hash in pack.physical_object_hashes() {
        assert!(
            source.has_blob(hash).await.expect("has_blob"),
            "each maintenance-published table pack physical object must be a recognized blob"
        );
        let mut reader = source
            .get_blob_reader(hash)
            .await
            .expect("get_blob_reader")
            .expect("table pack physical object must be fetchable");
        let mut bytes = Vec::new();
        let _ = reader
            .read_to_end(&mut bytes)
            .await
            .expect("read table pack physical object to completion");
        let batch = decode_parquet_rows(&bytes);
        use arrow_array::Array;
        let col = batch
            .column_by_name("label")
            .expect("label column")
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("label is a string array");
        for i in 0..col.len() {
            labels.push(col.value(i).to_string());
        }
    }
    labels.sort();
    assert_eq!(
        labels,
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string()
        ],
        "every table row survives pack-only maintenance"
    );
    drop(source);

    // Settlement: a repeated run at the same threshold finds the series
    // already at its bounded floor and repacks nothing further.
    let mut ship = Ship::open_pond(&pond_path)
        .await
        .expect("reopen pond for repeat run");
    let again = ship
        .collapse_versions(1)
        .await
        .expect("repeat pack maintenance for table series");
    assert_eq!(again.candidates, 1);
    assert_eq!(
        again.series_repacked, 0,
        "an already-bounded table series must not be repacked again"
    );
    assert_eq!(again.already_bounded, 1);
    assert_eq!(again.pack_objects_written, 0);
}

/// Finding 3: a `TablePhysicalSeries` whose live leaves mix a plain `Utf8`
/// `label` column with a logically-equivalent `Dictionary<Int32, Utf8>`
/// `label` column (both legal appends to the same series, since
/// `sync_store::content::schema_fingerprint` -- and the write-time
/// `series_schema_fingerprint` stability check -- normalizes a dictionary
/// column to its plain value type) must still repack successfully into one
/// bounded physical object, rather than `repack_table_series` failing (or
/// silently forcing the second leaf's raw dictionary/plain schema to match
/// the first's without a real cast). Every row, and its logical `label`
/// value, must survive byte/value-identical.
#[tokio::test]
async fn collapse_versions_repacks_a_table_series_mixing_dictionary_and_plain_label_columns() {
    let temp_dir = tempdir().expect("tempdir");
    let pond_path = temp_dir.path().join("dictionary_table_repack_pond");
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");

    ship.write_transaction(&meta("dictionary-table-series"), async move |fs| {
        let root = fs.root().await?;
        let _ = root.create_dir_path("data").await?;
        // Alternate plain-Utf8-labeled and Dictionary<Int32, Utf8>-labeled
        // leaves across four appends, so the run's canonical schema (fixed
        // by the *first* leaf) must accommodate later leaves physically
        // encoded the other way in both directions.
        let plain_a = table_batch(1_000_000, "a");
        let dict_b = table_batch_with_dictionary_label(2_000_000, "b");
        let plain_c = table_batch(3_000_000, "c");
        let dict_d = table_batch_with_dictionary_label(4_000_000, "d");
        for batch in [&plain_a, &dict_b, &plain_c, &dict_d] {
            let _ = root
                .write_series_from_batch("/data/events.table", batch, Some("timestamp"))
                .await?;
        }
        Ok(())
    })
    .await
    .expect("mixed dictionary/plain table series transaction");

    let root_before = compute_content_tree(&ship)
        .await
        .expect("root hash before repack")
        .root_tree_hash;

    let report = ship
        .collapse_versions(1)
        .await
        .expect("pack maintenance must succeed for a mixed dictionary/plain table series");
    assert_eq!(report.candidates, 1);
    assert_eq!(
        report.series_repacked, 1,
        "the mixed-encoding table series must be repacked, not rejected as a schema mismatch"
    );
    assert_eq!(report.unsupported_legacy, 0);
    assert!(
        report.pack_objects_written > 0,
        "repacking the mixed-encoding table series must publish at least one new physical \
         pack object"
    );

    let root_after = compute_content_tree(&ship)
        .await
        .expect("root hash after repack")
        .root_tree_hash;
    assert_eq!(
        root_after, root_before,
        "pack-only maintenance must never change the root tree hash"
    );

    drop(ship);
    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source after mixed dictionary/plain table repack");
    let tip = source
        .get_tip("")
        .await
        .expect("get tip")
        .expect("a content-changing commit exists");
    let commit_bytes = source
        .get_object(tip)
        .await
        .expect("get tip object")
        .expect("tip commit object is served");
    let commit = sync_store::content::Commit::decode(&commit_bytes).expect("decode tip commit");
    let manifest_bytes = source
        .get_object(commit.node_manifest_hash)
        .await
        .expect("get node manifest object")
        .expect("node manifest object is served");
    let manifest_entries =
        sync_store::content::decode_manifest(&manifest_bytes).expect("decode node manifest");
    let series_entry = manifest_entries
        .iter()
        .find(|e| e.name == "events.table" && e.entry_type == EntryType::TablePhysicalSeries)
        .expect("the mixed-encoding table series has a manifest entry");
    let series_hash = series_entry.child_hash;

    let series_dir = steward::get_data_path(&pond_path)
        .join(sync_store::pack_keys::PACKS_ROOT)
        .join(sync_store::pack_keys::series_dir_name(series_hash));
    let published: Vec<sync_store::content::ObjectHash> = std::fs::read_dir(&series_dir)
        .expect("read published pack advertisement directory")
        .filter_map(|e| e.ok())
        .filter_map(|e| sync_store::pack_keys::parse_pack_file_name(e.file_name().to_str()?).ok())
        .collect();
    assert_eq!(published.len(), 1, "one pack advertisement was published");
    let pack_hash = published[0];
    let pack_bytes = source
        .get_pack_index(series_hash, pack_hash)
        .await
        .expect("fetch published pack index")
        .expect("the published mixed-encoding table pack must be fetchable");
    let pack = PackIndex::decode(&pack_bytes).expect("decode published table pack index");

    // Every physical object's `label` column decodes as plain `Utf8` (the
    // canonical schema every leaf -- dictionary- or plain-encoded -- was
    // cast to), and every original logical label value survives.
    let mut labels: Vec<String> = Vec::new();
    for &hash in pack.physical_object_hashes() {
        let mut reader = source
            .get_blob_reader(hash)
            .await
            .expect("get_blob_reader")
            .expect("table pack physical object must be fetchable");
        let mut bytes = Vec::new();
        let _ = reader
            .read_to_end(&mut bytes)
            .await
            .expect("read table pack physical object to completion");
        let batch = decode_parquet_rows(&bytes);
        use arrow_array::Array;
        let col = batch
            .column_by_name("label")
            .expect("label column")
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect(
                "the maintenance-published physical object's label column must be plain Utf8, \
                 the canonical type a Dictionary<Int32, Utf8> leaf was cast to -- not still \
                 dictionary-encoded",
            );
        for i in 0..col.len() {
            labels.push(col.value(i).to_string());
        }
    }
    labels.sort();
    assert_eq!(
        labels,
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string()
        ],
        "every row survives repacking a series that mixes dictionary- and plain-encoded \
         leaves for the same logical label column"
    );
}

#[tokio::test]
async fn collapse_versions_splits_table_pack_at_schema_transitions() {
    let temp_dir = tempdir().expect("tempdir");
    let pond_path = temp_dir.path().join("evolved_table_repack_pond");
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");

    let first = table_batch(1_000_000, "a");
    let second = table_batch(2_000_000, "b");
    let third = evolved_table_batch(3_000_000, "c");
    let fourth = evolved_table_batch(4_000_000, "d");
    let fingerprint_a =
        sync_store::content::schema_fingerprint(&first.schema()).expect("fingerprint a");
    let fingerprint_b =
        sync_store::content::schema_fingerprint(&third.schema()).expect("fingerprint b");
    assert_ne!(fingerprint_a, fingerprint_b);

    ship.write_transaction(&meta("evolved-table-series"), async move |fs| {
        let root = fs.root().await?;
        let _ = root.create_dir_path("data").await?;
        for batch in [&first, &second, &third, &fourth] {
            let _ = root
                .write_series_from_batch("/data/events.table", batch, Some("timestamp"))
                .await?;
        }
        Ok(())
    })
    .await
    .expect("write schema-evolving table series");

    let root_before = compute_content_tree(&ship)
        .await
        .expect("root before repack")
        .root_tree_hash;
    let report = ship
        .collapse_versions(1)
        .await
        .expect("repack heterogeneous table series");
    assert_eq!(report.series_repacked, 1);
    let root_after = compute_content_tree(&ship)
        .await
        .expect("root after repack")
        .root_tree_hash;
    assert_eq!(root_after, root_before);

    drop(ship);
    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local source");
    let (_series_hash, pack) = maintained_pack_for_series(
        &source,
        &pond_path,
        "events.table",
        EntryType::TablePhysicalSeries,
    )
    .await;
    let descriptor_fingerprints: Vec<_> = pack
        .leaf_descriptors()
        .iter()
        .map(|descriptor| descriptor.schema_fingerprint())
        .collect();
    assert_eq!(
        descriptor_fingerprints,
        vec![
            Some(fingerprint_a),
            Some(fingerprint_a),
            Some(fingerprint_b),
            Some(fingerprint_b),
        ]
    );
    assert_eq!(
        pack.physical_object_hashes().len(),
        2,
        "the large row cap would produce one object without the mandatory schema boundary"
    );
    let mut object_fingerprints = Vec::new();
    for &hash in pack.physical_object_hashes() {
        let mut reader = source
            .get_blob_reader(hash)
            .await
            .expect("get object")
            .expect("object exists");
        let mut bytes = Vec::new();
        let _ = reader.read_to_end(&mut bytes).await.expect("read object");
        let batch = decode_parquet_rows(&bytes);
        object_fingerprints.push(
            sync_store::content::schema_fingerprint(&batch.schema())
                .expect("object schema fingerprint"),
        );
    }
    assert_eq!(object_fingerprints, vec![fingerprint_a, fingerprint_b]);
}

/// A physical object boundary is independent of a logical leaf boundary: a
/// leaf may straddle the cap between two physical pack objects. Three
/// unevenly-sized appends (2.5 MiB each, three leaves = 7.5 MiB, comfortably
/// over the file pack's 4 MiB per-object cap) force the bounded repack to
/// two physical objects, with the middle leaf's bytes split across both --
/// verified by fetching every physical object the maintenance-published
/// pack names through `LocalPondSource` (the exact surface a `pond://` or
/// clean-reader reconstruction uses) and confirming their concatenation
/// reconstructs the original series content byte-for-byte.
#[tokio::test]
async fn collapse_versions_produces_a_pack_whose_leaf_spans_a_physical_object_boundary() {
    use sync_store::content::{Commit, decode_manifest};

    let temp_dir = tempdir().expect("tempdir");
    let pond_path = temp_dir.path().join("boundary_pond");
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");

    // 2.5 MiB per leaf, non-repeating content per leaf so a garbled or
    // misattributed byte range is detectable.
    const LEAF_BYTES: usize = 2621440;
    let leaves: Vec<Vec<u8>> = (0u8..3)
        .map(|seed| {
            (0..LEAF_BYTES)
                .map(|i| ((i as u32 + u32::from(seed) * 977) % 251) as u8)
                .collect()
        })
        .collect();
    let full_content: Vec<u8> = leaves.concat();

    for (i, chunk) in leaves.iter().enumerate() {
        let bytes = chunk.clone();
        let first = i == 0;
        ship.write_transaction(&meta("boundary-append"), async move |fs| {
            let root = fs.root().await?;
            if first {
                _ = root.create_dir_path("data").await?;
            }
            let mut writer = root
                .async_writer_path_with_type("/data/boundary.series", EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(&bytes).await.map_other()?;
            writer.shutdown().await.map_other()?;
            Ok(())
        })
        .await
        .expect("append boundary series version");
    }

    let report = ship
        .collapse_versions(1)
        .await
        .expect("pack maintenance must succeed");
    assert_eq!(report.candidates, 1);
    assert_eq!(report.series_repacked, 1);
    drop(ship);

    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source after boundary repack");
    let tip = source
        .get_tip("")
        .await
        .expect("get tip")
        .expect("a content-changing commit exists");
    let commit_bytes = source
        .get_object(tip)
        .await
        .expect("get tip object")
        .expect("tip commit object is served");
    let commit = Commit::decode(&commit_bytes).expect("decode tip commit");
    let manifest_bytes = source
        .get_object(commit.node_manifest_hash)
        .await
        .expect("get node manifest object")
        .expect("node manifest object is served");
    let manifest_entries = decode_manifest(&manifest_bytes).expect("decode node manifest");
    let series_entry = manifest_entries
        .iter()
        .find(|e| e.name == "boundary.series" && e.entry_type == EntryType::FilePhysicalSeries)
        .expect("the boundary series has a manifest entry");
    let series_hash = series_entry.child_hash;

    let series_dir = steward::get_data_path(&pond_path)
        .join(sync_store::pack_keys::PACKS_ROOT)
        .join(sync_store::pack_keys::series_dir_name(series_hash));
    let published: Vec<sync_store::content::ObjectHash> = std::fs::read_dir(&series_dir)
        .expect("read published pack advertisement directory")
        .filter_map(|e| e.ok())
        .filter_map(|e| sync_store::pack_keys::parse_pack_file_name(e.file_name().to_str()?).ok())
        .collect();
    assert_eq!(published.len(), 1, "one pack advertisement was published");
    let pack_hash = published[0];

    let pack_bytes = source
        .get_pack_index(series_hash, pack_hash)
        .await
        .expect("fetch published pack index")
        .expect("the published pack must be fetchable");
    let pack = PackIndex::decode(&pack_bytes).expect("decode published pack index");
    assert_eq!(
        pack.physical_object_hashes().len(),
        2,
        "7.5 MiB of leaves over a 4 MiB per-object cap must bound to exactly two physical objects"
    );

    let mut reconstructed = Vec::new();
    for &hash in pack.physical_object_hashes() {
        assert!(
            source.has_blob(hash).await.expect("has_blob"),
            "each physical object of a boundary-crossing pack must be a recognized blob"
        );
        let mut reader = source
            .get_blob_reader(hash)
            .await
            .expect("get_blob_reader")
            .expect("each physical object must be readable");
        let mut bytes = Vec::new();
        let _ = reader
            .read_to_end(&mut bytes)
            .await
            .expect("read physical object to completion");
        reconstructed.extend(bytes);
    }
    assert_eq!(
        reconstructed, full_content,
        "concatenating the boundary-crossing pack's physical objects, fetched through \
         LocalPondSource, must reconstruct the original series content byte-for-byte, proving \
         the middle leaf's split across two physical objects was reassembled correctly"
    );
}

/// If a repack fails after durably writing its physical objects but before
/// publishing the pack index that would name them (simulated here by
/// making the series' own `_packs/series=<hex>` advertisement directory
/// unwritable), `collapse_versions` must fail cleanly: the call returns an
/// error, no pack index is published (no advertisement can ever name a
/// missing object, but equally no advertisement should exist at all here),
/// any physical objects already written remain merely harmless and
/// unreferenced, and the series' logical content is completely unaffected.
/// Restoring write access and retrying must then succeed and publish the
/// index, proving recovery from a partial failure is not itself corrupting.
#[tokio::test]
async fn collapse_versions_fails_cleanly_before_publishing_an_index() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempdir().expect("tempdir");
        let pond_path = temp_dir.path().join("failure_pond");
        let mut ship = Ship::create_pond(&pond_path, "test-host")
            .await
            .expect("create pond");

        let chunks: [&[u8]; 2] = [b"first-version-bytes", b"second-version-bytes"];
        let mut cumulative: Vec<u8> = Vec::new();
        for (i, chunk) in chunks.into_iter().enumerate() {
            cumulative.extend_from_slice(chunk);
            let bytes = chunk.to_vec();
            let first = i == 0;
            ship.write_transaction(&meta("failure-append"), async move |fs| {
                let root = fs.root().await?;
                if first {
                    _ = root.create_dir_path("data").await?;
                }
                let mut writer = root
                    .async_writer_path_with_type(
                        "/data/failure.series",
                        EntryType::FilePhysicalSeries,
                    )
                    .await?;
                writer.write_all(&bytes).await.map_other()?;
                writer.shutdown().await.map_other()?;
                Ok(())
            })
            .await
            .expect("append failure series version");
        }

        // Learn the exact series_hash the real run below will target, purely
        // by reading (no mutation), then pre-create its advertisement
        // directory read-only so `publish_pack_index`'s write into it fails
        // with a permission error -- after physical objects (a separate,
        // always-writable directory) have already been durably written.
        let candidates = ship
            .survey_pack_maintenance(1)
            .await
            .expect("survey pack maintenance");
        assert_eq!(candidates.len(), 1, "one real repack candidate");
        let series_hash = candidates[0]
            .series_hash
            .expect("a native v2 candidate has a series_hash");

        let series_dir = steward::get_data_path(&pond_path)
            .join(sync_store::pack_keys::PACKS_ROOT)
            .join(sync_store::pack_keys::series_dir_name(series_hash));
        std::fs::create_dir_all(&series_dir).expect("pre-create series pack directory");
        std::fs::set_permissions(&series_dir, std::fs::Permissions::from_mode(0o555))
            .expect("make series pack directory read-only");

        let result = ship.collapse_versions(1).await;
        assert!(
            result.is_err(),
            "a repack that cannot publish its index must fail, not silently succeed"
        );

        // Physical objects (a separate, still-writable directory) may have
        // been durably written -- that is fine, harmless, and exactly the
        // crash-safety story: they are simply unreferenced by any index.
        let objects_dir = steward::get_data_path(&pond_path)
            .join(sync_store::pack_keys::PACKS_ROOT)
            .join("objects");
        let _ = std::fs::read_dir(&objects_dir);

        // No pack advertisement exists: the read-only directory has no
        // parseable `pack=<hex>` entries in it.
        let published_during_failure: Vec<_> = std::fs::read_dir(&series_dir)
            .expect("read series directory after failed publish")
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                sync_store::pack_keys::parse_pack_file_name(e.file_name().to_str()?).ok()
            })
            .collect();
        assert!(
            published_during_failure.is_empty(),
            "no pack index may be published when the repack failed before publication"
        );

        // The series' logical content is completely unaffected by the
        // failed repack attempt.
        let read_meta = meta("read-after-failure");
        let tx = ship.begin_read(&read_meta).await.expect("begin read");
        let root = tx.root().await.expect("root");
        let content = root
            .read_file_path_to_vec("/data/failure.series")
            .await
            .expect("read content after failed repack");
        assert_eq!(
            content, cumulative,
            "content must be completely unaffected by a repack that failed before publication"
        );
        _ = tx.commit().await.expect("commit read");

        // Restore write access and retry: recovery must succeed and
        // actually publish the index this time, proving the earlier
        // failure left nothing corrupt behind.
        std::fs::set_permissions(&series_dir, std::fs::Permissions::from_mode(0o755))
            .expect("restore series pack directory permissions");
        let recovered = ship
            .collapse_versions(1)
            .await
            .expect("a retried repack must succeed once the directory is writable again");
        assert_eq!(
            recovered.series_repacked, 1,
            "the retried repack must actually repack the series this time"
        );

        let published_after_recovery: Vec<_> = std::fs::read_dir(&series_dir)
            .expect("read series directory after recovery")
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                sync_store::pack_keys::parse_pack_file_name(e.file_name().to_str()?).ok()
            })
            .collect();
        assert_eq!(
            published_after_recovery.len(),
            1,
            "recovery must publish exactly one pack index"
        );
    }
}

/// A maintenance-published `FilePhysicalSeries` pack must reconstruct
/// correctly through the *real* production verification/import path --
/// [`LocalPondSource`] + [`fetch_object_graph`] + [`rebuild_pond`] -- not
/// merely by manually concatenating/decoding its physical objects. This is
/// the same surface a `pond://` reader, a replica, or `pond pull` would
/// use, and (unlike manual concatenation) it exercises
/// `fetch_and_verify_file_pack`'s own cross-check that the pack's
/// advertised `physical_byte_count` equals the sum of the actually fetched
/// physical object bytes -- catching a regression to summing original
/// Oplog blob sizes instead (requirement 1), along with the pack's proofs,
/// exact-cover selection, leaf descriptors, and every object hash.
#[tokio::test]
async fn maintained_file_pack_reconstructs_through_fetch_object_graph_and_rebuild_pond() {
    let temp_dir = tempdir().expect("tempdir");
    let pond_path = temp_dir.path().join("file_pack_production_pond");
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");

    // Several appends, each large enough that three of them together
    // exceed the file pack's 4 MiB per-object cap, so the maintained pack
    // both crosses a physical object boundary and differs in physical
    // object count/hashes from the original one-object-per-append layout.
    const LEAF_BYTES: usize = 2_000_000;
    let leaves: Vec<Vec<u8>> = (0u8..3)
        .map(|seed| {
            (0..LEAF_BYTES)
                .map(|i| {
                    ((i as u32)
                        .wrapping_mul(2_654_435_761)
                        .wrapping_add(u32::from(seed))
                        % 251) as u8
                })
                .collect()
        })
        .collect();
    let full_content: Vec<u8> = leaves.concat();

    for (i, chunk) in leaves.iter().enumerate() {
        let bytes = chunk.clone();
        let first = i == 0;
        ship.write_transaction(&meta("production-path-append"), async move |fs| {
            let root = fs.root().await?;
            if first {
                _ = root.create_dir_path("data").await?;
            }
            let mut writer = root
                .async_writer_path_with_type(
                    "/data/production.series",
                    EntryType::FilePhysicalSeries,
                )
                .await?;
            writer.write_all(&bytes).await.map_other()?;
            writer.shutdown().await.map_other()?;
            Ok(())
        })
        .await
        .expect("append production-path series version");
    }

    let report = ship
        .collapse_versions(1)
        .await
        .expect("pack maintenance must succeed");
    assert_eq!(report.series_repacked, 1, "the series must be repacked");
    drop(ship);

    // The real production path: open the maintained pond as a source,
    // fetch its verified object graph, and rebuild a fresh destination pond
    // from it -- exactly what a clean reader or replica does.
    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source after file repack");
    let graph = fetch_object_graph(&source, "").await.expect(
        "fetch_object_graph must verify the maintained pack, including physical_byte_count",
    );
    assert!(!graph.is_empty());

    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");
    let outcome = rebuild_pond(&mut target, &source, &graph)
        .await
        .expect("rebuild must materialize the maintained file series");
    assert_eq!(outcome.series, 1);

    let tx = target
        .begin_read(&meta("read-rebuilt"))
        .await
        .expect("begin read");
    let root = tx.root().await.expect("root");
    let rebuilt = root
        .read_file_path_to_vec("/data/production.series")
        .await
        .expect("read rebuilt series content");
    assert_eq!(
        rebuilt, full_content,
        "the maintained file pack must reconstruct byte-for-byte through the real fetch/rebuild \
         production path"
    );
}

/// The table-series counterpart of
/// `maintained_file_pack_reconstructs_through_fetch_object_graph_and_rebuild_pond`:
/// a maintenance-published `TablePhysicalSeries` pack must reconstruct
/// correctly through the real [`LocalPondSource`] + [`fetch_object_graph`]
/// + [`rebuild_pond`] path. `rebuild_pond` itself only commits once the
/// destination's own content-tree fold (which recomputes every leaf hash
/// bottom-up from the actually-materialized rows) reproduces the verified
/// manifest root, so a successful call here proves every leaf, descriptor,
/// proof, and hash -- and, via `fetch_and_verify_table_pack`'s own
/// pre-check, the pack's `physical_byte_count` against its actually
/// fetched (re-encoded Parquet) physical object bytes, catching exactly
/// the regression requirement 1 describes.
#[tokio::test]
async fn maintained_table_pack_reconstructs_through_fetch_object_graph_and_rebuild_pond() {
    let temp_dir = tempdir().expect("tempdir");
    let pond_path = temp_dir.path().join("table_pack_production_pond");
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");

    let versions = [
        (1_000_000i64, "a"),
        (2_000_000i64, "b"),
        (3_000_000i64, "c"),
        (4_000_000i64, "d"),
    ];
    ship.write_transaction(&meta("production-path-table"), async move |fs| {
        let root = fs.root().await?;
        let _ = root.create_dir_path("data").await?;
        for (ts, label) in versions {
            let batch = table_batch(ts, label);
            let _ = root
                .write_series_from_batch("/data/production.table", &batch, Some("timestamp"))
                .await?;
        }
        Ok(())
    })
    .await
    .expect("table series transaction");

    let report = ship
        .collapse_versions(1)
        .await
        .expect("pack maintenance must succeed for a table series");
    assert_eq!(report.series_repacked, 1);
    drop(ship);

    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source after table repack");
    let graph = fetch_object_graph(&source, "").await.expect(
        "fetch_object_graph must verify the maintained table pack, including \
             physical_byte_count against its re-encoded Parquet bytes",
    );
    assert!(!graph.is_empty());

    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");
    let outcome = rebuild_pond(&mut target, &source, &graph)
        .await
        .expect("rebuild must materialize the maintained table series");
    assert_eq!(outcome.series, 1);

    // Verify every individual version's own rows survive byte-identically
    // through the real fetch/rebuild production path -- a direct
    // non-series-aware read of a multi-version `TablePhysicalSeries`
    // resolves to a single "current" version, so checking every version
    // explicitly (rather than guessing which one a bare read returns) is
    // the only way to prove the whole series round-trips, not just
    // whichever version happens to be "current".
    let tx = target
        .begin_read(&meta("read-rebuilt-table"))
        .await
        .expect("begin read");
    let root = tx.root().await.expect("root");
    let rebuilt_versions = root
        .list_file_versions("/data/production.table")
        .await
        .expect("list rebuilt table series versions");
    assert_eq!(
        rebuilt_versions.len(),
        versions.len(),
        "every original version must survive the maintenance repack + rebuild"
    );
    for ((ts, label), info) in versions.iter().zip(rebuilt_versions.iter()) {
        let raw = root
            .read_file_version("/data/production.table", info.version)
            .await
            .unwrap_or_else(|e| panic!("read rebuilt table series version {}: {e}", info.version));
        let batch = decode_parquet_rows(&raw);
        assert_eq!(
            batch,
            table_batch(*ts, label),
            "version {}'s rows must reconstruct byte-for-byte through the real fetch/rebuild \
             production path",
            info.version
        );
    }
}

/// Requirement 2's core acceptance test: repeated append-then-remaintain
/// cycles must not let pack advertisements/objects grow forever. Each
/// append changes the series' manifest hash (it folds over every live
/// leaf), so without pruning, every one of N repacks would leave its own
/// now-obsolete `series=<oldhash>` directory (and its physical objects)
/// behind forever -- unbounded, and the number of stale advertisements
/// grows linearly (making the *sweep* pass, which walks every remaining
/// advertisement, effectively quadratic in the number of cycles).
///
/// Finding 4's one-generation concurrent-reader grace period means a
/// no-longer-live series directory is not deleted the moment it stops
/// being live -- it is first marked stale, then only actually removed on
/// a *later* maintenance run that finds it still not live. So after each
/// cycle here at most **two** `series=<hash>` directories may remain (this
/// run's freshly selected pack, plus the immediately preceding cycle's,
/// now marked stale but not yet due for removal) -- never more, which is
/// what still proves growth is bounded rather than unbounded/quadratic.
#[tokio::test]
async fn repeated_append_and_remaintain_reclaims_obsolete_series_advertisements_and_objects() {
    let temp_dir = tempdir().expect("tempdir");
    let pond_path = temp_dir.path().join("reclaim_growth_pond");
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");

    let packs_root = steward::get_data_path(&pond_path).join(sync_store::pack_keys::PACKS_ROOT);

    let count_series_dirs = || -> usize {
        std::fs::read_dir(&packs_root)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
                    .filter(|e| {
                        e.file_name()
                            .to_str()
                            .is_some_and(|n| n.starts_with("series="))
                    })
                    .count()
            })
            .unwrap_or(0)
    };

    const CYCLES: usize = 6;
    for cycle in 0..CYCLES {
        let bytes = format!("cycle-{cycle}-payload-bytes-grow-a-little-each-time-{cycle}{cycle}")
            .into_bytes();
        let first = cycle == 0;
        ship.write_transaction(&meta("reclaim-growth-append"), async move |fs| {
            let root = fs.root().await?;
            if first {
                _ = root.create_dir_path("data").await?;
            }
            let mut writer = root
                .async_writer_path_with_type(
                    "/data/reclaim_growth.series",
                    EntryType::FilePhysicalSeries,
                )
                .await?;
            writer.write_all(&bytes).await.map_other()?;
            writer.shutdown().await.map_other()?;
            Ok(())
        })
        .await
        .expect("append reclaim-growth series version");

        let report = ship
            .collapse_versions(1)
            .await
            .expect("pack maintenance must succeed each cycle");
        if cycle == 0 {
            // A single leaf's own live-version count (`COUNT(*) == 1`) does
            // not exceed `threshold == 1`, so `survey_collapsible_series`'s
            // own `HAVING COUNT(*) > threshold` excludes it from discovery
            // entirely -- not a candidate at all yet, so no pack
            // advertisement is published until a second leaf actually
            // pushes it over threshold next cycle.
            assert_eq!(
                report.series_repacked, 0,
                "cycle {cycle}: a single-leaf series is not yet over threshold"
            );
            assert_eq!(report.already_bounded, 0);
            assert_eq!(report.candidates, 0);
        } else {
            assert_eq!(
                report.series_repacked, 1,
                "cycle {cycle}: each new append changes the series hash, so each cycle from \
                 here on is a fresh repack candidate"
            );
        }

        let live_dirs = count_series_dirs();
        // Cycle 0 publishes nothing (below threshold). Cycle 1 publishes
        // the very first advertisement ever, with no predecessor to mark
        // stale. From cycle 2 on, each cycle's fresh publish coexists with
        // exactly one leftover: the immediately preceding cycle's now
        // no-longer-live directory, marked stale this run but deferred one
        // more generation before actual removal (finding 4) -- so the
        // count settles at 2, never climbing higher as cycles continue.
        let expected_live_dirs = match cycle {
            0 => 0,
            1 => 1,
            _ => 2,
        };
        assert_eq!(
            live_dirs, expected_live_dirs,
            "cycle {cycle}: at most the current pack plus one stale-generation leftover may \
             remain after maintenance -- {live_dirs} found, proving growth is not unbounded"
        );
        if expected_live_dirs == 0 {
            continue;
        }

        // Every surviving series directory (whether this cycle's fresh
        // publish or the prior cycle's stale-marked leftover) still
        // carries exactly one pack advertisement: any earlier duplicate
        // for the *same* series hash (there should be none here, since the
        // hash changes every cycle, but this also guards regressions in
        // `retain_selected_pack_only`) must have been collapsed away too.
        let series_dirs: Vec<_> = std::fs::read_dir(&packs_root)
            .expect("read packs root")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|n| n.starts_with("series="))
            })
            .collect();
        assert_eq!(series_dirs.len(), expected_live_dirs);
        for series_dir in &series_dirs {
            let advertisements = std::fs::read_dir(series_dir.path())
                .expect("read a live series directory")
                .filter_map(|e| e.ok())
                .filter_map(|e| {
                    sync_store::pack_keys::parse_pack_file_name(e.file_name().to_str()?).ok()
                })
                .count();
            assert_eq!(
                advertisements, 1,
                "cycle {cycle}: every surviving series directory must carry exactly one pack \
                 advertisement"
            );
        }
    }

    // One more maintenance run with no new append: the previous cycle's
    // stale-marked leftover directory is still not live, and was already
    // marked stale by the last cycle above, so this trailing run must
    // finally reclaim it -- proving the grace period is bounded to one
    // generation, not indefinite (finding 4's next-cycle cleanup).
    let trailing_report = ship
        .collapse_versions(1)
        .await
        .expect("trailing idle maintenance run must succeed");
    assert_eq!(
        trailing_report.series_repacked, 0,
        "no new append happened, so nothing needed a fresh repack"
    );
    let live_dirs = count_series_dirs();
    assert_eq!(
        live_dirs, 1,
        "a trailing idle maintenance run must finally reclaim the one remaining stale-generation \
         leftover, settling back down to just the current live pack"
    );

    // Final end-to-end proof that reclamation did not corrupt anything:
    // the series' final content is exactly the last append's full history,
    // reconstructed through the real production fetch/rebuild path.
    drop(ship);
    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source after reclaim-growth cycles");
    let graph = fetch_object_graph(&source, "")
        .await
        .expect("fetch_object_graph after repeated reclaim cycles");
    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");
    let outcome = rebuild_pond(&mut target, &source, &graph)
        .await
        .expect("rebuild after repeated reclaim cycles");
    assert_eq!(outcome.series, 1);
    let tx = target
        .begin_read(&meta("read-final"))
        .await
        .expect("begin read");
    let root = tx.root().await.expect("root");
    let rebuilt = root
        .read_file_path_to_vec("/data/reclaim_growth.series")
        .await
        .expect("read rebuilt reclaim-growth series");
    let mut expected = Vec::new();
    for cycle in 0..CYCLES {
        expected.extend(
            format!("cycle-{cycle}-payload-bytes-grow-a-little-each-time-{cycle}{cycle}")
                .into_bytes(),
        );
    }
    assert_eq!(rebuilt, expected);
}

/// Extending a series requires a new manifest-bound pack index, but
/// deterministic fixed-size packing must reuse unchanged full prefix objects
/// and write only the changed tail.
#[tokio::test]
async fn maintenance_reuses_unchanged_prefix_objects_after_incremental_append() {
    const LEAF_BYTES: usize = 2 * 1024 * 1024;

    let temp_dir = tempdir().expect("tempdir");
    let pond_path = temp_dir.path().join("incremental_maintenance_pond");
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");
    let leaves: Vec<Vec<u8>> = (0u8..4)
        .map(|seed| {
            (0..LEAF_BYTES)
                .map(|offset| ((offset as u32 + u32::from(seed) * 977) % 251) as u8)
                .collect()
        })
        .collect();

    for (index, leaf) in leaves[..3].iter().enumerate() {
        let bytes = leaf.clone();
        ship.write_transaction(&meta("incremental-initial-append"), async move |fs| {
            let root = fs.root().await?;
            if index == 0 {
                _ = root.create_dir_path("data").await?;
            }
            let mut writer = root
                .async_writer_path_with_type(
                    "/data/incremental.series",
                    EntryType::FilePhysicalSeries,
                )
                .await?;
            writer.write_all(&bytes).await.map_other()?;
            writer.shutdown().await.map_other()?;
            Ok(())
        })
        .await
        .expect("append initial series leaf");
    }

    let first_report = ship
        .collapse_versions(1)
        .await
        .expect("initial pack maintenance");
    assert_eq!(first_report.series_repacked, 1);
    assert_eq!(
        first_report.pack_objects_written, 2,
        "three 2 MiB leaves must produce one full 4 MiB prefix and one 2 MiB tail"
    );

    let first_source = LocalPondSource::open(&pond_path)
        .await
        .expect("open source after initial maintenance");
    let (first_series_hash, first_pack) = maintained_pack_for_series(
        &first_source,
        &pond_path,
        "incremental.series",
        EntryType::FilePhysicalSeries,
    )
    .await;
    assert_eq!(first_pack.physical_object_hashes().len(), 2);
    drop(first_source);

    let appended = leaves[3].clone();
    ship.write_transaction(&meta("incremental-tail-append"), async move |fs| {
        let root = fs.root().await?;
        let mut writer = root
            .async_writer_path_with_type("/data/incremental.series", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(&appended).await.map_other()?;
        writer.shutdown().await.map_other()?;
        Ok(())
    })
    .await
    .expect("append new tail leaf");

    // Reopen so the logical-root baseline uses the authoritative latest Delta
    // snapshot. A just-finished write can leave this long-lived handle on the
    // pre-commit snapshot until its next transaction refresh.
    drop(ship);
    let mut ship = Ship::open_pond(&pond_path)
        .await
        .expect("reopen pond after tail append");
    let root_after_append = compute_content_tree(&ship)
        .await
        .expect("root after append")
        .root_tree_hash;

    let incremental_report = ship
        .collapse_versions(1)
        .await
        .expect("incremental pack maintenance");
    assert_eq!(incremental_report.series_repacked, 1);
    assert_eq!(
        incremental_report.pack_objects_written, 1,
        "the unchanged 4 MiB prefix object must be reused"
    );
    assert_eq!(
        incremental_report.pack_bytes_written,
        4 * 1024 * 1024,
        "only the old 2 MiB tail plus the new 2 MiB leaf should be written"
    );
    assert_eq!(
        compute_content_tree(&ship)
            .await
            .expect("root after incremental maintenance")
            .root_tree_hash,
        root_after_append,
        "physical incremental maintenance must not change logical identity"
    );

    let second_source = LocalPondSource::open(&pond_path)
        .await
        .expect("open source after incremental maintenance");
    let (second_series_hash, second_pack) = maintained_pack_for_series(
        &second_source,
        &pond_path,
        "incremental.series",
        EntryType::FilePhysicalSeries,
    )
    .await;
    assert_ne!(
        second_series_hash, first_series_hash,
        "the logical series hash must change when a leaf is appended"
    );
    assert_eq!(second_pack.physical_object_hashes().len(), 2);
    assert_eq!(
        second_pack.physical_object_hashes()[0],
        first_pack.physical_object_hashes()[0],
        "the complete 4 MiB prefix object must be reused content-addressedly"
    );
    assert_ne!(
        second_pack.physical_object_hashes()[1],
        first_pack.physical_object_hashes()[1],
        "the extended tail must receive a new physical object"
    );

    let graph = fetch_object_graph(&second_source, "")
        .await
        .expect("verify incrementally maintained pack");
    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");
    _ = rebuild_pond(&mut target, &second_source, &graph)
        .await
        .expect("rebuild incrementally maintained series");
    drop(second_source);

    let tx = target
        .begin_read(&meta("read-incremental-rebuild"))
        .await
        .expect("begin target read");
    let root = tx.root().await.expect("target root");
    assert_eq!(
        root.read_file_path_to_vec("/data/incremental.series")
            .await
            .expect("read rebuilt incremental series"),
        leaves.concat(),
        "incrementally maintained physical objects must reconstruct the full logical series"
    );
    _ = tx.commit().await.expect("commit target read");

    let settled = ship
        .collapse_versions(1)
        .await
        .expect("settled maintenance pass");
    assert_eq!(settled.series_repacked, 0);
    assert_eq!(settled.already_bounded, 1);
    assert_eq!(settled.pack_objects_written, 0);
    assert_eq!(settled.pack_bytes_written, 0);
}

/// Requirement 3's core acceptance test: a table series whose rows are few
/// but *wide* must settle (no further repack) on a second maintenance run,
/// even though the 8 MiB byte safeguard forces more physical objects than
/// the naive `ceil(rows / 100_000)` row-count estimate alone would predict.
/// Without a real bounded-layout marker/comparison, the first run's actual
/// object count would forever exceed that estimate, so every subsequent
/// run would needlessly re-decode and re-encode the whole series again.
#[tokio::test]
async fn wide_row_table_series_settles_on_second_maintenance_run_despite_byte_safeguard() {
    use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    let temp_dir = tempdir().expect("tempdir");
    let pond_path = temp_dir.path().join("wide_row_pond");
    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .expect("create pond");

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));

    // Uncompressed, dictionary-disabled Parquet (this codebase's pinned
    // deterministic writer properties) means encoded size tracks raw data
    // size closely: 3 appends x 4 rows x ~900 KB of non-repeating payload
    // each is ~10.8 MiB total, well over the 8 MiB byte safeguard, while
    // comfortably under the 100_000-row cap -- so a naive
    // `ceil(rows / 100_000) == 1` estimate drastically undercounts the
    // achievable (safeguard-bounded) object count. A trailing, much
    // smaller fourth leaf lands in its own physical object after the
    // safeguard has already flushed the first three (proving there really
    // are two physical objects, not merely one big one that happens to
    // exceed the cap).
    const ROWS_PER_APPEND: usize = 4;
    const WIDE_PAYLOAD_BYTES: usize = 900_000;
    const NARROW_PAYLOAD_BYTES: usize = 16;
    for leaf in 0..4i64 {
        let payload_bytes = if leaf < 3 {
            WIDE_PAYLOAD_BYTES
        } else {
            NARROW_PAYLOAD_BYTES
        };
        let mut ts = Vec::with_capacity(ROWS_PER_APPEND);
        let mut values = Vec::with_capacity(ROWS_PER_APPEND);
        let mut payloads: Vec<String> = Vec::with_capacity(ROWS_PER_APPEND);
        for row in 0..ROWS_PER_APPEND as i64 {
            let n = leaf * ROWS_PER_APPEND as i64 + row;
            ts.push(1_000_000 + n);
            values.push(n);
            // Non-repeating bytes per row so Parquet's per-row-group
            // encoding cannot trivially collapse this to nothing even
            // without a compression codec.
            let payload: String = (0..payload_bytes)
                .map(|i| {
                    let byte = ((i as i64 * 31 + n * 7) % 26) as u8;
                    (b'a' + byte) as char
                })
                .collect();
            payloads.push(payload);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampMicrosecondArray::from(ts)),
                Arc::new(Int64Array::from(values)),
                Arc::new(StringArray::from(payloads)),
            ],
        )
        .expect("build wide-row batch");
        ship.write_transaction(&meta("wide-row-append"), async move |fs| {
            let root = fs.root().await?;
            if leaf == 0 {
                _ = root.create_dir_path("data").await?;
            }
            let _ = root
                .write_series_from_batch("/data/wide.table", &batch, Some("timestamp"))
                .await?;
            Ok(())
        })
        .await
        .expect("append wide-row table version");
    }

    let first = ship
        .collapse_versions(1)
        .await
        .expect("first pack maintenance run must succeed");
    assert_eq!(first.candidates, 1);
    assert_eq!(
        first.series_repacked, 1,
        "the wide-row series must be repacked"
    );
    assert!(
        first.pack_objects_written >= 2,
        "the byte safeguard must force more than one physical object for ~10.8 MiB of \
         uncompressed payload over an 8 MiB cap: {} object(s) written",
        first.pack_objects_written
    );

    let second = ship
        .collapse_versions(1)
        .await
        .expect("second pack maintenance run must succeed");
    assert_eq!(
        second.series_repacked, 0,
        "a wide-row table series already at its safeguard-bounded layout must settle -- not be \
         re-decoded/re-encoded merely because the naive row-count estimate undercounts its \
         actual (correct) object count"
    );
    assert_eq!(second.already_bounded, 1);
    assert_eq!(second.pack_objects_written, 0);
}
