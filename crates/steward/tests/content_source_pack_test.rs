// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Contract tests for [`ContentSource`]'s pack-discovery methods
//! (`docs/logical-series-identity-design.md` delivery gate 3), exercised
//! against both implementations: [`ContentRemote`] (a `file://` object
//! store) and [`LocalPondSource`] (a `pond://` producer clone on disk).
//!
//! Both must agree on the same `_packs/series=<hex>/pack=<hex>` key layout
//! and the same strict validation (content-address and series-binding
//! checks), so a selector built against the [`ContentSource`] trait works
//! unmodified regardless of which backend serves it.

use steward::{ContentSource, LocalPondSource, Ship};
use sync_store::ContentRemote;
use sync_store::content::{
    ObjectHash, PackIndex, PackLeafDescriptor, PayloadKind, SeriesManifest, generate_range_proof,
    merkle_root,
};
use tempfile::tempdir;
use tinyfs::arrow::parquet::ParquetExt;
use tinyfs::async_helpers::convenience::create_file_path;
use tlogfs::PondUserMetadata;
use uuid::Uuid;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

async fn write_file(ship: &mut Ship, path: &str, bytes: &[u8]) {
    let bytes = bytes.to_vec();
    ship.write_transaction(&meta("write"), async move |fs| {
        let root = fs.root().await?;
        let _ = create_file_path(&root, path, &bytes).await?;
        Ok(())
    })
    .await
    .expect("write transaction");
}

fn h(s: &str) -> ObjectHash {
    ObjectHash::of_bytes(s.as_bytes())
}

/// Build a `(series_hash, PackIndex)` covering the whole of a small series.
fn build_series_and_pack(leaf_labels: &[&str], blob_label: &str) -> (ObjectHash, PackIndex) {
    let leaves: Vec<ObjectHash> = leaf_labels.iter().map(|s| h(s)).collect();
    let root = merkle_root(&leaves);
    let manifest = SeriesManifest::new(
        PayloadKind::File,
        None,
        leaves.len() as u64 * 3,
        leaves.len() as u64,
        None,
        None,
        None,
        root,
    )
    .expect("valid manifest");
    let series_hash = manifest.hash();
    let proof =
        generate_range_proof(&leaves, 0, leaves.len()).expect("range proof over whole series");
    let blob_hash = ObjectHash::of_bytes(blob_label.as_bytes());
    let descriptors = (0..leaves.len())
        .map(|_| PackLeafDescriptor::new(3, None, None, None).expect("valid descriptor"))
        .collect();
    let pack = PackIndex::new(
        series_hash,
        0,
        leaves.len() as u64,
        leaves.len() as u64,
        root,
        proof,
        vec![blob_hash],
        leaves.len() as u64 * 3,
        blob_label.len() as u64,
        descriptors,
    )
    .expect("valid pack index");
    (series_hash, pack)
}

async fn open_producer(label: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");
    let mut ship = Ship::create_pond(&pond_path, label)
        .await
        .expect("create pond");
    write_file(&mut ship, "/hello.txt", b"hello from the producer").await;
    drop(ship);
    (tmp, pond_path)
}

// -- ContentRemote as a ContentSource ---------------------------------------

#[tokio::test]
async fn content_remote_trait_object_lists_and_fetches_published_packs() {
    let dir = tempdir().expect("tempdir");
    let remote = ContentRemote::create_at(dir.path(), Uuid::new_v4())
        .await
        .expect("create remote");

    let (series_hash, pack) = build_series_and_pack(&["a", "b", "c"], "remote-blob");
    let blob_hash = pack.physical_object_hashes()[0];
    let pack_hash = remote
        .publish_pack(series_hash, &pack, &[(blob_hash, b"remote-blob".to_vec())])
        .await
        .expect("publish pack");

    let source: &dyn ContentSource = &remote;
    let listed = source
        .list_pack_hashes(series_hash)
        .await
        .expect("list pack hashes");
    assert_eq!(listed, std::collections::HashSet::from([pack_hash]));

    let fetched = source
        .get_pack_index(series_hash, pack_hash)
        .await
        .expect("fetch pack index")
        .expect("published pack must be fetchable through the trait");
    assert_eq!(fetched, pack.encode());
}

#[tokio::test]
async fn content_remote_trait_object_has_no_packs_for_a_fresh_series() {
    let dir = tempdir().expect("tempdir");
    let remote = ContentRemote::create_at(dir.path(), Uuid::new_v4())
        .await
        .expect("create remote");
    let source: &dyn ContentSource = &remote;

    let series_hash = ObjectHash::of_bytes(b"a series with no published packs");
    let listed = source
        .list_pack_hashes(series_hash)
        .await
        .expect("an empty pack namespace is not an error");
    assert!(listed.is_empty());
    assert_eq!(
        source
            .get_pack_index(series_hash, ObjectHash::of_bytes(b"anything"))
            .await
            .expect("fetch pack index"),
        None
    );
}

// -- LocalPondSource ---------------------------------------------------------

/// A `pond://` source over an ordinary v1 pond -- one that never had any
/// pack advertisement written -- lists no packs for any series, rather than
/// erroring: the `_packs/` directory simply does not exist yet.
#[tokio::test]
async fn local_pond_source_v1_pond_has_no_pack_advertisements() {
    let (_tmp, pond_path) = open_producer("v1-pond").await;
    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source");

    let series_hash = ObjectHash::of_bytes(b"a v1 series");
    let listed = source
        .list_pack_hashes(series_hash)
        .await
        .expect("no _packs directory is not an error");
    assert!(listed.is_empty());
    assert_eq!(
        source
            .get_pack_index(series_hash, ObjectHash::of_bytes(b"anything"))
            .await
            .expect("fetch pack index"),
        None
    );
}

/// A pack advertisement placed directly under the producer clone's
/// `_packs/series=<hex>/pack=<hex>` -- the same relative layout
/// [`ContentRemote`] uses -- is discovered and fetched byte-for-byte,
/// proving `LocalPondSource` reads a real persistent location rather than a
/// stub.
#[tokio::test]
async fn local_pond_source_discovers_a_manually_placed_pack_advertisement() {
    let (_tmp, pond_path) = open_producer("pack-pond").await;

    let (series_hash, pack) = build_series_and_pack(&["a", "b"], "local-blob");
    let pack_bytes = pack.encode();
    let pack_hash = ObjectHash::of_bytes(&pack_bytes);

    let series_dir = steward::get_data_path(&pond_path)
        .join("_packs")
        .join(format!("series={}", series_hash.to_hex()));
    std::fs::create_dir_all(&series_dir).expect("create series dir");
    std::fs::write(
        series_dir.join(format!("pack={}", pack_hash.to_hex())),
        &pack_bytes,
    )
    .expect("write pack advertisement");

    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source");
    let listed = source
        .list_pack_hashes(series_hash)
        .await
        .expect("list pack hashes");
    assert_eq!(listed, std::collections::HashSet::from([pack_hash]));

    let fetched = source
        .get_pack_index(series_hash, pack_hash)
        .await
        .expect("fetch pack index")
        .expect("manually placed advertisement must be discoverable");
    assert_eq!(fetched, pack_bytes);
}

/// A stray, non-`pack=<hex>` file under a series' `_packs` directory is
/// rejected by listing, mirroring [`ContentRemote`]'s strictness.
#[tokio::test]
async fn local_pond_source_rejects_malformed_pack_key() {
    let (_tmp, pond_path) = open_producer("malformed-pond").await;

    let series_hash = ObjectHash::of_bytes(b"malformed series");
    let series_dir = steward::get_data_path(&pond_path)
        .join("_packs")
        .join(format!("series={}", series_hash.to_hex()));
    std::fs::create_dir_all(&series_dir).expect("create series dir");
    std::fs::write(series_dir.join("not-a-pack-key"), b"junk").expect("write stray file");

    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source");
    let err = source
        .list_pack_hashes(series_hash)
        .await
        .expect_err("a malformed sibling key must be rejected");
    assert!(
        format!("{err}").contains("malformed"),
        "unexpected error: {err}"
    );
}

/// A pack advertisement whose decoded `series_hash` disagrees with the
/// series directory it was found under is rejected as a cross-series index.
#[tokio::test]
async fn local_pond_source_rejects_cross_series_index() {
    let (_tmp, pond_path) = open_producer("cross-series-pond").await;

    let (_series_a, _pack_a) = build_series_and_pack(&["a", "b"], "series-a-blob");
    let (series_b, pack_b) = build_series_and_pack(&["x", "y", "z"], "series-b-blob");
    let pack_b_bytes = pack_b.encode();
    let pack_b_hash = ObjectHash::of_bytes(&pack_b_bytes);

    // Publish series B's pack index under series A's own hash (a different
    // directory than what it declares internally).
    let series_a_hash = ObjectHash::of_bytes(b"a completely unrelated series identity");
    assert_ne!(series_a_hash, series_b);
    let series_dir = steward::get_data_path(&pond_path)
        .join("_packs")
        .join(format!("series={}", series_a_hash.to_hex()));
    std::fs::create_dir_all(&series_dir).expect("create series dir");
    std::fs::write(
        series_dir.join(format!("pack={}", pack_b_hash.to_hex())),
        &pack_b_bytes,
    )
    .expect("write pack advertisement");

    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source");
    let err = source
        .get_pack_index(series_a_hash, pack_b_hash)
        .await
        .expect_err("a pack index naming a foreign series must be rejected");
    assert!(
        format!("{err}").contains("cross-series"),
        "unexpected error: {err}"
    );
}

// -- BLOCKER 2: a real, never-pushed native v2 series ------------------------

/// End-to-end `pond://` fetch of a genuinely written, never-pushed native
/// v2 `FilePhysicalSeries`: `LocalPondSource` must discover and construct
/// its initial full-range pack on demand from persisted rows -- no
/// `pond push` and no manually placed `_packs/` advertisement -- and the
/// pack it serves must self-check against the series manifest it also
/// serves, and its physical objects (inline, since these chunks are small)
/// must be fetchable through the same source.
#[tokio::test]
async fn local_pond_source_serves_a_real_unpushed_native_series() {
    use steward::Ship;
    use sync_store::content::{
        Commit, SeriesManifest, decode_manifest, verify_pack_against_manifest,
    };
    use tinyfs::EntryType;
    use tokio::io::AsyncWriteExt;

    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");
    let mut ship = Ship::create_pond(&pond_path, "unpushed-series-pond")
        .await
        .expect("create pond");

    // Three small (inline) appends to one FilePhysicalSeries, via the
    // ordinary write path -- never pushed, never manually advertised.
    let chunks: Vec<Vec<u8>> = vec![
        b"chunk-zero".to_vec(),
        b"chunk-one-longer".to_vec(),
        b"chunk-two".to_vec(),
    ];
    for chunk in &chunks {
        let bytes = chunk.clone();
        ship.write_transaction(&meta("append"), async move |fs| {
            let root = fs.root().await?;
            let mut writer = root
                .async_writer_path_with_type("/native.series", EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(&bytes).await?;
            writer.shutdown().await?;
            Ok(())
        })
        .await
        .expect("append series version");
    }
    drop(ship);

    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source over the unpushed clone");

    // Walk tip -> commit -> node manifest -> the series' own manifest hash,
    // entirely through the public ContentSource trait (no test-only back
    // door into LocalPondSource's private series_material).
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
        .find(|e| e.name == "native.series" && e.entry_type == EntryType::FilePhysicalSeries)
        .expect("the written series has a manifest entry");
    let series_hash = series_entry.child_hash;

    let series_manifest_bytes = source
        .get_object(series_hash)
        .await
        .expect("get series manifest object")
        .expect("series manifest object is served");
    let series_manifest =
        SeriesManifest::decode(&series_manifest_bytes).expect("decode series manifest");
    assert_eq!(
        series_manifest.leaf_count(),
        chunks.len() as u64,
        "one logical leaf per append"
    );

    // No push, no manual advertisement: the pack must still be discoverable
    // and fetchable, synthesized on demand from persisted rows.
    let listed = source
        .list_pack_hashes(series_hash)
        .await
        .expect("list pack hashes for the unpushed series");
    assert_eq!(
        listed.len(),
        1,
        "an unpushed series still advertises exactly its synthesized initial pack"
    );
    let pack_hash = *listed.iter().next().expect("one pack hash");

    let pack_bytes = source
        .get_pack_index(series_hash, pack_hash)
        .await
        .expect("fetch synthesized pack index")
        .expect("the synthesized pack must be fetchable by its own hash");
    let pack = PackIndex::decode(&pack_bytes).expect("decode synthesized pack index");

    assert_eq!(
        pack.leaf_start(),
        0,
        "a fresh series' initial pack covers the whole range"
    );
    assert_eq!(pack.leaf_end(), chunks.len() as u64);
    assert_eq!(pack.total_leaf_count(), series_manifest.leaf_count());

    // Self-check: the pack's range proof must actually fold to the series
    // manifest's leaf_merkle_root over the SAME leaf hashes this test can
    // independently recompute from the plaintext it wrote (raw byte
    // FilePhysicalSeries appends carry no temporal bounds/attributes).
    let range_leaf_hashes: Vec<ObjectHash> = chunks
        .iter()
        .map(|c| {
            sync_store::content::file_leaf_hash(c, None, None, None)
                .expect("recompute file leaf hash")
        })
        .collect();
    verify_pack_against_manifest(series_hash, &series_manifest, &pack, &range_leaf_hashes)
        .expect("pack must self-check against the manifest it was constructed from");

    // Every physical object the pack references must actually be fetchable
    // through this same LocalPondSource -- these chunks are all well under
    // the large-file threshold, so they are inline objects, not external
    // blobs.
    for hash in pack.physical_object_hashes() {
        let is_external = source.has_blob(*hash).await.expect("has_blob");
        if is_external {
            let reader = source
                .get_blob_reader(*hash)
                .await
                .expect("get_blob_reader")
                .expect("external physical object referenced by the pack must be readable");
            drop(reader);
        } else {
            let bytes = source
                .get_object(*hash)
                .await
                .expect("get_object")
                .expect("inline physical object referenced by the pack must be fetchable");
            assert!(
                chunks.iter().any(|c| c == &bytes),
                "fetched inline physical object must be one of the series' own appended chunks"
            );
        }
    }

    // Idempotent/deterministic: synthesizing again (a second list+fetch)
    // must reproduce byte-identical pack bytes.
    let listed_again = source
        .list_pack_hashes(series_hash)
        .await
        .expect("list pack hashes again");
    assert_eq!(listed_again, listed);
    let pack_bytes_again = source
        .get_pack_index(series_hash, pack_hash)
        .await
        .expect("fetch synthesized pack index again")
        .expect("still fetchable");
    assert_eq!(pack_bytes_again, pack_bytes);
}

// -- Table series and external/large series coverage (item 6) --------------

/// A single-row parquet batch with a `timestamp` (microseconds) column and a
/// string `label`, used to append table series versions.
fn table_batch(ts_micros: i64, label: &str) -> arrow_array::RecordBatch {
    use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("label", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(TimestampMicrosecondArray::from(vec![ts_micros])),
            std::sync::Arc::new(StringArray::from(vec![label])),
        ],
    )
    .expect("table batch")
}

/// Decode every row of `schema_bytes` (one physical Parquet object's raw
/// bytes) into a single concatenated `RecordBatch`, mirroring how the real
/// v2 materializer decodes a table pack's physical objects
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

/// A `pond://` (`LocalPondSource`) `TablePhysicalSeries` case: appends three
/// table-series versions via the real native writer
/// (`write_series_from_batch`), then walks the same public `ContentSource`
/// path the file-series test above uses to reach the series' synthesized
/// initial pack, and additionally verifies `PackIndex::physical_byte_count`
/// against the real fetched object bytes and decodes the fetched Parquet
/// payload to prove every row survives readback.
#[tokio::test]
async fn local_pond_source_serves_a_real_unpushed_native_table_series() {
    use steward::Ship;
    use sync_store::content::{Commit, SeriesManifest, decode_manifest};
    use tinyfs::EntryType;

    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");
    let mut ship = Ship::create_pond(&pond_path, "unpushed-table-series-pond")
        .await
        .expect("create pond");

    let versions = [
        (1_000_000i64, "a"),
        (2_000_000i64, "b"),
        (3_000_000i64, "c"),
    ];
    ship.write_transaction(&meta("table-series"), async move |fs| {
        let root = fs.root().await?;
        for (ts, label) in versions {
            let batch = table_batch(ts, label);
            let _ = root
                .write_series_from_batch("/native.table", &batch, Some("timestamp"))
                .await?;
        }
        Ok(())
    })
    .await
    .expect("table series transaction");
    drop(ship);

    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source over the unpushed table series");

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
        .find(|e| e.name == "native.table" && e.entry_type == EntryType::TablePhysicalSeries)
        .expect("the written table series has a manifest entry");
    let series_hash = series_entry.child_hash;

    let series_manifest_bytes = source
        .get_object(series_hash)
        .await
        .expect("get series manifest object")
        .expect("series manifest object is served");
    let series_manifest =
        SeriesManifest::decode(&series_manifest_bytes).expect("decode series manifest");
    assert_eq!(
        series_manifest.leaf_count(),
        versions.len() as u64,
        "one logical leaf per table-series append"
    );

    let listed = source
        .list_pack_hashes(series_hash)
        .await
        .expect("list pack hashes for the unpushed table series");
    assert_eq!(listed.len(), 1, "exactly one synthesized initial pack");
    let pack_hash = *listed.iter().next().expect("one pack hash");
    let pack_bytes = source
        .get_pack_index(series_hash, pack_hash)
        .await
        .expect("fetch synthesized pack index")
        .expect("the synthesized table pack must be fetchable");
    let pack = PackIndex::decode(&pack_bytes).expect("decode synthesized table pack index");

    assert_eq!(pack.leaf_end(), versions.len() as u64);
    assert_eq!(pack.total_leaf_count(), series_manifest.leaf_count());

    // Fetch every physical object the pack names, decode each to rows, and
    // independently sum real fetched byte lengths to cross-check
    // `physical_byte_count` -- proving that declared aggregate against
    // fetched reality, not merely against itself.
    let mut fetched_bytes_total: u64 = 0;
    let mut all_rows_labels: Vec<String> = Vec::new();
    for hash in pack.physical_object_hashes() {
        let is_external = source.has_blob(*hash).await.expect("has_blob");
        assert!(
            !is_external,
            "these small single-row parquet objects must stay inline, not external"
        );
        let bytes = source
            .get_object(*hash)
            .await
            .expect("get_object")
            .expect("inline physical table object referenced by the pack must be fetchable");
        fetched_bytes_total += bytes.len() as u64;
        let batch = decode_parquet_rows(&bytes);
        let labels = batch
            .column_by_name("label")
            .expect("label column")
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("label is a string array");
        use arrow_array::Array;
        for i in 0..labels.len() {
            all_rows_labels.push(labels.value(i).to_string());
        }
    }
    assert_eq!(
        fetched_bytes_total,
        pack.physical_byte_count(),
        "PackIndex::physical_byte_count must equal the real fetched physical object bytes"
    );
    all_rows_labels.sort();
    assert_eq!(
        all_rows_labels,
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
        "every logical row survives readback through the pack's physical objects"
    );
}

/// A `pond://` (`LocalPondSource`) external/large `FilePhysicalSeries` case:
/// one append whose content exceeds `tlogfs::large_files::LARGE_FILE_THRESHOLD`
/// so it is stored as an external blob, not inlined. Verifies the
/// synthesized pack's sole physical object is discoverable via the
/// external-blob path (`has_blob`/`get_blob_reader`, not `get_object`),
/// that `physical_byte_count` matches the real streamed byte count, and
/// that the full external payload reads back byte-for-byte.
#[tokio::test]
async fn local_pond_source_serves_a_real_unpushed_external_large_series() {
    use steward::Ship;
    use sync_store::content::{Commit, SeriesManifest, decode_manifest};
    use tinyfs::EntryType;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");
    let mut ship = Ship::create_pond(&pond_path, "unpushed-external-series-pond")
        .await
        .expect("create pond");

    // One append comfortably over the large-file threshold, with
    // non-repeating content so a truncated/garbled readback is detectable.
    let large_bytes: Vec<u8> = (0..(tlogfs::large_files::LARGE_FILE_THRESHOLD + 4096))
        .map(|i| (i % 251) as u8)
        .collect();
    {
        let bytes = large_bytes.clone();
        ship.write_transaction(&meta("external-series"), async move |fs| {
            let root = fs.root().await?;
            let mut writer = root
                .async_writer_path_with_type("/native.external", EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(&bytes).await?;
            writer.shutdown().await?;
            Ok(())
        })
        .await
        .expect("external series transaction");
    }
    drop(ship);

    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source over the unpushed external series");

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
        .find(|e| e.name == "native.external" && e.entry_type == EntryType::FilePhysicalSeries)
        .expect("the written external series has a manifest entry");
    let series_hash = series_entry.child_hash;

    let series_manifest_bytes = source
        .get_object(series_hash)
        .await
        .expect("get series manifest object")
        .expect("series manifest object is served");
    let series_manifest =
        SeriesManifest::decode(&series_manifest_bytes).expect("decode series manifest");
    assert_eq!(
        series_manifest.leaf_count(),
        1,
        "one logical leaf, one append"
    );

    let listed = source
        .list_pack_hashes(series_hash)
        .await
        .expect("list pack hashes for the unpushed external series");
    assert_eq!(listed.len(), 1, "exactly one synthesized initial pack");
    let pack_hash = *listed.iter().next().expect("one pack hash");
    let pack_bytes = source
        .get_pack_index(series_hash, pack_hash)
        .await
        .expect("fetch synthesized pack index")
        .expect("the synthesized external-series pack must be fetchable");
    let pack = PackIndex::decode(&pack_bytes).expect("decode synthesized pack index");
    assert_eq!(pack.physical_object_hashes().len(), 1);
    let hash = pack.physical_object_hashes()[0];

    let is_external = source.has_blob(hash).await.expect("has_blob");
    assert!(
        is_external,
        "content over LARGE_FILE_THRESHOLD must be stored as an external blob, not inlined"
    );
    let mut reader = source
        .get_blob_reader(hash)
        .await
        .expect("get_blob_reader")
        .expect("the external physical object referenced by the pack must be readable");
    let mut read_back = Vec::new();
    let _ = reader
        .read_to_end(&mut read_back)
        .await
        .expect("read external blob to completion");

    assert_eq!(
        read_back.len() as u64,
        pack.physical_byte_count(),
        "PackIndex::physical_byte_count must equal the real streamed external blob length"
    );
    assert_eq!(
        read_back, large_bytes,
        "the full external payload must read back byte-for-byte, not truncated or garbled"
    );
}

/// `LocalPondSource` must be able to fetch a *maintenance-published* pack's
/// physical objects, not only the ordinary v1 external-blob closure it
/// captures at `open()` time, and not only the on-demand synthesized
/// initial pack the two tests above exercise.
///
/// `steward::pack_maintenance::run_pack_maintenance` (reached through
/// `Ship::collapse_versions`) repacks several externalized per-append blobs
/// into a smaller, bounded set of *new*, differently-content-addressed
/// physical objects under this pond's own local `data/_packs/objects/`
/// sidecar -- distinct from `_large_files/` and from the objects
/// `LocalPondSource::open`'s ordinary tree walk resolves. Before this
/// session's wiring, `has_blob`/`list_blobs`/`get_blob_reader` only ever
/// consulted that ordinary closure, so a repacked series' new physical
/// objects were unfetchable through this source even though a real pack
/// index naming them was published. This test forces that repack (multiple
/// externalized appends over the large-file threshold, well within one
/// pack's bounded byte cap so they land in a single new physical object),
/// then fetches the *published* pack (not the synthesized one) through the
/// public `ContentSource` surface and confirms every physical object it
/// names is fetchable and reconstructs the exact original series bytes.
#[tokio::test]
async fn local_pond_source_fetches_a_maintenance_published_pack_object() {
    use steward::Ship;
    use sync_store::content::{Commit, decode_manifest};
    use tinyfs::EntryType;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let tmp = tempdir().expect("tempdir");
    let pond_path = tmp.path().join("pond");
    let mut ship = Ship::create_pond(&pond_path, "maintenance-pack-fetch-pond")
        .await
        .expect("create pond");

    // Five appends, each safely over the large-file threshold with
    // non-repeating content, so each becomes its own externalized blob
    // (five physical objects) while their combined bytes stay comfortably
    // within one bounded pack object's byte cap -- forcing a real repack
    // down to fewer physical objects without crossing a second one.
    let threshold = tlogfs::large_files::LARGE_FILE_THRESHOLD;
    let chunks: Vec<Vec<u8>> = (0u8..5)
        .map(|seed| {
            (0..(threshold + 4096))
                .map(|i| ((i as u32 + u32::from(seed) * 97) % 251) as u8)
                .collect()
        })
        .collect();
    for chunk in &chunks {
        let bytes = chunk.clone();
        ship.write_transaction(&meta("maintenance-append"), async move |fs| {
            let root = fs.root().await?;
            let mut writer = root
                .async_writer_path_with_type("/native.maintained", EntryType::FilePhysicalSeries)
                .await?;
            writer.write_all(&bytes).await?;
            writer.shutdown().await?;
            Ok(())
        })
        .await
        .expect("append maintained series version");
    }
    let full_content: Vec<u8> = chunks.concat();

    let report = ship
        .collapse_versions(1)
        .await
        .expect("pack-only maintenance must succeed");
    assert!(
        report.series_repacked >= 1,
        "the five-append series must be a genuine repack candidate: {report}"
    );
    assert!(
        report.pack_objects_written >= 1,
        "maintenance must have durably written at least one new physical pack object: {report}"
    );
    drop(ship);

    let source = LocalPondSource::open(&pond_path)
        .await
        .expect("open local pond source after maintenance");

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
        .find(|e| e.name == "native.maintained" && e.entry_type == EntryType::FilePhysicalSeries)
        .expect("the maintained series has a manifest entry");
    let series_hash = series_entry.child_hash;

    // The *published* pack advertisement directory on disk -- maintenance's
    // real repack output -- read directly, so this test cannot accidentally
    // pass merely by exercising `list_pack_hashes`'s on-demand synthesized
    // pack fallback instead of the maintenance-published one.
    let series_dir = steward::get_data_path(&pond_path)
        .join(sync_store::pack_keys::PACKS_ROOT)
        .join(sync_store::pack_keys::series_dir_name(series_hash));
    let published: Vec<ObjectHash> = std::fs::read_dir(&series_dir)
        .expect("read published pack advertisement directory")
        .filter_map(|e| e.ok())
        .filter_map(|e| sync_store::pack_keys::parse_pack_file_name(e.file_name().to_str()?).ok())
        .collect();
    assert_eq!(
        published.len(),
        1,
        "maintenance must have published exactly one pack advertisement for this series"
    );
    let pack_hash = published[0];

    // The BLOCKING selection-bug regression check: once a real, on-disk
    // advertisement set already forms a full exact cover of this series'
    // leaf range, `list_pack_hashes` (the exact-cover candidate set every
    // `pond://` fetch path selects from) must resolve to *only* that
    // maintained hash -- the synthesized initial pack must be a
    // fallback-only candidate, never added alongside a real full cover.
    let listed_hashes = source
        .list_pack_hashes(series_hash)
        .await
        .expect("list pack hashes after maintenance");
    assert_eq!(
        listed_hashes,
        std::collections::HashSet::from([pack_hash]),
        "once real on-disk advertisements already form a full exact cover, list_pack_hashes \
         must report only the maintained/published pack hash {pack_hash} -- never also \
         synthesizing and offering the initial pack as a spurious extra candidate"
    );

    let pack_bytes = source
        .get_pack_index(series_hash, pack_hash)
        .await
        .expect("fetch published pack index")
        .expect("the maintenance-published pack must be fetchable");
    let pack = PackIndex::decode(&pack_bytes).expect("decode published pack index");
    assert!(
        pack.physical_object_hashes().len() < chunks.len(),
        "the published pack must be more bounded than one physical object per append: {} \
         objects for {} appends",
        pack.physical_object_hashes().len(),
        chunks.len()
    );

    // Every listed blob hash the source reports must include this pack's
    // physical objects (`list_blobs`'s union with `_packs/objects/`), and
    // each must be independently fetchable via `has_blob`/`get_blob_reader`
    // -- the exact surface `fetch_blob` relies on for a `pond://` or
    // clean-reader reconstruction.
    let listed_blobs = source.list_blobs().await.expect("list_blobs");
    let mut reconstructed = Vec::new();
    for &hash in pack.physical_object_hashes() {
        assert!(
            listed_blobs.contains(&hash),
            "list_blobs must report every maintenance-published physical object"
        );
        assert!(
            source.has_blob(hash).await.expect("has_blob"),
            "has_blob must recognize a maintenance-published physical object"
        );
        let mut reader = source
            .get_blob_reader(hash)
            .await
            .expect("get_blob_reader")
            .expect("a maintenance-published physical object must be readable");
        let mut bytes = Vec::new();
        let _ = reader
            .read_to_end(&mut bytes)
            .await
            .expect("read maintenance-published physical object to completion");
        reconstructed.extend(bytes);
    }

    assert_eq!(
        reconstructed, full_content,
        "concatenating every physical object a maintenance-published pack names, fetched \
         entirely through LocalPondSource, must reconstruct the original series content \
         byte-for-byte"
    );
}
