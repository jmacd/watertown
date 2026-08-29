// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the v2 (`watertown.series.v1`) dual reader
//! (`docs/logical-series-identity-design.md` delivery gate 4):
//! `steward::fetch_object_graph`'s pack discovery, physical-object
//! fetch/verification, and its explicit refusal to materialize a verified
//! v2 series.
//!
//! The native v2 writer landed in delivery gate 7 (see
//! `crates/tlogfs/src/series_identity.rs` and the `watertown.series.v1` fold in
//! `crates/steward/src/content_tree.rs`), but every fixture here is still
//! constructed by hand at the wire-object level -- directly encoding tree,
//! commit, manifest, series-manifest, and pack-index bytes and pushing them
//! into a [`ContentRemote`] -- rather than produced by any real Watertown
//! write path, so that the dual reader's dispatch and verification logic can
//! be exercised in isolation from the writer. Fixtures now build a
//! `watertown.commit.v1` commit (the compatibility fence gate 7 introduced) whose
//! tree carries a `watertown.series.v1` child hash, matching what a real v2 writer
//! publishes; old `dp.commit.3` readers reject these roots outright by
//! construction (unrecognized magic), which is the fence's intended effect.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::tempdir;
use uuid::Uuid;

use steward::{
    FetchedGraph, FetchedObject, PondUserMetadata, Ship, fetch_object_graph, rebuild_pond,
};
use sync_store::ContentRemote;
use sync_store::content::{
    Commit, ContentModelVersion, ManifestEntry, ObjectHash, PackIndex, PackLeafDescriptor,
    PayloadKind, Provenance, SeriesManifest, TreeEntry, VersionMeta, encode_canonical_attributes,
    encode_manifest, encode_series, encode_tree, file_leaf_hash, generate_range_proof,
    manifest_hash as sync_manifest_hash, merkle_root, node_merkle_rebuild_root, schema_fingerprint,
    table_leaf_hash, tree_hash,
};
use tinyfs::EntryType;

fn meta(label: &str) -> PondUserMetadata {
    PondUserMetadata::new(vec!["test".into(), label.into()])
}

fn pid() -> Uuid {
    Uuid::from_u128(0xf11e_0000_0000_0000_0000_0000_0000_0000)
}

/// A real, deterministic, parseable node id for a manifest entry named
/// `name`, distinct from [`tinyfs::ROOT_UUID`] and stable across calls with
/// the same name (so re-fetching the same fixture reproduces the same ids).
/// Deterministic, parseable node id for a synthetic fixture entry. Real node
/// ids are UUID7-shaped with the entry type folded into byte 6 (see
/// `tinyfs::NodeID::generate`); `rebuild_pond`'s destination-side logic reads
/// that nibble back out (`NodeID::entry_type`), so a fixture id that doesn't
/// carry it panics deep in materialization ("Unknown EntryType") rather than
/// merely mismatching content. Reproduce the same encoding here so hand-built
/// fixtures decode like a real pond's ids would.
/// A fixed mtime (microseconds since epoch) for fixtures that must survive a
/// real `rebuild_pond` commit: the destination write adopts a series' *last*
/// leaf's `replicated_mtime` verbatim (see `content_pull.rs::replicated_mtime`),
/// so a fixture's `VersionMeta.timestamp` must be some concrete, deterministic
/// value for its precommit tree-hash re-fold to match what was advertised.
const FIXTURE_MTIME: i64 = 1_700_000_000_000_000;

fn node_id_for(name: &str, entry_type: EntryType) -> String {
    let digest = blake3::hash(name.as_bytes());
    let mut bytes: [u8; 16] = digest.as_bytes()[..16].try_into().expect("16 bytes");
    bytes[6] = 0x70 | (entry_type as u8);
    Uuid::from_bytes(bytes).to_string()
}

/// Push a fresh root commit whose directory holds exactly `entries`
/// (`(name, entry_type, child_hash)`), each with a real (parseable)
/// deterministic node id derived from its name in the node manifest,
/// together with every object in `objects`. The synthetic root entry uses
/// the same reserved [`tinyfs::ROOT_UUID`] a real pond's own root directory
/// is created with, so a destination's already-materialized root is
/// recognized as the same node rather than planned as a stray deletion.
///
/// Entries carry no node metadata (`ManifestEntry`/`TreeEntry::bare`), which
/// is sufficient for fetch-only (gate-4) tests below, but is a mismatch a
/// `rebuild_pond` commit's own precommit tree-hash re-fold will catch:
/// `rebuild_pond` always writes a concrete version (with a real mtime) for
/// anything it materializes. Tests that actually run `rebuild_pond` to a
/// successful commit must use [`push_root_versioned`] instead, supplying the
/// exact `VersionMeta` the destination write will reproduce.
async fn push_root(
    remote: &mut ContentRemote,
    entries: &[(&str, EntryType, ObjectHash)],
    objects: Vec<(ObjectHash, Vec<u8>)>,
) -> ObjectHash {
    let entries: Vec<(&str, EntryType, ObjectHash, Vec<VersionMeta>)> = entries
        .iter()
        .map(|(name, et, hash)| (*name, *et, *hash, Vec::new()))
        .collect();
    push_root_versioned(remote, &entries, objects).await
}

/// As [`push_root`], but each entry also carries the exact `VersionMeta`
/// list its `rebuild_pond`-materialized node will end up with, so the
/// fixture's advertised tree/manifest hashes are ones a successful
/// materialization can actually reproduce and match at precommit. For a
/// series node this is the *aggregate* single-element list `rebuild_pond`
/// itself works with (`replicated_mtime` reads only `versions.last()`), not
/// one entry per logical leaf.
async fn push_root_versioned(
    remote: &mut ContentRemote,
    entries: &[(&str, EntryType, ObjectHash, Vec<VersionMeta>)],
    mut objects: Vec<(ObjectHash, Vec<u8>)>,
) -> ObjectHash {
    let tree_entries: Vec<TreeEntry> = entries
        .iter()
        .map(|(name, et, hash, versions)| TreeEntry::new(*name, *et, *hash, versions.clone()))
        .collect();
    let tree_bytes = encode_tree(&tree_entries).expect("encode tree");
    let root_hash = tree_hash(&tree_entries).expect("tree hash");

    let mut manifest_entries = vec![ManifestEntry::bare(
        tinyfs::ROOT_UUID,
        "",
        "",
        EntryType::DirectoryPhysical,
        root_hash,
    )];
    for (name, et, hash, versions) in entries {
        manifest_entries.push(ManifestEntry::new(
            node_id_for(name, *et),
            tinyfs::ROOT_UUID,
            *name,
            *et,
            *hash,
            versions.clone(),
        ));
    }
    let manifest_bytes = encode_manifest(&manifest_entries).expect("encode manifest");
    let manifest_hash_val = sync_manifest_hash(&manifest_entries).expect("manifest hash");
    let manifest_root = node_merkle_rebuild_root(&manifest_entries).expect("manifest merkle root");

    let commit = Commit::new(
        ContentModelVersion::LogicalSeriesV2,
        root_hash,
        None,
        manifest_hash_val,
        manifest_root,
        Provenance {
            pond_id: "test-pond".to_string(),
            seq: 1,
            time_micros: 0,
            author: "test".to_string(),
            request: "synthetic gate-4 fixture".to_string(),
        },
    );
    let commit_bytes = commit.encode();
    let commit_hash = commit.hash();

    objects.push((root_hash, tree_bytes));
    objects.push((manifest_hash_val, manifest_bytes));
    objects.push((commit_hash, commit_bytes));
    let _ = remote
        .push_commit(&objects, "main", commit_hash)
        .await
        .expect("push commit");
    commit_hash
}

/// One real file-series fixture: `leaf_lens` partitions `bytes` into
/// logical leaves, `object_lens` partitions the same `bytes` into physical
/// objects (independently of leaf boundaries, per the design doc), and every
/// leaf hash is the *real* [`file_leaf_hash`] of its own byte range.
struct FilePackFixture {
    manifest: SeriesManifest,
    series_hash: ObjectHash,
    pack: PackIndex,
    pack_hash: ObjectHash,
    physical_objects: Vec<(ObjectHash, Vec<u8>)>,
    leaf_hashes: Vec<ObjectHash>,
}

fn build_file_pack(bytes: &[u8], leaf_lens: &[usize], object_lens: &[usize]) -> FilePackFixture {
    assert_eq!(
        leaf_lens.iter().sum::<usize>(),
        bytes.len(),
        "leaf_lens must partition bytes exactly"
    );
    assert_eq!(
        object_lens.iter().sum::<usize>(),
        bytes.len(),
        "object_lens must partition bytes exactly"
    );

    let mut leaf_hashes = Vec::with_capacity(leaf_lens.len());
    let mut descriptors = Vec::with_capacity(leaf_lens.len());
    let mut offset = 0usize;
    for &len in leaf_lens {
        let slice = &bytes[offset..offset + len];
        leaf_hashes.push(file_leaf_hash(slice, None, None, None).expect("real leaf hash"));
        descriptors
            .push(PackLeafDescriptor::new(len as u64, None, None, None).expect("descriptor"));
        offset += len;
    }
    let root = merkle_root(&leaf_hashes);
    let manifest = SeriesManifest::new_v2(
        PayloadKind::File,
        bytes.len() as u64,
        leaf_lens.len() as u64,
        None,
        None,
        None,
        root,
    )
    .expect("valid manifest");
    let series_hash = manifest.hash();

    let mut physical_objects = Vec::with_capacity(object_lens.len());
    offset = 0;
    for &len in object_lens {
        let slice = bytes[offset..offset + len].to_vec();
        let object_hash = ObjectHash::of_bytes(&slice);
        physical_objects.push((object_hash, slice));
        offset += len;
    }
    let proof =
        generate_range_proof(&leaf_hashes, 0, leaf_hashes.len()).expect("whole-range proof");
    let pack = PackIndex::new_v2(
        series_hash,
        0,
        leaf_lens.len() as u64,
        leaf_lens.len() as u64,
        root,
        proof,
        physical_objects.iter().map(|(h, _)| *h).collect(),
        bytes.len() as u64,
        bytes.len() as u64,
        descriptors,
    )
    .expect("valid pack index");
    let pack_hash = pack.hash();

    FilePackFixture {
        manifest,
        series_hash,
        pack,
        pack_hash,
        physical_objects,
        leaf_hashes,
    }
}

async fn publish(remote: &mut ContentRemote, fixture: &FilePackFixture) {
    let published = remote
        .publish_pack(
            fixture.series_hash,
            &fixture.pack,
            &fixture.physical_objects,
        )
        .await
        .expect("publish pack");
    assert_eq!(published, fixture.pack_hash);
    seed_series_manifest(remote, &fixture.manifest).await;
}

/// Seed a `watertown.series.v1` manifest object into the inline `objects` partition
/// so it is reachable via ordinary [`ContentRemote::get_object`], exactly as
/// a real tree-referenced series manifest would be. Packs are deliberately
/// *not* reachable this way (design doc: "Physical pack index"), but the
/// manifest itself always is -- this is the fixture-construction equivalent
/// of what a real (not-yet-existing) v2 writer's commit would do. The `ref`
/// write here is a harmless placeholder: [`push_root`]'s own later
/// `push_commit` call overwrites the same `"main"` ref with the real tip,
/// while every object already written (puts are idempotent, keyed by hash)
/// remains.
async fn seed_series_manifest(remote: &mut ContentRemote, manifest: &SeriesManifest) {
    let hash = manifest.hash();
    let _ = remote
        .push_commit(&[(hash, manifest.encode())], "main", hash)
        .await
        .expect("seed series manifest object");
}

fn i64_string_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]))
}

fn write_parquet(schema: &Arc<Schema>, batch: &RecordBatch) -> Vec<u8> {
    let props = WriterProperties::builder()
        .set_max_row_group_size(2)
        .build();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).expect("writer");
    writer.write(batch).expect("write batch");
    let _ = writer.close().expect("close writer");
    buf
}

fn batch(schema: &Arc<Schema>, ids: &[i64], labels: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(labels.to_vec())),
        ],
    )
    .expect("record batch")
}

/// One real table-series (Parquet) fixture: `leaf_row_counts` partitions the
/// full `rows` (id, label pairs) into logical leaves, `object_row_counts`
/// partitions the same rows into independent Parquet physical objects, and
/// every leaf hash is the real [`table_leaf_hash`] of its own row range.
struct TablePackFixture {
    manifest: SeriesManifest,
    series_hash: ObjectHash,
    pack: PackIndex,
    physical_objects: Vec<(ObjectHash, Vec<u8>)>,
    leaf_hashes: Vec<ObjectHash>,
}

fn build_table_pack(
    schema: &Arc<Schema>,
    rows: &[(i64, &str)],
    leaf_row_counts: &[usize],
    object_row_counts: &[usize],
) -> TablePackFixture {
    assert_eq!(leaf_row_counts.iter().sum::<usize>(), rows.len());
    assert_eq!(object_row_counts.iter().sum::<usize>(), rows.len());

    let fingerprint = schema_fingerprint(schema).expect("schema fingerprint");

    let mut leaf_hashes = Vec::with_capacity(leaf_row_counts.len());
    let mut descriptors = Vec::with_capacity(leaf_row_counts.len());
    let mut offset = 0usize;
    for &count in leaf_row_counts {
        let slice = &rows[offset..offset + count];
        let ids: Vec<i64> = slice.iter().map(|(id, _)| *id).collect();
        let labels: Vec<&str> = slice.iter().map(|(_, l)| *l).collect();
        let b = batch(schema, &ids, &labels);
        leaf_hashes.push(table_leaf_hash(schema, &[b], None, None, None).expect("real leaf hash"));
        descriptors.push(
            PackLeafDescriptor::new_with_schema(count as u64, Some(fingerprint), None, None, None)
                .expect("descriptor"),
        );
        offset += count;
    }
    let root = merkle_root(&leaf_hashes);
    let manifest = SeriesManifest::new_v2(
        PayloadKind::Table,
        rows.len() as u64,
        leaf_row_counts.len() as u64,
        None,
        None,
        None,
        root,
    )
    .expect("valid manifest");
    let series_hash = manifest.hash();

    let mut physical_objects = Vec::with_capacity(object_row_counts.len());
    offset = 0;
    let mut total_bytes = 0u64;
    for &count in object_row_counts {
        let slice = &rows[offset..offset + count];
        let ids: Vec<i64> = slice.iter().map(|(id, _)| *id).collect();
        let labels: Vec<&str> = slice.iter().map(|(_, l)| *l).collect();
        let b = batch(schema, &ids, &labels);
        let bytes = write_parquet(schema, &b);
        total_bytes += bytes.len() as u64;
        let object_hash = ObjectHash::of_bytes(&bytes);
        physical_objects.push((object_hash, bytes));
        offset += count;
    }
    let proof =
        generate_range_proof(&leaf_hashes, 0, leaf_hashes.len()).expect("whole-range proof");
    let pack = PackIndex::new_v2(
        series_hash,
        0,
        leaf_row_counts.len() as u64,
        leaf_row_counts.len() as u64,
        root,
        proof,
        physical_objects.iter().map(|(h, _)| *h).collect(),
        rows.len() as u64,
        total_bytes,
        descriptors,
    )
    .expect("valid pack index");

    TablePackFixture {
        manifest,
        series_hash,
        pack,
        physical_objects,
        leaf_hashes,
    }
}

/// As [`build_table_pack`], but every leaf carries the same explicit
/// `(min_event_time, max_event_time)` range (and the manifest's own
/// aggregate matches it) instead of `None`/`None`. A `TablePhysicalSeries`
/// write choke point always requires *some* temporal metadata before
/// shutdown (real production leaves always carry real event times; see
/// `content_pull.rs`'s write-invariant cleanup), so a materialization test
/// that isn't specifically exercising the bounds-absent/inference path
/// needs an explicit range rather than relying on `infer_temporal_bounds`
/// detecting a timestamp column this fixture's schema doesn't have.
fn build_table_pack_timed(
    schema: &Arc<Schema>,
    rows: &[(i64, &str)],
    leaf_row_counts: &[usize],
    object_row_counts: &[usize],
    event_time: (i64, i64),
) -> TablePackFixture {
    assert_eq!(leaf_row_counts.iter().sum::<usize>(), rows.len());
    assert_eq!(object_row_counts.iter().sum::<usize>(), rows.len());
    let (min, max) = event_time;

    let fingerprint = schema_fingerprint(schema).expect("schema fingerprint");
    // A write choke point always stamps the timestamp-column name it used
    // into the leaf's own extended attributes whenever explicit temporal
    // bounds are set (`tlogfs::series_identity::stamp_logical_leaf` hashes
    // *with* `entry.extended_attributes`, not the incoming descriptor's
    // attributes) -- so a real production pack for a table series with
    // event-time bounds always carries this same canonical value in every
    // leaf descriptor, never `None`. Match that here so leaf hashes verify
    // and re-stamp identically end to end.
    let attrs_json = r#"{"watertown.timestamp_column":"Timestamp"}"#;
    let canonical_attrs = encode_canonical_attributes(attrs_json).expect("canonical attributes");

    let mut leaf_hashes = Vec::with_capacity(leaf_row_counts.len());
    let mut descriptors = Vec::with_capacity(leaf_row_counts.len());
    let mut offset = 0usize;
    for &count in leaf_row_counts {
        let slice = &rows[offset..offset + count];
        let ids: Vec<i64> = slice.iter().map(|(id, _)| *id).collect();
        let labels: Vec<&str> = slice.iter().map(|(_, l)| *l).collect();
        let b = batch(schema, &ids, &labels);
        leaf_hashes.push(
            table_leaf_hash(schema, &[b], Some(min), Some(max), Some(attrs_json))
                .expect("real leaf hash"),
        );
        descriptors.push(
            PackLeafDescriptor::new_with_schema(
                count as u64,
                Some(fingerprint),
                Some(min),
                Some(max),
                Some(canonical_attrs.clone()),
            )
            .expect("descriptor"),
        );
        offset += count;
    }
    let root = merkle_root(&leaf_hashes);
    // The aggregate fold's `logical_attributes` adopts the *last* leaf's own
    // canonical attributes verbatim (`content_tree.rs::build_series_manifest`),
    // which is this same value since every leaf here names the same column.
    let logical_attributes = Some(canonical_attrs);
    let manifest = SeriesManifest::new_v2(
        PayloadKind::Table,
        rows.len() as u64,
        leaf_row_counts.len() as u64,
        Some(min),
        Some(max),
        logical_attributes,
        root,
    )
    .expect("valid manifest");
    let series_hash = manifest.hash();

    let mut physical_objects = Vec::with_capacity(object_row_counts.len());
    offset = 0;
    let mut total_bytes = 0u64;
    for &count in object_row_counts {
        let slice = &rows[offset..offset + count];
        let ids: Vec<i64> = slice.iter().map(|(id, _)| *id).collect();
        let labels: Vec<&str> = slice.iter().map(|(_, l)| *l).collect();
        let b = batch(schema, &ids, &labels);
        let bytes = write_parquet(schema, &b);
        total_bytes += bytes.len() as u64;
        let object_hash = ObjectHash::of_bytes(&bytes);
        physical_objects.push((object_hash, bytes));
        offset += count;
    }
    let proof =
        generate_range_proof(&leaf_hashes, 0, leaf_hashes.len()).expect("whole-range proof");
    let pack = PackIndex::new_v2(
        series_hash,
        0,
        leaf_row_counts.len() as u64,
        leaf_row_counts.len() as u64,
        root,
        proof,
        physical_objects.iter().map(|(h, _)| *h).collect(),
        rows.len() as u64,
        total_bytes,
        descriptors,
    )
    .expect("valid pack index");

    TablePackFixture {
        manifest,
        series_hash,
        pack,
        physical_objects,
        leaf_hashes,
    }
}

async fn publish_table(remote: &mut ContentRemote, fixture: &TablePackFixture) {
    let _ = remote
        .publish_pack(
            fixture.series_hash,
            &fixture.pack,
            &fixture.physical_objects,
        )
        .await
        .expect("publish pack");
    seed_series_manifest(remote, &fixture.manifest).await;
}

// -- File-pack fetch: success, boundary crossing, streaming ------------------

#[tokio::test]
async fn file_pack_fetch_succeeds_with_leaves_crossing_physical_object_boundaries() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    // 12 bytes total; leaves are [5, 7]; objects are [4, 4, 4] -- neither
    // leaf boundary (5) nor the other (12) lines up with an object boundary
    // (4, 8, 12), so both leaves cross at least one physical-object seam.
    let bytes = b"abcdefghijkl".to_vec();
    let fixture = build_file_pack(&bytes, &[5, 7], &[4, 4, 4]);
    publish(&mut remote, &fixture).await;

    let _ = push_root(
        &mut remote,
        &[(
            "series.dat",
            EntryType::FilePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch graph");
    match graph.objects.get(&fixture.series_hash) {
        Some(FetchedObject::SeriesV2(v2)) => {
            assert_eq!(v2.manifest, fixture.manifest);
            assert_eq!(v2.leaf_hashes, fixture.leaf_hashes);
            assert_eq!(v2.packs.len(), 1);
            assert_eq!(v2.packs[0].0, fixture.pack_hash);
            assert_eq!(v2.physical_object_hashes.len(), 3);
        }
        other => panic!("expected SeriesV2, got {other:?}"),
    }
    // Every physical object streamed as an ordinary external blob, reusable
    // by a future materializer exactly like a v1 version blob.
    for (object_hash, _) in &fixture.physical_objects {
        assert!(
            matches!(
                graph.objects.get(object_hash),
                Some(FetchedObject::External)
            ),
            "physical object {object_hash} should be registered as External"
        );
    }
}

#[tokio::test]
async fn file_pack_fetch_rejects_missing_physical_object() {
    let dir = tempdir().expect("tempdir");
    let remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let bytes = b"abcdefgh".to_vec();
    let fixture = build_file_pack(&bytes, &[4, 4], &[8]);

    // Publish the pack index without ever uploading its declared physical
    // object: `publish_pack` itself refuses a pack whose named objects are
    // not all present in the blob store, which is exactly what a
    // missing upload looks like from the remote's perspective.
    let err = remote
        .publish_pack(fixture.series_hash, &fixture.pack, &[])
        .await
        .expect_err("publish must refuse a pack naming an absent physical object");
    assert!(format!("{err}").contains("not present"), "{err}");
}

#[tokio::test]
async fn file_pack_fetch_rejects_truncated_physical_content() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    // Correct fixture over 8 bytes / two 4-byte leaves, but the pack index
    // is republished naming a physical object that is really only the
    // first 6 bytes -- fewer than the 8 bytes its own descriptors declare.
    // `publish_pack`'s own presence check only requires *a* blob at that
    // exact hash to exist, not that the pack's logical accounting is
    // internally sound against real content, so this specific
    // inconsistency can only be caught by fetch-time decoding.
    let bytes = b"abcdefgh".to_vec();
    let fixture = build_file_pack(&bytes, &[4, 4], &[8]);
    let truncated_bytes = bytes[..6].to_vec();
    let truncated_hash = ObjectHash::of_bytes(&truncated_bytes);
    let truncated_pack = PackIndex::new_v2(
        fixture.series_hash,
        0,
        2,
        2,
        fixture.pack.range_root(),
        fixture.pack.range_proof().clone(),
        vec![truncated_hash],
        fixture.pack.logical_count(),
        truncated_bytes.len() as u64,
        fixture.pack.leaf_descriptors().to_vec(),
    )
    .expect("valid pack shape");

    let _ = remote
        .publish_pack(
            fixture.series_hash,
            &truncated_pack,
            &[(truncated_hash, truncated_bytes)],
        )
        .await
        .expect("publish truncated-content pack");
    seed_series_manifest(&mut remote, &fixture.manifest).await;
    let _ = push_root(
        &mut remote,
        &[(
            "series.dat",
            EntryType::FilePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("truncated physical content must be rejected");
    assert!(format!("{err}").contains("truncated"), "{err}");
}

#[tokio::test]
async fn file_pack_fetch_rejects_trailing_physical_bytes() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    // The pack index still declares two 4-byte leaves (8 bytes total), but
    // the sole physical object it names now has 2 extra trailing bytes no
    // descriptor accounts for.
    let bytes = b"abcdefgh".to_vec();
    let fixture = build_file_pack(&bytes, &[4, 4], &[8]);
    let padded_bytes = b"abcdefghXX".to_vec();
    let padded_hash = ObjectHash::of_bytes(&padded_bytes);
    let padded_pack = PackIndex::new_v2(
        fixture.series_hash,
        0,
        2,
        2,
        fixture.pack.range_root(),
        fixture.pack.range_proof().clone(),
        vec![padded_hash],
        fixture.pack.logical_count(),
        padded_bytes.len() as u64,
        fixture.pack.leaf_descriptors().to_vec(),
    )
    .expect("valid pack shape");

    let _ = remote
        .publish_pack(
            fixture.series_hash,
            &padded_pack,
            &[(padded_hash, padded_bytes)],
        )
        .await
        .expect("publish padded-content pack");
    seed_series_manifest(&mut remote, &fixture.manifest).await;
    let _ = push_root(
        &mut remote,
        &[(
            "series.dat",
            EntryType::FilePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("trailing physical bytes must be rejected");
    assert!(
        format!("{err}").contains("trailing") || format!("{err}").contains("physical_byte_count"),
        "{err}"
    );
}

#[tokio::test]
async fn file_pack_fetch_rejects_wrong_descriptor_metadata() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let bytes = b"abcdefgh".to_vec();
    let fixture = build_file_pack(&bytes, &[4, 4], &[8]);

    // Build a tampered pack: identical physical objects and range proof, but
    // one descriptor now lies about its bounds. The recomputed leaf hash
    // (which bakes descriptor bounds into its preimage) will then disagree
    // with the real per-leaf hash used to build the manifest's Merkle root.
    let mut tampered_descriptors: Vec<PackLeafDescriptor> =
        fixture.pack.leaf_descriptors().to_vec();
    tampered_descriptors[0] =
        PackLeafDescriptor::new(4, Some(999), Some(999), None).expect("tampered descriptor");
    let tampered_pack = PackIndex::new_v2(
        fixture.series_hash,
        0,
        2,
        2,
        fixture.pack.range_root(),
        fixture.pack.range_proof().clone(),
        fixture.pack.physical_object_hashes().to_vec(),
        fixture.pack.logical_count(),
        fixture.pack.physical_byte_count(),
        tampered_descriptors,
    )
    .expect("valid pack shape");

    let _ = remote
        .publish_pack(
            fixture.series_hash,
            &tampered_pack,
            &fixture.physical_objects,
        )
        .await
        .expect("publish tampered pack");
    seed_series_manifest(&mut remote, &fixture.manifest).await;

    let _ = push_root(
        &mut remote,
        &[(
            "series.dat",
            EntryType::FilePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("tampered descriptor metadata must be rejected");
    assert!(format!("{err}").contains("failed verification"), "{err}");
}

#[tokio::test]
async fn file_pack_fetch_rejects_wrong_proof() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    // Two independent 4-leaf series with the same shape (four 4-byte
    // leaves) but different content, so a range proof over the same
    // sub-range `[1, 3)` has the same *shape* for both but different
    // embedded sibling hashes.
    let leaves_of = |bytes: &[u8]| -> Vec<ObjectHash> {
        bytes
            .chunks(4)
            .map(|chunk| file_leaf_hash(chunk, None, None, None).unwrap())
            .collect()
    };
    let bytes_a = b"0123456789abcdef".to_vec();
    let bytes_b = b"ABCDEFGHIJKLMNOP".to_vec();
    let leaves_a = leaves_of(&bytes_a);
    let leaves_b = leaves_of(&bytes_b);
    let root_a = merkle_root(&leaves_a);
    let manifest_a =
        SeriesManifest::new_v2(PayloadKind::File, 16, 4, None, None, None, root_a).unwrap();
    let series_hash_a = manifest_a.hash();

    // A genuine partial-range proof for A's own middle range would be
    // `generate_range_proof(&leaves_a, 1, 3)`; splice in series B's proof
    // for the identical `(total, start, end)` shape instead.
    let wrong_proof = generate_range_proof(&leaves_b, 1, 3).unwrap();
    let object_bytes = bytes_a[4..12].to_vec();
    let object_hash = ObjectHash::of_bytes(&object_bytes);
    let spliced_pack = PackIndex::new_v2(
        series_hash_a,
        1,
        3,
        4,
        root_a,
        wrong_proof,
        vec![object_hash],
        8,
        8,
        vec![
            PackLeafDescriptor::new(4, None, None, None).unwrap(),
            PackLeafDescriptor::new(4, None, None, None).unwrap(),
        ],
    )
    .expect("proof shape still matches (total, start, end)");

    let _ = remote
        .publish_pack(series_hash_a, &spliced_pack, &[(object_hash, object_bytes)])
        .await
        .expect("publish spliced pack");

    // Publish genuinely valid packs for the remaining leaves (0 and 3) so an
    // exact cover of the whole series exists and selection is forced to
    // include the tampered middle pack rather than simply failing to find
    // any cover at all.
    let left_bytes = bytes_a[..4].to_vec();
    let left_hash = ObjectHash::of_bytes(&left_bytes);
    let left_pack = PackIndex::new_v2(
        series_hash_a,
        0,
        1,
        4,
        root_a,
        generate_range_proof(&leaves_a, 0, 1).unwrap(),
        vec![left_hash],
        4,
        4,
        vec![PackLeafDescriptor::new(4, None, None, None).unwrap()],
    )
    .unwrap();
    let right_bytes = bytes_a[12..].to_vec();
    let right_hash = ObjectHash::of_bytes(&right_bytes);
    let right_pack = PackIndex::new_v2(
        series_hash_a,
        3,
        4,
        4,
        root_a,
        generate_range_proof(&leaves_a, 3, 4).unwrap(),
        vec![right_hash],
        4,
        4,
        vec![PackLeafDescriptor::new(4, None, None, None).unwrap()],
    )
    .unwrap();
    let _ = remote
        .publish_pack(series_hash_a, &left_pack, &[(left_hash, left_bytes)])
        .await
        .expect("publish left pack");
    let _ = remote
        .publish_pack(series_hash_a, &right_pack, &[(right_hash, right_bytes)])
        .await
        .expect("publish right pack");

    seed_series_manifest(&mut remote, &manifest_a).await;
    let _ = push_root(
        &mut remote,
        &[("series.dat", EntryType::FilePhysicalSeries, series_hash_a)],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("a proof spliced from a different series must be rejected");
    assert!(format!("{err}").contains("verification"), "{err}");
}

#[tokio::test]
async fn file_pack_fetch_rejects_payload_kind_mismatch() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let fixture = build_file_pack(b"abcdefgh", &[4, 4], &[8]);
    publish(&mut remote, &fixture).await;

    // The tree entry claims this is a *table* series, but the manifest
    // declares `PayloadKind::File`.
    let _ = push_root(
        &mut remote,
        &[(
            "series.tbl",
            EntryType::TablePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("payload-kind mismatch must be rejected");
    assert!(format!("{err}").contains("payload kind"), "{err}");
}

#[tokio::test]
async fn cached_v2_series_still_checks_each_tree_entry_kind() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let fixture = build_file_pack(b"abcdefgh", &[4, 4], &[8]);
    publish(&mut remote, &fixture).await;

    // Canonical name order fetches the valid file entry first. The table
    // entry then reaches the graph-dedup path for the same series hash and
    // must still be rejected rather than inheriting the first entry's check.
    let _ = push_root(
        &mut remote,
        &[
            (
                "a-file.dat",
                EntryType::FilePhysicalSeries,
                fixture.series_hash,
            ),
            (
                "b-table.tbl",
                EntryType::TablePhysicalSeries,
                fixture.series_hash,
            ),
        ],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("every tree reference must agree with the v2 payload kind");
    assert!(format!("{err}").contains("payload kind"), "{err}");
}

#[tokio::test]
async fn file_pack_fetch_rejects_when_no_pack_is_advertised() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let fixture = build_file_pack(b"abcdefgh", &[4, 4], &[8]);
    // Deliberately never publish any pack for this series.
    seed_series_manifest(&mut remote, &fixture.manifest).await;
    let _ = push_root(
        &mut remote,
        &[(
            "series.dat",
            EntryType::FilePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("a series with no advertised packs must fail to fetch");
    assert!(format!("{err}").contains("cover"), "{err}");
}

#[tokio::test]
async fn file_pack_fetch_rejects_a_gap_between_two_partial_layouts() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let bytes = b"abcdefgh".to_vec();
    // Build the whole-series manifest/leaves once, then publish only a pack
    // covering leaf 0 -- leaf 1 is never covered, so no exact cover exists.
    let leaf_hashes = vec![
        file_leaf_hash(&bytes[..4], None, None, None).unwrap(),
        file_leaf_hash(&bytes[4..], None, None, None).unwrap(),
    ];
    let root = merkle_root(&leaf_hashes);
    let manifest =
        SeriesManifest::new_v2(PayloadKind::File, 8, 2, None, None, None, root).expect("manifest");
    let series_hash = manifest.hash();
    let object_hash = ObjectHash::of_bytes(&bytes[..4]);
    let proof = generate_range_proof(&leaf_hashes, 0, 1).unwrap();
    let pack = PackIndex::new_v2(
        series_hash,
        0,
        1,
        2,
        root,
        proof,
        vec![object_hash],
        4,
        4,
        vec![PackLeafDescriptor::new(4, None, None, None).unwrap()],
    )
    .unwrap();
    let _ = remote
        .publish_pack(series_hash, &pack, &[(object_hash, bytes[..4].to_vec())])
        .await
        .expect("publish partial pack");
    seed_series_manifest(&mut remote, &manifest).await;

    let _ = push_root(
        &mut remote,
        &[("series.dat", EntryType::FilePhysicalSeries, series_hash)],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("a gap in coverage must fail to fetch");
    assert!(format!("{err}").contains("cover"), "{err}");
}

#[tokio::test]
async fn file_pack_fetch_verifies_with_either_of_two_valid_layouts() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let bytes = b"abcdefghijkl".to_vec();
    // Layout 1: one pack covering the whole series.
    let whole = build_file_pack(&bytes, &[4, 4, 4], &[12]);
    // Layout 2: the identical logical series, split into two packs at leaf 1
    // (a different, independently valid physical layout for the same
    // content -- design doc invariant 5).
    let leaf_hashes = whole.leaf_hashes.clone();
    let root = merkle_root(&leaf_hashes);
    assert_eq!(root, whole.manifest.leaf_merkle_root());

    let object_a_hash = ObjectHash::of_bytes(&bytes[..4]);
    let object_b_hash = ObjectHash::of_bytes(&bytes[4..]);
    let proof_a = generate_range_proof(&leaf_hashes, 0, 1).unwrap();
    let pack_a = PackIndex::new_v2(
        whole.series_hash,
        0,
        1,
        3,
        root,
        proof_a,
        vec![object_a_hash],
        4,
        4,
        vec![PackLeafDescriptor::new(4, None, None, None).unwrap()],
    )
    .unwrap();
    let proof_b = generate_range_proof(&leaf_hashes, 1, 3).unwrap();
    let pack_b = PackIndex::new_v2(
        whole.series_hash,
        1,
        3,
        3,
        root,
        proof_b,
        vec![object_b_hash],
        8,
        8,
        vec![
            PackLeafDescriptor::new(4, None, None, None).unwrap(),
            PackLeafDescriptor::new(4, None, None, None).unwrap(),
        ],
    )
    .unwrap();

    let _ = remote
        .publish_pack(
            whole.series_hash,
            &pack_a,
            &[(object_a_hash, bytes[..4].to_vec())],
        )
        .await
        .expect("publish pack a");
    let _ = remote
        .publish_pack(
            whole.series_hash,
            &pack_b,
            &[(object_b_hash, bytes[4..].to_vec())],
        )
        .await
        .expect("publish pack b");
    seed_series_manifest(&mut remote, &whole.manifest).await;

    let _ = push_root(
        &mut remote,
        &[(
            "series.dat",
            EntryType::FilePhysicalSeries,
            whole.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch must succeed with a valid two-pack exact cover");
    match graph.objects.get(&whole.series_hash) {
        Some(FetchedObject::SeriesV2(v2)) => {
            assert_eq!(v2.leaf_hashes, leaf_hashes);
            // The two-pack cover, not the single whole-series pack (which was
            // never published in this test), must have been selected.
            assert_eq!(v2.packs.len(), 2);
        }
        other => panic!("expected SeriesV2, got {other:?}"),
    }
}

// -- Table (Parquet) pack fetch ----------------------------------------------

#[tokio::test]
async fn table_pack_fetch_succeeds_with_leaves_crossing_objects_and_batches() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");
    let schema = i64_string_schema();
    let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")];
    // Leaves [2, 4]; objects [3, 3] -- leaf 1 (rows 2..6) crosses the object
    // seam at row 3, and `set_max_row_group_size(2)` in `write_parquet`
    // encourages each 3-row object to decode as more than one row group /
    // `RecordBatch`, so leaf 1 also spans multiple decoded batches.
    let fixture = build_table_pack(&schema, &rows, &[2, 4], &[3, 3]);
    publish_table(&mut remote, &fixture).await;

    let _ = push_root(
        &mut remote,
        &[(
            "series.tbl",
            EntryType::TablePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch graph");
    match graph.objects.get(&fixture.series_hash) {
        Some(FetchedObject::SeriesV2(v2)) => {
            assert_eq!(v2.leaf_hashes, fixture.leaf_hashes);
            assert_eq!(v2.manifest, fixture.manifest);
        }
        other => panic!("expected SeriesV2, got {other:?}"),
    }
}

#[tokio::test]
async fn table_pack_fetch_rejects_schema_mismatch() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");
    let schema = i64_string_schema();
    let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b")];
    let fixture = build_table_pack(&schema, &rows, &[2], &[2]);

    // Republish the pack, but swap its sole physical object for one encoded
    // with a *different* schema (an extra column), so the manifest's
    // declared `schema_fingerprint` no longer matches the physical object's
    // real Parquet schema.
    let wrong_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("extra", DataType::Int64, true),
    ]));
    let wrong_batch = RecordBatch::try_new(
        wrong_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![9, 9])),
        ],
    )
    .unwrap();
    let wrong_bytes = write_parquet(&wrong_schema, &wrong_batch);
    let wrong_hash = ObjectHash::of_bytes(&wrong_bytes);
    let mismatched_pack = PackIndex::new_v2(
        fixture.series_hash,
        0,
        1,
        1,
        fixture.pack.range_root(),
        fixture.pack.range_proof().clone(),
        vec![wrong_hash],
        2,
        wrong_bytes.len() as u64,
        fixture.pack.leaf_descriptors().to_vec(),
    )
    .unwrap();
    let _ = remote
        .publish_pack(
            fixture.series_hash,
            &mismatched_pack,
            &[(wrong_hash, wrong_bytes)],
        )
        .await
        .expect("publish mismatched-schema pack");
    seed_series_manifest(&mut remote, &fixture.manifest).await;

    let _ = push_root(
        &mut remote,
        &[(
            "series.tbl",
            EntryType::TablePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("a schema-mismatched physical object must be rejected");
    assert!(format!("{err}").contains("schema"), "{err}");
}

#[tokio::test]
async fn table_pack_fetch_rejects_tampered_descriptor_schema() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");
    let schema = i64_string_schema();
    let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b")];
    let fixture = build_table_pack(&schema, &rows, &[2], &[2]);
    let wrong_schema = Arc::new(Schema::new(vec![Field::new(
        "other",
        DataType::Int64,
        false,
    )]));
    let descriptor = PackLeafDescriptor::new_with_schema(
        2,
        Some(schema_fingerprint(&wrong_schema).expect("wrong fingerprint")),
        None,
        None,
        None,
    )
    .expect("tampered descriptor");
    let tampered = PackIndex::new_v2(
        fixture.series_hash,
        0,
        1,
        1,
        fixture.pack.range_root(),
        fixture.pack.range_proof().clone(),
        fixture.pack.physical_object_hashes().to_vec(),
        fixture.pack.logical_count(),
        fixture.pack.physical_byte_count(),
        vec![descriptor],
    )
    .expect("valid pack shape");
    let _ = remote
        .publish_pack(fixture.series_hash, &tampered, &fixture.physical_objects)
        .await
        .expect("publish tampered pack");
    seed_series_manifest(&mut remote, &fixture.manifest).await;
    let _ = push_root(
        &mut remote,
        &[(
            "series.tbl",
            EntryType::TablePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("descriptor schema tampering must be rejected");
    assert!(format!("{err}").contains("schema fingerprint"), "{err}");
}

#[tokio::test]
async fn table_pack_fetch_inherits_legacy_v1_manifest_schema() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");
    let schema = i64_string_schema();
    let batch = batch(&schema, &[1, 2], &["a", "b"]);
    let fingerprint = schema_fingerprint(&schema).expect("fingerprint");
    let leaf_hash =
        table_leaf_hash(&schema, std::slice::from_ref(&batch), None, None, None).expect("leaf");
    let manifest = SeriesManifest::new(
        PayloadKind::Table,
        Some(fingerprint),
        2,
        1,
        None,
        None,
        None,
        merkle_root(&[leaf_hash]),
    )
    .expect("legacy manifest");
    let bytes = write_parquet(&schema, &batch);
    let object_hash = ObjectHash::of_bytes(&bytes);
    let pack = PackIndex::new(
        manifest.hash(),
        0,
        1,
        1,
        manifest.leaf_merkle_root(),
        generate_range_proof(&[leaf_hash], 0, 1).expect("proof"),
        vec![object_hash],
        2,
        bytes.len() as u64,
        vec![PackLeafDescriptor::new(2, None, None, None).expect("legacy descriptor")],
    )
    .expect("legacy pack");
    let fixture = TablePackFixture {
        series_hash: manifest.hash(),
        manifest,
        pack,
        physical_objects: vec![(object_hash, bytes)],
        leaf_hashes: vec![leaf_hash],
    };
    publish_table(&mut remote, &fixture).await;
    let _ = push_root(
        &mut remote,
        &[(
            "legacy.tbl",
            EntryType::TablePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("legacy homogeneous table pack must verify");
    let Some(FetchedObject::SeriesV2(series)) = graph.objects.get(&fixture.series_hash) else {
        panic!("expected fetched native series");
    };
    assert_eq!(
        series.packs[0].1.leaf_descriptors()[0].schema_fingerprint(),
        None,
        "a decoded v1 descriptor does not intrinsically carry the inherited schema"
    );
}

#[tokio::test]
async fn table_pack_fetch_rejects_row_truncation() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");
    let schema = i64_string_schema();
    let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b"), (3, "c"), (4, "d")];
    let fixture = build_table_pack(&schema, &rows, &[2, 2], &[4]);

    // Republish with a physical object holding one row fewer than declared.
    let short_batch = batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    let short_bytes = write_parquet(&schema, &short_batch);
    let short_hash = ObjectHash::of_bytes(&short_bytes);
    let truncated_pack = PackIndex::new_v2(
        fixture.series_hash,
        0,
        2,
        2,
        fixture.pack.range_root(),
        fixture.pack.range_proof().clone(),
        vec![short_hash],
        4,
        short_bytes.len() as u64,
        fixture.pack.leaf_descriptors().to_vec(),
    )
    .unwrap();
    let _ = remote
        .publish_pack(
            fixture.series_hash,
            &truncated_pack,
            &[(short_hash, short_bytes)],
        )
        .await
        .expect("publish truncated pack");
    seed_series_manifest(&mut remote, &fixture.manifest).await;

    let _ = push_root(
        &mut remote,
        &[(
            "series.tbl",
            EntryType::TablePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("row truncation must be rejected");
    assert!(format!("{err}").contains("row"), "{err}");
}

#[tokio::test]
async fn table_pack_fetch_rejects_corrupt_parquet_bytes() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");
    let schema = i64_string_schema();
    let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b")];
    let fixture = build_table_pack(&schema, &rows, &[2], &[2]);

    let garbage = b"not a parquet file at all, just junk bytes".to_vec();
    let garbage_hash = ObjectHash::of_bytes(&garbage);
    let corrupt_pack = PackIndex::new_v2(
        fixture.series_hash,
        0,
        1,
        1,
        fixture.pack.range_root(),
        fixture.pack.range_proof().clone(),
        vec![garbage_hash],
        2,
        garbage.len() as u64,
        fixture.pack.leaf_descriptors().to_vec(),
    )
    .unwrap();
    let _ = remote
        .publish_pack(
            fixture.series_hash,
            &corrupt_pack,
            &[(garbage_hash, garbage)],
        )
        .await
        .expect("publish corrupt-parquet pack");
    seed_series_manifest(&mut remote, &fixture.manifest).await;

    let _ = push_root(
        &mut remote,
        &[(
            "series.tbl",
            EntryType::TablePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let err = fetch_object_graph(&remote, "main")
        .await
        .expect_err("corrupt parquet bytes must be rejected");
    assert!(format!("{err}").contains("parquet"), "{err}");
}

// -- Mixed v1/v2 fetch --------------------------------------------------------

/// A synthetic tree containing *both* a real v1 (`dp.series.1`) series and a
/// v2 (`watertown.series.v1`) series side by side, under one `watertown.commit.v1` commit.
/// This is a **test-only fixture**: after the reset, a real writer only
/// ever emits `watertown.series.v1` entries (delivery gate 7 removed the v1 writer
/// path entirely, and old `dp.commit.3` history is not expected to remain
/// openable), but mixing both kinds under one commit is still the most
/// direct way to prove the dual reader dispatches each entry to the correct
/// [`FetchedObject`] variant within a single fetch.
#[tokio::test]
async fn mixed_v1_and_v2_series_fetch_reaches_correct_graph_variants() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    // v1 series: two ordinary version blobs.
    let v1_version_a_bytes = b"version a bytes".to_vec();
    let v1_version_b_bytes = b"version b bytes".to_vec();
    let v1_versions = vec![
        ObjectHash::of_bytes(&v1_version_a_bytes),
        ObjectHash::of_bytes(&v1_version_b_bytes),
    ];
    let v1_series_bytes = encode_series(&v1_versions);
    let v1_series_hash = ObjectHash::of_bytes(&v1_series_bytes);

    // v2 series: a small, fully verifiable file pack.
    let v2_fixture = build_file_pack(b"abcdefgh", &[4, 4], &[8]);
    publish(&mut remote, &v2_fixture).await;

    let _ = push_root(
        &mut remote,
        &[
            ("legacy.dat", EntryType::FilePhysicalSeries, v1_series_hash),
            (
                "modern.dat",
                EntryType::FilePhysicalSeries,
                v2_fixture.series_hash,
            ),
        ],
        vec![
            (v1_series_hash, v1_series_bytes),
            (v1_versions[0], v1_version_a_bytes),
            (v1_versions[1], v1_version_b_bytes),
        ],
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("mixed v1/v2 fetch must succeed");

    match graph.objects.get(&v1_series_hash) {
        Some(FetchedObject::Series(versions)) => assert_eq!(versions, &v1_versions),
        other => panic!("expected v1 Series, got {other:?}"),
    }
    match graph.objects.get(&v2_fixture.series_hash) {
        Some(FetchedObject::SeriesV2(v2)) => {
            assert_eq!(v2.leaf_hashes, v2_fixture.leaf_hashes);
        }
        other => panic!("expected SeriesV2, got {other:?}"),
    }
}

// -- v2 materialized on rebuild ------------------------------------------------

/// A verified v2 series is materialized into the destination pond exactly:
/// `rebuild_pond` writes one destination version per logical leaf (not per
/// physical pack object), and the materialized bytes read back
/// byte-for-byte equal to what the fixture encoded, even though its leaf
/// boundaries (4, 4) cross a differently-shaped physical object layout (a
/// single 8-byte object).
#[tokio::test]
async fn rebuild_materializes_a_verified_v2_file_series() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let fixture = build_file_pack(b"abcdefgh", &[4, 4], &[8]);
    publish(&mut remote, &fixture).await;
    let versions = vec![VersionMeta {
        timestamp: Some(FIXTURE_MTIME),
        ..Default::default()
    }];
    let _ = push_root_versioned(
        &mut remote,
        &[(
            "series.dat",
            EntryType::FilePhysicalSeries,
            fixture.series_hash,
            versions,
        )],
        Vec::new(),
    )
    .await;

    let graph: FetchedGraph = fetch_object_graph(&remote, "main")
        .await
        .expect("v2 fetch verification succeeds");
    assert!(matches!(
        graph.objects.get(&fixture.series_hash),
        Some(FetchedObject::SeriesV2(_))
    ));

    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");

    let outcome = rebuild_pond(&mut target, &remote, &graph)
        .await
        .expect("rebuild must materialize the verified v2 series");
    assert_eq!(outcome.series, 1);

    let tx = target.begin_read(&meta("read")).await.expect("begin read");
    let root = tx.root().await.expect("root");
    let bytes = root
        .read_file_path_to_vec("series.dat")
        .await
        .expect("read materialized series");
    assert_eq!(bytes, b"abcdefgh");
}

/// A verified v2 file series whose leaves cross physical object boundaries
/// (the same layout exercised by the dual-reader fetch test above) must
/// still materialize with each leaf's bytes landing in the right place --
/// physical object seams are invisible to the destination.
#[tokio::test]
async fn rebuild_materializes_a_v2_file_series_crossing_object_boundaries() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let bytes = b"abcdefghijkl".to_vec();
    let fixture = build_file_pack(&bytes, &[5, 7], &[4, 4, 4]);
    publish(&mut remote, &fixture).await;
    let versions = vec![VersionMeta {
        timestamp: Some(FIXTURE_MTIME),
        ..Default::default()
    }];
    let _ = push_root_versioned(
        &mut remote,
        &[(
            "series.dat",
            EntryType::FilePhysicalSeries,
            fixture.series_hash,
            versions,
        )],
        Vec::new(),
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch graph");

    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");
    let outcome = rebuild_pond(&mut target, &remote, &graph)
        .await
        .expect("rebuild must materialize leaves crossing object boundaries");
    assert_eq!(outcome.series, 1);

    let tx = target.begin_read(&meta("read")).await.expect("begin read");
    let root = tx.root().await.expect("root");
    let read_back = root
        .read_file_path_to_vec("series.dat")
        .await
        .expect("read materialized series");
    assert_eq!(read_back, bytes);
}

/// The same verified v2 table series the dual reader exercises across two
/// physical objects and mismatched row-group batches must also materialize
/// correctly: each logical leaf becomes one destination Parquet version with
/// its own rows, re-encoded deterministically from the decoded batches.
#[tokio::test]
async fn rebuild_materializes_a_v2_table_series_crossing_objects_and_batches() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let schema = i64_string_schema();
    let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")];
    // Same layout as the dual-reader's own boundary-crossing coverage:
    // leaves [2, 4] over objects [3, 3], so leaf 1 both crosses an object
    // seam and spans more than one decoded row-group/batch. Explicit
    // event-time bounds (as every real table series carries) so the write
    // choke point's temporal-metadata requirement is satisfied directly
    // rather than through `infer_temporal_bounds`, which needs a real
    // timestamp column this synthetic (i64, string) schema doesn't have.
    let fixture = build_table_pack_timed(&schema, &rows, &[2, 4], &[3, 3], (1_000, 2_000));
    publish_table(&mut remote, &fixture).await;
    let versions = vec![VersionMeta {
        timestamp: Some(FIXTURE_MTIME),
        min_event_time: Some(1_000),
        max_event_time: Some(2_000),
        // A table write choke point stamps the timestamp-column name into
        // this same canonical form whenever temporal metadata is set (see
        // `tlogfs::schema::ExtendedAttributes::set_timestamp_column`/
        // `content_tree.rs::canonical_attributes`); a fixture that will
        // survive a real `rebuild_pond` commit must advertise the same
        // value the destination's own fold will read back.
        extended_attributes: Some(r#"{"watertown.timestamp_column":"Timestamp"}"#.to_string()),
    }];
    let _ = push_root_versioned(
        &mut remote,
        &[(
            "series.table",
            EntryType::TablePhysicalSeries,
            fixture.series_hash,
            versions,
        )],
        Vec::new(),
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch graph");
    assert!(matches!(
        graph.objects.get(&fixture.series_hash),
        Some(FetchedObject::SeriesV2(_))
    ));

    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");
    let outcome = rebuild_pond(&mut target, &remote, &graph)
        .await
        .expect("rebuild must materialize the v2 table series");
    assert_eq!(outcome.series, 1);

    // `rebuild_pond` itself already validates (precommit) that the
    // destination's own content-tree fold reproduces the advertised
    // node-manifest root before committing -- since the fold recomputes
    // every leaf hash bottom-up, this proves the whole series (all rows, in
    // every leaf, across the object/batch boundary crossing above) was
    // materialized exactly, matching the fetch-verified leaf hashes.
    // Directly confirm readback of the earliest
    // version's own rows too: a plain (non-series-aware) read of a
    // `TablePhysicalSeries` path returns its first/creating version's
    // Parquet content, not every live version concatenated (unlike a
    // `FilePhysicalSeries`, which does concatenate on read) -- full
    // multi-version querying goes through the series-aware table provider,
    // not this direct path.
    use tinyfs::arrow::parquet::ParquetExt;
    let read_batch = {
        let tx = target.begin_read(&meta("read")).await.expect("begin read");
        let root = tx.root().await.expect("root");
        root.read_table_as_batch("series.table")
            .await
            .expect("read materialized table series")
    };
    let expected_first_leaf = batch(&schema, &[1, 2], &["a", "b"]);
    assert_eq!(read_batch, expected_first_leaf);
}

#[tokio::test]
async fn rebuild_materializes_heterogeneous_table_leaf_schemas() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let schema_a = i64_string_schema();
    let schema_b = Arc::new(Schema::new(vec![
        Field::new("measurement", DataType::Int64, false),
        Field::new("description", DataType::Utf8, true),
    ]));
    let batch_a = batch(&schema_a, &[1], &["a"]);
    let batch_b = batch(&schema_b, &[2], &["b"]);
    let fingerprint_a = schema_fingerprint(&schema_a).expect("fingerprint a");
    let fingerprint_b = schema_fingerprint(&schema_b).expect("fingerprint b");
    let attrs_json = r#"{"watertown.timestamp_column":"Timestamp"}"#;
    let attrs = encode_canonical_attributes(attrs_json).expect("canonical attributes");
    let leaf_hashes = vec![
        table_leaf_hash(
            &schema_a,
            std::slice::from_ref(&batch_a),
            Some(1_000),
            Some(2_000),
            Some(attrs_json),
        )
        .expect("leaf a hash"),
        table_leaf_hash(
            &schema_b,
            std::slice::from_ref(&batch_b),
            Some(1_000),
            Some(2_000),
            Some(attrs_json),
        )
        .expect("leaf b hash"),
    ];
    let root = merkle_root(&leaf_hashes);
    let manifest = SeriesManifest::new_v2(
        PayloadKind::Table,
        2,
        2,
        Some(1_000),
        Some(2_000),
        Some(attrs.clone()),
        root,
    )
    .expect("manifest");
    let bytes_a = write_parquet(&schema_a, &batch_a);
    let bytes_b = write_parquet(&schema_b, &batch_b);
    let physical_objects = vec![
        (ObjectHash::of_bytes(&bytes_a), bytes_a),
        (ObjectHash::of_bytes(&bytes_b), bytes_b),
    ];
    let descriptors = vec![
        PackLeafDescriptor::new_with_schema(
            1,
            Some(fingerprint_a),
            Some(1_000),
            Some(2_000),
            Some(attrs.clone()),
        )
        .expect("descriptor a"),
        PackLeafDescriptor::new_with_schema(
            1,
            Some(fingerprint_b),
            Some(1_000),
            Some(2_000),
            Some(attrs),
        )
        .expect("descriptor b"),
    ];
    let pack = PackIndex::new_v2(
        manifest.hash(),
        0,
        2,
        2,
        root,
        generate_range_proof(&leaf_hashes, 0, 2).expect("proof"),
        physical_objects.iter().map(|(hash, _)| *hash).collect(),
        2,
        physical_objects
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum(),
        descriptors,
    )
    .expect("pack");
    let fixture = TablePackFixture {
        series_hash: manifest.hash(),
        manifest,
        pack,
        physical_objects,
        leaf_hashes,
    };
    publish_table(&mut remote, &fixture).await;
    let _ = push_root_versioned(
        &mut remote,
        &[(
            "series.table",
            EntryType::TablePhysicalSeries,
            fixture.series_hash,
            vec![VersionMeta {
                timestamp: Some(FIXTURE_MTIME),
                min_event_time: Some(1_000),
                max_event_time: Some(2_000),
                extended_attributes: Some(attrs_json.to_string()),
            }],
        )],
        Vec::new(),
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch heterogeneous series");
    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");
    let outcome = rebuild_pond(&mut target, &remote, &graph)
        .await
        .expect("materialize heterogeneous series");
    assert_eq!(outcome.series, 1);
}

/// A leaf descriptor carrying only one of `min_event_time`/`max_event_time`
/// is a state gate-4 fetch verification does not reject (it only checks
/// hashes/proofs, not writer-side invariants about paired bounds), but a
/// legitimate writer never produces it. The materializer must refuse to
/// guess (never calling `infer_temporal_bounds`, which would silently change
/// the persisted bounds away from what the already-verified leaf hash was
/// computed against) and abort before any destination mutation.
#[tokio::test]
async fn rebuild_aborts_cleanly_on_an_asymmetric_temporal_descriptor() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let bytes = b"abcdefgh".to_vec();
    // One leaf, deliberately built with only `min_event_time` set.
    let leaf_hash =
        file_leaf_hash(&bytes, Some(1_000), None, None).expect("real leaf hash with min only");
    let descriptor =
        PackLeafDescriptor::new(bytes.len() as u64, Some(1_000), None, None).expect("descriptor");
    let leaf_hashes = vec![leaf_hash];
    let root = merkle_root(&leaf_hashes);
    let manifest = SeriesManifest::new_v2(
        PayloadKind::File,
        bytes.len() as u64,
        1,
        Some(1_000),
        None,
        None,
        root,
    )
    .expect("valid manifest");
    let series_hash = manifest.hash();
    let object_hash = ObjectHash::of_bytes(&bytes);
    let physical_objects = vec![(object_hash, bytes.clone())];
    let proof = generate_range_proof(&leaf_hashes, 0, 1).expect("whole-range proof");
    let pack = PackIndex::new_v2(
        series_hash,
        0,
        1,
        1,
        root,
        proof,
        vec![object_hash],
        bytes.len() as u64,
        bytes.len() as u64,
        vec![descriptor],
    )
    .expect("valid pack index");
    let pack_hash = pack.hash();
    let fixture = FilePackFixture {
        manifest,
        series_hash,
        pack,
        pack_hash,
        physical_objects,
        leaf_hashes,
    };
    publish(&mut remote, &fixture).await;
    let _ = push_root(
        &mut remote,
        &[(
            "series.dat",
            EntryType::FilePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("gate-4 fetch verification only checks internal pack self-consistency");

    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");
    let version_before = target.data_persistence().table().version();

    let err = rebuild_pond(&mut target, &remote, &graph)
        .await
        .expect_err("materialization must reject an asymmetric temporal descriptor");
    let msg = format!("{err}");
    assert!(msg.contains("min_event_time"), "{msg}");
    assert_eq!(
        target.data_persistence().table().version(),
        version_before,
        "a rejected materialization must not advance the destination table"
    );
}

// -- Release blocker item 1: zero-leaf series materialization ---------------

/// A legitimately empty (never-appended-to) `FilePhysicalSeries` -- a
/// `watertown.series.v1` manifest with `leaf_count() == 0` -- has no packs to fetch
/// ([`sync_store::content::select_exact_cover`] special-cases this) and no
/// leaf-bearing version to reproduce, but the node itself must still be
/// created: an empty file at the destination, matching exactly what a real
/// writer produces for a zero-byte first version.
#[tokio::test]
async fn rebuild_materializes_an_empty_v2_file_series() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let empty_root = merkle_root(&[]);
    let manifest = SeriesManifest::new_v2(PayloadKind::File, 0, 0, None, None, None, empty_root)
        .expect("valid empty-series manifest");
    let series_hash = manifest.hash();
    seed_series_manifest(&mut remote, &manifest).await;

    let versions = vec![VersionMeta {
        timestamp: Some(FIXTURE_MTIME),
        ..Default::default()
    }];
    let _ = push_root_versioned(
        &mut remote,
        &[(
            "series.dat",
            EntryType::FilePhysicalSeries,
            series_hash,
            versions,
        )],
        Vec::new(),
    )
    .await;

    let graph: FetchedGraph = fetch_object_graph(&remote, "main")
        .await
        .expect("v2 fetch verification succeeds for a zero-leaf manifest");
    assert!(matches!(
        graph.objects.get(&series_hash),
        Some(FetchedObject::SeriesV2(_))
    ));

    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");

    let outcome = rebuild_pond(&mut target, &remote, &graph)
        .await
        .expect("rebuild must materialize the empty series node");
    assert_eq!(outcome.series, 1);

    let tx = target.begin_read(&meta("read")).await.expect("begin read");
    let root = tx.root().await.expect("root");
    let bytes = root
        .read_file_path_to_vec("series.dat")
        .await
        .expect("read materialized empty series");
    assert!(bytes.is_empty(), "the materialized series must be empty");
}

/// As above, but for a `TablePhysicalSeries`: `SeriesManifest::new`
/// unconditionally requires a schema fingerprint for `PayloadKind::Table`
/// even at `leaf_count() == 0` (unlike a file series), so a well-formed
/// zero-leaf table manifest still carries one -- yet the destination can
/// never actually WRITE a materialized node this state describes: the only
/// way to create a `TablePhysicalSeries` row at all is a zero-content write
/// (no leaves to reconstruct), and a zero-content write can never itself
/// carry a schema fingerprint (a real schema is only ever known from
/// decoding nonempty Parquet bytes -- see
/// `tlogfs::error::TLogFSError::SeriesTableRequiresSchemaBearingFirstVersion`,
/// which rejects exactly this at the write choke point too). Materializing
/// it would create a node this same destination could never fold back into
/// a valid manifest on its own next commit, so it must be rejected clearly
/// here, before any destination mutation, rather than silently accepted or
/// left to fail opaquely later (release blocker item 1,
/// `docs/logical-series-identity-design.md`).
#[tokio::test]
async fn rebuild_rejects_an_empty_v2_table_series() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let empty_root = merkle_root(&[]);
    let manifest = SeriesManifest::new_v2(PayloadKind::Table, 0, 0, None, None, None, empty_root)
        .expect("valid empty-table-series manifest");
    let series_hash = manifest.hash();
    seed_series_manifest(&mut remote, &manifest).await;

    let versions = vec![VersionMeta {
        timestamp: Some(FIXTURE_MTIME),
        ..Default::default()
    }];
    let _ = push_root_versioned(
        &mut remote,
        &[(
            "series.tbl",
            EntryType::TablePhysicalSeries,
            series_hash,
            versions,
        )],
        Vec::new(),
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("v2 fetch verification succeeds for a zero-leaf table manifest");
    assert!(matches!(
        graph.objects.get(&series_hash),
        Some(FetchedObject::SeriesV2(_))
    ));

    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");
    let version_before = target.data_persistence().table().version();

    let err = rebuild_pond(&mut target, &remote, &graph)
        .await
        .expect_err("materialization must reject a zero-leaf table series with a clear error");
    let msg = format!("{err}");
    assert!(
        msg.contains("leaf_count") || msg.contains("schema fingerprint"),
        "expected a clear zero-leaf-table diagnostic, got: {msg}"
    );
    assert_eq!(
        target.data_persistence().table().version(),
        version_before,
        "a rejected materialization must not advance the destination table"
    );
}

// -- Release blocker item 2: exact logical attributes / temporal bounds -----

/// A table leaf descriptor whose logical attributes carry an arbitrary
/// extra key alongside `watertown.timestamp_column` must round-trip exactly:
/// `rebuild_pond`'s own precommit fold recomputes the destination's node
/// manifest from what it actually wrote and rejects the commit if it
/// disagrees with the advertised root, so a successful rebuild here is
/// itself proof the extra key's exact canonical bytes were reproduced (not
/// merely the well-known `timestamp_column` -- see release blocker item 2's
/// `apply_descriptor_exact_attributes`/`OpLogFileWriter::
/// set_exact_logical_attributes`, `docs/logical-series-identity-design.md`).
#[tokio::test]
async fn rebuild_round_trips_an_arbitrary_extra_logical_attribute_key() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let schema = i64_string_schema();
    let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b"), (3, "c")];
    let fingerprint = schema_fingerprint(&schema).expect("schema fingerprint");
    // Canonical attributes carry both the well-known timestamp-column key
    // AND an arbitrary sibling key a real `ExtendedAttributes::set_raw`
    // caller might set (e.g. a unit annotation) -- deliberately not in
    // sorted order here to also exercise canonicalization.
    let attrs_json = r#"{"custom.units":"celsius","watertown.timestamp_column":"Timestamp"}"#;
    let canonical_attrs = encode_canonical_attributes(attrs_json).expect("canonical attributes");

    let b = batch(&schema, &[1, 2, 3], &["a", "b", "c"]);
    let leaf_hash = table_leaf_hash(
        &schema,
        std::slice::from_ref(&b),
        Some(1_000),
        Some(3_000),
        Some(attrs_json),
    )
    .expect("real leaf hash");
    let descriptor = PackLeafDescriptor::new_with_schema(
        rows.len() as u64,
        Some(fingerprint),
        Some(1_000),
        Some(3_000),
        Some(canonical_attrs.clone()),
    )
    .expect("descriptor");
    let leaf_hashes = vec![leaf_hash];
    let root = merkle_root(&leaf_hashes);
    let logical_attributes = Some(canonical_attrs);
    let manifest = SeriesManifest::new_v2(
        PayloadKind::Table,
        rows.len() as u64,
        1,
        Some(1_000),
        Some(3_000),
        logical_attributes,
        root,
    )
    .expect("valid manifest");
    let series_hash = manifest.hash();

    let bytes = write_parquet(&schema, &b);
    let object_hash = ObjectHash::of_bytes(&bytes);
    let physical_objects = vec![(object_hash, bytes.clone())];
    let proof = generate_range_proof(&leaf_hashes, 0, 1).expect("whole-range proof");
    let pack = PackIndex::new_v2(
        series_hash,
        0,
        1,
        1,
        root,
        proof,
        vec![object_hash],
        rows.len() as u64,
        bytes.len() as u64,
        vec![descriptor],
    )
    .expect("valid pack index");
    let fixture = TablePackFixture {
        manifest,
        series_hash,
        pack,
        physical_objects,
        leaf_hashes,
    };
    publish_table(&mut remote, &fixture).await;

    let versions = vec![VersionMeta {
        timestamp: Some(FIXTURE_MTIME),
        min_event_time: Some(1_000),
        max_event_time: Some(3_000),
        extended_attributes: Some(attrs_json.to_string()),
    }];
    let _ = push_root_versioned(
        &mut remote,
        &[(
            "series.table",
            EntryType::TablePhysicalSeries,
            fixture.series_hash,
            versions,
        )],
        Vec::new(),
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("fetch graph");

    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");
    let outcome = rebuild_pond(&mut target, &remote, &graph).await.expect(
        "rebuild must materialize the series and reproduce the exact canonical attributes \
         (a mismatch would fail the precommit tree-hash re-fold)",
    );
    assert_eq!(outcome.series, 1);

    use tinyfs::arrow::parquet::ParquetExt;
    let read_batch = {
        let tx = target.begin_read(&meta("read")).await.expect("begin read");
        let root = tx.root().await.expect("root");
        root.read_table_as_batch("series.table")
            .await
            .expect("read materialized table series")
    };
    assert_eq!(read_batch, b);
}

/// A table leaf descriptor with NEITHER `min_event_time` nor
/// `max_event_time` set (as opposed to the asymmetric case above, which sets
/// only one) is a state `build_table_pack` itself produces by default: a
/// legitimate table pack with no temporal bounds at all. The materializer
/// must reject this clearly during planning/fetch (never calling
/// `infer_temporal_bounds` on the already-verified, already-decoded leaf
/// bytes to invent identity inputs the leaf hash was never computed against
/// -- release blocker item 2, `docs/logical-series-identity-design.md`)
/// rather than silently guessing or corrupting the destination.
#[tokio::test]
async fn rebuild_rejects_a_table_series_with_no_temporal_bounds() {
    let dir = tempdir().expect("tempdir");
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create remote");

    let schema = i64_string_schema();
    let rows: Vec<(i64, &str)> = vec![(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")];
    let fixture = build_table_pack(&schema, &rows, &[2, 4], &[3, 3]);
    publish_table(&mut remote, &fixture).await;

    let _ = push_root(
        &mut remote,
        &[(
            "series.tbl",
            EntryType::TablePhysicalSeries,
            fixture.series_hash,
        )],
        Vec::new(),
    )
    .await;

    let graph = fetch_object_graph(&remote, "main")
        .await
        .expect("gate-4 fetch verification only checks internal pack self-consistency");

    let target_dir = tempdir().expect("tempdir");
    let mut target = Ship::create_pond(target_dir.path().join("pond"), "target")
        .await
        .expect("create target pond");
    let version_before = target.data_persistence().table().version();

    let err = rebuild_pond(&mut target, &remote, &graph)
        .await
        .expect_err("materialization must reject a table series with no temporal bounds");
    let msg = format!("{err}");
    assert!(
        msg.contains("bounds") || msg.contains("event_time") || msg.contains("temporal"),
        "expected a clear temporal-bounds diagnostic, got: {msg}"
    );
    assert_eq!(
        target.data_persistence().table().version(),
        version_before,
        "a rejected materialization must not advance the destination table"
    );
}
