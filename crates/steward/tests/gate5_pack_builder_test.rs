// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for the gate-5 pack builder/repacker
//! (`docs/logical-series-identity-design.md` delivery gate 5):
//! `sync_store::content::{build_file_pack, build_table_pack}` produce
//! packs that publish through [`ContentRemote::publish_pack`] to clean
//! remotes and fetch through the existing gate-4 dual reader
//! (`steward::fetch_object_graph`), proving that two different builder-
//! chosen physical layouts of the *same* logical series fetch to identical
//! ordered verified leaf hashes and an identical manifest while their
//! physical object sets differ -- "repacking does not change series
//! identity" (design doc invariant 2), now demonstrated with the pack
//! builder itself rather than by hand-encoding pack bytes.
//!
//! This test never wires the builder into `pond maintain`,
//! `Ship::collapse_versions`, tlogfs, or any real v2 write/materialization
//! path: it only proves the builder's output is publishable and fetchable
//! by the existing remote/reader machinery. `FetchedSeriesV2` stores
//! recomputed *hashes*, not decoded rows/bytes, so this test compares
//! `leaf_hashes`/`manifest` across layouts; separate `sync-store` unit
//! tests (`content::series_pack_builder::tests`) decode the builder's own
//! Parquet/file outputs directly to prove row/byte content is identical
//! across layouts.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use tempfile::tempdir;
use uuid::Uuid;

use steward::{FetchedObject, fetch_object_graph};
use sync_store::ContentRemote;
use sync_store::content::{
    BuiltSeriesPack, Commit, ContentModelVersion, FileLeafInput, FilePackLayout, ManifestEntry,
    ObjectHash, PayloadKind, Provenance, SeriesManifest, TableLeafInput, TablePackLayout,
    TreeEntry, build_file_pack, build_table_pack, encode_manifest, encode_tree,
    manifest_hash as sync_manifest_hash, node_merkle_rebuild_root, tree_hash,
};
use tinyfs::EntryType;

fn pid() -> Uuid {
    Uuid::from_u128(0xf11e_0005_0000_0000_0000_0000_0000_0000)
}

/// Push a fresh root commit whose directory holds exactly one series entry
/// named `series_name`, together with every object in `objects`. Mirrors
/// `content_pull_v2_test.rs`'s `push_root` helper, narrowed to the single
/// series entry this test needs.
async fn push_series_root(
    remote: &mut ContentRemote,
    series_name: &str,
    entry_type: EntryType,
    series_hash: ObjectHash,
    mut objects: Vec<(ObjectHash, Vec<u8>)>,
) -> ObjectHash {
    let tree_entries = vec![TreeEntry::bare(series_name, entry_type, series_hash)];
    let tree_bytes = encode_tree(&tree_entries).expect("encode tree");
    let root_hash = tree_hash(&tree_entries).expect("tree hash");

    let manifest_entries = vec![
        ManifestEntry::bare("root", "", "", EntryType::DirectoryPhysical, root_hash),
        ManifestEntry::bare(
            format!("node-{series_name}"),
            "root",
            series_name,
            entry_type,
            series_hash,
        ),
    ];
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
            request: "gate-5 pack builder fixture".to_string(),
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

/// Publish `built` to a fresh, clean remote and seed its series manifest
/// object, then push a root commit naming it, returning the remote ready to
/// fetch from.
async fn publish_built_pack(
    dir: &tempfile::TempDir,
    series_name: &str,
    entry_type: EntryType,
    manifest: &SeriesManifest,
    manifest_hash: ObjectHash,
    built: &BuiltSeriesPack,
) -> ContentRemote {
    let mut remote = ContentRemote::create_at(dir.path(), pid())
        .await
        .expect("create clean remote");
    let published = remote
        .publish_pack(manifest_hash, &built.index, &built.physical_objects)
        .await
        .expect("publish builder-produced pack");
    assert_eq!(published, built.index.hash());
    let _ = push_series_root(
        &mut remote,
        series_name,
        entry_type,
        manifest_hash,
        vec![(manifest_hash, manifest.encode())],
    )
    .await;
    remote
}

fn i64_string_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]))
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

/// Two builder-produced file-pack layouts of the same logical series,
/// published to two independent clean remotes, fetch to identical verified
/// leaf hashes and an identical manifest -- while their physical object
/// sets differ.
#[tokio::test]
async fn file_series_repack_layouts_fetch_identical_logical_content() {
    let bytes = b"the quick brown fox jumps over the lazy dog!!".to_vec();
    // Leaves at [10, 12, 24]; neither boundary lines up with either
    // layout's object boundary below.
    let leaf_lens = [10usize, 12, 23];
    assert_eq!(leaf_lens.iter().sum::<usize>(), bytes.len());

    let mut leaves = Vec::with_capacity(leaf_lens.len());
    let mut offset = 0usize;
    for &len in &leaf_lens {
        let slice = bytes[offset..offset + len].to_vec();
        leaves.push(FileLeafInput::new(slice, None, None, None).expect("file leaf input"));
        offset += len;
    }
    let leaf_hashes: Vec<ObjectHash> = leaves.iter().map(FileLeafInput::leaf_hash).collect();
    let root = sync_store::content::merkle_root(&leaf_hashes);
    let manifest = SeriesManifest::new(
        PayloadKind::File,
        None,
        bytes.len() as u64,
        leaf_lens.len() as u64,
        None,
        None,
        None,
        root,
    )
    .expect("valid manifest");
    let manifest_hash = manifest.hash();

    // Layout A: one object for the whole series.
    let layout_a = FilePackLayout::new(1000).expect("layout a");
    let built_a = build_file_pack(
        manifest_hash,
        &manifest,
        &leaf_hashes,
        0,
        &leaves,
        &layout_a,
    )
    .expect("build layout a");
    // Layout B: a small cap that splits leaves unevenly across many objects.
    let layout_b = FilePackLayout::new(7).expect("layout b");
    let built_b = build_file_pack(
        manifest_hash,
        &manifest,
        &leaf_hashes,
        0,
        &leaves,
        &layout_b,
    )
    .expect("build layout b");

    assert_eq!(built_a.physical_objects.len(), 1);
    assert!(built_b.physical_objects.len() > 1);
    let object_hashes_a: std::collections::HashSet<_> =
        built_a.physical_objects.iter().map(|(h, _)| *h).collect();
    let object_hashes_b: std::collections::HashSet<_> =
        built_b.physical_objects.iter().map(|(h, _)| *h).collect();
    assert_ne!(
        object_hashes_a, object_hashes_b,
        "the two layouts must choose different physical object sets"
    );
    assert_ne!(
        built_a.index.hash(),
        built_b.index.hash(),
        "different physical layouts must produce different pack hashes"
    );

    let dir_a = tempdir().expect("tempdir a");
    let remote_a = publish_built_pack(
        &dir_a,
        "series.dat",
        EntryType::FilePhysicalSeries,
        &manifest,
        manifest_hash,
        &built_a,
    )
    .await;
    let dir_b = tempdir().expect("tempdir b");
    let remote_b = publish_built_pack(
        &dir_b,
        "series.dat",
        EntryType::FilePhysicalSeries,
        &manifest,
        manifest_hash,
        &built_b,
    )
    .await;

    let graph_a = fetch_object_graph(&remote_a, "main")
        .await
        .expect("fetch graph a");
    let graph_b = fetch_object_graph(&remote_b, "main")
        .await
        .expect("fetch graph b");

    let v2_a = match graph_a.objects.get(&manifest_hash) {
        Some(FetchedObject::SeriesV2(v2)) => v2.as_ref(),
        other => panic!("expected SeriesV2 in graph a, got {other:?}"),
    };
    let v2_b = match graph_b.objects.get(&manifest_hash) {
        Some(FetchedObject::SeriesV2(v2)) => v2.as_ref(),
        other => panic!("expected SeriesV2 in graph b, got {other:?}"),
    };

    // Identical logical identity: same manifest, same ordered verified leaf
    // hashes, regardless of layout.
    assert_eq!(v2_a.manifest_hash, manifest_hash);
    assert_eq!(v2_b.manifest_hash, manifest_hash);
    assert_eq!(v2_a.manifest, manifest);
    assert_eq!(v2_b.manifest, manifest);
    assert_eq!(v2_a.manifest, v2_b.manifest);
    assert_eq!(v2_a.leaf_hashes, leaf_hashes);
    assert_eq!(v2_b.leaf_hashes, leaf_hashes);
    assert_eq!(v2_a.leaf_hashes, v2_b.leaf_hashes);

    // Different physical layout: different physical object sets fetched.
    let fetched_physical_a: std::collections::HashSet<_> =
        v2_a.physical_object_hashes.iter().copied().collect();
    let fetched_physical_b: std::collections::HashSet<_> =
        v2_b.physical_object_hashes.iter().copied().collect();
    assert_ne!(fetched_physical_a, fetched_physical_b);
    assert_eq!(fetched_physical_a, object_hashes_a);
    assert_eq!(fetched_physical_b, object_hashes_b);
}

/// Two builder-produced table-pack layouts of the same logical series,
/// published to two independent clean remotes, fetch to identical verified
/// leaf hashes and an identical manifest -- while their physical (Parquet)
/// object sets differ.
#[tokio::test]
async fn table_series_repack_layouts_fetch_identical_logical_content() {
    let schema = i64_string_schema();
    let rows: Vec<(i64, &str)> = (1..=20).map(|i| (i, "row")).collect();
    let leaf_row_counts = [5usize, 7, 8];
    assert_eq!(leaf_row_counts.iter().sum::<usize>(), rows.len());

    let fingerprint = sync_store::content::schema_fingerprint(&schema).expect("fingerprint");
    let mut leaves = Vec::with_capacity(leaf_row_counts.len());
    let mut offset = 0usize;
    for &count in &leaf_row_counts {
        let slice = &rows[offset..offset + count];
        let ids: Vec<i64> = slice.iter().map(|(id, _)| *id).collect();
        let labels: Vec<&str> = slice.iter().map(|(_, l)| *l).collect();
        let b = batch(&schema, &ids, &labels);
        leaves.push(
            TableLeafInput::new(schema.clone(), vec![b], None, None, None)
                .expect("table leaf input"),
        );
        offset += count;
    }
    let leaf_hashes: Vec<ObjectHash> = leaves.iter().map(TableLeafInput::leaf_hash).collect();
    let root = sync_store::content::merkle_root(&leaf_hashes);
    let manifest = SeriesManifest::new(
        PayloadKind::Table,
        Some(fingerprint),
        rows.len() as u64,
        leaf_row_counts.len() as u64,
        None,
        None,
        None,
        root,
    )
    .expect("valid manifest");
    let manifest_hash = manifest.hash();

    // Layout A: one object for the whole series.
    let layout_a = TablePackLayout::new(1000).expect("layout a");
    let built_a = build_table_pack(
        manifest_hash,
        &manifest,
        &leaf_hashes,
        0,
        &leaves,
        &layout_a,
    )
    .expect("build layout a");
    // Layout B: a small row cap that splits leaves across many objects.
    let layout_b = TablePackLayout::new(4).expect("layout b");
    let built_b = build_table_pack(
        manifest_hash,
        &manifest,
        &leaf_hashes,
        0,
        &leaves,
        &layout_b,
    )
    .expect("build layout b");

    assert_eq!(built_a.physical_objects.len(), 1);
    assert!(built_b.physical_objects.len() > 1);
    let object_hashes_a: std::collections::HashSet<_> =
        built_a.physical_objects.iter().map(|(h, _)| *h).collect();
    let object_hashes_b: std::collections::HashSet<_> =
        built_b.physical_objects.iter().map(|(h, _)| *h).collect();
    assert_ne!(
        object_hashes_a, object_hashes_b,
        "the two layouts must choose different physical object sets"
    );
    assert_ne!(built_a.index.hash(), built_b.index.hash());

    let dir_a = tempdir().expect("tempdir a");
    let remote_a = publish_built_pack(
        &dir_a,
        "series.parquet",
        EntryType::TablePhysicalSeries,
        &manifest,
        manifest_hash,
        &built_a,
    )
    .await;
    let dir_b = tempdir().expect("tempdir b");
    let remote_b = publish_built_pack(
        &dir_b,
        "series.parquet",
        EntryType::TablePhysicalSeries,
        &manifest,
        manifest_hash,
        &built_b,
    )
    .await;

    let graph_a = fetch_object_graph(&remote_a, "main")
        .await
        .expect("fetch graph a");
    let graph_b = fetch_object_graph(&remote_b, "main")
        .await
        .expect("fetch graph b");

    let v2_a = match graph_a.objects.get(&manifest_hash) {
        Some(FetchedObject::SeriesV2(v2)) => v2.as_ref(),
        other => panic!("expected SeriesV2 in graph a, got {other:?}"),
    };
    let v2_b = match graph_b.objects.get(&manifest_hash) {
        Some(FetchedObject::SeriesV2(v2)) => v2.as_ref(),
        other => panic!("expected SeriesV2 in graph b, got {other:?}"),
    };

    // Identical logical identity across layouts. Note: `FetchedSeriesV2`
    // stores recomputed leaf *hashes*, never decoded rows -- this is not a
    // claim that the fetched graph holds row content, only that its
    // verified hashes agree. `sync-store`'s own
    // `content::series_pack_builder::tests` decode the builder's Parquet
    // bytes directly to prove row content and order are identical across
    // layouts.
    assert_eq!(v2_a.manifest, manifest);
    assert_eq!(v2_b.manifest, manifest);
    assert_eq!(v2_a.manifest, v2_b.manifest);
    assert_eq!(v2_a.leaf_hashes, leaf_hashes);
    assert_eq!(v2_b.leaf_hashes, leaf_hashes);
    assert_eq!(v2_a.leaf_hashes, v2_b.leaf_hashes);

    let fetched_physical_a: std::collections::HashSet<_> =
        v2_a.physical_object_hashes.iter().copied().collect();
    let fetched_physical_b: std::collections::HashSet<_> =
        v2_b.physical_object_hashes.iter().copied().collect();
    assert_ne!(fetched_physical_a, fetched_physical_b);
    assert_eq!(fetched_physical_a, object_hashes_a);
    assert_eq!(fetched_physical_b, object_hashes_b);
}
