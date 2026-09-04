// SPDX-License-Identifier: Apache-2.0

//! Integration tests for [`ContentRemote`]'s pack-advertisement namespace
//! (`docs/logical-series-identity-design.md` delivery gate 3): the
//! non-logical `_packs/series=<hex>/pack=<hex>` discovery protocol.

use sync_store::ContentRemote;
use sync_store::content::{
    ObjectHash, PackIndex, PackLeafDescriptor, PayloadKind, SeriesManifest, generate_range_proof,
    merkle_root,
};
use tempfile::TempDir;
use uuid::Uuid;

fn pid() -> Uuid {
    Uuid::from_u128(0xc0_0000_0000_0000_0000_0000_0000_0000)
}

fn h(s: &str) -> ObjectHash {
    ObjectHash::of_bytes(s.as_bytes())
}

/// Build one [`PackLeafDescriptor`] per leaf in `[start, end)`, each
/// declaring a logical count of `7` (matching this test module's fixtures,
/// which always give every leaf the same aggregate-count-per-leaf shape) and
/// no bounds or attributes.
fn descriptors(start: usize, end: usize) -> Vec<PackLeafDescriptor> {
    (start..end)
        .map(|_| PackLeafDescriptor::new(7, None, None, None).unwrap())
        .collect()
}

/// Build a `(series_hash, PackIndex, physical_blobs)` triple: a series
/// manifest over `leaf_labels`, and a pack index covering the whole range
/// with one physical blob whose bytes are `blob_label`.
fn build_series_and_pack(
    leaf_labels: &[&str],
    blob_label: &str,
) -> (ObjectHash, PackIndex, Vec<(ObjectHash, Vec<u8>)>) {
    let leaves: Vec<ObjectHash> = leaf_labels.iter().map(|s| h(s)).collect();
    let root = merkle_root(&leaves);
    let manifest = SeriesManifest::new(
        PayloadKind::File,
        leaves.len() as u64 * 7,
        leaves.len() as u64,
        None,
        None,
        None,
        root,
    )
    .unwrap();
    let series_hash = manifest.hash();
    let proof = generate_range_proof(&leaves, 0, leaves.len()).unwrap();
    let blob_bytes = format!("physical blob: {blob_label}").into_bytes();
    let blob_hash = ObjectHash::of_bytes(&blob_bytes);
    let pack = PackIndex::new(
        series_hash,
        0,
        leaves.len() as u64,
        leaves.len() as u64,
        root,
        proof,
        vec![blob_hash],
        leaves.len() as u64 * 7,
        blob_bytes.len() as u64,
        descriptors(0, leaves.len()),
    )
    .unwrap();
    (series_hash, pack, vec![(blob_hash, blob_bytes)])
}

/// Build a two-pack layout splitting the same series at `split`.
fn build_split_layout(
    leaf_labels: &[&str],
    split: usize,
    blob_label_a: &str,
    blob_label_b: &str,
) -> (ObjectHash, PackIndex, PackIndex, Vec<(ObjectHash, Vec<u8>)>) {
    let leaves: Vec<ObjectHash> = leaf_labels.iter().map(|s| h(s)).collect();
    let root = merkle_root(&leaves);
    let manifest = SeriesManifest::new(
        PayloadKind::File,
        leaves.len() as u64 * 7,
        leaves.len() as u64,
        None,
        None,
        None,
        root,
    )
    .unwrap();
    let series_hash = manifest.hash();

    let blob_a_bytes = format!("physical blob: {blob_label_a}").into_bytes();
    let blob_a_hash = ObjectHash::of_bytes(&blob_a_bytes);
    let proof_a = generate_range_proof(&leaves, 0, split).unwrap();
    let pack_a = PackIndex::new(
        series_hash,
        0,
        split as u64,
        leaves.len() as u64,
        root,
        proof_a,
        vec![blob_a_hash],
        split as u64 * 7,
        blob_a_bytes.len() as u64,
        descriptors(0, split),
    )
    .unwrap();

    let blob_b_bytes = format!("physical blob: {blob_label_b}").into_bytes();
    let blob_b_hash = ObjectHash::of_bytes(&blob_b_bytes);
    let proof_b = generate_range_proof(&leaves, split, leaves.len()).unwrap();
    let pack_b = PackIndex::new(
        series_hash,
        split as u64,
        leaves.len() as u64,
        leaves.len() as u64,
        root,
        proof_b,
        vec![blob_b_hash],
        (leaves.len() - split) as u64 * 7,
        blob_b_bytes.len() as u64,
        descriptors(split, leaves.len()),
    )
    .unwrap();

    (
        series_hash,
        pack_a,
        pack_b,
        vec![(blob_a_hash, blob_a_bytes), (blob_b_hash, blob_b_bytes)],
    )
}

/// Publishing a pack index makes it discoverable via one prefix listing and
/// fetchable by its exact series-scoped key, and its bytes round-trip
/// byte-for-byte.
#[tokio::test]
async fn publish_then_list_and_fetch_round_trips() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (series_hash, pack, blobs) = build_series_and_pack(&["a", "b", "c", "d"], "one");
    let pack_hash = remote
        .publish_pack(series_hash, &pack, &blobs)
        .await
        .unwrap();
    assert_eq!(pack_hash, pack.hash());

    let listed = remote.list_pack_hashes(series_hash).await.unwrap();
    assert_eq!(listed, std::collections::HashSet::from([pack_hash]));

    let fetched = remote
        .get_pack_index_bytes(series_hash, pack_hash)
        .await
        .unwrap()
        .expect("pack advertisement must be present");
    assert_eq!(fetched, pack.encode());
}

/// A pack index whose declared physical blob was never uploaded, and is not
/// already present, is refused: the index never becomes visible, proving the
/// "blobs first, index last" ordering rather than merely asserting it.
#[tokio::test]
async fn index_not_visible_when_referenced_blob_absent() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (series_hash, pack, _blobs) = build_series_and_pack(&["a", "b", "c"], "missing");
    let err = remote
        .publish_pack(series_hash, &pack, &[])
        .await
        .expect_err("publish must fail: the declared blob was never uploaded");
    assert!(
        format!("{err}").contains("not present"),
        "unexpected error: {err}"
    );

    assert!(
        remote
            .list_pack_hashes(series_hash)
            .await
            .unwrap()
            .is_empty(),
        "an incompletely-published pack must not be listed"
    );
    assert_eq!(
        remote
            .get_pack_index_bytes(series_hash, pack.hash())
            .await
            .unwrap(),
        None,
        "an incompletely-published pack must not be fetchable"
    );
}

/// Item 3 (`docs/logical-series-identity-design.md`): passing a physical
/// object's hash in `known_present` lets `publish_pack_with_known_present`
/// publish successfully even though that hash was never actually uploaded
/// and is not otherwise present on the remote -- directly proving the
/// exact-presence re-check is skipped for a hash the caller vouches for,
/// not merely that publication "still works" in the ordinary case where the
/// object happens to be present anyway.
#[tokio::test]
async fn known_present_hash_skips_the_exact_presence_recheck() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (series_hash, pack, blobs) = build_series_and_pack(&["a", "b", "c"], "vouched");
    let (blob_hash, _blob_bytes) = &blobs[0];

    // Without known_present, this is exactly `index_not_visible_when_referenced_blob_absent`:
    // the pack is refused because its one declared physical object was never
    // uploaded.
    let err = remote
        .publish_pack(series_hash, &pack, &[])
        .await
        .expect_err("without proof, an unpresent declared object must be refused");
    assert!(format!("{err}").contains("not present"));

    // With the same (still-never-uploaded) hash named in `known_present`,
    // publication succeeds: the caller's proof is trusted instead of being
    // re-verified, and the pack becomes visible.
    let known_present = std::collections::HashSet::from([*blob_hash]);
    let pack_hash = remote
        .publish_pack_with_known_present(series_hash, &pack, &[], &known_present)
        .await
        .expect("known_present hash must be trusted, not re-probed");
    assert_eq!(pack_hash, pack.hash());

    let listed = remote.list_pack_hashes(series_hash).await.unwrap();
    assert_eq!(listed, std::collections::HashSet::from([pack_hash]));

    // The pack is visible, but -- because the caller's proof was never
    // actually true here -- the physical object it names genuinely still
    // does not exist; `known_present` is a caller-supplied waiver of the
    // recheck, not a magic upload.
    assert!(!remote.has_blob(*blob_hash).await.unwrap());
    assert!(remote.get_object(*blob_hash).await.unwrap().is_none());
}

/// A hash the pack declares but that is absent from BOTH the store and
/// `known_present` is still checked exactly and refused: `known_present`
/// narrows re-probing to hashes it actually names, it never widens trust to
/// every physical object a pack declares.
#[tokio::test]
async fn known_present_does_not_cover_hashes_it_does_not_name() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (series_hash, pack, _blobs) = build_series_and_pack(&["a", "b"], "unvouched");

    // known_present names some OTHER, unrelated hash -- not the pack's own
    // declared physical object.
    let unrelated = ObjectHash::of_bytes(b"an unrelated object hash");
    let known_present = std::collections::HashSet::from([unrelated]);

    let err = remote
        .publish_pack_with_known_present(series_hash, &pack, &[], &known_present)
        .await
        .expect_err("a declared object not covered by known_present must still be checked exactly");
    assert!(
        format!("{err}").contains("not present"),
        "unexpected error: {err}"
    );
    assert!(
        remote
            .list_pack_hashes(series_hash)
            .await
            .unwrap()
            .is_empty(),
        "an incompletely-published pack must not be listed"
    );
}

/// Republishing the identical pack index (with its blob already present) is
/// idempotent: no error, and exactly one advertisement is ever listed.
#[tokio::test]
async fn republish_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (series_hash, pack, blobs) = build_series_and_pack(&["a", "b"], "idem");
    let first = remote
        .publish_pack(series_hash, &pack, &blobs)
        .await
        .unwrap();
    let second = remote
        .publish_pack(series_hash, &pack, &blobs)
        .await
        .unwrap();
    assert_eq!(first, second);

    let listed = remote.list_pack_hashes(series_hash).await.unwrap();
    assert_eq!(listed.len(), 1, "republishing must not duplicate entries");

    // Idempotent even when the caller no longer supplies the blob bytes,
    // since the blob is already durable.
    remote
        .publish_pack(series_hash, &pack, &[])
        .await
        .expect("republish without blob bytes succeeds once the blob is present");
}

/// Two independently verified pack layouts for the same series coexist and
/// both remain discoverable/fetchable -- a clean replica may reconstruct
/// from either.
#[tokio::test]
async fn two_layouts_for_one_series_coexist_and_are_discoverable() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (series_hash, whole, whole_blobs) = build_series_and_pack(&["a", "b", "c", "d"], "whole");
    let whole_hash = remote
        .publish_pack(series_hash, &whole, &whole_blobs)
        .await
        .unwrap();

    let (series_hash_2, half_a, half_b, split_blobs) =
        build_split_layout(&["a", "b", "c", "d"], 2, "half-a", "half-b");
    assert_eq!(series_hash, series_hash_2, "same logical series");
    let half_a_hash = remote
        .publish_pack(series_hash, &half_a, &split_blobs)
        .await
        .unwrap();
    let half_b_hash = remote
        .publish_pack(series_hash, &half_b, &split_blobs)
        .await
        .unwrap();

    let listed = remote.list_pack_hashes(series_hash).await.unwrap();
    assert_eq!(
        listed,
        std::collections::HashSet::from([whole_hash, half_a_hash, half_b_hash])
    );
    for hash in [whole_hash, half_a_hash, half_b_hash] {
        assert!(
            remote
                .get_pack_index_bytes(series_hash, hash)
                .await
                .unwrap()
                .is_some(),
            "pack {hash} must be fetchable"
        );
    }
}

/// Publishing a pack advertises no logical state: the Delta table version
/// and every ref stay exactly where they were.
#[tokio::test]
async fn publication_does_not_change_delta_version_or_refs() {
    let dir = TempDir::new().unwrap();
    let mut remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (h_commit, commit) = (ObjectHash::of_bytes(b"a commit"), b"a commit".to_vec());
    let _ = remote
        .push_commit(&[(h_commit, commit)], "main", h_commit)
        .await
        .unwrap();

    let version_before = remote.delta_version();
    let tip_before = remote.get_tip("main").await.unwrap();

    let (series_hash, pack, blobs) = build_series_and_pack(&["a", "b"], "no-ref-change");
    let _ = remote
        .publish_pack(series_hash, &pack, &blobs)
        .await
        .unwrap();

    assert_eq!(
        remote.delta_version(),
        version_before,
        "publishing a pack must not create a Delta commit"
    );
    assert_eq!(
        remote.get_tip("main").await.unwrap(),
        tip_before,
        "publishing a pack must not move any ref"
    );
}

/// `publish_pack` refuses a pack index that declares a different
/// `series_hash` than the series directory it is asked to publish under.
#[tokio::test]
async fn publish_rejects_cross_series_pack() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (series_a, _pack_a, _blobs_a) = build_series_and_pack(&["a", "b"], "series-a");
    let (_series_b, pack_b, blobs_b) = build_series_and_pack(&["x", "y", "z"], "series-b");

    let err = remote
        .publish_pack(series_a, &pack_b, &blobs_b)
        .await
        .expect_err("a pack declaring a different series_hash must be refused");
    assert!(
        format!("{err}").contains("cross-series"),
        "unexpected error: {err}"
    );
    assert!(remote.list_pack_hashes(series_a).await.unwrap().is_empty());
}

/// A stray file under a series' `_packs` directory that is not a
/// `pack=<hex>` key is rejected by listing rather than silently skipped.
#[tokio::test]
async fn list_rejects_malformed_key() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (series_hash, pack, blobs) = build_series_and_pack(&["a", "b"], "malformed-sibling");
    let _ = remote
        .publish_pack(series_hash, &pack, &blobs)
        .await
        .unwrap();

    let series_dir = dir
        .path()
        .join("_packs")
        .join(format!("series={}", series_hash.to_hex()));
    std::fs::write(series_dir.join("not-a-pack-key"), b"junk").unwrap();

    let err = remote
        .list_pack_hashes(series_hash)
        .await
        .expect_err("a malformed sibling key must be rejected, not skipped");
    assert!(
        format!("{err}").contains("malformed"),
        "unexpected error: {err}"
    );
}

/// A pack advertisement whose stored bytes do not hash to the key naming it
/// is rejected as a content-address mismatch.
#[tokio::test]
async fn fetch_rejects_hash_mismatch() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (series_hash, pack, _blobs) = build_series_and_pack(&["a", "b"], "hash-mismatch");
    let claimed_hash = pack.hash();
    let wrong_bytes = b"these are not the pack index bytes".to_vec();
    assert_ne!(ObjectHash::of_bytes(&wrong_bytes), claimed_hash);

    let series_dir = dir
        .path()
        .join("_packs")
        .join(format!("series={}", series_hash.to_hex()));
    std::fs::create_dir_all(&series_dir).unwrap();
    std::fs::write(
        series_dir.join(format!("pack={}", claimed_hash.to_hex())),
        &wrong_bytes,
    )
    .unwrap();

    let err = remote
        .get_pack_index_bytes(series_hash, claimed_hash)
        .await
        .expect_err("content-address mismatch must be rejected");
    assert!(
        format!("{err}").contains("content-address mismatch"),
        "unexpected error: {err}"
    );
}

/// A pack index that decodes fine but declares a different `series_hash`
/// than the directory it was found under is rejected as a cross-series
/// index, even though its own key hash is internally consistent.
#[tokio::test]
async fn fetch_rejects_cross_series_index() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let (series_a, _pack_a, _blobs_a) = build_series_and_pack(&["a", "b"], "true-series-a");
    let (_series_b, pack_b, _blobs_b) = build_series_and_pack(&["x", "y", "z"], "true-series-b");
    let pack_b_bytes = pack_b.encode();
    let pack_b_hash = ObjectHash::of_bytes(&pack_b_bytes);

    // Place series B's pack index bytes directly under series A's directory,
    // bypassing `publish_pack` (which would itself refuse this).
    let series_a_dir = dir
        .path()
        .join("_packs")
        .join(format!("series={}", series_a.to_hex()));
    std::fs::create_dir_all(&series_a_dir).unwrap();
    std::fs::write(
        series_a_dir.join(format!("pack={}", pack_b_hash.to_hex())),
        &pack_b_bytes,
    )
    .unwrap();

    let err = remote
        .get_pack_index_bytes(series_a, pack_b_hash)
        .await
        .expect_err("a pack index naming a foreign series must be rejected");
    assert!(
        format!("{err}").contains("cross-series"),
        "unexpected error: {err}"
    );
}

/// A pond with no published pack advertisements at all returns an empty
/// list, not an error: v1 series simply have nothing advertised yet.
#[tokio::test]
async fn no_advertisements_yields_empty_list_not_an_error() {
    let dir = TempDir::new().unwrap();
    let remote = ContentRemote::create_at(dir.path(), pid()).await.unwrap();

    let never_published = ObjectHash::of_bytes(b"a series with no packs");
    let listed = remote
        .list_pack_hashes(never_published)
        .await
        .expect("an empty pack namespace is not an error");
    assert!(listed.is_empty());
    assert_eq!(
        remote
            .get_pack_index_bytes(never_published, ObjectHash::of_bytes(b"anything"))
            .await
            .unwrap(),
        None
    );
}
