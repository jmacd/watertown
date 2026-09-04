// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Content-graph fetch: the consumer side of the content-addressed remote
//! (design Section 8.5, Fork 2).
//!
//! This module implements the *fetch walk*: given a [`ContentSource`] and its
//! tip commit, descend the object graph by `child_hash` and collect the
//! reachable, verified object closure.  It does no tlogfs rebuild yet; it
//! produces the in-memory [`FetchedGraph`] a rebuild consumes, and it is the
//! point at which content addressing is checked: every fetched object's bytes
//! are re-hashed and must equal the key they were fetched under.
//!
//! Descent is driven by [`EntryType`], exactly mirroring the producer's fold
//! (Section 9): physical directories are tree objects whose entries are
//! recursed into; physical files and symlinks are leaf blobs; series are
//! series objects whose version blobs are leaves; dynamic and computed nodes
//! are recipe leaves whose generated children are not in the graph.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::ChunkReader;

use crate::content_source::ContentSource;
use sync_store::content::{
    Commit, IncrementalFileLeafHasher, ManifestEntry, ObjectHash, PackIndex, PackLeafDescriptor,
    PayloadKind, SeriesManifest, TreeEntry, VersionMeta, decode_manifest, decode_recipe,
    decode_tree, effective_leaf_schema_fingerprint, encode_table_leaf_parquet, schema_fingerprint,
    select_exact_cover, table_leaf_hash_canonical, verify_pack_against_manifest,
};
use tinyfs::{EntryType, NodeID, WD};
use tlogfs::PondUserMetadata;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{Ship, StewardError};

/// A fetched content object, in the structured form a rebuild needs, alongside
/// its exact bytes (kept so the rebuild can write file content and re-verify).
#[derive(Debug, Clone)]
pub enum FetchedObject {
    /// A directory: its decoded, canonical-order entries.
    Tree(Vec<TreeEntry>),
    /// A leaf blob: a file version's bytes, a symlink target, or recipe bytes.
    Blob(Vec<u8>),
    /// A large leaf blob that lives out-of-row in the remote blob store and is
    /// deliberately *not* buffered (Decision D7).  Its bytes are streamed from
    /// the remote straight into the local writer at rebuild time, keyed by this
    /// object's hash; only its presence is recorded here.
    External,
    /// A verified `watertown.series.v2` logical series
    /// (`docs/logical-series-identity-design.md` delivery gate 4).
    ///
    /// By the time this variant exists in [`FetchedGraph::objects`], the
    /// series manifest, every advertised pack candidate, every physical
    /// object the selected exact cover names, and every logical leaf hash
    /// recomputed from real decoded content have all been fetched and
    /// cryptographically verified against each other (see
    /// [`fetch_series_v2`]). Planning/apply code dispatches this variant to
    /// native materialization ([`plan_series_v2_leaves`]/
    /// [`materialize_series_v2`]).
    SeriesV2(Box<FetchedSeriesV2>),
}

/// The immutable, verified state of one fetched `watertown.series.v2` logical series.
///
/// Every field here has already been cryptographically bound to every other:
/// `leaf_hashes` were recomputed from the real decoded content of
/// `physical_object_hashes` (per the packs in `packs`), each pack was checked
/// with [`verify_pack_against_manifest`] against `manifest`, and `manifest`
/// is the object that hashed to `manifest_hash` (the same hash the owning
/// `TreeEntry.child_hash` named). See [`fetch_series_v2`] for exactly how
/// this is built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedSeriesV2 {
    /// The `watertown.series.v2` object's own content address -- the hash the owning
    /// tree entry's `child_hash` named.
    pub manifest_hash: ObjectHash,
    /// The decoded series manifest.
    pub manifest: SeriesManifest,
    /// The selected exact-cover packs, `(pack_hash, decoded PackIndex)`, in
    /// increasing leaf-range order (as returned by
    /// [`select_exact_cover`]) -- together they exactly tile
    /// `[0, manifest.leaf_count())` with no gap and no overlap.
    pub packs: Vec<(ObjectHash, PackIndex)>,
    /// Every logical leaf's identity hash, in leaf order across the whole
    /// series (`0..manifest.leaf_count()`), recomputed from the selected
    /// packs' real decoded physical content -- not merely copied from any
    /// pack's own declared data.
    pub leaf_hashes: Vec<ObjectHash>,
    /// Every physical object hash the selected packs name, in first-seen
    /// order across `packs`, deduplicated. Each one is also present in
    /// [`FetchedGraph::objects`] as a [`FetchedObject::Blob`] or
    /// [`FetchedObject::External`] entry, so the materializer can reuse the
    /// inline/external adoption path apply already uses.
    pub physical_object_hashes: Vec<ObjectHash>,
}

/// The verified object closure reachable from a remote tip commit.
#[derive(Debug, Clone, Default)]
pub struct FetchedGraph {
    /// The tip commit's hash.
    pub tip: Option<ObjectHash>,
    /// The commit chain from the tip back toward genesis, tip first, limited to
    /// commits present on the remote.
    pub commits: Vec<(ObjectHash, Commit)>,
    /// Every reachable object keyed by content hash.  Inline entries carry their
    /// bytes (verified to hash to the key); large external blobs are recorded as
    /// [`FetchedObject::External`] with no bytes.
    pub objects: BTreeMap<ObjectHash, FetchedObject>,
    /// Raw bytes of every fetched *inline* object, keyed by content hash.  Large
    /// external blobs are absent here by design -- they are never buffered.
    pub bytes: BTreeMap<ObjectHash, Vec<u8>>,
    /// Hashes of large leaf blobs that live in the remote blob store and are
    /// streamed rather than buffered (Decision D7).  Every hash here also has a
    /// [`FetchedObject::External`] entry in `objects`.
    pub external_blobs: BTreeSet<ObjectHash>,
    /// The tip commit's node manifest: one entry per node, recording the
    /// source's `node_id` alongside its parent, name, type, and content
    /// address (Section 4.5).  Empty when the graph is empty.  Kept out of
    /// `objects`/`bytes` because the manifest is pond-specific identity, not
    /// part of the dedup-shareable pure-content closure.
    pub manifest: Vec<ManifestEntry>,
}

impl FetchedGraph {
    /// The tip commit's root tree hash, or `None` if the graph is empty.
    #[must_use]
    pub fn root_tree_hash(&self) -> Option<ObjectHash> {
        self.commits.first().map(|(_, c)| c.root_tree_hash)
    }

    /// Total number of distinct objects fetched.
    #[must_use]
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// True if no objects were fetched.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

/// Fetch the verified object closure reachable from `ref_name`'s tip on
/// `remote`.
///
/// Returns an empty graph if the ref does not exist.  Otherwise fetches the tip
/// commit, walks its parent chain as far as the remote holds commits, and
/// descends the tip commit's root tree by `child_hash`, fetching every
/// reachable tree, blob, and series object exactly once.
///
/// # Errors
///
/// Returns an error if a referenced object is absent from the remote, if any
/// fetched object's bytes do not hash to the key it was fetched under, or if a
/// structured object fails to decode.
pub async fn fetch_object_graph(
    remote: &dyn ContentSource,
    ref_name: &str,
) -> Result<FetchedGraph, StewardError> {
    let Some(tip) = remote
        .get_tip(ref_name)
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?
    else {
        return Ok(FetchedGraph::default());
    };

    // Snapshot the whole `objects` partition once so every inline-object read
    // below is an in-memory lookup rather than a per-hash full-table Delta scan
    // (turns an O(objects x table-size) clone into a single scan).  The snapshot
    // is per-operation: clear it before returning so a later read or re-pull
    // never sees stale bytes.
    remote
        .preload_objects()
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?;

    let result = descend_from_tip(remote, tip).await;
    remote.clear_object_cache();
    result
}

/// Build the fetched graph from `tip`: walk the commit chain, then descend the
/// tip commit's root tree.  Assumes the caller has preloaded the object cache.
async fn descend_from_tip(
    remote: &dyn ContentSource,
    tip: ObjectHash,
) -> Result<FetchedGraph, StewardError> {
    let mut graph = FetchedGraph {
        tip: Some(tip),
        ..FetchedGraph::default()
    };

    // Walk the commit chain from the tip toward genesis, stopping at the first
    // commit the remote does not hold.
    let mut next = Some(tip);
    while let Some(commit_hash) = next {
        let Some(commit_bytes) = remote
            .get_object(commit_hash)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))?
        else {
            break;
        };
        verify(commit_hash, &commit_bytes)?;
        let commit = Commit::decode(&commit_bytes)
            .map_err(|e| StewardError::Content(format!("decode commit: {e}")))?;
        next = commit.parent_commit_hash;
        graph.commits.push((commit_hash, commit));
    }

    // Descend the tip commit's root tree, fetching the full reachable closure.
    if let Some((_, tip_commit)) = graph.commits.first() {
        let root = tip_commit.root_tree_hash;
        let manifest_hash = tip_commit.node_manifest_hash;
        // Populated on first use by `fetch_blob`, so a closure with no external
        // blobs never spends a request asking about them.
        let mut blob_index: Option<HashSet<ObjectHash>> = None;
        fetch_tree(remote, root, &mut graph, &mut blob_index).await?;
        graph.manifest = fetch_manifest(remote, manifest_hash).await?;
    }

    Ok(graph)
}

/// Fetch and decode the tip commit's node manifest, verifying its bytes hash to
/// the commit's `node_manifest_hash` (Section 4.5).
async fn fetch_manifest(
    remote: &dyn ContentSource,
    manifest_hash: ObjectHash,
) -> Result<Vec<ManifestEntry>, StewardError> {
    let bytes = fetch_verified(remote, manifest_hash).await?;
    decode_manifest(&bytes).map_err(|e| StewardError::Content(format!("decode manifest: {e}")))
}

/// Recursively fetch a tree object and everything reachable from its entries.
async fn fetch_tree(
    remote: &dyn ContentSource,
    tree_hash: ObjectHash,
    graph: &mut FetchedGraph,
    blob_index: &mut Option<HashSet<ObjectHash>>,
) -> Result<(), StewardError> {
    // Iterative worklist to avoid async recursion on the directory tree.
    let mut stack = vec![tree_hash];
    while let Some(hash) = stack.pop() {
        if graph.objects.contains_key(&hash) {
            continue;
        }
        let bytes = fetch_verified(remote, hash).await?;
        let entries =
            decode_tree(&bytes).map_err(|e| StewardError::Content(format!("decode tree: {e}")))?;
        let _ = graph
            .objects
            .insert(hash, FetchedObject::Tree(entries.clone()));
        let _ = graph.bytes.insert(hash, bytes);

        for entry in entries {
            match entry.entry_type {
                EntryType::DirectoryPhysical => stack.push(entry.child_hash),
                EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
                    fetch_series(
                        remote,
                        entry.child_hash,
                        entry.entry_type,
                        graph,
                        blob_index,
                    )
                    .await?;
                }
                EntryType::FilePhysicalVersion
                | EntryType::TablePhysicalVersion
                | EntryType::Symlink
                | EntryType::DirectoryDynamic
                | EntryType::FileDynamic
                | EntryType::TableDynamic => {
                    fetch_blob(remote, entry.child_hash, graph, blob_index).await?;
                }
            }
        }
    }
    Ok(())
}

/// Fetch a `watertown.series.v2` object and everything it names.
///
/// `entry_type` is the owning tree entry's declared kind
/// (`FilePhysicalSeries` or `TablePhysicalSeries`); it must
/// agree with the manifest's own [`PayloadKind`] (`docs/logical-series-
/// identity-design.md` delivery gate 4), since nothing else ties a
/// `watertown.series.v2` object's payload kind to the directory position naming it.
async fn fetch_series(
    remote: &dyn ContentSource,
    series_hash: ObjectHash,
    entry_type: EntryType,
    graph: &mut FetchedGraph,
    blob_index: &mut Option<HashSet<ObjectHash>>,
) -> Result<(), StewardError> {
    if let Some(existing) = graph.objects.get(&series_hash) {
        return match existing {
            FetchedObject::SeriesV2(series) => {
                let expected_kind = expected_payload_kind(entry_type);
                if series.manifest.payload_kind() != expected_kind {
                    Err(StewardError::Content(format!(
                        "series {series_hash} manifest declares payload kind {:?} but another tree \
                         entry is {entry_type:?} (expects {expected_kind:?})",
                        series.manifest.payload_kind()
                    )))
                } else {
                    Ok(())
                }
            }
            _ => Err(StewardError::Content(format!(
                "object {series_hash} was already fetched as a non-series object"
            ))),
        };
    }
    let bytes = fetch_verified(remote, series_hash).await?;
    let manifest = SeriesManifest::decode(&bytes)
        .map_err(|e| StewardError::Content(format!("decode series: {e}")))?;
    fetch_series_v2(
        remote,
        series_hash,
        entry_type,
        manifest,
        bytes,
        graph,
        blob_index,
    )
    .await
}

/// Map a series-carrying tree entry type to the [`PayloadKind`] its
/// `watertown.series.v2` manifest must declare.
///
/// Only ever called with a series entry type (the two callers -- [`fetch_tree`]'s
/// match arm and [`fetch_series`] -- both guarantee that), so any other value
/// is a caller bug rather than untrusted input.
fn expected_payload_kind(entry_type: EntryType) -> PayloadKind {
    match entry_type {
        EntryType::FilePhysicalSeries => PayloadKind::File,
        EntryType::TablePhysicalSeries => PayloadKind::Table,
        other => unreachable!("fetch_series is only called for series entry types, got {other:?}"),
    }
}

/// Fetch, discover, and fully verify a `watertown.series.v2` logical series
/// (`docs/logical-series-identity-design.md` delivery gate 4).
///
/// This is the heart of the dual reader's v2 side. It:
///
/// 1. checks `manifest.payload_kind()` against the owning tree entry's
///    declared type;
/// 2. lists every pack hash [`ContentSource::list_pack_hashes`] advertises
///    for this series, fetches and decodes each one (rejecting a malformed
///    or vanished candidate outright rather than silently skipping it);
/// 3. chooses a deterministic exact cover with [`select_exact_cover`];
/// 4. for every selected pack, in cover order, fetches every physical object
///    it names (registering each as an ordinary [`FetchedObject::Blob`] or
///    [`FetchedObject::External`] entry so a future materializer can reuse
///    them exactly as apply already reuses v1 version blobs), decodes and
///    concatenates them in order, partitions the result by the pack's
///    [`PackLeafDescriptor`]s, and recomputes every logical leaf hash from
///    that real content;
/// 5. checks each pack with [`verify_pack_against_manifest`];
/// 6. checks that the selected packs' logical counts sum to
///    `manifest.logical_count()` and that their descriptors' aggregate
///    event-time bounds agree with the manifest's own aggregate bounds.
///
/// Only after every one of those checks passes does this insert a
/// [`FetchedObject::SeriesV2`] into `graph` -- verification happens
/// completely before that insertion, never after.
async fn fetch_series_v2(
    remote: &dyn ContentSource,
    series_hash: ObjectHash,
    entry_type: EntryType,
    manifest: SeriesManifest,
    manifest_bytes: Vec<u8>,
    graph: &mut FetchedGraph,
    blob_index: &mut Option<HashSet<ObjectHash>>,
) -> Result<(), StewardError> {
    let expected_kind = expected_payload_kind(entry_type);
    if manifest.payload_kind() != expected_kind {
        return Err(StewardError::Content(format!(
            "series {series_hash} manifest declares payload kind {:?} but its tree entry is {entry_type:?} (expects {expected_kind:?})",
            manifest.payload_kind()
        )));
    }

    // Discover every advertised pack candidate. A candidate that vanishes
    // between listing and fetch, or that fails to decode, is a hard error:
    // discovery must never silently proceed with a partial candidate set,
    // since that could make an otherwise-uncoverable series appear to have
    // no valid cover, or could paper over a corrupt advertisement.
    let candidate_hashes = remote
        .list_pack_hashes(series_hash)
        .await
        .map_err(|e| StewardError::Content(format!("list packs for series {series_hash}: {e}")))?;
    let mut candidates: Vec<(ObjectHash, PackIndex)> = Vec::with_capacity(candidate_hashes.len());
    for pack_hash in candidate_hashes {
        let bytes = remote
            .get_pack_index(series_hash, pack_hash)
            .await
            .map_err(|e| {
                StewardError::Content(format!("fetch pack {pack_hash} for series {series_hash}: {e}"))
            })?
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "pack {pack_hash} was advertised for series {series_hash} but vanished before it could be fetched"
                ))
            })?;
        let computed = ObjectHash::of_bytes(&bytes);
        if computed != pack_hash {
            return Err(StewardError::Content(format!(
                "pack advertisement for series {series_hash} hashes to {computed} but was fetched as {pack_hash}"
            )));
        }
        let pack = PackIndex::decode(&bytes).map_err(|e| {
            StewardError::Content(format!(
                "decode pack {pack_hash} for series {series_hash}: {e}"
            ))
        })?;
        for (descriptor_index, descriptor) in pack.leaf_descriptors().iter().enumerate() {
            let _ = effective_leaf_schema_fingerprint(&manifest, &pack, descriptor).map_err(|e| {
                StewardError::Content(format!(
                    "pack {pack_hash} descriptor {descriptor_index} is incompatible with series \
                     {series_hash}: {e}"
                ))
            })?;
        }
        candidates.push((pack_hash, pack));
    }

    let selected_hashes = select_exact_cover(series_hash, manifest.leaf_count(), &candidates)
        .map_err(|e| {
            StewardError::Content(format!("select pack cover for series {series_hash}: {e}"))
        })?;
    let candidates_by_hash: HashMap<ObjectHash, PackIndex> = candidates.into_iter().collect();
    let mut selected_packs: Vec<(ObjectHash, PackIndex)> =
        Vec::with_capacity(selected_hashes.len());
    for pack_hash in selected_hashes {
        let pack = candidates_by_hash.get(&pack_hash).cloned().ok_or_else(|| {
            StewardError::Content(format!(
                "select_exact_cover chose pack {pack_hash} that was not among the fetched candidates \
                 (internal inconsistency)"
            ))
        })?;
        selected_packs.push((pack_hash, pack));
    }

    // Fetch, decode, and verify every selected pack's physical content in
    // cover order, recomputing real leaf hashes as we go.
    let mut all_leaf_hashes: Vec<ObjectHash> = Vec::with_capacity(manifest.leaf_count() as usize);
    let mut physical_object_hashes: Vec<ObjectHash> = Vec::new();
    let mut seen_physical: HashSet<ObjectHash> = HashSet::new();
    let mut total_logical: u64 = 0;

    for (pack_hash, pack) in &selected_packs {
        let leaf_hashes = match manifest.payload_kind() {
            PayloadKind::File => {
                fetch_and_verify_file_pack(remote, pack, graph, blob_index).await?
            }
            PayloadKind::Table => {
                fetch_and_verify_table_pack(remote, pack, &manifest, graph, blob_index).await?
            }
        };
        verify_pack_against_manifest(series_hash, &manifest, pack, &leaf_hashes).map_err(|e| {
            StewardError::Content(format!(
                "pack {pack_hash} failed verification against series {series_hash}: {e}"
            ))
        })?;
        all_leaf_hashes.extend(leaf_hashes);
        total_logical = total_logical.checked_add(pack.logical_count()).ok_or_else(|| {
            StewardError::Content(format!(
                "aggregate logical_count across selected packs for series {series_hash} overflows u64"
            ))
        })?;
        for &object_hash in pack.physical_object_hashes() {
            if seen_physical.insert(object_hash) {
                physical_object_hashes.push(object_hash);
            }
        }
    }

    if total_logical != manifest.logical_count() {
        return Err(StewardError::Content(format!(
            "series {series_hash}: selected packs cover {total_logical} logical unit(s) but the manifest declares logical_count {}",
            manifest.logical_count()
        )));
    }
    verify_aggregate_bounds(series_hash, &manifest, &selected_packs)?;

    let series_v2 = FetchedSeriesV2 {
        manifest_hash: series_hash,
        manifest,
        packs: selected_packs,
        leaf_hashes: all_leaf_hashes,
        physical_object_hashes,
    };
    let _ = graph
        .objects
        .insert(series_hash, FetchedObject::SeriesV2(Box::new(series_v2)));
    let _ = graph.bytes.insert(series_hash, manifest_bytes);
    Ok(())
}

/// Check that the aggregate event-time bounds derivable from every selected
/// pack's leaf descriptors agree with `manifest`'s own aggregate bounds
/// (`docs/logical-series-identity-design.md`: a manifest's bounds are the
/// aggregate minimum/maximum over every leaf that carried one). Because the
/// selected packs together exactly cover `[0, manifest.leaf_count())`, their
/// descriptors collectively describe every leaf in the series -- so this is
/// a real cross-check, not a subset comparison.
fn verify_aggregate_bounds(
    series_hash: ObjectHash,
    manifest: &SeriesManifest,
    selected_packs: &[(ObjectHash, PackIndex)],
) -> Result<(), StewardError> {
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;
    for (_, pack) in selected_packs {
        for descriptor in pack.leaf_descriptors() {
            if let Some(v) = descriptor.min_event_time() {
                min = Some(min.map_or(v, |cur| cur.min(v)));
            }
            if let Some(v) = descriptor.max_event_time() {
                max = Some(max.map_or(v, |cur| cur.max(v)));
            }
        }
    }
    if min != manifest.min_event_time() {
        return Err(StewardError::Content(format!(
            "series {series_hash}: selected packs' aggregate min_event_time {min:?} does not match manifest's {:?}",
            manifest.min_event_time()
        )));
    }
    if max != manifest.max_event_time() {
        return Err(StewardError::Content(format!(
            "series {series_hash}: selected packs' aggregate max_event_time {max:?} does not match manifest's {:?}",
            manifest.max_event_time()
        )));
    }
    Ok(())
}

/// Fetch, register, and return one physical pack object's location: either
/// its buffered bytes (inline) or a marker that it must be streamed
/// (external), exactly the same duality [`fetch_blob`] already records for
/// ordinary v1 version blobs -- reused here so pack physical objects are
/// available for a future materializer through the identical apply-time
/// path.
async fn fetch_physical_object(
    remote: &dyn ContentSource,
    hash: ObjectHash,
    graph: &mut FetchedGraph,
    blob_index: &mut Option<HashSet<ObjectHash>>,
) -> Result<(), StewardError> {
    fetch_blob(remote, hash, graph, blob_index).await
}

/// Fetch, decode, and verify one file-payload pack
/// (`docs/logical-series-identity-design.md` delivery gate 4): stream every
/// physical object it names, in order, feeding the concatenated bytes into
/// an [`IncrementalFileLeafHasher`]-per-leaf partitioner so a leaf that
/// crosses a physical-object boundary is handled transparently and no
/// physical object -- however large -- is ever buffered whole for an
/// external blob.
///
/// Returns the pack's recomputed leaf hashes in order, ready for
/// [`verify_pack_against_manifest`].
async fn fetch_and_verify_file_pack(
    remote: &dyn ContentSource,
    pack: &PackIndex,
    graph: &mut FetchedGraph,
    blob_index: &mut Option<HashSet<ObjectHash>>,
) -> Result<Vec<ObjectHash>, StewardError> {
    let mut partitioner = FileLeafPartitioner::new(pack.leaf_descriptors());
    let mut total_physical_bytes: u64 = 0;

    for &object_hash in pack.physical_object_hashes() {
        fetch_physical_object(remote, object_hash, graph, blob_index).await?;
        match graph.objects.get(&object_hash) {
            Some(FetchedObject::Blob(bytes)) => {
                total_physical_bytes += bytes.len() as u64;
                partitioner.feed(bytes)?;
            }
            Some(FetchedObject::External) => {
                let mut reader = remote
                    .get_blob_reader(object_hash)
                    .await
                    .map_err(|e| StewardError::Content(format!("stream physical object {object_hash}: {e}")))?
                    .ok_or_else(|| {
                        StewardError::Content(format!(
                            "physical object {object_hash} vanished from the remote blob store during streaming"
                        ))
                    })?;
                let mut hasher = blake3::Hasher::new();
                let mut buf = vec![0u8; 256 * 1024];
                loop {
                    let n = reader.read(&mut buf).await.map_err(|e| {
                        StewardError::Content(format!("read physical object {object_hash}: {e}"))
                    })?;
                    if n == 0 {
                        break;
                    }
                    let _ = hasher.update(&buf[..n]);
                    total_physical_bytes += n as u64;
                    partitioner.feed(&buf[..n])?;
                }
                let computed = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
                if computed != object_hash {
                    return Err(StewardError::Content(format!(
                        "physical object hashes to {computed} but was fetched as {object_hash}"
                    )));
                }
            }
            other => {
                return Err(StewardError::Content(format!(
                    "expected a physical blob object at {object_hash} but found {other:?}"
                )));
            }
        }
    }
    if total_physical_bytes != pack.physical_byte_count() {
        return Err(StewardError::Content(format!(
            "pack declares physical_byte_count {} but its physical objects total {total_physical_bytes} byte(s)",
            pack.physical_byte_count()
        )));
    }
    partitioner.finish()
}

/// Streaming partitioner for a file pack: consumes concatenated physical
/// bytes in arbitrary chunks (which may cross leaf boundaries either way --
/// several tiny leaves in one chunk, or one leaf spanning many chunks/objects)
/// and produces exactly one recomputed leaf hash per descriptor, in order.
struct FileLeafPartitioner<'a> {
    descriptors: std::slice::Iter<'a, PackLeafDescriptor>,
    current: Option<IncrementalFileLeafHasher>,
    hashes: Vec<ObjectHash>,
}

impl<'a> FileLeafPartitioner<'a> {
    fn new(descriptors: &'a [PackLeafDescriptor]) -> Self {
        Self {
            descriptors: descriptors.iter(),
            current: None,
            hashes: Vec::with_capacity(descriptors.len()),
        }
    }

    fn feed(&mut self, mut chunk: &[u8]) -> Result<(), StewardError> {
        while !chunk.is_empty() {
            if self.current.is_none() {
                let Some(descriptor) = self.descriptors.next() else {
                    return Err(StewardError::Content(
                        "file pack's physical content extends beyond its declared leaf descriptors (trailing bytes)"
                            .to_string(),
                    ));
                };
                let hasher = IncrementalFileLeafHasher::new(
                    descriptor.logical_count(),
                    descriptor.min_event_time(),
                    descriptor.max_event_time(),
                    descriptor.logical_attributes(),
                )
                .map_err(StewardError::Content)?;
                self.current = Some(hasher);
            }
            let hasher = self.current.as_mut().expect("just set above");
            let remaining = hasher.remaining();
            let take = remaining.min(chunk.len() as u64) as usize;
            hasher
                .write(&chunk[..take])
                .map_err(StewardError::Content)?;
            chunk = &chunk[take..];
            if hasher.remaining() == 0 {
                let finished = self.current.take().expect("present, just written to");
                self.hashes
                    .push(finished.finish().map_err(StewardError::Content)?);
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<ObjectHash>, StewardError> {
        if self.current.is_some() {
            return Err(StewardError::Content(
                "file pack's physical content ended mid-leaf (truncated)".to_string(),
            ));
        }
        if self.descriptors.next().is_some() {
            return Err(StewardError::Content(
                "file pack's physical content is shorter than its declared leaf descriptors"
                    .to_string(),
            ));
        }
        Ok(self.hashes)
    }
}

/// Fetch, decode, and verify one table-payload (Parquet) pack
/// (`docs/logical-series-identity-design.md` delivery gate 4): decode every
/// physical object in order and partition the concatenated,
/// row-order-preserving `RecordBatch` stream by the pack's descriptors.
/// Each object is checked against the effective schema fingerprint of the
/// leaf currently being reconstructed. A schema transition is accepted only
/// at a physical-object boundary.
///
/// Buffering here is bounded *per physical object*, not per pack:
/// [`decode_table_object`] decodes one object's row groups into memory
/// before this function feeds them onward, so a pack spanning many objects
/// never holds more than one object's decoded rows at a time. Within that,
/// [`TableLeafPartitioner`] itself buffers at most one logical leaf's
/// batches while assembling its canonical row hash. A future refinement
/// could stream row groups one at a time instead of a whole object, but a
/// single Parquet physical object is already a bounded, individually-sized
/// unit exactly like an ordinary v1 version blob -- never "the whole pack".
///
/// Returns the pack's recomputed leaf hashes in order, ready for
/// [`verify_pack_against_manifest`].
async fn fetch_and_verify_table_pack(
    remote: &dyn ContentSource,
    pack: &PackIndex,
    manifest: &SeriesManifest,
    graph: &mut FetchedGraph,
    blob_index: &mut Option<HashSet<ObjectHash>>,
) -> Result<Vec<ObjectHash>, StewardError> {
    let mut partitioner = TableLeafPartitioner::new(manifest, pack);
    let mut total_rows: u64 = 0;
    let mut total_physical_bytes: u64 = 0;

    for &object_hash in pack.physical_object_hashes() {
        let expected_fingerprint = partitioner.expected_object_fingerprint()?;
        fetch_physical_object(remote, object_hash, graph, blob_index).await?;
        let (schema, batches) = match graph.objects.get(&object_hash) {
            Some(FetchedObject::Blob(bytes)) => {
                total_physical_bytes += bytes.len() as u64;
                // Release blocker item 3
                // (`docs/logical-series-identity-design.md`): check the
                // running total against the pack's declared
                // `physical_byte_count` before Parquet-decoding this
                // object, so a malformed/oversized declaration fails fast
                // rather than paying for decompression first.
                if total_physical_bytes > pack.physical_byte_count() {
                    return Err(StewardError::Content(format!(
                        "pack declares physical_byte_count {} but its physical objects already \
                         total at least {total_physical_bytes} byte(s) through object \
                         {object_hash} -- aborting before decoding it",
                        pack.physical_byte_count()
                    )));
                }
                let source = bytes::Bytes::from(bytes.clone());
                decode_table_object(source, expected_fingerprint)
                    .await
                    .map_err(|e| {
                        StewardError::Content(format!("decode physical object {object_hash}: {e}"))
                    })?
            }
            Some(FetchedObject::External) => {
                let (file, byte_count) = spool_external_object(remote, object_hash).await?;
                total_physical_bytes += byte_count;
                // Same fail-fast check as the inline-blob arm above, applied
                // to the actual spooled byte count rather than a trusted
                // metadata field.
                if total_physical_bytes > pack.physical_byte_count() {
                    return Err(StewardError::Content(format!(
                        "pack declares physical_byte_count {} but its physical objects already \
                         total at least {total_physical_bytes} byte(s) through object \
                         {object_hash} -- aborting before decoding it",
                        pack.physical_byte_count()
                    )));
                }
                decode_table_object(file, expected_fingerprint)
                    .await
                    .map_err(|e| {
                        StewardError::Content(format!("decode physical object {object_hash}: {e}"))
                    })?
            }
            other => {
                return Err(StewardError::Content(format!(
                    "expected a physical blob object at {object_hash} but found {other:?}"
                )));
            }
        };
        let canonical_schema =
            sync_store::content::canonicalize_schema(&schema).map_err(StewardError::Content)?;
        for batch in batches {
            total_rows += batch.num_rows() as u64;
            let mut columns = Vec::with_capacity(batch.num_columns());
            for (column, field) in batch.columns().iter().zip(canonical_schema.fields()) {
                columns.push(arrow_cast::cast(column, field.data_type()).map_err(|e| {
                    StewardError::Content(format!(
                        "normalize physical object {object_hash} column {:?}: {e}",
                        field.name()
                    ))
                })?);
            }
            let normalized =
                RecordBatch::try_new(Arc::clone(&canonical_schema), columns).map_err(|e| {
                    StewardError::Content(format!(
                        "normalize physical object {object_hash} schema: {e}"
                    ))
                })?;
            partitioner.feed(normalized, expected_fingerprint)?;
        }
    }
    if total_physical_bytes != pack.physical_byte_count() {
        return Err(StewardError::Content(format!(
            "pack declares physical_byte_count {} but its physical objects total {total_physical_bytes} byte(s)",
            pack.physical_byte_count()
        )));
    }
    if total_rows != pack.logical_count() {
        return Err(StewardError::Content(format!(
            "table pack decoded {total_rows} row(s) but declares logical_count {}",
            pack.logical_count()
        )));
    }
    partitioner.finish()
}

/// Streaming partitioner for a table pack: consumes decoded `RecordBatch`es
/// in order, splitting them by each descriptor's row count (a leaf may span
/// batches or physical objects), and produces exactly one recomputed leaf
/// hash per descriptor. Holds at most one logical leaf's batches at a time.
struct TableLeafPartitioner<'a> {
    manifest: &'a SeriesManifest,
    pack: &'a PackIndex,
    descriptors: &'a [PackLeafDescriptor],
    next_descriptor: usize,
    current_descriptor: Option<usize>,
    current_schema: Option<Arc<Schema>>,
    current_batches: Vec<RecordBatch>,
    current_rows: u64,
    hashes: Vec<ObjectHash>,
}

impl<'a> TableLeafPartitioner<'a> {
    fn new(manifest: &'a SeriesManifest, pack: &'a PackIndex) -> Self {
        Self {
            manifest,
            pack,
            descriptors: pack.leaf_descriptors(),
            next_descriptor: 0,
            current_descriptor: None,
            current_schema: None,
            current_batches: Vec::new(),
            current_rows: 0,
            hashes: Vec::with_capacity(pack.leaf_descriptors().len()),
        }
    }

    fn descriptor_fingerprint(&self, descriptor_index: usize) -> Result<ObjectHash, StewardError> {
        let descriptor = self.descriptors.get(descriptor_index).ok_or_else(|| {
            StewardError::Content(
                "table pack's physical content extends beyond its declared leaf descriptors"
                    .to_string(),
            )
        })?;
        effective_leaf_schema_fingerprint(self.manifest, self.pack, descriptor)
            .map_err(StewardError::Content)?
            .ok_or_else(|| {
                StewardError::Content(
                    "table pack leaf resolved to no effective schema fingerprint".to_string(),
                )
            })
    }

    fn expected_object_fingerprint(&self) -> Result<ObjectHash, StewardError> {
        self.descriptor_fingerprint(self.current_descriptor.unwrap_or(self.next_descriptor))
    }

    fn feed(
        &mut self,
        mut batch: RecordBatch,
        object_fingerprint: ObjectHash,
    ) -> Result<(), StewardError> {
        while batch.num_rows() > 0 {
            if self.current_descriptor.is_none() {
                let descriptor_index = self.next_descriptor;
                let expected = self.descriptor_fingerprint(descriptor_index)?;
                if expected != object_fingerprint {
                    return Err(StewardError::Content(format!(
                        "table physical object with schema fingerprint {object_fingerprint} \
                         crosses a leaf schema transition; descriptor {descriptor_index} requires \
                         {expected}"
                    )));
                }
                self.next_descriptor += 1;
                self.current_descriptor = Some(descriptor_index);
                self.current_schema = Some(batch.schema());
            }
            let descriptor_index = self.current_descriptor.expect("just set above");
            let descriptor = &self.descriptors[descriptor_index];
            let expected = self.descriptor_fingerprint(descriptor_index)?;
            if expected != object_fingerprint {
                return Err(StewardError::Content(format!(
                    "table logical leaf {descriptor_index} spans physical objects with different \
                     schema fingerprints ({expected} then {object_fingerprint})"
                )));
            }
            let needed = descriptor.logical_count() - self.current_rows;
            let take = needed.min(batch.num_rows() as u64) as usize;
            self.current_batches.push(batch.slice(0, take));
            self.current_rows += take as u64;
            batch = batch.slice(take, batch.num_rows() - take);
            if self.current_rows == descriptor.logical_count() {
                let hash = table_leaf_hash_canonical(
                    self.current_schema
                        .as_ref()
                        .expect("current table leaf always has a schema"),
                    &self.current_batches,
                    descriptor.min_event_time(),
                    descriptor.max_event_time(),
                    descriptor.logical_attributes(),
                )
                .map_err(StewardError::Content)?;
                self.hashes.push(hash);
                self.current_batches.clear();
                self.current_rows = 0;
                self.current_descriptor = None;
                self.current_schema = None;
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<Vec<ObjectHash>, StewardError> {
        if self.current_descriptor.is_some() || !self.current_batches.is_empty() {
            return Err(StewardError::Content(
                "table pack's physical content ended mid-leaf (truncated rows)".to_string(),
            ));
        }
        if self.next_descriptor != self.descriptors.len() {
            return Err(StewardError::Content(
                "table pack's physical content has fewer rows than its declared leaf descriptors"
                    .to_string(),
            ));
        }
        Ok(self.hashes)
    }
}

/// Decode one Parquet physical object, checking its canonical schema
/// fingerprint against `expected_fingerprint` before returning any rows.
///
/// Runs on a blocking thread ([`tokio::task::spawn_blocking`]): both the
/// synchronous Parquet reader and, for a spooled external object, its
/// underlying file I/O would otherwise block the async runtime.
///
/// Also reused by `crate::pack_maintenance`'s table repack path to decode a
/// persisted table leaf's own Parquet bytes back into rows before
/// re-encoding them into a bounded physical pack object, so both directions
/// (fetch-and-verify here, and repack there) decode Parquet the same way.
pub(crate) async fn decode_table_object<T>(
    reader: T,
    expected_fingerprint: ObjectHash,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), StewardError>
where
    T: ChunkReader + 'static,
{
    tokio::task::spawn_blocking(move || decode_table_object_blocking(reader, expected_fingerprint))
        .await
        .map_err(|e| StewardError::Content(format!("parquet decode task panicked: {e}")))?
        .map_err(StewardError::Content)
}

/// The synchronous half of [`decode_table_object`]: open the Parquet reader,
/// verify its schema fingerprint, and decode every row group into
/// `RecordBatch`es.
fn decode_table_object_blocking<T>(
    reader: T,
    expected_fingerprint: ObjectHash,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), String>
where
    T: ChunkReader + 'static,
{
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)
        .map_err(|e| format!("open parquet: {e}"))?;
    let schema = builder.schema().clone();
    let fingerprint =
        schema_fingerprint(&schema).map_err(|e| format!("schema fingerprint: {e}"))?;
    if fingerprint != expected_fingerprint {
        return Err(format!(
            "physical object schema fingerprint {fingerprint} does not match manifest schema_fingerprint {expected_fingerprint}"
        ));
    }
    let batch_reader = builder
        .build()
        .map_err(|e| format!("build parquet reader: {e}"))?;
    let mut batches = Vec::new();
    for batch in batch_reader {
        batches.push(batch.map_err(|e| format!("decode parquet batch: {e}"))?);
    }
    Ok((schema, batches))
}

/// Stream an external physical object's bytes into a spooled, unlinked
/// temporary file (`docs/logical-series-identity-design.md` delivery gate
/// 4), verifying its content hash as bytes pass through, so a table pack's
/// Parquet decoder -- which needs seekable [`ChunkReader`] access, not a
/// one-pass [`tokio::io::AsyncRead`] -- can be pointed at a real file rather
/// than requiring the whole object to be buffered in memory.
///
/// [`tempfile::tempfile`] is used rather than a named path: on every
/// platform it supports, the file is unlinked from the filesystem namespace
/// immediately (or is otherwise not nameable), so its storage is reclaimed
/// automatically when the last handle closes -- including on a panic or an
/// early return via `?` -- with no separate cleanup step that could be
/// skipped.
///
/// Returns the rewound file (positioned at the start, ready to read) plus
/// the exact byte count streamed, so the caller can cross-check it against
/// the pack's declared `physical_byte_count`.
async fn spool_external_object(
    remote: &dyn ContentSource,
    hash: ObjectHash,
) -> Result<(std::fs::File, u64), StewardError> {
    let mut reader = remote
        .get_blob_reader(hash)
        .await
        .map_err(|e| StewardError::Content(format!("open external object {hash}: {e}")))?
        .ok_or_else(|| {
            StewardError::Content(format!(
                "external object {hash} vanished from the remote blob store while spooling"
            ))
        })?;

    let std_file = tempfile::tempfile()
        .map_err(|e| StewardError::Content(format!("create spool file for {hash}: {e}")))?;
    let mut file = tokio::fs::File::from_std(std_file);
    let mut hasher = blake3::Hasher::new();
    let mut total: u64 = 0;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = reader
            .read(&mut buf)
            .await
            .map_err(|e| StewardError::Content(format!("read external object {hash}: {e}")))?;
        if n == 0 {
            break;
        }
        let _ = hasher.update(&buf[..n]);
        file.write_all(&buf[..n])
            .await
            .map_err(|e| StewardError::Content(format!("spool external object {hash}: {e}")))?;
        total += n as u64;
    }
    file.flush()
        .await
        .map_err(|e| StewardError::Content(format!("flush spool file for {hash}: {e}")))?;

    let computed = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
    if computed != hash {
        return Err(StewardError::Content(format!(
            "external object hashes to {computed} but was fetched as {hash}"
        )));
    }

    let _ = file
        .seek(std::io::SeekFrom::Start(0))
        .await
        .map_err(|e| StewardError::Content(format!("rewind spool file for {hash}: {e}")))?;
    let std_file = file.into_std().await;
    Ok((std_file, total))
}

/// Record a leaf blob object.  A blob may be inline (small, an `objects` row) or
/// external (large, in the remote blob store by hash).  Inline blobs are fetched
/// and verified now; external blobs are recorded by hash only and streamed at
/// rebuild time so a multi-gigabyte value never lands in a single buffer
/// (Decision D7).  Either way the rebuild adopts the bytes by hash.
async fn fetch_blob(
    remote: &dyn ContentSource,
    hash: ObjectHash,
    graph: &mut FetchedGraph,
    blob_index: &mut Option<HashSet<ObjectHash>>,
) -> Result<(), StewardError> {
    if graph.objects.contains_key(&hash) {
        return Ok(());
    }
    if let Some(bytes) = remote
        .get_object(hash)
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?
    {
        verify(hash, &bytes)?;
        let _ = graph
            .objects
            .insert(hash, FetchedObject::Blob(bytes.clone()));
        let _ = graph.bytes.insert(hash, bytes);
        return Ok(());
    }
    // Not an inline row: it must be a large external blob in the remote blob
    // store.  Confirm its presence now so a missing object still fails the fetch
    // early, but do not download it -- its bytes stream at rebuild time.
    //
    // Presence comes from a single listing taken on first use, not a probe per
    // blob: probing costs a request for every blob in the closure, which is a
    // cost proportional to the pond's history rather than to what the pull
    // actually transfers.
    let index = match blob_index {
        Some(index) => index,
        none => none.insert(
            remote
                .list_blobs()
                .await
                .map_err(|e| StewardError::Content(e.to_string()))?,
        ),
    };
    if !index.contains(&hash) {
        return Err(StewardError::Content(format!(
            "object {} is absent from the remote (inline and blob store)",
            hash.to_hex()
        )));
    }
    let _ = graph.objects.insert(hash, FetchedObject::External);
    let _ = graph.external_blobs.insert(hash);
    Ok(())
}

/// Fetch an object's bytes and verify they hash to the requested key.
async fn fetch_verified(
    remote: &dyn ContentSource,
    hash: ObjectHash,
) -> Result<Vec<u8>, StewardError> {
    let bytes = remote
        .get_object(hash)
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?
        .ok_or_else(|| {
            StewardError::Content(format!(
                "object {} is absent from the remote",
                hash.to_hex()
            ))
        })?;
    verify(hash, &bytes)?;
    Ok(bytes)
}

/// Enforce the content-addressing invariant: the bytes must hash to the key.
fn verify(hash: ObjectHash, bytes: &[u8]) -> Result<(), StewardError> {
    let actual = ObjectHash::of_bytes(bytes);
    if actual != hash {
        return Err(StewardError::Content(format!(
            "fetched object hashes to {} but was fetched as {}",
            actual.to_hex(),
            hash.to_hex()
        )));
    }
    Ok(())
}

/// The result of rebuilding a pond from a fetched object graph.  Counts reflect
/// nodes *created* in this rebuild; an incremental pull that only versions or
/// renames existing nodes reports zeros here.
#[derive(Debug, Clone, Default)]
pub struct RebuildOutcome {
    /// The tip commit's root tree hash that was rebuilt.
    pub root_tree_hash: Option<ObjectHash>,
    /// Number of directories created.
    pub dirs: usize,
    /// Number of single-version files/tables created.
    pub files: usize,
    /// Number of symlinks created.
    pub symlinks: usize,
    /// Number of multi-version series created.
    pub series: usize,
    /// Number of dynamic nodes created.
    pub dynamic: usize,
}

/// The source of one file/series version's bytes in an apply plan.  Small blobs
/// are buffered inline; large blobs are named by hash and streamed from the
/// remote blob store at apply time so they are never held in memory (D7).
#[derive(Debug, Clone)]
enum VersionSource {
    /// A buffered small blob's bytes.
    Inline(Vec<u8>),
    /// A large external blob to stream from the remote by content hash.
    External(ObjectHash),
}

/// One version to write during a rebuild: where its bytes come from, plus the
/// node metadata the source recorded for it.
///
/// The metadata rides alongside the bytes because a replica cannot derive it:
/// a raw JSON-lines blob's event-time range depends on the *source pond's*
/// ingest configuration (which field, which unit), which the replica never
/// sees. Without carrying it, every replicated series version would land with
/// NULL bounds, and consumers that key off those bounds -- notably the
/// temporal-reduce rollup cache -- would treat each one as spanning all time
/// and rebuild all history on every run.
#[derive(Debug, Clone)]
struct PlannedVersion {
    /// Where this version's bytes come from.
    source: VersionSource,
    /// Node metadata to reapply on the replica.
    meta: VersionMeta,
}

/// One filesystem operation in an incremental rebuild plan, in apply order.
///
/// The plan is a `node_id`-keyed diff of the fetched source manifest against
/// the target's current node state (Decision D8).  Deletions come first
/// (deepest-first), then creates/renames/versions in breadth-first order so a
/// parent directory is always materialized before its children.
#[derive(Debug, Clone)]
enum ApplyOp {
    /// Rename a node within its parent (identity and history preserved).
    Rename {
        parent: String,
        old: String,
        new: String,
    },
    /// Ensure a directory exists under `parent` as `name` with the adopted
    /// `node_id`, then register its working directory for descent.  `create`
    /// distinguishes adopting a new node from opening an existing one.
    Dir {
        parent: String,
        name: String,
        node_id: String,
        create: bool,
    },
    /// Create (adopting `node_id`) or append to a physical file / table /
    /// series node.  `versions` are the version blobs to write in order: every
    /// version on create, only the appended suffix on update.  `entry_type`
    /// drives writer finalization (series infer temporal bounds).  Each version
    /// is either a buffered small blob or a large external blob streamed from
    /// the remote at apply time (D7).
    File {
        parent: String,
        name: String,
        node_id: String,
        create: bool,
        entry_type: EntryType,
        versions: Vec<PlannedVersion>,
        /// When set, the first written version replaces (collapses) every
        /// version the target already held -- replicating a source-side series
        /// compaction. `versions` then holds the full post-collapse list, not an
        /// appended suffix.
        collapse_first: bool,
    },
    /// Create (adopting `node_id`) or rewrite a symlink.  A rewrite re-adopts
    /// the same `node_id` after unlinking, so identity is preserved.
    Symlink {
        parent: String,
        name: String,
        node_id: String,
        create: bool,
        target: String,
        /// The source's mtime, adopted verbatim (see [`VersionMeta::timestamp`]).
        mtime: Option<i64>,
    },
    /// Create (adopting `node_id`) or rewrite a dynamic node from its recipe.
    Dynamic {
        parent: String,
        name: String,
        node_id: String,
        create: bool,
        factory: String,
        config: Vec<u8>,
        /// The source's mtime, adopted verbatim (see [`VersionMeta::timestamp`]).
        mtime: Option<i64>,
    },
    /// Unlink a target node that is absent from the source.
    Delete { parent_path: String, name: String },
    /// Create (adopting `node_id`) or append to a native `watertown.series.v2` v2
    /// logical series (`docs/logical-series-identity-design.md`, release
    /// blocker item 1). Unlike [`ApplyOp::File`], a v2 series carries no
    /// buffered version list here: its physical content already lives in the
    /// fetched [`FetchedGraph`] (`graph.objects`/`graph.bytes`, exactly like
    /// an ordinary v1 version blob), so apply resolves `manifest_hash` back
    /// into the graph's verified [`FetchedSeriesV2`] and reconstructs each
    /// needed logical leaf from its packs' physical objects.
    SeriesV2 {
        parent: String,
        name: String,
        node_id: String,
        create: bool,
        entry_type: EntryType,
        /// The `watertown.series.v2` manifest object's hash -- this node's
        /// `child_hash` -- naming the verified [`FetchedSeriesV2`] in
        /// `graph.objects` to materialize from.
        manifest_hash: ObjectHash,
        /// The first (0-based, whole-series) logical leaf index this
        /// operation must write; every earlier leaf is walked, to stay
        /// correctly positioned in the reconstructed physical stream, but
        /// never buffered or written, since the target already holds it.
        leaves_from: u64,
        /// The source's aggregate mtime for this series
        /// ([`replicated_mtime`]), adopted verbatim on the *last* leaf this
        /// operation writes so the destination's own subsequent fold
        /// recomputes the identical aggregate `VersionMeta` (mtime is not
        /// part of the `watertown.series.v2` manifest hash, but is part of the
        /// destination's own `build_series_manifest` aggregation, which
        /// takes it from the latest leaf-bearing version).
        replicated_mtime: Option<i64>,
    },
}

/// Rebuild or incrementally update a tlogfs pond from a fetched object graph
/// (design Section 8.5).
///
/// The fetched node manifest carries the source's real `node_id`s; the consumer
/// adopts them so the rebuilt pond is row-identical to the source and every
/// later pull is a `node_id`-keyed diff (Decision D8).  The target need not be
/// empty: this computes the target's current node state, diffs it against the
/// source manifest by `node_id`, and applies the difference -- creating new
/// nodes (with adopted ids), appending file/series versions, renaming moved
/// nodes in place, and deleting nodes absent from the source -- in a single
/// transaction.
///
/// # Errors
///
/// Returns an error if the graph is empty or carries no manifest, if the graph
/// references an object it does not contain, if a node's `entry_type` changed
/// or it was reparented (both unsupported), if a symlink target is not valid
/// UTF-8, if a recipe fails to decode, or if a write fails.  A source-side series
/// compaction (the incoming versions replace rather than extend the held ones) is
/// replicated, not rejected.  After applying, the read-side fold of `target` must
/// equal the tip's root tree hash and the rebuilt node manifest hash must equal
/// the tip commit's `node_manifest_hash`; a mismatch is an error.
pub async fn rebuild_pond(
    target: &mut Ship,
    remote: &dyn ContentSource,
    graph: &FetchedGraph,
) -> Result<RebuildOutcome, StewardError> {
    let root = graph
        .root_tree_hash()
        .ok_or_else(|| StewardError::Content("cannot rebuild from an empty graph".to_string()))?;
    if graph.manifest.is_empty() {
        return Err(StewardError::Content(
            "fetched graph has no node manifest".to_string(),
        ));
    }
    let tip_manifest_hash = graph
        .commits
        .first()
        .map(|(_, c)| c.node_manifest_hash)
        .ok_or_else(|| StewardError::Content("fetched graph has no tip commit".to_string()))?;
    let tip_manifest_root = graph
        .commits
        .first()
        .map(|(_, c)| c.node_manifest_root)
        .ok_or_else(|| StewardError::Content("fetched graph has no tip commit".to_string()))?;

    let (target_nodes, target_series, target_series_leaves) =
        crate::content_tree::build_target_state(target).await?;

    // Reject a manifest that is inconsistent with the fetched tree closure
    // before any mutation, so a hostile/corrupt remote cannot commit an
    // inconsistent tree that the post-apply fold would only catch after commit.
    verify_manifest_matches_tree(graph)?;

    let (ops, outcome) = plan_node_diff(
        graph,
        root,
        &target_nodes,
        &target_series,
        &target_series_leaves,
    )?;

    let root_node_id = src_root_id(graph)?.to_string();
    let mut tx = target
        .begin_write(&PondUserMetadata::new(vec!["pull".to_string()]))
        .await?;
    tx.expect_content_roots(root, tip_manifest_hash, tip_manifest_root);
    let apply_result = async {
        let root_wd = tx.root().await?;
        apply_ops(&root_node_id, root_wd, &ops, remote, graph).await
    }
    .await;
    if let Err(error) = apply_result {
        return Err(tx.abort_preserving(error).await);
    }
    _ = tx.commit().await?;

    Ok(outcome)
}

/// Cross-pond import: rebuild a *foreign* pond's tree under its own `pond_id`
/// partition (Section 8.5.2, mount scoping), so a mount entry at the import
/// path resolves into it.  Unlike [`rebuild_pond`] -- which mirrors the source
/// at the local root and adopts the local pond_id -- this writes the source's
/// nodes beneath the foreign pond's well-known root, diffing against whatever of
/// the foreign tree is already present, and advances only the foreign pond's
/// seq allocator so the local pond's contiguous numbering is untouched.
///
/// # Errors
///
/// Same conditions as [`rebuild_pond`], computed over `foreign_pond_id`: the
/// graph must carry a manifest, references must resolve, and the rebuilt tree
/// must fold to the tip root tree hash with a matching node manifest.
pub async fn import_pond(
    target: &mut Ship,
    remote: &dyn ContentSource,
    graph: &FetchedGraph,
    foreign_pond_id: uuid7::Uuid,
) -> Result<RebuildOutcome, StewardError> {
    import_pond_inner(target, remote, graph, foreign_pond_id, None, false).await
}

/// Atomically import a foreign pond, materialize its local mount, and pin the
/// imported tip. A failed import cannot leave either the foreign rows or the
/// local graft metadata partially committed.
pub async fn import_graft(
    target: &mut Ship,
    remote: &dyn ContentSource,
    graph: &FetchedGraph,
    foreign_pond_id: uuid7::Uuid,
    name: &str,
    mount_path: &str,
) -> Result<RebuildOutcome, StewardError> {
    let graft = prepare_graft(graph, foreign_pond_id, name, mount_path)?;
    import_pond_inner(target, remote, graph, foreign_pond_id, Some(graft), false).await
}

/// Atomically discard and recreate one foreign pond partition together with
/// its mount and pin. Local content and other foreign partitions are untouched.
pub async fn replace_graft(
    target: &mut Ship,
    remote: &dyn ContentSource,
    graph: &FetchedGraph,
    foreign_pond_id: uuid7::Uuid,
    name: &str,
    mount_path: &str,
) -> Result<RebuildOutcome, StewardError> {
    let graft = prepare_graft(graph, foreign_pond_id, name, mount_path)?;
    import_pond_inner(target, remote, graph, foreign_pond_id, Some(graft), true).await
}

fn prepare_graft(
    graph: &FetchedGraph,
    foreign_pond_id: uuid7::Uuid,
    name: &str,
    mount_path: &str,
) -> Result<PreparedGraft, StewardError> {
    let pinned_tip = graph
        .tip
        .ok_or_else(|| StewardError::Content("cannot graft a graph with no tip".to_string()))?;
    let pin = crate::GraftPin {
        foreign_pond_id: foreign_pond_id.to_string(),
        mount_path: mount_path.to_string(),
        pinned_tip: pinned_tip.to_hex(),
    };
    let pin_yaml = pin
        .to_yaml()
        .map_err(|error| StewardError::Content(format!("serialize graft pin: {error}")))?;
    let (parent, leaf) = crate::split_mount_path(mount_path).map_err(StewardError::Content)?;
    Ok(PreparedGraft {
        parent: parent.to_string(),
        leaf: leaf.to_string(),
        pin_path: crate::GraftPin::pin_path(name),
        pin_name: name.to_string(),
        pin_yaml,
    })
}

struct PreparedGraft {
    parent: String,
    leaf: String,
    pin_path: String,
    pin_name: String,
    pin_yaml: String,
}

async fn import_pond_inner(
    target: &mut Ship,
    remote: &dyn ContentSource,
    graph: &FetchedGraph,
    foreign_pond_id: uuid7::Uuid,
    graft: Option<PreparedGraft>,
    replace: bool,
) -> Result<RebuildOutcome, StewardError> {
    let root = graph
        .root_tree_hash()
        .ok_or_else(|| StewardError::Content("cannot import an empty graph".to_string()))?;
    if graph.manifest.is_empty() {
        return Err(StewardError::Content(
            "fetched graph has no node manifest".to_string(),
        ));
    }
    let tip_manifest_hash = graph
        .commits
        .first()
        .map(|(_, c)| c.node_manifest_hash)
        .ok_or_else(|| StewardError::Content("fetched graph has no tip commit".to_string()))?;
    let tip_manifest_root = graph
        .commits
        .first()
        .map(|(_, c)| c.node_manifest_root)
        .ok_or_else(|| StewardError::Content("fetched graph has no tip commit".to_string()))?;

    let foreign_id = foreign_pond_id.to_string();
    let (target_nodes, target_series, target_series_leaves) =
        crate::content_tree::build_target_state_for_pond(target, &foreign_id).await?;

    // Reject a manifest that is inconsistent with the fetched tree closure
    // before any mutation (see verify_manifest_matches_tree).
    verify_manifest_matches_tree(graph)?;

    let (ops, outcome) = if replace {
        plan_full_replacement(graph, root, &target_nodes)?
    } else {
        plan_node_diff(
            graph,
            root,
            &target_nodes,
            &target_series,
            &target_series_leaves,
        )?
    };

    let root_node_id = src_root_id(graph)?.to_string();
    let first_import = target_nodes.is_empty();
    let tx = target
        .begin_write(&PondUserMetadata::new(vec![
            "pull".to_string(),
            "import".to_string(),
        ]))
        .await?;
    let apply_result = async {
        if first_import {
            tx.initialize_foreign_root(foreign_pond_id).await?;
        }
        let foreign_node = tx.foreign_root_node(foreign_pond_id).await?;
        let foreign_np = tinyfs::NodePath {
            node: foreign_node,
            path: "/".into(),
        };
        let root_wd = tx.wd(&foreign_np, foreign_np.clone()).await?;
        if let Some(graft) = &graft {
            use tinyfs::EntryType;

            let root = tx.root().await?;
            let _ = root.create_dir_all(&graft.parent).await?;
            let parent_wd = root.open_dir_path(&graft.parent).await?;
            let foreign_node = tx.foreign_root_node(foreign_pond_id).await?;
            if let Some(existing) = parent_wd.entry(&graft.leaf).await? {
                let existing_pond = existing.pond_id.as_deref().ok_or_else(|| {
                    StewardError::Aborted(format!(
                        "mount path `{}/{}` contains local content; refusing scoped graft replacement",
                        graft.parent, graft.leaf
                    ))
                })?;
                if existing_pond != foreign_id {
                    return Err(StewardError::Aborted(format!(
                        "mount path `{}/{}` belongs to pond {}; refusing to replace graft {}",
                        graft.parent, graft.leaf, existing_pond, foreign_id
                    )));
                }
                if replace {
                    parent_wd.remove_entry(&graft.leaf).await?;
                    let _ = parent_wd.insert_node(&graft.leaf, foreign_node).await?;
                } else if existing.child_node_id != foreign_node.id().node_id() {
                    return Err(StewardError::Aborted(format!(
                        "mount path `{}/{}` points to foreign node {} instead of root {}",
                        graft.parent,
                        graft.leaf,
                        existing.child_node_id,
                        foreign_node.id().node_id()
                    )));
                }
            } else {
                let _ = parent_wd.insert_node(&graft.leaf, foreign_node).await?;
            }

            let _ = root.create_dir_all(crate::SYS_DIR).await?;
            let _ = root.create_dir_all(crate::SYS_GRAFTS_DIR).await?;
            let pin_is_current = if root.exists(&graft.pin_path).await {
                root.read_file_path_to_vec(&graft.pin_path).await? == graft.pin_yaml.as_bytes()
            } else {
                false
            };
            if !pin_is_current && root.exists(&graft.pin_path).await {
                let grafts_dir = root.open_dir_path(crate::SYS_GRAFTS_DIR).await?;
                grafts_dir.remove_entry(&graft.pin_name).await?;
            }
            if !pin_is_current {
                let mut writer = root
                    .async_writer_path_with_type(
                        &graft.pin_path,
                        EntryType::FilePhysicalVersion,
                    )
                    .await?;
                writer.write_all(graft.pin_yaml.as_bytes()).await?;
                writer.shutdown().await?;
            }
        }
        apply_ops(&root_node_id, root_wd, &ops, remote, graph).await?;
        let uncommitted = tx.state()?.uncommitted_live_rows().await?;
        let committed_table = tx.data_persistence()?.table().clone();
        crate::content_tree::in_txn_content_state(committed_table, uncommitted, &foreign_id).await
    }
    .await;
    let preview = match apply_result {
        Ok(preview) => preview,
        Err(error) => return Err(tx.abort_preserving(error).await),
    };
    let validation = preview_validation_error(
        graph,
        &preview,
        root,
        tip_manifest_hash,
        tip_manifest_root,
        "imported foreign tree",
    );
    let validation = match validation {
        Ok(validation) => validation,
        Err(error) => return Err(tx.abort_preserving(error).await),
    };
    if let Some(error) = validation {
        return Err(tx.abort(error).await);
    }
    _ = tx.commit().await?;

    // Advance only the foreign pond's seq frontier so the local allocator stays
    // contiguous; the highest source seq is the foreign tip's commit seq.
    let foreign_seq = graph
        .commits
        .iter()
        .map(|(_, c)| c.provenance.seq)
        .max()
        .unwrap_or(0);
    target
        .data_persistence_mut()
        .sync_last_txn_seq(&foreign_id, foreign_seq);

    Ok(outcome)
}
fn src_root_id(graph: &FetchedGraph) -> Result<&str, StewardError> {
    graph
        .manifest
        .iter()
        .find(|e| e.parent_node_id.is_empty() && e.name.is_empty())
        .map(|e| e.node_id.as_str())
        .ok_or_else(|| StewardError::Content("manifest has no root entry".to_string()))
}

fn first_replica_divergence(
    graph: &FetchedGraph,
    actual_nodes: &HashMap<String, ManifestEntry>,
    actual_series: &HashMap<String, Vec<ObjectHash>>,
) -> Result<Option<String>, StewardError> {
    let mut expected_nodes: BTreeMap<&str, &ManifestEntry> = BTreeMap::new();
    for entry in &graph.manifest {
        if expected_nodes
            .insert(entry.node_id.as_str(), entry)
            .is_some()
        {
            return Ok(Some(format!(
                "source manifest contains duplicate node {}",
                entry.node_id
            )));
        }
    }
    let actual_nodes_sorted: BTreeMap<&str, &ManifestEntry> = actual_nodes
        .values()
        .map(|entry| (entry.node_id.as_str(), entry))
        .collect();

    for node_id in expected_nodes
        .keys()
        .chain(actual_nodes_sorted.keys())
        .copied()
        .collect::<BTreeSet<_>>()
    {
        let Some(expected) = expected_nodes.get(node_id) else {
            let actual = actual_nodes_sorted[node_id];
            return Ok(Some(format!(
                "unexpected node {node_id} ({:?} {:?})",
                actual.entry_type, actual.name
            )));
        };
        let Some(actual) = actual_nodes_sorted.get(node_id) else {
            return Ok(Some(format!(
                "missing node {node_id} ({:?} {:?})",
                expected.entry_type, expected.name
            )));
        };
        if expected.parent_node_id != actual.parent_node_id {
            return Ok(Some(format!(
                "node {node_id} parent differs: expected {:?}, actual {:?}",
                expected.parent_node_id, actual.parent_node_id
            )));
        }
        if expected.name != actual.name {
            return Ok(Some(format!(
                "node {node_id} name differs: expected {:?}, actual {:?}",
                expected.name, actual.name
            )));
        }
        if expected.entry_type != actual.entry_type {
            return Ok(Some(format!(
                "node {node_id} type differs: expected {:?}, actual {:?}",
                expected.entry_type, actual.entry_type
            )));
        }
    }

    for (node_id, expected) in &expected_nodes {
        let actual = actual_nodes_sorted[node_id];
        if matches!(
            expected.entry_type,
            EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
        ) {
            let expected_versions = &series_v2(graph, expected.child_hash)?.leaf_hashes;
            let actual_versions = actual_series
                .get(*node_id)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let compared = expected_versions.len().min(actual_versions.len());
            for index in 0..compared {
                if expected_versions[index] != actual_versions[index] {
                    return Ok(Some(format!(
                        "series node {node_id} ({:?}) leaf {index} differs: expected {}, actual {}",
                        expected.name,
                        expected_versions[index].to_hex(),
                        actual_versions[index].to_hex()
                    )));
                }
            }
            if expected_versions.len() != actual_versions.len() {
                return Ok(Some(format!(
                    "series node {node_id} ({:?}) leaf count differs: expected {}, actual {}",
                    expected.name,
                    expected_versions.len(),
                    actual_versions.len()
                )));
            }
        } else if expected.entry_type != EntryType::DirectoryPhysical
            && expected.child_hash != actual.child_hash
        {
            return Ok(Some(format!(
                "node {node_id} ({:?}) content hash differs: expected {}, actual {}",
                expected.name,
                expected.child_hash.to_hex(),
                actual.child_hash.to_hex()
            )));
        }

        let compared = expected.versions.len().min(actual.versions.len());
        for index in 0..compared {
            let expected_meta = &expected.versions[index];
            let actual_meta = &actual.versions[index];
            if expected_meta.timestamp != actual_meta.timestamp {
                return Ok(Some(format!(
                    "node {node_id} ({:?}) version {index} timestamp differs: expected {:?}, actual {:?}",
                    expected.name, expected_meta.timestamp, actual_meta.timestamp
                )));
            }
            if expected_meta.min_event_time != actual_meta.min_event_time {
                return Ok(Some(format!(
                    "node {node_id} ({:?}) version {index} min_event_time differs: expected {:?}, actual {:?}",
                    expected.name, expected_meta.min_event_time, actual_meta.min_event_time
                )));
            }
            if expected_meta.max_event_time != actual_meta.max_event_time {
                return Ok(Some(format!(
                    "node {node_id} ({:?}) version {index} max_event_time differs: expected {:?}, actual {:?}",
                    expected.name, expected_meta.max_event_time, actual_meta.max_event_time
                )));
            }
            if expected_meta.extended_attributes != actual_meta.extended_attributes {
                return Ok(Some(format!(
                    "node {node_id} ({:?}) version {index} extended_attributes differ: expected {:?}, actual {:?}",
                    expected.name,
                    expected_meta.extended_attributes,
                    actual_meta.extended_attributes
                )));
            }
        }
        if expected.versions.len() != actual.versions.len() {
            return Ok(Some(format!(
                "node {node_id} ({:?}) metadata count differs: expected {}, actual {}",
                expected.name,
                expected.versions.len(),
                actual.versions.len()
            )));
        }
    }

    for (node_id, expected) in expected_nodes {
        let actual = actual_nodes_sorted[node_id];
        if expected.child_hash != actual.child_hash {
            return Ok(Some(format!(
                "directory node {node_id} ({:?}) derived hash differs: expected {}, actual {}",
                expected.name,
                expected.child_hash.to_hex(),
                actual.child_hash.to_hex()
            )));
        }
    }

    Ok(None)
}

fn preview_validation_error(
    graph: &FetchedGraph,
    preview: &crate::content_tree::FoldedContentState,
    expected_root: ObjectHash,
    expected_manifest_hash: ObjectHash,
    expected_manifest_root: ObjectHash,
    label: &str,
) -> Result<Option<String>, StewardError> {
    let mismatch = if preview.root_tree_hash != expected_root {
        Some(format!(
            "{label} would fold to {} but the tip root tree is {}",
            preview.root_tree_hash.to_hex(),
            expected_root.to_hex()
        ))
    } else if preview.node_manifest_hash != expected_manifest_hash {
        Some(format!(
            "{label} node manifest would hash to {} but the tip commit's manifest is {}",
            preview.node_manifest_hash.to_hex(),
            expected_manifest_hash.to_hex()
        ))
    } else if preview.node_manifest_root != expected_manifest_root {
        Some(format!(
            "{label} node manifest Merkle root would be {} but the tip commit's root is {}",
            preview.node_manifest_root.to_hex(),
            expected_manifest_root.to_hex()
        ))
    } else {
        None
    };
    let Some(mut mismatch) = mismatch else {
        return Ok(None);
    };

    let actual_nodes: HashMap<String, ManifestEntry> = preview
        .manifest
        .iter()
        .cloned()
        .map(|entry| (entry.node_id.clone(), entry))
        .collect();
    match first_replica_divergence(graph, &actual_nodes, &preview.series_leaf_hashes)? {
        Some(detail) => mismatch.push_str(&format!("; first divergence: {detail}")),
        None => mismatch.push_str("; manifests match field-by-field"),
    }
    Ok(Some(mismatch))
}

/// Verify the fetched node manifest is structurally consistent with the fetched
/// tree closure, *before* any mutation.
///
/// Every object is already hash-verified against its key, and `plan_node_diff`
/// rejects any manifest `child_hash` absent from the closure.  But the manifest
/// (node_id-keyed identity) and the tree objects (pure content) are independent
/// byte streams hashed under separate keys, so a hostile remote can publish a
/// manifest that reuses real, in-closure hashes in a *different shape* than the
/// tree that folds to the tip root -- e.g. an extra entry pointing a second
/// name at an existing blob, or a child moved under a different directory.  The
/// pull applies the manifest, so such an inconsistency would commit durably and
/// only be caught by the post-apply fold *after* the transaction is committed,
/// poisoning subsequent diffs on retry.  This check closes that window: for the
/// root and every physical directory it requires that the set of
/// `(name, entry_type, child_hash)` its manifest children declare exactly equals
/// the entries of the tree object stored at that directory's tree hash.  When
/// they all match, faithfully applying the manifest is guaranteed to fold back
/// to the tip's `root_tree_hash`.
fn verify_manifest_matches_tree(graph: &FetchedGraph) -> Result<(), StewardError> {
    let Some(root_tree) = graph.root_tree_hash() else {
        return Ok(());
    };
    let root_id = src_root_id(graph)?.to_string();

    // Group manifest children by parent node_id.
    let mut children: HashMap<&str, Vec<&ManifestEntry>> = HashMap::new();
    for e in &graph.manifest {
        if e.node_id != root_id {
            children
                .entry(e.parent_node_id.as_str())
                .or_default()
                .push(e);
        }
    }

    // The root manifest entry must name the tip's root tree hash as its content
    // address, or the manifest describes a tree other than the one we fetched.
    let root_entry = graph
        .manifest
        .iter()
        .find(|e| e.node_id == root_id)
        .ok_or_else(|| StewardError::Content("manifest has no root entry".to_string()))?;
    if root_entry.child_hash != root_tree {
        return Err(StewardError::Content(format!(
            "manifest root child_hash {} does not match the tip root tree {}",
            root_entry.child_hash.to_hex(),
            root_tree.to_hex()
        )));
    }

    // Every physical directory carries a tree object; its manifest children must
    // exactly match that tree object's entries.  Dynamic directories and leaves
    // carry a recipe/blob/series hash instead, so they are compared only as the
    // child of their own parent (above), not descended here.
    for dir in graph
        .manifest
        .iter()
        .filter(|e| e.entry_type == EntryType::DirectoryPhysical)
    {
        let tree_entries = match graph.objects.get(&dir.child_hash) {
            Some(FetchedObject::Tree(entries)) => entries,
            _ => {
                return Err(StewardError::Content(format!(
                    "directory node {} references {} which is not a tree object in the closure",
                    dir.node_id,
                    dir.child_hash.to_hex()
                )));
            }
        };
        let mut expected: Vec<(&str, EntryType, ObjectHash)> = tree_entries
            .iter()
            .map(|t| (t.name.as_str(), t.entry_type, t.child_hash))
            .collect();
        expected.sort_by(|a, b| a.0.cmp(b.0));

        let mut actual: Vec<(&str, EntryType, ObjectHash)> = children
            .get(dir.node_id.as_str())
            .map(|kids| {
                kids.iter()
                    .map(|k| (k.name.as_str(), k.entry_type, k.child_hash))
                    .collect()
            })
            .unwrap_or_default();
        actual.sort_by(|a, b| a.0.cmp(b.0));

        if expected != actual {
            return Err(StewardError::Content(format!(
                "manifest children of directory {} do not match its tree object {}: \
                 the remote's node manifest is inconsistent with its content tree",
                dir.node_id,
                dir.child_hash.to_hex()
            )));
        }
    }

    Ok(())
}

/// One node's desired name change within a single directory.
struct RenameIntent {
    /// The node's current name in the target.
    old: String,
    /// The node's name in the source (its final name after the pull).
    new: String,
    /// The node's adopted `node_id`, used to mint a unique temporary name when
    /// a rename cycle must be broken.
    node_id: String,
}

/// Emit a collision-safe sequence of rename ops for one directory's children.
///
/// Within a directory a rename's target name can only be occupied by another
/// node that is itself being renamed away: two nodes cannot share a name in the
/// source tree, so the target of `old -> new` never collides with a sibling that
/// keeps its name. Simple chains therefore resolve by repeatedly applying any
/// rename whose target is already free. A cycle (an `a<->b` swap or a longer
/// rotation) has no such rename; it is broken by first moving one node to a
/// unique temporary name (freeing its old name so the rest of the cycle can
/// proceed), then renaming that temporary to its final name once the name frees.
fn emit_collision_safe_renames(
    parent: &str,
    intents: Vec<RenameIntent>,
    mut reserved_names: BTreeSet<String>,
    ops: &mut Vec<ApplyOp>,
) {
    // Pending renames keyed by the name each currently occupies. A target `new`
    // is blocked exactly while it is still a key here (some node has not yet
    // vacated it).
    let mut pending: HashMap<String, RenameIntent> = HashMap::new();
    for intent in intents {
        if intent.old != intent.new {
            let _ = pending.insert(intent.old.clone(), intent);
        }
    }

    loop {
        // Apply every rename whose target is currently free, in a deterministic
        // order so the emitted plan is stable.
        let mut free: Vec<String> = pending
            .iter()
            .filter(|(_, intent)| !pending.contains_key(&intent.new))
            .map(|(old, _)| old.clone())
            .collect();
        free.sort();

        if !free.is_empty() {
            for old in free {
                if let Some(intent) = pending.remove(&old) {
                    ops.push(ApplyOp::Rename {
                        parent: parent.to_string(),
                        old: intent.old,
                        new: intent.new,
                    });
                }
            }
            continue;
        }

        if pending.is_empty() {
            break;
        }

        // Only cycles remain: break one by staging its lexicographically first
        // node through a unique temporary name. The node_id makes the temporary
        // name unique and collision-free against any real sibling.
        let victim = pending
            .keys()
            .min()
            .cloned()
            .expect("pending is non-empty in the cycle branch");
        let intent = pending.remove(&victim).expect("victim key is present");
        let base = format!(".pull-rename-tmp-{}", intent.node_id);
        let mut temp = base.clone();
        let mut suffix = 0_u64;
        while reserved_names.contains(&temp) || pending.contains_key(&temp) {
            suffix += 1;
            temp = format!("{base}-{suffix}");
        }
        let _ = reserved_names.insert(temp.clone());
        ops.push(ApplyOp::Rename {
            parent: parent.to_string(),
            old: intent.old,
            new: temp.clone(),
        });
        let _ = pending.insert(
            temp.clone(),
            RenameIntent {
                old: temp,
                new: intent.new,
                node_id: intent.node_id,
            },
        );
    }
}

/// Diff the fetched source manifest against the target's current node state,
/// keyed by `node_id`, producing the ordered apply plan and the create counts.
fn plan_full_replacement(
    graph: &FetchedGraph,
    root: ObjectHash,
    target_nodes: &HashMap<String, ManifestEntry>,
) -> Result<(Vec<ApplyOp>, RebuildOutcome), StewardError> {
    let root_id = src_root_id(graph)?;
    let mut existing: Vec<&ManifestEntry> = target_nodes
        .values()
        .filter(|entry| entry.node_id != root_id)
        .collect();
    existing.sort_by_key(|entry| std::cmp::Reverse(target_depth(&entry.node_id, target_nodes)));
    let mut deletes = existing
        .into_iter()
        .map(|entry| ApplyOp::Delete {
            parent_path: target_path(&entry.parent_node_id, target_nodes),
            name: entry.name.clone(),
        })
        .collect::<Vec<_>>();
    let (mut creates, outcome) = plan_node_diff(
        graph,
        root,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
    )?;
    deletes.append(&mut creates);
    Ok((deletes, outcome))
}

fn plan_node_diff(
    graph: &FetchedGraph,
    root: ObjectHash,
    target_nodes: &HashMap<String, ManifestEntry>,
    target_series: &HashMap<String, Vec<ObjectHash>>,
    target_series_leaves: &HashMap<String, Vec<ObjectHash>>,
) -> Result<(Vec<ApplyOp>, RebuildOutcome), StewardError> {
    let root_id = src_root_id(graph)?.to_string();

    // Index the source manifest by node_id and by parent for breadth-first
    // ordering (parents before children).
    let mut source_by_id: HashMap<&str, &ManifestEntry> = HashMap::new();
    let mut children: HashMap<&str, Vec<&ManifestEntry>> = HashMap::new();
    for entry in &graph.manifest {
        let _ = source_by_id.insert(entry.node_id.as_str(), entry);
        if entry.node_id != root_id {
            children
                .entry(entry.parent_node_id.as_str())
                .or_default()
                .push(entry);
        }
    }
    for kids in children.values_mut() {
        kids.sort_by(|a, b| a.name.cmp(&b.name));
    }

    let mut ops = Vec::new();
    let mut outcome = RebuildOutcome {
        root_tree_hash: Some(root),
        ..RebuildOutcome::default()
    };

    // Deletions first: target nodes absent from the source, deepest-first so a
    // directory is emptied before it is unlinked.
    let mut deletions: Vec<&ManifestEntry> = target_nodes
        .values()
        .filter(|t| t.node_id != root_id && !source_by_id.contains_key(t.node_id.as_str()))
        .collect();
    deletions.sort_by_key(|t| std::cmp::Reverse(target_depth(&t.node_id, target_nodes)));
    for t in deletions {
        ops.push(ApplyOp::Delete {
            parent_path: target_path(&t.parent_node_id, target_nodes),
            name: t.name.clone(),
        });
    }

    // Creates / renames / versions in breadth-first order from the root.
    let mut queue: VecDeque<&str> = VecDeque::new();
    queue.push_back(root_id.as_str());
    while let Some(parent_id) = queue.pop_front() {
        let Some(kids) = children.get(parent_id) else {
            continue;
        };

        // Renames for this directory are planned first, as a collision-safe
        // batch. A source-side rename preserves a node's identity, so a name
        // swap (a<->b) or longer rename cycle among siblings shows up as two or
        // more renames whose targets each land on a name another not-yet-moved
        // sibling still holds. Applying them naively one at a time aborts on the
        // first collision; emit_collision_safe_renames stages cycles through a
        // temporary name so the whole rotation lands. Emitting every rename in
        // this directory before any create also lets a newly adopted node take
        // a name an existing sibling is vacating in the same pull.
        let mut renames = Vec::new();
        for entry in kids {
            if let Some(t) = target_nodes.get(&entry.node_id)
                && t.parent_node_id == entry.parent_node_id
                && t.name != entry.name
            {
                renames.push(RenameIntent {
                    old: t.name.clone(),
                    new: entry.name.clone(),
                    node_id: entry.node_id.clone(),
                });
            }
        }
        let reserved_names = kids
            .iter()
            .map(|entry| entry.name.clone())
            .chain(
                target_nodes
                    .values()
                    .filter(|entry| entry.parent_node_id == parent_id)
                    .map(|entry| entry.name.clone()),
            )
            .collect();
        emit_collision_safe_renames(parent_id, renames, reserved_names, &mut ops);

        for entry in kids {
            plan_one(
                entry,
                graph,
                target_nodes,
                target_series,
                target_series_leaves,
                &mut ops,
                &mut outcome,
            )?;
            if entry.entry_type == EntryType::DirectoryPhysical {
                queue.push_back(entry.node_id.as_str());
            }
        }
    }

    Ok((ops, outcome))
}

/// Plan the operations for a single source node against its target twin.
fn plan_one(
    entry: &ManifestEntry,
    graph: &FetchedGraph,
    target_nodes: &HashMap<String, ManifestEntry>,
    _target_series: &HashMap<String, Vec<ObjectHash>>,
    target_series_leaves: &HashMap<String, Vec<ObjectHash>>,
    ops: &mut Vec<ApplyOp>,
    outcome: &mut RebuildOutcome,
) -> Result<(), StewardError> {
    let existing = target_nodes.get(&entry.node_id);
    let create = existing.is_none();

    if let Some(t) = existing {
        if t.parent_node_id != entry.parent_node_id {
            return Err(StewardError::Content(format!(
                "node {} was reparented from {} to {}; reparenting is not supported",
                entry.node_id, t.parent_node_id, entry.parent_node_id
            )));
        }
        if t.entry_type != entry.entry_type {
            return Err(StewardError::Content(format!(
                "node {} changed entry type from {:?} to {:?}; this is not supported",
                entry.node_id, t.entry_type, entry.entry_type
            )));
        }
        // A name change (t.name != entry.name) is not emitted here: renames are
        // planned as a collision-safe per-directory batch in plan_node_diff,
        // before this node's create/version op, so swaps and cycles land.
    }

    let content_changed = existing.is_none_or(|t| t.child_hash != entry.child_hash);

    // The fold commits to each version's metadata -- mtime, event-time bounds,
    // extended attributes -- alongside its bytes, so a source-side change to
    // metadata alone still moves this entry's contribution to its parent's tree
    // hash and every ancestor's.  Pruning on `child_hash` alone would plan no
    // op at all, leave the replica holding stale metadata, and then fail the
    // post-apply fold -- durably, because the commit lands before the fold runs,
    // so every retry re-diffs against the same stale state and fails again.
    // (`content_diff::diff_dir` prunes on both for the same reason.)
    let meta_changed = existing.is_some_and(|t| t.versions != entry.versions);
    let needs_write = create || content_changed || meta_changed;

    match entry.entry_type {
        EntryType::DirectoryPhysical => {
            if create {
                outcome.dirs += 1;
            }
            ops.push(ApplyOp::Dir {
                parent: entry.parent_node_id.clone(),
                name: entry.name.clone(),
                node_id: entry.node_id.clone(),
                create,
            });
        }
        EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
            if create {
                outcome.files += 1;
            }
            let versions = if needs_write {
                vec![planned_version(
                    graph,
                    entry.child_hash,
                    entry.versions.first(),
                )?]
            } else {
                Vec::new()
            };
            ops.push(ApplyOp::File {
                parent: entry.parent_node_id.clone(),
                name: entry.name.clone(),
                node_id: entry.node_id.clone(),
                create,
                entry_type: entry.entry_type,
                versions,
                collapse_first: false,
            });
        }
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
            if create {
                outcome.series += 1;
            }
            let series = series_v2(graph, entry.child_hash)?;
            let leaves_from = plan_series_v2_leaves(
                entry,
                series,
                target_series_leaves,
                existing.map(|t| t.child_hash),
            )?;
            // Always emit the op on create (adopting the node even
            // if, defensively, it turned out to need no leaves), and
            // otherwise only when there is a real suffix to append.
            if create || leaves_from < series.leaf_hashes.len() as u64 {
                ops.push(ApplyOp::SeriesV2 {
                    parent: entry.parent_node_id.clone(),
                    name: entry.name.clone(),
                    node_id: entry.node_id.clone(),
                    create,
                    entry_type: entry.entry_type,
                    manifest_hash: entry.child_hash,
                    leaves_from,
                    replicated_mtime: replicated_mtime(entry),
                });
            }
        }
        EntryType::Symlink => {
            if create {
                outcome.symlinks += 1;
            }
            if needs_write {
                let bytes = blob_bytes(graph, entry.child_hash)?;
                let target = String::from_utf8(bytes).map_err(|e| {
                    StewardError::Content(format!("symlink target is not utf-8: {e}"))
                })?;
                ops.push(ApplyOp::Symlink {
                    parent: entry.parent_node_id.clone(),
                    name: entry.name.clone(),
                    node_id: entry.node_id.clone(),
                    create,
                    target,
                    mtime: replicated_mtime(entry),
                });
            }
        }
        EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic => {
            if create {
                outcome.dynamic += 1;
            }
            if needs_write {
                let bytes = blob_bytes(graph, entry.child_hash)?;
                let (factory, config) = decode_recipe(&bytes).map_err(|e| {
                    StewardError::Content(format!("decode recipe for {}: {e}", entry.name))
                })?;
                ops.push(ApplyOp::Dynamic {
                    parent: entry.parent_node_id.clone(),
                    name: entry.name.clone(),
                    node_id: entry.node_id.clone(),
                    create,
                    factory,
                    config,
                    mtime: replicated_mtime(entry),
                });
            }
        }
    }
    Ok(())
}

/// The mtime a single-version node (symlink, dynamic recipe) should adopt.
///
/// Such a node has exactly one [`VersionMeta`], so the last entry is the node's
/// current state; a node whose source recorded no mtime gets the local clock,
/// as before.
fn replicated_mtime(entry: &ManifestEntry) -> Option<i64> {
    entry.versions.last().and_then(|meta| meta.timestamp)
}

/// Resolve a `watertown.series.v2` object to its verified [`FetchedSeriesV2`] state.
///
/// # Errors
///
/// Returns an error if the object at `series_hash` is not a verified series.
fn series_v2(
    graph: &FetchedGraph,
    series_hash: ObjectHash,
) -> Result<&FetchedSeriesV2, StewardError> {
    match graph.objects.get(&series_hash) {
        Some(FetchedObject::SeriesV2(series)) => Ok(series),
        Some(_) => Err(StewardError::Content(format!(
            "expected a watertown.series.v2 series at {} but found a different object shape",
            series_hash.to_hex()
        ))),
        None => Err(StewardError::Content(format!(
            "series object {} missing from graph",
            series_hash.to_hex()
        ))),
    }
}

/// Decide the suffix of a v2 logical series' leaves the target still needs
/// (release blocker item 1, `docs/logical-series-identity-design.md`).
///
/// A `watertown.series.v2` series' `child_hash` is its manifest hash, which is a pure
/// function of its whole logical content (leaf hashes, aggregate bounds,
/// schema, attributes -- everything except mtime); an unchanged `child_hash`
/// therefore means an unchanged logical state, full stop.
///
/// `pond maintain --collapse-versions` is an explicit no-op on a logical
/// series, so a verified series never legitimately un-prefixes what a
/// caught-up mirror already holds. If the target's already-materialized leaf
/// hashes are not an exact prefix of the source's, that is corruption or an
/// unsupported non-append change, not a case to reconcile by rewriting.
///
/// # Errors
///
/// Returns an error if the target's currently-held v2 leaf hashes are
/// unknown for a node whose `child_hash` changed, or if they are not a
/// prefix of the source's verified leaf hashes.
fn plan_series_v2_leaves(
    entry: &ManifestEntry,
    series: &FetchedSeriesV2,
    target_series_leaves: &HashMap<String, Vec<ObjectHash>>,
    existing_child_hash: Option<ObjectHash>,
) -> Result<u64, StewardError> {
    let incoming = &series.leaf_hashes;
    if let Some(child_hash) = existing_child_hash
        && child_hash == entry.child_hash
    {
        return Ok(incoming.len() as u64);
    }
    let held: &[ObjectHash] = match existing_child_hash {
        None => &[],
        Some(_) => target_series_leaves
            .get(&entry.node_id)
            .map(Vec::as_slice)
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "v2 series node {} changed but its current logical leaves are unknown",
                    entry.node_id
                ))
            })?,
    };
    if incoming.len() < held.len() || incoming[..held.len()] != *held {
        return Err(StewardError::Content(format!(
            "v2 series node {} diverged from its previously materialized logical leaves: a \
             verified v2 series never rewrites or collapses leaf history, so this can only mean \
             corruption or an unsupported non-append change",
            entry.node_id
        )));
    }
    Ok(held.len() as u64)
}

/// Apply an ordered plan within an open transaction, adopting source node ids.
/// Small versions write from buffered bytes; large external versions stream from
/// the remote blob store straight into the writer, never buffered (D7). A v2
/// series' logical leaves are reconstructed from `graph`'s already-fetched
/// physical objects and independently re-verified before being written (see
/// [`materialize_series_v2`]).
async fn apply_ops(
    root_node_id: &str,
    root_wd: WD,
    ops: &[ApplyOp],
    remote: &dyn ContentSource,
    graph: &FetchedGraph,
) -> Result<(), StewardError> {
    let mut dir_wd: HashMap<String, WD> = HashMap::new();
    let _ = dir_wd.insert(root_node_id.to_string(), root_wd.clone());

    for op in ops {
        match op {
            ApplyOp::Delete { parent_path, name } => {
                let pwd = if parent_path.is_empty() {
                    root_wd.clone()
                } else {
                    root_wd.open_dir_path(parent_path).await?
                };
                pwd.remove_entry(name).await?;
            }
            ApplyOp::Rename { parent, old, new } => {
                parent_wd(&dir_wd, parent)?.rename_entry(old, new).await?;
            }
            ApplyOp::Dir {
                parent,
                name,
                node_id,
                create,
            } => {
                let pwd = parent_wd(&dir_wd, parent)?.clone();
                let child = if *create {
                    pwd.insert_directory_with_id(name, parse_node_id(node_id)?)
                        .await?
                } else {
                    pwd.open_dir_path(name).await?
                };
                let _ = dir_wd.insert(node_id.clone(), child);
            }
            ApplyOp::File {
                parent,
                name,
                node_id,
                create,
                entry_type,
                versions,
                collapse_first,
            } => {
                let pwd = parent_wd(&dir_wd, parent)?;
                let mut remaining = versions.iter();
                // A collapsing rewrite always targets an existing node, so its
                // first version goes through the collapsing writer below, never
                // the create path.
                if *create {
                    // The first version is written through the writer returned
                    // at creation: a pending file has no row to re-resolve by
                    // path yet.  An adopted file always has at least one
                    // version, but tolerate an empty create defensively.
                    if let Some(first) = remaining.next() {
                        let writer = pwd
                            .create_file_with_id(name, parse_node_id(node_id)?)
                            .await?;
                        write_version(pwd, name, writer, first, *entry_type, remote).await?;
                    }
                } else if *collapse_first && let Some(first) = remaining.next() {
                    // Replicate a source-side compaction: the first version
                    // starts a fresh baseline and supersedes every version the
                    // target already held, so its fold matches the source.
                    let writer = pwd
                        .async_writer_path_collapsing_with_type(name, *entry_type)
                        .await?;
                    write_version(pwd, name, writer, first, *entry_type, remote).await?;
                }
                for version in remaining {
                    let writer = pwd.async_writer_path_with_type(name, *entry_type).await?;
                    write_version(pwd, name, writer, version, *entry_type, remote).await?;
                }
            }
            ApplyOp::Symlink {
                parent,
                name,
                node_id,
                create,
                target,
                mtime,
            } => {
                let pwd = parent_wd(&dir_wd, parent)?;
                if !create {
                    pwd.remove_entry(name).await?;
                }
                pwd.insert_symlink_with_id(name, parse_node_id(node_id)?, target, *mtime)
                    .await?;
            }
            ApplyOp::Dynamic {
                parent,
                name,
                node_id,
                create,
                factory,
                config,
                mtime,
            } => {
                let pwd = parent_wd(&dir_wd, parent)?;
                if !create {
                    pwd.remove_entry(name).await?;
                }
                pwd.insert_dynamic_with_id(
                    name,
                    parse_node_id(node_id)?,
                    factory,
                    config.clone(),
                    *mtime,
                )
                .await?;
            }
            ApplyOp::SeriesV2 {
                parent,
                name,
                node_id,
                create,
                entry_type,
                manifest_hash,
                leaves_from,
                replicated_mtime,
            } => {
                let pwd = parent_wd(&dir_wd, parent)?.clone();
                let series = series_v2(graph, *manifest_hash)?;
                materialize_series_v2(
                    &pwd,
                    name,
                    parse_node_id(node_id)?,
                    *create,
                    *entry_type,
                    series,
                    graph,
                    remote,
                    *leaves_from,
                    *replicated_mtime,
                )
                .await?;
            }
        }
    }
    Ok(())
}

/// Write one file/series version through `writer`, then finalize it.  An inline
/// version copies buffered bytes; an external version streams from the remote
/// blob store in bounded chunks, re-hashing to enforce content addressing so a
/// large blob never lands in a single buffer (D7).
async fn write_version(
    parent_wd: &WD,
    name: &str,
    mut writer: std::pin::Pin<Box<dyn tinyfs::FileMetadataWriter>>,
    version: &PlannedVersion,
    entry_type: EntryType,
    remote: &dyn ContentSource,
) -> Result<(), StewardError> {
    match &version.source {
        VersionSource::Inline(bytes) => {
            writer.write_all(bytes).await?;
        }
        VersionSource::External(hash) => {
            stream_external_blob(&mut writer, *hash, remote).await?;
        }
    }
    finalize_writer(parent_wd, name, writer, entry_type, &version.meta).await
}

/// Stream a large external blob from the remote blob store into `writer` in
/// bounded chunks, hashing as it passes; the streamed bytes must hash to `hash`
/// or content addressing is violated and the rebuild fails.
async fn stream_external_blob(
    writer: &mut std::pin::Pin<Box<dyn tinyfs::FileMetadataWriter>>,
    hash: ObjectHash,
    remote: &dyn ContentSource,
) -> Result<(), StewardError> {
    use tokio::io::AsyncReadExt;
    let mut reader = remote
        .get_blob_reader(hash)
        .await
        .map_err(|e| StewardError::Content(format!("open external blob: {e}")))?
        .ok_or_else(|| {
            StewardError::Content(format!(
                "external blob {} vanished from the remote before rebuild",
                hash.to_hex()
            ))
        })?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 8 * 1024 * 1024];
    loop {
        let n = reader
            .read(&mut buf)
            .await
            .map_err(|e| StewardError::Content(format!("read external blob: {e}")))?;
        if n == 0 {
            break;
        }
        let _ = hasher.update(&buf[..n]);
        writer.write_all(&buf[..n]).await?;
    }
    let computed = ObjectHash::from_bytes(*hasher.finalize().as_bytes());
    if computed != hash {
        return Err(StewardError::Content(format!(
            "external blob streamed as {} but hashes to {}",
            hash.to_hex(),
            computed.to_hex()
        )));
    }
    Ok(())
}

/// Finalize a version writer, reapplying the node metadata the source recorded.
///
/// The mtime, when carried, is adopted verbatim so the mirrored version keeps
/// the timestamp it was originally written with rather than claiming to have
/// been modified at pull time. When the source recorded an event-time range,
/// the replica sets it explicitly so the replicated node carries the same
/// bounds as the original. Otherwise a table series can still recover its range
/// from the parquet footer it just wrote (which also shuts the writer down);
/// every other kind just closes, leaving the bounds NULL as before.
pub(crate) async fn finalize_writer(
    parent_wd: &WD,
    name: &str,
    mut writer: std::pin::Pin<Box<dyn tinyfs::FileMetadataWriter>>,
    entry_type: EntryType,
    meta: &VersionMeta,
) -> Result<(), StewardError> {
    if let Some(mtime) = meta.timestamp {
        writer.set_mtime(mtime);
    }
    if let Some((min, max)) = meta.bounds() {
        writer.set_temporal_metadata(min, max, timestamp_column(meta));
        writer.shutdown().await?;
    } else if entry_type == EntryType::TablePhysicalSeries {
        let _ = writer.infer_temporal_bounds().await?;
    } else {
        writer.shutdown().await?;
    }
    if let Some(json) = &meta.extended_attributes {
        let attributes = tlogfs::schema::ExtendedAttributes::from_json(json).map_err(|error| {
            StewardError::Content(format!("decode extended attributes for {name:?}: {error}"))
        })?;
        parent_wd
            .set_extended_attributes(name, attributes.attributes)
            .await?;
    }
    Ok(())
}

/// The timestamp column named by a version's replicated extended attributes,
/// falling back to the system default when they say nothing.
fn timestamp_column(meta: &VersionMeta) -> String {
    meta.extended_attributes
        .as_deref()
        .and_then(|json| tlogfs::schema::ExtendedAttributes::from_json(json).ok())
        .map_or_else(
            || "Timestamp".to_string(),
            |attrs| attrs.timestamp_column().to_string(),
        )
}

/// Materialize a verified `watertown.series.v2` logical series into the destination
/// as native tlogfs rows (release blocker item 1,
/// `docs/logical-series-identity-design.md`).
///
/// Writes exactly one Oplog append per logical leaf
/// (`crates/tlogfs/src/series_identity.rs`), in leaf order, skipping every
/// leaf before `leaves_from` (already held by the target) and independently
/// re-verifying every leaf about to be written -- recomputed from `graph`'s
/// already-fetched, already-verified pack physical objects using the exact
/// same canonical-hash algorithm [`fetch_series_v2`] used to build `series`
/// -- against `series.leaf_hashes[i]` *before* it is ever handed to a
/// writer, so a divergent reconstruction is caught before any write, not
/// merely before commit.
///
/// Reuses `graph.objects`/`graph.bytes`, which already hold every selected
/// pack's physical objects as ordinary [`FetchedObject::Blob`]/
/// [`FetchedObject::External`] entries exactly like a v1 version blob, so no
/// additional remote round-trip happens for content already durable in
/// `graph`; a large external physical object is streamed from `remote` and
/// re-hashed exactly as [`stream_external_blob`] does for v1.
async fn materialize_series_v2(
    pwd: &WD,
    name: &str,
    node_id: NodeID,
    create: bool,
    entry_type: EntryType,
    series: &FetchedSeriesV2,
    graph: &FetchedGraph,
    remote: &dyn ContentSource,
    leaves_from: u64,
    replicated_mtime: Option<i64>,
) -> Result<(), StewardError> {
    match entry_type {
        EntryType::FilePhysicalSeries => {
            materialize_file_series_v2(
                pwd,
                name,
                node_id,
                create,
                series,
                graph,
                remote,
                leaves_from,
                replicated_mtime,
            )
            .await
        }
        EntryType::TablePhysicalSeries => {
            materialize_table_series_v2(
                pwd,
                name,
                node_id,
                create,
                series,
                graph,
                remote,
                leaves_from,
                replicated_mtime,
            )
            .await
        }
        other => Err(StewardError::Content(format!(
            "materialize_series_v2 called for non-series entry type {other:?}"
        ))),
    }
}

/// Reapply one v2 logical leaf's own event-time bounds, timestamp-column
/// attribute, and any other canonical logical attributes on `writer`,
/// reproducing byte-identical logical-attributes JSON to what the source
/// wrote -- exact bytes via
/// [`tinyfs::FileMetadataWriter::set_exact_logical_attributes`], not just
/// the single `timestamp_column` key `set_temporal_metadata` alone can
/// carry (release blocker item 2, `docs/logical-series-identity-design.md`)
/// -- so the destination's own `stamp_logical_leaf` recomputes the
/// identical leaf hash from identical inputs (`min_event_time`,
/// `max_event_time`, `extended_attributes`).
///
/// # Errors
///
/// Returns an error if the descriptor carries only one of `min_event_time`/
/// `max_event_time` (a real writer always sets both together via
/// `set_temporal_metadata`, or neither) -- treated as unsupported/corrupt
/// rather than guessed at.
fn apply_descriptor_bounds(
    writer: &mut std::pin::Pin<Box<dyn tinyfs::FileMetadataWriter>>,
    descriptor: &PackLeafDescriptor,
) -> Result<(), StewardError> {
    match (descriptor.min_event_time(), descriptor.max_event_time()) {
        (Some(min), Some(max)) => {
            writer.set_temporal_metadata(min, max, descriptor_timestamp_column(descriptor)?);
        }
        (None, None) => {}
        _ => {
            return Err(StewardError::Content(
                "v2 leaf descriptor carries only one of min_event_time/max_event_time; a \
                 legitimate writer always sets both together or neither"
                    .to_string(),
            ));
        }
    }
    apply_descriptor_exact_attributes(writer, descriptor);
    Ok(())
}

/// Pass a v2 leaf descriptor's canonical logical-attributes bytes through
/// verbatim to the destination writer (release blocker item 2,
/// `docs/logical-series-identity-design.md`), so keys beyond the single
/// well-known `timestamp_column` -- anything a source leaf set via a raw
/// attribute setter -- round-trip exactly rather than being silently
/// dropped by `set_temporal_metadata`'s single-string reconstruction. A
/// no-op when the descriptor carries no logical attributes at all.
fn apply_descriptor_exact_attributes(
    writer: &mut std::pin::Pin<Box<dyn tinyfs::FileMetadataWriter>>,
    descriptor: &PackLeafDescriptor,
) {
    if let Some(bytes) = descriptor.logical_attributes() {
        writer.set_exact_logical_attributes(bytes.to_vec());
    }
}

/// The timestamp column a v2 leaf descriptor's canonical logical attributes
/// name, falling back to the system default when it names none. Reads the
/// well-known key directly out of the raw JSON rather than through
/// [`tlogfs::schema::ExtendedAttributes::from_json`], which requires every
/// attribute value in the map to be a string -- canonical logical attributes
/// may legitimately carry non-string sibling values (release blocker item 2,
/// `docs/logical-series-identity-design.md`), and those must not prevent
/// recovering this one column name.
fn descriptor_timestamp_column(descriptor: &PackLeafDescriptor) -> Result<String, StewardError> {
    match descriptor.logical_attributes() {
        None => Ok("Timestamp".to_string()),
        Some(bytes) => {
            let json = std::str::from_utf8(bytes).map_err(|e| {
                StewardError::Content(format!(
                    "v2 leaf logical attributes are not valid utf-8: {e}"
                ))
            })?;
            let value: serde_json::Value = serde_json::from_str(json).map_err(|e| {
                StewardError::Content(format!(
                    "v2 leaf logical attributes are not valid json: {e}"
                ))
            })?;
            match value.get(tlogfs::schema::watertown::TIMESTAMP_COLUMN) {
                None => Ok("Timestamp".to_string()),
                Some(serde_json::Value::String(column)) => Ok(column.clone()),
                Some(other) => Err(StewardError::Content(format!(
                    "v2 leaf logical attributes' '{}' key is not a string: {other}",
                    tlogfs::schema::watertown::TIMESTAMP_COLUMN
                ))),
            }
        }
    }
}

/// Materialize a `watertown.series.v2` `FilePhysicalSeries` whose manifest declares
/// `leaf_count() == 0` (release blocker item 1,
/// `docs/logical-series-identity-design.md`): a legitimately empty,
/// metadata-only series that has never carried a logical leaf (for example a
/// file series created and immediately shut down with zero bytes).
///
/// Only ever called for a `FilePhysicalSeries`: an equivalent
/// `TablePhysicalSeries` state cannot be materialized at all (see
/// [`materialize_table_series_v2`]'s explicit rejection) since
/// [`sync_store::content::SeriesManifest::new`] unconditionally requires a
/// schema fingerprint for [`sync_store::content::PayloadKind::Table`]
/// regardless of `leaf_count`, and a zero-content write can never carry one.
///
/// Such a series has no packs to cover it
/// ([`sync_store::content::select_exact_cover`] special-cases `leaf_count ==
/// 0` to an empty cover) and [`build_series_manifest`]-equivalent source
/// folding never attributes any leaf-bearing version's metadata to it
/// either, so there is nothing to reproduce beyond the node's existence and
/// its replicated mtime: an empty create, exactly mirroring what a real
/// writer produces for a zero-byte first version (`tlogfs`'s
/// `store_file_content_ref` already gives a content-empty
/// `FilePhysicalSeries` write its own deterministic default metadata --
/// `FileMetadata::Data`, a null-bounds `Series` row -- so this materializer
/// does not need to, and must not, invent temporal bounds or attributes of
/// its own).
///
/// A no-op when `create` is `false`: the only way [`plan_one`] ever emits a
/// v2 series op for a `leaf_count() == 0` manifest is on first creation (see
/// [`plan_series_v2_leaves`]/[`plan_one`]'s doc comments), so an adopt-only
/// call here would mean the target already holds this exact (unchanged,
/// still-empty) state.
async fn materialize_empty_series(
    pwd: &WD,
    name: &str,
    node_id: NodeID,
    create: bool,
    replicated_mtime: Option<i64>,
) -> Result<(), StewardError> {
    if !create {
        return Ok(());
    }
    let mut writer = pwd.create_file_with_id(name, node_id).await?;
    if let Some(mtime) = replicated_mtime {
        writer.set_mtime(mtime);
    }
    writer.shutdown().await?;
    Ok(())
}

/// Reconstruct and materialize a v2 `FilePhysicalSeries`' logical leaves.
///
/// Mirrors [`FileLeafPartitioner`]'s streaming, physical-object-boundary-
/// agnostic partitioning exactly (a leaf may span physical objects, several
/// leaves may share one object), but additionally buffers -- only for a leaf
/// at or after `leaves_from` -- the leaf's own bytes so they can be written,
/// after independent hash re-verification, through the same per-version
/// writer API a v1 rebuild already uses.
async fn materialize_file_series_v2(
    pwd: &WD,
    name: &str,
    node_id: NodeID,
    create: bool,
    series: &FetchedSeriesV2,
    graph: &FetchedGraph,
    remote: &dyn ContentSource,
    leaves_from: u64,
    replicated_mtime: Option<i64>,
) -> Result<(), StewardError> {
    let total_leaves = series.manifest.leaf_count();
    if total_leaves == 0 {
        return materialize_empty_series(pwd, name, node_id, create, replicated_mtime).await;
    }
    let mut leaf_index: u64 = 0;
    let mut node_created = false;
    // Leaf-in-progress state, live only while one leaf's bytes are being
    // assembled from (possibly many) physical objects/chunks. Never carried
    // across a pack boundary: packs tile disjoint leaf ranges, so a
    // leftover `hasher` at the end of a pack's objects is corruption.
    let mut hasher: Option<IncrementalFileLeafHasher> = None;
    let mut buffer: Option<Vec<u8>> = None;
    let mut descriptor: Option<&PackLeafDescriptor> = None;

    for (pack_hash, pack) in &series.packs {
        let mut descriptors = pack.leaf_descriptors().iter();
        for &object_hash in pack.physical_object_hashes() {
            match graph.objects.get(&object_hash) {
                Some(FetchedObject::Blob(bytes)) => {
                    let bytes = bytes.clone();
                    feed_file_chunk(
                        &bytes,
                        &mut descriptors,
                        pwd,
                        name,
                        node_id,
                        create,
                        &mut node_created,
                        &mut leaf_index,
                        leaves_from,
                        &series.leaf_hashes,
                        &mut hasher,
                        &mut buffer,
                        &mut descriptor,
                        replicated_mtime,
                        total_leaves,
                    )
                    .await?;
                }
                Some(FetchedObject::External) => {
                    let mut reader = remote
                        .get_blob_reader(object_hash)
                        .await
                        .map_err(|e| {
                            StewardError::Content(format!(
                                "stream physical object {object_hash}: {e}"
                            ))
                        })?
                        .ok_or_else(|| {
                            StewardError::Content(format!(
                                "physical object {object_hash} vanished from the remote blob store during streaming"
                            ))
                        })?;
                    let mut object_hasher = blake3::Hasher::new();
                    let mut buf = vec![0u8; 256 * 1024];
                    loop {
                        let n = reader.read(&mut buf).await.map_err(|e| {
                            StewardError::Content(format!(
                                "read physical object {object_hash}: {e}"
                            ))
                        })?;
                        if n == 0 {
                            break;
                        }
                        let _ = object_hasher.update(&buf[..n]);
                        feed_file_chunk(
                            &buf[..n],
                            &mut descriptors,
                            pwd,
                            name,
                            node_id,
                            create,
                            &mut node_created,
                            &mut leaf_index,
                            leaves_from,
                            &series.leaf_hashes,
                            &mut hasher,
                            &mut buffer,
                            &mut descriptor,
                            replicated_mtime,
                            total_leaves,
                        )
                        .await?;
                    }
                    let computed = ObjectHash::from_bytes(*object_hasher.finalize().as_bytes());
                    if computed != object_hash {
                        return Err(StewardError::Content(format!(
                            "physical object hashes to {computed} but was fetched as {object_hash}"
                        )));
                    }
                }
                other => {
                    return Err(StewardError::Content(format!(
                        "expected a physical blob object at {object_hash} but found {other:?}"
                    )));
                }
            }
        }
        if hasher.is_some() {
            return Err(StewardError::Content(format!(
                "file series pack {pack_hash} left a logical leaf incomplete at its own \
                 boundary (a leaf never spans packs)"
            )));
        }
        if descriptors.next().is_some() {
            return Err(StewardError::Content(format!(
                "file series pack {pack_hash} has fewer physical bytes than its declared leaf \
                 descriptors"
            )));
        }
    }
    if leaf_index != total_leaves {
        return Err(StewardError::Content(format!(
            "file series materialized {leaf_index} leaf/leaves but the manifest declares \
             {total_leaves}"
        )));
    }
    Ok(())
}

/// Feed one chunk of a v2 file series' concatenated physical byte stream
/// through the leaf partitioner, writing out any leaf it completes at or
/// after `leaves_from`.
#[allow(clippy::too_many_arguments)]
async fn feed_file_chunk<'d>(
    mut chunk: &[u8],
    descriptors: &mut std::slice::Iter<'d, PackLeafDescriptor>,
    pwd: &WD,
    name: &str,
    node_id: NodeID,
    create: bool,
    node_created: &mut bool,
    leaf_index: &mut u64,
    leaves_from: u64,
    leaf_hashes: &[ObjectHash],
    hasher: &mut Option<IncrementalFileLeafHasher>,
    buffer: &mut Option<Vec<u8>>,
    current_descriptor: &mut Option<&'d PackLeafDescriptor>,
    replicated_mtime: Option<i64>,
    total_leaves: u64,
) -> Result<(), StewardError> {
    while !chunk.is_empty() {
        if hasher.is_none() {
            let Some(d) = descriptors.next() else {
                return Err(StewardError::Content(
                    "file series' physical content extends beyond its declared leaf \
                     descriptors (trailing bytes)"
                        .to_string(),
                ));
            };
            *current_descriptor = Some(d);
            *hasher = Some(
                IncrementalFileLeafHasher::new(
                    d.logical_count(),
                    d.min_event_time(),
                    d.max_event_time(),
                    d.logical_attributes(),
                )
                .map_err(StewardError::Content)?,
            );
            *buffer = if *leaf_index >= leaves_from {
                // `d.logical_count()` is untrusted here -- this descriptor's
                // hash has not been verified against `leaf_hashes` yet, so a
                // malicious/corrupt remote could name an enormous count
                // purely to force a huge allocation before that check ever
                // runs (release blocker item 3,
                // `docs/logical-series-identity-design.md`). Grow the
                // buffer from empty instead of preallocating from it.
                Some(Vec::new())
            } else {
                None
            };
        }
        let h = hasher.as_mut().expect("just set above");
        let remaining = h.remaining();
        let take = remaining.min(chunk.len() as u64) as usize;
        h.write(&chunk[..take]).map_err(StewardError::Content)?;
        if let Some(buf) = buffer.as_mut() {
            buf.extend_from_slice(&chunk[..take]);
        }
        chunk = &chunk[take..];
        if h.remaining() == 0 {
            let finished_hasher = hasher.take().expect("present, just written to");
            let d = current_descriptor.take().expect("present, just written to");
            let computed = finished_hasher.finish().map_err(StewardError::Content)?;
            let idx = *leaf_index as usize;
            let expected = leaf_hashes.get(idx).copied().ok_or_else(|| {
                StewardError::Content(format!(
                    "file series leaf index {idx} has no expected hash (internal inconsistency: \
                     {} leaf hash(es))",
                    leaf_hashes.len()
                ))
            })?;
            if computed != expected {
                return Err(StewardError::Content(format!(
                    "file series leaf {idx} reconstructed to {computed} but the fetch-verified \
                     leaf hash is {expected}; aborting before write so a divergent \
                     reconstruction never lands"
                )));
            }
            if let Some(bytes) = buffer.take() {
                let is_last = *leaf_index + 1 == total_leaves;
                let mut writer = if create && !*node_created {
                    *node_created = true;
                    pwd.create_file_with_id(name, node_id).await?
                } else {
                    pwd.async_writer_path_with_type(name, EntryType::FilePhysicalSeries)
                        .await?
                };
                writer.write_all(&bytes).await?;
                if is_last && let Some(mtime) = replicated_mtime {
                    writer.set_mtime(mtime);
                }
                // Apply bounds before shutdown, but shut the writer down
                // regardless of the outcome: an already-open writer must
                // never be dropped without `shutdown()` (it panics on data
                // loss), and the whole transaction aborts on any error here
                // anyway, so persisting this leaf's bytes into the
                // in-progress (never-committed) transaction is harmless.
                let bounds_result = apply_descriptor_bounds(&mut writer, d);
                writer.shutdown().await?;
                bounds_result?;
            }
            *leaf_index += 1;
        }
    }
    Ok(())
}

/// Reconstruct and materialize a v2 `TablePhysicalSeries`' logical leaves.
///
/// Mirrors [`TableLeafPartitioner`]'s decode-and-split-by-row-count exactly
/// (a leaf may span batches or physical objects), but for a leaf at or after
/// `leaves_from` also keeps its buffered `RecordBatch`es to re-encode into
/// fresh, deterministic Parquet bytes
/// ([`sync_store::content::encode_table_leaf_parquet`], the same encoder the
/// pack builder uses) once the reconstructed rows' hash independently
/// re-verifies against `series.leaf_hashes[i]`.
async fn materialize_table_series_v2(
    pwd: &WD,
    name: &str,
    node_id: NodeID,
    create: bool,
    series: &FetchedSeriesV2,
    graph: &FetchedGraph,
    remote: &dyn ContentSource,
    leaves_from: u64,
    replicated_mtime: Option<i64>,
) -> Result<(), StewardError> {
    let total_leaves = series.manifest.leaf_count();
    if total_leaves == 0 {
        return Err(StewardError::Content(format!(
            "v2 table series {name:?} (node {node_id}) declares leaf_count() == 0; the current \
             TablePhysicalSeries writer cannot create a schema-less zero-byte table version, so \
             this empty table series cannot yet be materialized"
        )));
    }
    let mut leaf_index: u64 = 0;
    let mut node_created = false;
    let mut current_descriptor: Option<usize> = None;
    let mut current_batches: Vec<RecordBatch> = Vec::new();
    let mut current_rows: u64 = 0;
    let mut current_schema: Option<Arc<Schema>> = None;

    for (pack_hash, pack) in &series.packs {
        let descriptors = pack.leaf_descriptors();
        let mut next_descriptor = 0usize;
        for &object_hash in pack.physical_object_hashes() {
            let descriptor_index = current_descriptor.unwrap_or(next_descriptor);
            let descriptor = descriptors.get(descriptor_index).ok_or_else(|| {
                StewardError::Content(
                    "table series' physical content extends beyond its declared leaf descriptors"
                        .to_string(),
                )
            })?;
            let expected_fingerprint =
                effective_leaf_schema_fingerprint(&series.manifest, pack, descriptor)
                    .map_err(StewardError::Content)?
                    .ok_or_else(|| {
                        StewardError::Content(
                            "table series leaf resolved to no effective schema fingerprint"
                                .to_string(),
                        )
                    })?;
            let (schema, batches) = match graph.objects.get(&object_hash) {
                Some(FetchedObject::Blob(bytes)) => {
                    let source = bytes::Bytes::from(bytes.clone());
                    decode_table_object(source, expected_fingerprint)
                        .await
                        .map_err(|e| {
                            StewardError::Content(format!(
                                "decode physical object {object_hash}: {e}"
                            ))
                        })?
                }
                Some(FetchedObject::External) => {
                    let (file, _byte_count) = spool_external_object(remote, object_hash).await?;
                    decode_table_object(file, expected_fingerprint)
                        .await
                        .map_err(|e| {
                            StewardError::Content(format!(
                                "decode physical object {object_hash}: {e}"
                            ))
                        })?
                }
                other => {
                    return Err(StewardError::Content(format!(
                        "expected a physical blob object at {object_hash} but found {other:?}"
                    )));
                }
            };
            let canonical_schema =
                sync_store::content::canonicalize_schema(&schema).map_err(StewardError::Content)?;
            for batch in batches {
                let mut columns = Vec::with_capacity(batch.num_columns());
                for (column, field) in batch.columns().iter().zip(canonical_schema.fields()) {
                    columns.push(arrow_cast::cast(column, field.data_type()).map_err(|e| {
                        StewardError::Content(format!(
                            "normalize physical object {object_hash} column {:?}: {e}",
                            field.name()
                        ))
                    })?);
                }
                let normalized = RecordBatch::try_new(Arc::clone(&canonical_schema), columns)
                    .map_err(|e| {
                        StewardError::Content(format!(
                            "normalize physical object {object_hash} schema: {e}"
                        ))
                    })?;
                feed_table_batch(
                    normalized,
                    expected_fingerprint,
                    &series.manifest,
                    pack,
                    descriptors,
                    &mut next_descriptor,
                    pwd,
                    name,
                    node_id,
                    create,
                    &mut node_created,
                    &mut leaf_index,
                    leaves_from,
                    &series.leaf_hashes,
                    &mut current_descriptor,
                    &mut current_batches,
                    &mut current_rows,
                    &mut current_schema,
                    replicated_mtime,
                    total_leaves,
                )
                .await?;
            }
        }
        if current_descriptor.is_some() {
            return Err(StewardError::Content(format!(
                "table series pack {pack_hash} left a logical leaf incomplete at its own \
                 boundary (a leaf never spans packs)"
            )));
        }
        if next_descriptor != descriptors.len() {
            return Err(StewardError::Content(format!(
                "table series pack {pack_hash} has fewer rows than its declared leaf descriptors"
            )));
        }
    }
    if leaf_index != total_leaves {
        return Err(StewardError::Content(format!(
            "table series materialized {leaf_index} leaf/leaves but the manifest declares \
             {total_leaves}"
        )));
    }
    Ok(())
}

/// Feed one decoded `RecordBatch` of a v2 table series' physical content
/// through the leaf partitioner, writing out any leaf it completes at or
/// after `leaves_from`.
#[allow(clippy::too_many_arguments)]
async fn feed_table_batch(
    mut batch: RecordBatch,
    object_fingerprint: ObjectHash,
    manifest: &SeriesManifest,
    pack: &PackIndex,
    descriptors: &[PackLeafDescriptor],
    next_descriptor: &mut usize,
    pwd: &WD,
    name: &str,
    node_id: NodeID,
    create: bool,
    node_created: &mut bool,
    leaf_index: &mut u64,
    leaves_from: u64,
    leaf_hashes: &[ObjectHash],
    current_descriptor: &mut Option<usize>,
    current_batches: &mut Vec<RecordBatch>,
    current_rows: &mut u64,
    current_schema: &mut Option<Arc<Schema>>,
    replicated_mtime: Option<i64>,
    total_leaves: u64,
) -> Result<(), StewardError> {
    while batch.num_rows() > 0 {
        if current_descriptor.is_none() {
            let descriptor_index = *next_descriptor;
            let Some(d) = descriptors.get(descriptor_index) else {
                return Err(StewardError::Content(
                    "table series' physical content extends beyond its declared leaf \
                     descriptors (trailing rows)"
                        .to_string(),
                ));
            };
            let expected = effective_leaf_schema_fingerprint(manifest, pack, d)
                .map_err(StewardError::Content)?
                .ok_or_else(|| {
                    StewardError::Content(
                        "table series leaf resolved to no effective schema fingerprint".to_string(),
                    )
                })?;
            if expected != object_fingerprint {
                return Err(StewardError::Content(format!(
                    "table physical object with schema fingerprint {object_fingerprint} crosses \
                     a leaf schema transition; descriptor {descriptor_index} requires {expected}"
                )));
            }
            *next_descriptor += 1;
            *current_descriptor = Some(descriptor_index);
            *current_schema = Some(batch.schema());
        }
        let descriptor_index = current_descriptor.expect("just set above");
        let d = &descriptors[descriptor_index];
        let expected = effective_leaf_schema_fingerprint(manifest, pack, d)
            .map_err(StewardError::Content)?
            .ok_or_else(|| {
                StewardError::Content(
                    "table series leaf resolved to no effective schema fingerprint".to_string(),
                )
            })?;
        if expected != object_fingerprint {
            return Err(StewardError::Content(format!(
                "table logical leaf {descriptor_index} spans physical objects with different \
                 schema fingerprints ({expected} then {object_fingerprint})"
            )));
        }
        let needed = d.logical_count() - *current_rows;
        let take = needed.min(batch.num_rows() as u64) as usize;
        current_batches.push(batch.slice(0, take));
        *current_rows += take as u64;
        batch = batch.slice(take, batch.num_rows() - take);
        if *current_rows == d.logical_count() {
            let hash = table_leaf_hash_canonical(
                current_schema
                    .as_ref()
                    .expect("current table leaf always has a schema"),
                current_batches,
                d.min_event_time(),
                d.max_event_time(),
                d.logical_attributes(),
            )
            .map_err(StewardError::Content)?;
            let idx = *leaf_index as usize;
            let expected = leaf_hashes.get(idx).copied().ok_or_else(|| {
                StewardError::Content(format!(
                    "table series leaf index {idx} has no expected hash (internal \
                     inconsistency: {} leaf hash(es))",
                    leaf_hashes.len()
                ))
            })?;
            if hash != expected {
                return Err(StewardError::Content(format!(
                    "table series leaf {idx} reconstructed to {hash} but the fetch-verified \
                     leaf hash is {expected}; aborting before write so a divergent \
                     reconstruction never lands"
                )));
            }
            if *leaf_index >= leaves_from {
                let parquet_bytes = encode_table_leaf_parquet(
                    current_schema
                        .as_ref()
                        .expect("current table leaf always has a schema"),
                    current_batches,
                )
                .map_err(StewardError::Content)?;
                let is_last = *leaf_index + 1 == total_leaves;
                let mut writer = if create && !*node_created {
                    *node_created = true;
                    pwd.create_file_with_id(name, node_id).await?
                } else {
                    pwd.async_writer_path_with_type(name, EntryType::TablePhysicalSeries)
                        .await?
                };
                writer.write_all(&parquet_bytes).await?;
                if is_last && let Some(mtime) = replicated_mtime {
                    writer.set_mtime(mtime);
                }
                // A table series' write choke point requires temporal
                // metadata before shutdown. When the descriptor carries an
                // explicit range, set it (shutting down unconditionally
                // afterward, since an open writer must never be dropped
                // without `shutdown()` -- it panics on data loss -- even
                // when this whole transaction is about to abort). When it
                // carries neither bound, every real `TablePhysicalSeries`
                // writer (`store_file_content_ref`'s choke point,
                // `crates/tlogfs/src/persistence.rs`) always requires both
                // bounds together for nonempty content, so a descriptor with
                // neither is not a state any legitimate source can produce;
                // reject explicitly rather than inventing identity inputs
                // via `infer_temporal_bounds` from the just-written parquet
                // footer, which would silently diverge from whatever
                // (nonexistent) bounds the source actually committed to
                // (release blocker item 2,
                // `docs/logical-series-identity-design.md`).
                match (d.min_event_time(), d.max_event_time()) {
                    (Some(min), Some(max)) => {
                        let column = descriptor_timestamp_column(d);
                        match column {
                            Ok(column) => {
                                writer.set_temporal_metadata(min, max, column);
                                apply_descriptor_exact_attributes(&mut writer, d);
                                writer.shutdown().await?;
                            }
                            Err(e) => {
                                writer.shutdown().await?;
                                return Err(e);
                            }
                        }
                    }
                    (None, None) => {
                        writer.shutdown().await?;
                        return Err(StewardError::Content(format!(
                            "table series leaf {idx} carries no temporal bounds; a \
                             legitimate TablePhysicalSeries writer always establishes both \
                             min_event_time and max_event_time for nonempty content, so this \
                             descriptor cannot be materialized without inventing identity \
                             inputs"
                        )));
                    }
                    _ => {
                        writer.shutdown().await?;
                        return Err(StewardError::Content(
                            "v2 leaf descriptor carries only one of \
                             min_event_time/max_event_time; a legitimate writer always sets \
                             both together or neither"
                                .to_string(),
                        ));
                    }
                }
            }
            current_batches.clear();
            *current_rows = 0;
            *current_descriptor = None;
            *current_schema = None;
            *leaf_index += 1;
        }
    }
    Ok(())
}

/// Look up a parent directory's working directory by `node_id`, erroring if it
/// was not materialized earlier in the breadth-first plan.
fn parent_wd<'a>(dir_wd: &'a HashMap<String, WD>, node_id: &str) -> Result<&'a WD, StewardError> {
    dir_wd.get(node_id).ok_or_else(|| {
        StewardError::Content(format!(
            "parent directory {node_id} was not materialized before its child"
        ))
    })
}

/// Parse a manifest `node_id` string into a [`NodeID`].
fn parse_node_id(node_id: &str) -> Result<NodeID, StewardError> {
    NodeID::from_hex_string(node_id)
        .map_err(|e| StewardError::Content(format!("invalid node_id {node_id}: {e}")))
}

/// Depth of a target node from the root (root is 0), by walking parents.
fn target_depth(node_id: &str, target_nodes: &HashMap<String, ManifestEntry>) -> usize {
    let mut depth = 0;
    let mut current = node_id;
    while let Some(entry) = target_nodes.get(current) {
        if entry.parent_node_id.is_empty() {
            break;
        }
        depth += 1;
        current = &entry.parent_node_id;
    }
    depth
}

/// Reconstruct the absolute path of a target directory node from its manifest
/// parent chain (empty string for the root).
fn target_path(node_id: &str, target_nodes: &HashMap<String, ManifestEntry>) -> String {
    let mut names = Vec::new();
    let mut current = node_id;
    while let Some(entry) = target_nodes.get(current) {
        if entry.parent_node_id.is_empty() {
            break;
        }
        names.push(entry.name.as_str());
        current = &entry.parent_node_id;
    }
    names.reverse();
    if names.is_empty() {
        String::new()
    } else {
        format!("/{}", names.join("/"))
    }
}

/// Look up a leaf blob's bytes in the fetched graph.  Only valid for inline
/// blobs (symlink targets, recipes); a large external blob has no buffered
/// bytes and must be streamed instead (see [`version_source`]).
fn blob_bytes(graph: &FetchedGraph, hash: ObjectHash) -> Result<Vec<u8>, StewardError> {
    match graph.objects.get(&hash) {
        Some(FetchedObject::Blob(bytes)) => Ok(bytes.clone()),
        Some(FetchedObject::External) => Err(StewardError::Content(format!(
            "object {} is a large external blob and cannot be buffered here",
            hash.to_hex()
        ))),
        Some(_) => Err(StewardError::Content(format!(
            "expected a blob at {} but found a structured object",
            hash.to_hex()
        ))),
        None => Err(StewardError::Content(format!(
            "blob object {} missing from graph",
            hash.to_hex()
        ))),
    }
}

/// Resolve a file/series version blob to its apply-time source: buffered bytes
/// for an inline small blob, or the hash for a large external blob to stream.
fn version_source(graph: &FetchedGraph, hash: ObjectHash) -> Result<VersionSource, StewardError> {
    match graph.objects.get(&hash) {
        Some(FetchedObject::Blob(bytes)) => Ok(VersionSource::Inline(bytes.clone())),
        Some(FetchedObject::External) => Ok(VersionSource::External(hash)),
        Some(_) => Err(StewardError::Content(format!(
            "expected a blob at {} but found a structured object",
            hash.to_hex()
        ))),
        None => Err(StewardError::Content(format!(
            "blob object {} missing from graph",
            hash.to_hex()
        ))),
    }
}

/// Resolve a planned series version: its bytes source plus the node metadata
/// the source's directory entry recorded for it.
fn planned_version(
    graph: &FetchedGraph,
    hash: ObjectHash,
    meta: Option<&VersionMeta>,
) -> Result<PlannedVersion, StewardError> {
    Ok(PlannedVersion {
        source: version_source(graph, hash)?,
        meta: meta.cloned().unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use sync_store::content::{generate_range_proof, merkle_root};

    #[test]
    fn table_partitioner_rejects_object_crossing_schema_transition() {
        let schema_a = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let schema_b = Arc::new(Schema::new(vec![Field::new(
            "measurement",
            DataType::Int64,
            false,
        )]));
        let fingerprint_a = schema_fingerprint(&schema_a).expect("fingerprint a");
        let fingerprint_b = schema_fingerprint(&schema_b).expect("fingerprint b");
        let leaves = vec![
            ObjectHash::of_bytes(b"leaf-a"),
            ObjectHash::of_bytes(b"leaf-b"),
        ];
        let manifest = SeriesManifest::new(
            PayloadKind::Table,
            2,
            2,
            None,
            None,
            None,
            merkle_root(&leaves),
        )
        .expect("manifest");
        let descriptors = vec![
            PackLeafDescriptor::new_with_schema(1, Some(fingerprint_a), None, None, None)
                .expect("descriptor a"),
            PackLeafDescriptor::new_with_schema(1, Some(fingerprint_b), None, None, None)
                .expect("descriptor b"),
        ];
        let pack = PackIndex::new(
            manifest.hash(),
            0,
            2,
            2,
            manifest.leaf_merkle_root(),
            generate_range_proof(&leaves, 0, 2).expect("proof"),
            vec![ObjectHash::of_bytes(b"object")],
            2,
            100,
            descriptors,
        )
        .expect("pack");
        let batch = RecordBatch::try_new(
            schema_a,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("batch");
        let mut partitioner = TableLeafPartitioner::new(&manifest, &pack);
        let err = partitioner
            .feed(batch, fingerprint_a)
            .expect_err("one physical object must not cross the transition");
        assert!(err.to_string().contains("crosses a leaf schema transition"));
    }
}
