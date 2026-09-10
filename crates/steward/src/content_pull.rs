// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Content-graph fetch: the consumer side of the content-addressed remote
//! (design Section 8.5, Fork 2).
//!
//! This module implements native-v2 fetch. An initial clone traverses the
//! requested persistent manifest map and snapshot. A consumer with a known
//! publication head walks only newer immutable publication records and their
//! changed manifest/object set. Every fetched object's bytes are re-hashed
//! against its raw object-store key before a rebuild can consume it.
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
use sync_store::PublicationState;
use sync_store::content::{
    Commit, IncrementalFileLeafHasher, ManifestChange, ManifestEntry, ManifestMapEditor,
    ManifestMapNode, ManifestRecord, ManifestRecordChild, ObjectHash, PackDescriptor, PackIndex,
    PackLeafDescriptor, PackObjectSpan, PayloadKind, PublicationRecord, SeriesManifest, TreeEntry,
    VersionMeta, decode_manifest_root, decode_recipe, decode_tree,
    effective_leaf_schema_fingerprint, encode_table_leaf_parquet, schema_fingerprint,
    table_leaf_hash_canonical, verify_complete_pack_against_manifest, verify_pack_against_manifest,
};
use tinyfs::{EntryType, NodeID, WD};
use tlogfs::PondUserMetadata;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{Ship, StewardError};

/// Bound the aggregate inline payload retained by the exact suffix query.
///
/// Physical objects at or above `LARGE_FILE_THRESHOLD` are external and
/// streamed, so they do not contribute. Refusing a pathological collection of
/// tiny inline objects before querying is safer than risking an allocator OOM,
/// and aligns the retained payload ceiling with the default remote burst gate.
const MAX_INLINE_PACK_PREFETCH_BYTES: u64 = 64 * 1024 * 1024;

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
    /// A verified `watertown.series.v3` logical series
    /// (`docs/logical-series-identity-design.md` delivery gate 4).
    ///
    /// By the time this variant exists in [`FetchedGraph::objects`], the
    /// series manifest and selected v4 pack metadata have been fetched and
    /// authenticated. Physical objects remain unfetched until materialization
    /// knows the destination's durable logical prefix.
    SeriesV2(Box<FetchedSeriesV2>),
    /// One decoded persistent manifest-map node.
    ManifestNode(ManifestMapNode),
}

/// The immutable, verified state of one fetched `watertown.series.v3` logical series.
///
/// `leaf_hashes` come from v3 descriptors whose range proofs were verified
/// against `manifest`. Physical pack objects are intentionally not fetched
/// until materialization, after the destination prefix has been validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedSeriesV2 {
    /// The `watertown.series.v3` object's own content address -- the hash the owning
    /// tree entry's `child_hash` named.
    pub manifest_hash: ObjectHash,
    /// The decoded series manifest.
    pub manifest: SeriesManifest,
    /// The fetched linked segment packs, `(pack_hash, decoded PackIndex)`, in
    /// increasing leaf-range order. A fresh clone receives a complete linked
    /// chain covering `[0, manifest.leaf_count())`; an incremental fetch may
    /// retain only the publication-window suffix.
    pub packs: Vec<(ObjectHash, PackIndex)>,
    /// First whole-series leaf index represented in [`Self::leaf_hashes`].
    pub leaf_start: u64,
    /// Logical leaf hashes represented by the fetched segment suffix, in
    /// whole-series order starting at [`Self::leaf_start`].
    pub leaf_hashes: Vec<ObjectHash>,
    /// Immutable prefix series state immediately before [`Self::leaf_start`],
    /// present only for a bounded incremental fetch.
    pub base_series_hash: Option<ObjectHash>,
    /// Decoded manifest for [`Self::base_series_hash`].
    pub base_manifest: Option<SeriesManifest>,
    /// Every physical object hash the fetched segment packs name, in first-seen
    /// order across `packs`, deduplicated. These are metadata references only;
    /// the corresponding payloads are fetched on demand during materialization.
    pub physical_object_hashes: Vec<ObjectHash>,
}

/// The verified object closure reachable from a remote tip commit.
#[derive(Debug, Clone, Default)]
pub struct FetchedGraph {
    /// The tip commit's hash.
    pub tip: Option<ObjectHash>,
    /// Fixed-size active publication row that named this graph.
    pub publication_state: Option<PublicationState>,
    /// The fetched commit ancestry, tip first. [`fetch_object_graph`] reads to
    /// genesis; [`fetch_object_graph_since`] stops after the requested known
    /// ancestor, when present.
    pub commits: Vec<(ObjectHash, Commit)>,
    /// Compatibility view containing one validated interpretation per payload.
    ///
    /// Because one hash may serve multiple semantic roles, production
    /// materialization uses `trees`, `series`, `blob_hashes`, and `bytes`
    /// instead of assuming this enum is exhaustive.
    pub objects: BTreeMap<ObjectHash, FetchedObject>,
    /// Independently validated tree interpretations keyed by payload hash.
    pub trees: BTreeMap<ObjectHash, Vec<TreeEntry>>,
    /// Independently validated series interpretations keyed by payload hash.
    pub series: BTreeMap<ObjectHash, Box<FetchedSeriesV2>>,
    /// Payload hashes authenticated for use as leaf blobs.
    pub blob_hashes: BTreeSet<ObjectHash>,
    /// Raw bytes of every fetched *inline* object, keyed by content hash.  Large
    /// external blobs are absent here by design -- they are never buffered.
    pub bytes: BTreeMap<ObjectHash, Vec<u8>>,
    /// Hashes of large leaf blobs that live in the remote blob store and are
    /// streamed rather than buffered (Decision D7).
    pub external_blobs: BTreeSet<ObjectHash>,
    /// The tip commit's node manifest: one entry per node, recording the
    /// source's `node_id` alongside its parent, name, type, and content
    /// address (Section 4.5).  Empty when the graph is empty.  Kept out of
    /// `objects`/`bytes` because the manifest is pond-specific identity, not
    /// part of the dedup-shareable pure-content closure.
    pub manifest: Vec<ManifestEntry>,
    /// Net changed records when `manifest_complete` is false.
    pub manifest_changes: Vec<ManifestChange>,
    /// Whether `manifest` is the complete current snapshot rather than only
    /// changed upserts.
    pub manifest_complete: bool,
    /// Immutable packs learned from publication records, grouped by series.
    pub publication_packs: BTreeMap<ObjectHash, Vec<PackDescriptor>>,
    /// Publication records newer than the requested durable boundary, newest
    /// first. This is the authenticated publication window used for
    /// apply-before-ack recovery.
    pub publication_records: Vec<(ObjectHash, PublicationRecord)>,
    /// Exact publication record at the requested durable boundary, excluded
    /// from [`Self::publication_records`] so its object inventory is not
    /// fetched again.
    pub publication_boundary: Option<(ObjectHash, PublicationRecord)>,
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
    fetch_object_graph_since(remote, ref_name, None).await
}

/// Fetch the verified object closure at `ref_name`, stopping the commit walk
/// after `known_ancestor` has been fetched.
///
/// The boundary commit is included so callers can prove that the fetched tip
/// descends from their durable prior tip. If the boundary is not in the
/// ancestry, the walk continues to genesis, allowing the caller to reject the
/// non-fast-forward update without trusting unverified lineage.
pub async fn fetch_object_graph_since(
    remote: &dyn ContentSource,
    ref_name: &str,
    known_ancestor: Option<ObjectHash>,
) -> Result<FetchedGraph, StewardError> {
    let Some(state) = remote
        .get_publication_state(ref_name)
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?
    else {
        return Ok(FetchedGraph::default());
    };
    fetch_object_graph_at_publication(remote, state, known_ancestor).await
}

/// Fetch one exact immutable publication state, optionally stopping at a
/// snapshot boundary.
///
/// Unlike [`fetch_object_graph_since`], this does not reread the mutable active
/// publication row. Callers that already selected a state therefore fetch and
/// apply exactly that state even if the remote advances concurrently.
pub async fn fetch_object_graph_at_publication(
    remote: &dyn ContentSource,
    state: PublicationState,
    known_ancestor: Option<ObjectHash>,
) -> Result<FetchedGraph, StewardError> {
    let boundary = known_ancestor
        .map(PublicationBoundary::Snapshot)
        .unwrap_or(PublicationBoundary::None);
    descend_from_publication(remote, state, boundary).await
}

/// Fetch one exact immutable publication state back to an exact structured
/// acknowledgement.
///
/// The acknowledgement generation bounds publication-record reads before any
/// history is traversed, and its immutable record hash is required at the
/// exact generation boundary.
pub async fn fetch_object_graph_from_acknowledgement(
    remote: &dyn ContentSource,
    state: PublicationState,
    acknowledged: &PublicationState,
) -> Result<FetchedGraph, StewardError> {
    descend_from_publication(
        remote,
        state,
        PublicationBoundary::Acknowledged(acknowledged.clone()),
    )
    .await
}

/// Authenticate one active publication row against its immutable record and
/// snapshot commit using only fixed-key point reads.
pub async fn authenticate_publication_head(
    remote: &dyn ContentSource,
    state: &PublicationState,
) -> Result<(), StewardError> {
    let _ = authenticated_publication_head_commit(remote, state).await?;
    Ok(())
}

/// Authenticate one active row and return its verified tip commit.
pub async fn authenticated_publication_head_commit(
    remote: &dyn ContentSource,
    state: &PublicationState,
) -> Result<Commit, StewardError> {
    if remote.pond_id() != state.pond_id {
        return Err(StewardError::Content(format!(
            "publication row pond {} does not match source pond {}",
            state.pond_id,
            remote.pond_id()
        )));
    }
    let record = remote
        .get_publication_record(state.publication_record)
        .await?
        .ok_or_else(|| {
            StewardError::Content(format!(
                "publication record {} is absent",
                state.publication_record
            ))
        })?;
    if record.hash() != state.publication_record
        || record.pond_id != state.pond_id
        || record.ref_name != state.ref_name
        || record.format != state.format
        || record.snapshot_tip != state.snapshot_tip
        || record.manifest_root != state.manifest_root
    {
        return Err(StewardError::Content(
            "publication head record disagrees with active row identity".to_string(),
        ));
    }
    let bytes = fetch_verified(remote, state.snapshot_tip).await?;
    let commit = Commit::decode(&bytes).map_err(|error| {
        StewardError::Content(format!(
            "decode publication tip commit {}: {error}",
            state.snapshot_tip
        ))
    })?;
    if commit.hash() != state.snapshot_tip || commit.manifest_root != state.manifest_root {
        return Err(StewardError::Content(format!(
            "publication tip {} does not match its commit and manifest root",
            state.snapshot_tip
        )));
    }
    Ok(commit)
}

async fn descend_from_publication(
    remote: &dyn ContentSource,
    state: PublicationState,
    boundary: PublicationBoundary,
) -> Result<FetchedGraph, StewardError> {
    let known_ancestor = boundary.snapshot_tip();
    let mut graph = FetchedGraph {
        tip: Some(state.snapshot_tip),
        publication_state: Some(state.clone()),
        ..FetchedGraph::default()
    };
    let window = fetch_publication_chain(remote, &state, &boundary).await?;
    let records = &window.records;
    graph.publication_records = window.records.clone();
    graph.publication_boundary = window.boundary;
    let mut descriptors = BTreeSet::new();
    let mut pack_descriptors = BTreeSet::new();
    for (_, record) in records {
        descriptors.extend(record.introduced_objects.iter().copied());
        pack_descriptors.extend(record.introduced_packs.iter().copied());
    }
    for descriptor in &pack_descriptors {
        graph
            .publication_packs
            .entry(descriptor.series_hash)
            .or_default()
            .push(*descriptor);
    }
    for packs in graph.publication_packs.values_mut() {
        packs.sort_unstable();
        packs.dedup();
    }

    let mut commit_bytes = HashMap::new();
    for descriptor in descriptors
        .iter()
        .filter(|descriptor| descriptor.kind == sync_store::content::ContentObjectKind::Commit)
    {
        let bytes = remote.get_object(descriptor.hash).await?.ok_or_else(|| {
            StewardError::Content(format!(
                "publication names missing commit {}",
                descriptor.hash
            ))
        })?;
        verify(descriptor.hash, &bytes)?;
        let _ = commit_bytes.insert(descriptor.hash, bytes);
    }
    if let std::collections::hash_map::Entry::Vacant(entry) = commit_bytes.entry(state.snapshot_tip)
    {
        let bytes = remote
            .get_object(state.snapshot_tip)
            .await?
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "publication tip commit {} is absent",
                    state.snapshot_tip
                ))
            })?;
        verify(state.snapshot_tip, &bytes)?;
        let _ = entry.insert(bytes);
    }
    graph.commits =
        walk_bounded_commit_delta(remote, state.snapshot_tip, known_ancestor, commit_bytes).await?;
    let tip_commit = graph
        .commits
        .first()
        .map(|(_, commit)| commit)
        .ok_or_else(|| StewardError::Content("publication has no tip commit".to_string()))?;
    if tip_commit.manifest_root != state.manifest_root {
        return Err(StewardError::Content(format!(
            "publication row manifest {} disagrees with tip commit {}",
            state.manifest_root, tip_commit.manifest_root
        )));
    }

    let tip_root_tree = tip_commit.root_tree_hash;
    if known_ancestor.is_none() {
        graph.manifest_complete = true;
        let records = fetch_complete_manifest(remote, state.manifest_root, &mut graph).await?;
        graph.manifest = records.iter().map(|record| record.entry.clone()).collect();
        fetch_tree(remote, tip_root_tree, &mut graph, true).await?;
    } else {
        let changes = squash_publication_changes(records)?;
        graph.manifest = changes
            .iter()
            .filter_map(|change| change.after.as_ref().map(|record| record.entry.clone()))
            .collect();
        graph.manifest_changes = changes;
        let changed_entries = graph.manifest.clone();
        for entry in changed_entries {
            match entry.entry_type {
                EntryType::DirectoryPhysical => {
                    let bytes = fetch_verified(remote, entry.child_hash).await?;
                    let tree = decode_tree(&bytes)
                        .map_err(|error| StewardError::Content(format!("decode tree: {error}")))?;
                    let _ = graph
                        .objects
                        .entry(entry.child_hash)
                        .or_insert_with(|| FetchedObject::Tree(tree.clone()));
                    let _ = graph.trees.insert(entry.child_hash, tree);
                    let _ = graph.bytes.insert(entry.child_hash, bytes);
                }
                EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
                    fetch_series(
                        remote,
                        entry.child_hash,
                        entry.entry_type,
                        &mut graph,
                        false,
                    )
                    .await?;
                }
                EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
                    fetch_blob(remote, entry.child_hash, &mut graph, false).await?;
                }
                EntryType::Symlink
                | EntryType::DirectoryDynamic
                | EntryType::FileDynamic
                | EntryType::TableDynamic => {
                    fetch_blob(remote, entry.child_hash, &mut graph, true).await?;
                }
            }
        }
    }
    Ok(graph)
}

struct PublicationWindow {
    records: Vec<(ObjectHash, PublicationRecord)>,
    boundary: Option<(ObjectHash, PublicationRecord)>,
}

enum PublicationBoundary {
    None,
    Snapshot(ObjectHash),
    Acknowledged(PublicationState),
}

impl PublicationBoundary {
    fn snapshot_tip(&self) -> Option<ObjectHash> {
        match self {
            Self::None => None,
            Self::Snapshot(snapshot_tip) => Some(*snapshot_tip),
            Self::Acknowledged(state) => Some(state.snapshot_tip),
        }
    }
}

async fn fetch_publication_chain(
    remote: &dyn ContentSource,
    state: &PublicationState,
    boundary: &PublicationBoundary,
) -> Result<PublicationWindow, StewardError> {
    if state.generation <= 0 {
        return Err(StewardError::Content(format!(
            "publication generation must be positive, got {}",
            state.generation
        )));
    }
    if let PublicationBoundary::Acknowledged(acknowledged) = boundary {
        if state.pond_id != acknowledged.pond_id
            || state.ref_name != acknowledged.ref_name
            || state.format != acknowledged.format
        {
            return Err(StewardError::Content(
                "publication state does not match acknowledgement identity".to_string(),
            ));
        }
        if acknowledged.generation <= 0 || state.generation <= acknowledged.generation {
            return Err(StewardError::Content(format!(
                "publication generation {} does not advance acknowledged generation {}",
                state.generation, acknowledged.generation
            )));
        }
    }

    let max_reads = match boundary {
        PublicationBoundary::None => 1,
        PublicationBoundary::Snapshot(_) => usize::try_from(state.generation).map_err(|_| {
            StewardError::Content(format!(
                "publication generation {} does not fit this platform",
                state.generation
            ))
        })?,
        PublicationBoundary::Acknowledged(acknowledged) => {
            let newer = state
                .generation
                .checked_sub(acknowledged.generation)
                .ok_or_else(|| {
                    StewardError::Content("publication generation delta overflow".to_string())
                })?;
            usize::try_from(newer)
                .map_err(|_| {
                    StewardError::Content(format!(
                        "publication generation delta {newer} does not fit this platform"
                    ))
                })?
                .checked_add(1)
                .ok_or_else(|| {
                    StewardError::Content("publication record read bound overflow".to_string())
                })?
        }
    };

    let mut records = Vec::new();
    let mut next = Some(state.publication_record);
    let mut seen = HashSet::new();
    for read_index in 0..max_reads {
        let hash = next.ok_or_else(|| {
            StewardError::Content(match boundary {
                PublicationBoundary::Acknowledged(acknowledged) => format!(
                    "publication history ended before acknowledged record {} at generation {}",
                    acknowledged.publication_record, acknowledged.generation
                ),
                PublicationBoundary::Snapshot(known) => {
                    format!("publication history does not contain known snapshot {known}")
                }
                PublicationBoundary::None => {
                    "publication head record is unexpectedly absent".to_string()
                }
            })
        })?;
        if !seen.insert(hash) {
            return Err(StewardError::Content(format!(
                "cycle in publication records at {hash}"
            )));
        }
        let record = remote
            .get_publication_record(hash)
            .await?
            .ok_or_else(|| StewardError::Content(format!("publication record {hash} is absent")))?;
        if record.hash() != hash
            || record.pond_id != state.pond_id
            || record.ref_name != state.ref_name
            || record.format != state.format
        {
            return Err(StewardError::Content(format!(
                "publication record {hash} has mismatched identity"
            )));
        }
        if records.is_empty()
            && (record.snapshot_tip != state.snapshot_tip
                || record.manifest_root != state.manifest_root)
        {
            return Err(StewardError::Content(
                "publication head disagrees with active row".to_string(),
            ));
        }

        match boundary {
            PublicationBoundary::None => {
                records.push((hash, record));
                return Ok(PublicationWindow {
                    records,
                    boundary: None,
                });
            }
            PublicationBoundary::Snapshot(known) if record.snapshot_tip == *known => {
                return Ok(PublicationWindow {
                    records,
                    boundary: Some((hash, record)),
                });
            }
            PublicationBoundary::Acknowledged(acknowledged) if read_index + 1 == max_reads => {
                if hash != acknowledged.publication_record
                    || record.snapshot_tip != acknowledged.snapshot_tip
                    || record.manifest_root != acknowledged.manifest_root
                {
                    return Err(StewardError::Content(format!(
                        "publication record at acknowledged generation {} does not match the \
                         exact acknowledgement boundary",
                        acknowledged.generation
                    )));
                }
                return Ok(PublicationWindow {
                    records,
                    boundary: Some((hash, record)),
                });
            }
            PublicationBoundary::Acknowledged(acknowledged)
                if hash == acknowledged.publication_record
                    || record.snapshot_tip == acknowledged.snapshot_tip =>
            {
                return Err(StewardError::Content(format!(
                    "publication reached acknowledgement generation {} before the advertised \
                     generation boundary",
                    acknowledged.generation
                )));
            }
            PublicationBoundary::Snapshot(_) | PublicationBoundary::Acknowledged(_) => {}
        }
        next = record.parent_publication_record;
        records.push((hash, record));
    }
    Err(StewardError::Content(match boundary {
        PublicationBoundary::Snapshot(known) => format!(
            "publication history does not contain known snapshot {known} within advertised \
             generation {}",
            state.generation
        ),
        PublicationBoundary::Acknowledged(acknowledged) => format!(
            "publication history did not reach acknowledged record {} at generation {}",
            acknowledged.publication_record, acknowledged.generation
        ),
        PublicationBoundary::None => "publication head record was not returned".to_string(),
    }))
}

async fn walk_bounded_commit_delta(
    remote: &dyn ContentSource,
    tip: ObjectHash,
    known_ancestor: Option<ObjectHash>,
    mut objects: HashMap<ObjectHash, Vec<u8>>,
) -> Result<Vec<(ObjectHash, Commit)>, StewardError> {
    let mut commits = Vec::new();
    let mut next = Some(tip);
    let mut seen = HashSet::new();
    while let Some(hash) = next {
        if !seen.insert(hash) {
            return Err(StewardError::Content(format!(
                "cycle in published commit delta at {hash}"
            )));
        }
        let bytes = match objects.remove(&hash) {
            Some(bytes) => bytes,
            None if Some(hash) == known_ancestor => {
                remote.get_object(hash).await?.ok_or_else(|| {
                    StewardError::Content(format!("known boundary commit {hash} is absent"))
                })?
            }
            None if known_ancestor.is_none() && commits.is_empty() => remote
                .get_object(hash)
                .await?
                .ok_or_else(|| StewardError::Content(format!("tip commit {hash} is absent")))?,
            None => {
                return Err(StewardError::Content(match known_ancestor {
                    Some(known) => format!(
                        "remote tip {tip} does not descend from known snapshot {known}: \
                         publication delta is missing commit {hash}"
                    ),
                    None => format!("publication delta is missing commit {hash}"),
                }));
            }
        };
        verify(hash, &bytes)?;
        let commit = Commit::decode(&bytes)
            .map_err(|error| StewardError::Content(format!("decode commit {hash}: {error}")))?;
        next = commit.parent_commit_hash;
        commits.push((hash, commit));
        if Some(hash) == known_ancestor || known_ancestor.is_none() {
            break;
        }
    }
    Ok(commits)
}

async fn fetch_complete_manifest(
    remote: &dyn ContentSource,
    root: ObjectHash,
    graph: &mut FetchedGraph,
) -> Result<Vec<ManifestRecord>, StewardError> {
    let mut records = Vec::new();
    let mut stack = vec![root];
    let mut seen = HashSet::new();
    while let Some(hash) = stack.pop() {
        if !seen.insert(hash) {
            continue;
        }

        let bytes = fetch_verified(remote, hash).await?;
        let node = ManifestMapNode::decode(&bytes).map_err(|error| {
            StewardError::Content(format!("decode manifest node {hash}: {error}"))
        })?;
        let _ = graph.bytes.insert(hash, bytes);
        let _ = graph
            .objects
            .entry(hash)
            .or_insert_with(|| FetchedObject::ManifestNode(node.clone()));
        match node {
            ManifestMapNode::Leaf { record, .. } => records.push(record),
            ManifestMapNode::Branch { left, right, .. } => {
                stack.push(right);
                stack.push(left);
            }
        }
    }
    records.sort_by(|left, right| left.node_id().as_bytes().cmp(right.node_id().as_bytes()));
    Ok(records)
}

/// Fetch and authenticate a complete persistent manifest map.
///
/// Initial clone, capsule tooling, and explicit diagnostics use this. Ordinary
/// incremental consumers apply publication-record changes instead.
pub async fn fetch_manifest_records(
    remote: &dyn ContentSource,
    root: ObjectHash,
) -> Result<Vec<ManifestRecord>, StewardError> {
    let mut graph = FetchedGraph::default();
    fetch_complete_manifest(remote, root, &mut graph).await
}

fn squash_publication_changes(
    records_newest_first: &[(ObjectHash, PublicationRecord)],
) -> Result<Vec<ManifestChange>, StewardError> {
    let mut changes = BTreeMap::<String, ManifestChange>::new();
    for (_, record) in records_newest_first.iter().rev() {
        for change in &record.manifest_changes {
            let node_id = change.node_id().to_string();
            match changes.get_mut(&node_id) {
                Some(existing) => {
                    if existing.after != change.before {
                        return Err(StewardError::Content(format!(
                            "publication manifest deltas are discontinuous at node {node_id}"
                        )));
                    }
                    existing.after = change.after.clone();
                }
                None => {
                    let _ = changes.insert(node_id, change.clone());
                }
            }
        }
    }
    Ok(changes
        .into_values()
        .filter(|change| change.before != change.after)
        .collect())
}

/// Recursively fetch a tree object and everything reachable from its entries.
async fn fetch_tree(
    remote: &dyn ContentSource,
    tree_hash: ObjectHash,
    graph: &mut FetchedGraph,
    complete_series_chain: bool,
) -> Result<(), StewardError> {
    // Iterative worklist to avoid async recursion on the directory tree.
    let mut stack = vec![tree_hash];
    while let Some(hash) = stack.pop() {
        if graph.trees.contains_key(&hash) {
            continue;
        }
        let bytes = fetch_verified(remote, hash).await?;
        let entries =
            decode_tree(&bytes).map_err(|e| StewardError::Content(format!("decode tree: {e}")))?;
        let _ = graph
            .objects
            .entry(hash)
            .or_insert_with(|| FetchedObject::Tree(entries.clone()));
        let _ = graph.trees.insert(hash, entries.clone());
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
                        complete_series_chain,
                    )
                    .await?;
                }
                EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
                    fetch_blob(remote, entry.child_hash, graph, false).await?;
                }
                EntryType::Symlink
                | EntryType::DirectoryDynamic
                | EntryType::FileDynamic
                | EntryType::TableDynamic => {
                    fetch_blob(remote, entry.child_hash, graph, true).await?;
                }
            }
        }
    }
    Ok(())
}

/// Fetch a `watertown.series.v3` object and everything it names.
///
/// `entry_type` is the owning tree entry's declared kind
/// (`FilePhysicalSeries` or `TablePhysicalSeries`); it must
/// agree with the manifest's own [`PayloadKind`] (`docs/logical-series-
/// identity-design.md` delivery gate 4), since nothing else ties a
/// `watertown.series.v3` object's payload kind to the directory position naming it.
async fn fetch_series(
    remote: &dyn ContentSource,
    series_hash: ObjectHash,
    entry_type: EntryType,
    graph: &mut FetchedGraph,
    complete_chain: bool,
) -> Result<(), StewardError> {
    if let Some(series) = graph.series.get(&series_hash) {
        let expected_kind = expected_payload_kind(entry_type);
        return if series.manifest.payload_kind() != expected_kind {
            Err(StewardError::Content(format!(
                "series {series_hash} manifest declares payload kind {:?} but another tree entry \
                 is {entry_type:?} (expects {expected_kind:?})",
                series.manifest.payload_kind()
            )))
        } else {
            Ok(())
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
        complete_chain,
    )
    .await
}

/// Map a series-carrying tree entry type to the [`PayloadKind`] its
/// `watertown.series.v3` manifest must declare.
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

/// Fetch, discover, and authenticate a `watertown.series.v3` logical series
/// (`docs/logical-series-identity-design.md` delivery gate 4).
///
/// This is the series-chain reader. It:
///
/// 1. checks `manifest.payload_kind()` against the owning tree entry's
///    declared type;
/// 2. point-reads the current series locator, never a pack-prefix listing;
/// 3. verifies each linked suffix pack against that segment state's
///    independently fetched manifest;
/// 4. stops an incremental walk at the first parent outside the bounded
///    publication window, or walks to a root/consolidated pack for a fresh
///    clone;
/// 5. proves every child state is an exact Merkle append of its parent; and
/// 6. for a complete clone, recomputes the current complete leaf Merkle root
///    from every collected descriptor.
///
/// Payload bytes are fetched later by [`materialize_series_v2`], after
/// [`plan_series_v2_leaves`] has verified the destination's durable prefix.
async fn fetch_series_v2(
    remote: &dyn ContentSource,
    series_hash: ObjectHash,
    entry_type: EntryType,
    manifest: SeriesManifest,
    manifest_bytes: Vec<u8>,
    graph: &mut FetchedGraph,
    complete_chain: bool,
) -> Result<(), StewardError> {
    let expected_kind = expected_payload_kind(entry_type);
    if manifest.payload_kind() != expected_kind {
        return Err(StewardError::Content(format!(
            "series {series_hash} manifest declares payload kind {:?} but its tree entry is {entry_type:?} (expects {expected_kind:?})",
            manifest.payload_kind()
        )));
    }

    if manifest.leaf_count() == 0 {
        let series_v2 = FetchedSeriesV2 {
            manifest_hash: series_hash,
            manifest,
            packs: Vec::new(),
            leaf_start: 0,
            leaf_hashes: Vec::new(),
            base_series_hash: None,
            base_manifest: None,
            physical_object_hashes: Vec::new(),
        };
        let _ = graph
            .objects
            .entry(series_hash)
            .or_insert_with(|| FetchedObject::SeriesV2(Box::new(series_v2.clone())));
        let _ = graph.series.insert(series_hash, Box::new(series_v2));
        let _ = graph.bytes.insert(series_hash, manifest_bytes);
        return Ok(());
    }

    let mut current_hash = series_hash;
    let mut current_manifest = manifest.clone();
    let mut newest_first = Vec::<(ObjectHash, PackIndex)>::new();
    let mut base = None;
    let mut seen_series = HashSet::new();
    loop {
        if !seen_series.insert(current_hash) {
            return Err(StewardError::Content(format!(
                "cycle in linked series segments at {current_hash}"
            )));
        }
        let descriptor = if complete_chain {
            match remote.get_consolidated_series_pack(current_hash).await? {
                Some(descriptor) => Some(descriptor),
                None => remote.get_series_pack(current_hash).await?,
            }
        } else {
            remote.get_series_pack(current_hash).await?
        }
        .ok_or_else(|| {
            StewardError::Content(format!(
                "series {current_hash} has no canonical pack-segment locator"
            ))
        })?;
        let pack = fetch_pack(remote, descriptor).await?;
        let range_leaf_hashes = pack
            .leaf_descriptors()
            .iter()
            .map(PackLeafDescriptor::logical_leaf_hash)
            .collect::<Vec<_>>();
        verify_series_segment(current_hash, &current_manifest, &pack, &range_leaf_hashes)?;
        let parent_hash = pack.parent_series_hash();
        newest_first.push((descriptor.pack_hash, pack));

        let Some(parent_hash) = parent_hash else {
            break;
        };
        let parent_bytes = fetch_verified(remote, parent_hash).await?;
        let parent_manifest = SeriesManifest::decode(&parent_bytes).map_err(|error| {
            StewardError::Content(format!(
                "decode parent series manifest {parent_hash}: {error}"
            ))
        })?;
        verify_series_extension(
            parent_hash,
            &parent_manifest,
            current_hash,
            &current_manifest,
            &newest_first.last().expect("just pushed").1,
            &range_leaf_hashes,
        )?;
        let continue_chain = complete_chain || graph.publication_packs.contains_key(&parent_hash);
        if !continue_chain {
            base = Some((parent_hash, parent_manifest));
            break;
        }
        current_hash = parent_hash;
        current_manifest = parent_manifest;
    }

    newest_first.reverse();
    let leaf_start = newest_first
        .first()
        .map_or(manifest.leaf_count(), |(_, pack)| pack.leaf_start());
    let mut all_leaf_hashes = Vec::new();
    let mut physical_object_hashes = Vec::new();
    let mut seen_physical = HashSet::new();
    for (_, pack) in &newest_first {
        all_leaf_hashes.extend(
            pack.leaf_descriptors()
                .iter()
                .map(PackLeafDescriptor::logical_leaf_hash),
        );
        for &object_hash in pack.physical_object_hashes() {
            if seen_physical.insert(object_hash) {
                physical_object_hashes.push(object_hash);
            }
        }
    }
    if let Some((_, base_manifest)) = &base {
        let extended = base_manifest
            .merkle_frontier()
            .extended(&all_leaf_hashes)
            .map_err(StewardError::Content)?;
        if extended != *manifest.merkle_frontier() {
            return Err(StewardError::Content(format!(
                "linked series suffix for {series_hash} does not extend its base to the current \
                 Merkle frontier"
            )));
        }
    } else {
        if leaf_start != 0
            || u64::try_from(all_leaf_hashes.len()).ok() != Some(manifest.leaf_count())
            || sync_store::content::merkle_root(&all_leaf_hashes) != manifest.leaf_merkle_root()
        {
            return Err(StewardError::Content(format!(
                "complete linked segment chain for {series_hash} does not reconstruct the current \
                 complete leaf Merkle root"
            )));
        }
    }

    let series_v2 = FetchedSeriesV2 {
        manifest_hash: series_hash,
        manifest,
        packs: newest_first,
        leaf_start,
        leaf_hashes: all_leaf_hashes,
        base_series_hash: base.as_ref().map(|(hash, _)| *hash),
        base_manifest: base.map(|(_, manifest)| manifest),
        physical_object_hashes,
    };
    let _ = graph
        .objects
        .entry(series_hash)
        .or_insert_with(|| FetchedObject::SeriesV2(Box::new(series_v2.clone())));
    let _ = graph.series.insert(series_hash, Box::new(series_v2));
    let _ = graph.bytes.insert(series_hash, manifest_bytes);
    Ok(())
}

async fn ensure_series_coverage(
    remote: &dyn ContentSource,
    series: &mut FetchedSeriesV2,
    desired_leaf_start: u64,
    prefer_consolidated: bool,
) -> Result<(), StewardError> {
    if desired_leaf_start > series.manifest.leaf_count() {
        return Err(StewardError::Content(format!(
            "requested series frontier {desired_leaf_start} exceeds current leaf count {}",
            series.manifest.leaf_count()
        )));
    }
    while series.leaf_start > desired_leaf_start {
        let current_hash = series.base_series_hash.ok_or_else(|| {
            StewardError::Content(format!(
                "series {} linked chain ends at leaf {}, before required frontier {}",
                series.manifest_hash, series.leaf_start, desired_leaf_start
            ))
        })?;
        let current_manifest = series.base_manifest.clone().ok_or_else(|| {
            StewardError::Content(format!(
                "series {} has a base hash without its authenticated manifest",
                series.manifest_hash
            ))
        })?;
        let descriptor = if prefer_consolidated {
            match remote.get_consolidated_series_pack(current_hash).await? {
                Some(descriptor) => Some(descriptor),
                None => remote.get_series_pack(current_hash).await?,
            }
        } else {
            remote.get_series_pack(current_hash).await?
        }
        .ok_or_else(|| {
            StewardError::Content(format!(
                "series {current_hash} has no canonical pack-segment locator"
            ))
        })?;
        let pack = fetch_pack(remote, descriptor).await?;
        let leaf_hashes = pack
            .leaf_descriptors()
            .iter()
            .map(PackLeafDescriptor::logical_leaf_hash)
            .collect::<Vec<_>>();
        verify_series_segment(current_hash, &current_manifest, &pack, &leaf_hashes)?;
        if pack.leaf_end() != series.leaf_start {
            return Err(StewardError::Content(format!(
                "series {} predecessor pack ends at leaf {}, expected {}",
                series.manifest_hash,
                pack.leaf_end(),
                series.leaf_start
            )));
        }

        let parent = match pack.parent_series_hash() {
            Some(parent_hash) => {
                let bytes = fetch_verified(remote, parent_hash).await?;
                let parent_manifest = SeriesManifest::decode(&bytes).map_err(|error| {
                    StewardError::Content(format!(
                        "decode parent series manifest {parent_hash}: {error}"
                    ))
                })?;
                verify_series_extension(
                    parent_hash,
                    &parent_manifest,
                    current_hash,
                    &current_manifest,
                    &pack,
                    &leaf_hashes,
                )?;
                Some((parent_hash, parent_manifest))
            }
            None => None,
        };

        series.packs.insert(0, (descriptor.pack_hash, pack));
        _ = series.leaf_hashes.splice(0..0, leaf_hashes);
        series.leaf_start = series
            .packs
            .first()
            .expect("predecessor pack was inserted")
            .1
            .leaf_start();
        series.base_series_hash = parent.as_ref().map(|(hash, _)| *hash);
        series.base_manifest = parent.map(|(_, manifest)| manifest);
    }

    let mut physical_object_hashes = Vec::new();
    let mut seen = HashSet::new();
    for (_, pack) in &series.packs {
        for &hash in pack.physical_object_hashes() {
            if seen.insert(hash) {
                physical_object_hashes.push(hash);
            }
        }
    }
    series.physical_object_hashes = physical_object_hashes;
    Ok(())
}

async fn align_series_fetches_to_target(
    remote: &dyn ContentSource,
    graph: &mut FetchedGraph,
    target: &TargetPlanningState,
) -> Result<(), StewardError> {
    if graph.manifest_complete {
        return Ok(());
    }
    let mut desired = HashMap::<ObjectHash, (u64, bool)>::new();
    for change in &graph.manifest_changes {
        let Some(after) = &change.after else {
            continue;
        };
        if !matches!(
            after.entry.entry_type,
            EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
        ) {
            continue;
        }
        let (leaf_start, fresh) = match target.nodes.get(after.entry.node_id.as_str()) {
            None => (0, true),
            Some(existing) if existing.child_hash == after.entry.child_hash => continue,
            Some(_) => {
                let prior = target
                    .series_manifests
                    .get(after.entry.node_id.as_str())
                    .ok_or_else(|| {
                        StewardError::Content(format!(
                            "changed series node {} has no authenticated prior manifest",
                            after.entry.node_id
                        ))
                    })?;
                (prior.leaf_count(), false)
            }
        };
        let _ = desired
            .entry(after.entry.child_hash)
            .and_modify(|(current, any_fresh)| {
                *current = (*current).min(leaf_start);
                *any_fresh |= fresh;
            })
            .or_insert((leaf_start, fresh));
    }
    for (series_hash, (leaf_start, fresh)) in desired {
        let series = graph.series.get_mut(&series_hash).ok_or_else(|| {
            StewardError::Content(format!(
                "series object {series_hash} is missing from the fetched graph"
            ))
        })?;
        ensure_series_coverage(remote, series, leaf_start, fresh).await?;
        if let Some(FetchedObject::SeriesV2(compatibility)) = graph.objects.get_mut(&series_hash) {
            *compatibility = series.clone();
        }
    }
    Ok(())
}

async fn fetch_pack(
    remote: &dyn ContentSource,
    descriptor: PackDescriptor,
) -> Result<PackIndex, StewardError> {
    let pack_hash = descriptor.pack_hash;
    let series_hash = descriptor.series_hash;
    let bytes = remote
        .get_publication_pack(descriptor)
        .await
        .map_err(|error| {
            StewardError::Content(format!(
                "fetch pack {pack_hash} for series {series_hash}: {error}"
            ))
        })?
        .ok_or_else(|| {
            StewardError::Content(format!(
                "canonical pack segment {pack_hash} for series {series_hash} is absent"
            ))
        })?;
    let computed = ObjectHash::of_bytes(&bytes);
    if computed != pack_hash {
        return Err(StewardError::Content(format!(
            "pack segment for series {series_hash} hashes to {computed} but was fetched as \
             {pack_hash}"
        )));
    }
    let pack = PackIndex::decode(&bytes).map_err(|error| {
        StewardError::Content(format!(
            "decode pack {pack_hash} for series {series_hash}: {error}"
        ))
    })?;
    if pack.series_hash() != series_hash {
        return Err(StewardError::Content(format!(
            "pack {pack_hash} declares series {}, expected {series_hash}",
            pack.series_hash()
        )));
    }
    pack.validate_series_segment()
        .map_err(|error| StewardError::Content(format!("pack {pack_hash}: {error}")))?;
    Ok(pack)
}

fn verify_series_segment(
    series_hash: ObjectHash,
    manifest: &SeriesManifest,
    pack: &PackIndex,
    range_leaf_hashes: &[ObjectHash],
) -> Result<(), StewardError> {
    if pack.parent_series_hash().is_none() {
        verify_complete_pack_against_manifest(series_hash, manifest, pack).map_err(|error| {
            StewardError::Content(format!(
                "pack {} failed verification against series {series_hash}: {error}",
                pack.hash()
            ))
        })?;
    } else {
        verify_pack_against_manifest(series_hash, manifest, pack, range_leaf_hashes).map_err(
            |error| {
                StewardError::Content(format!(
                    "pack {} failed verification against series {series_hash}: {error}",
                    pack.hash()
                ))
            },
        )?;
    }
    Ok(())
}

fn verify_series_extension(
    parent_hash: ObjectHash,
    parent: &SeriesManifest,
    series_hash: ObjectHash,
    manifest: &SeriesManifest,
    pack: &PackIndex,
    range_leaf_hashes: &[ObjectHash],
) -> Result<(), StewardError> {
    if pack.parent_series_hash() != Some(parent_hash) {
        return Err(StewardError::Content(format!(
            "pack {} links to {:?}, expected parent series {parent_hash}",
            pack.hash(),
            pack.parent_series_hash()
        )));
    }
    if parent.payload_kind() != manifest.payload_kind() {
        return Err(StewardError::Content(format!(
            "series {series_hash} changes payload kind from parent {parent_hash}"
        )));
    }
    if pack.leaf_start() != parent.leaf_count() {
        return Err(StewardError::Content(format!(
            "pack {} starts at leaf {}, parent series {parent_hash} has {} leaves",
            pack.hash(),
            pack.leaf_start(),
            parent.leaf_count()
        )));
    }
    let expected_logical = parent
        .logical_count()
        .checked_add(pack.logical_count())
        .ok_or_else(|| StewardError::Content("linked series logical count overflow".to_string()))?;
    if expected_logical != manifest.logical_count() {
        return Err(StewardError::Content(format!(
            "series {series_hash} logical count {} is not parent {} plus suffix {}",
            manifest.logical_count(),
            parent.logical_count(),
            pack.logical_count()
        )));
    }
    let (suffix_min, suffix_max) = descriptor_bounds(pack.leaf_descriptors());
    let expected_min = match (parent.min_event_time(), suffix_min) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    };
    let expected_max = match (parent.max_event_time(), suffix_max) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    };
    if expected_min != manifest.min_event_time() || expected_max != manifest.max_event_time() {
        return Err(StewardError::Content(format!(
            "series {series_hash} aggregate bounds do not equal parent {parent_hash} plus suffix"
        )));
    }
    let suffix_attributes = pack
        .leaf_descriptors()
        .last()
        .and_then(PackLeafDescriptor::logical_attributes);
    if suffix_attributes != manifest.logical_attributes() {
        return Err(StewardError::Content(format!(
            "series {series_hash} latest logical attributes do not match its appended suffix"
        )));
    }
    let extended = parent
        .merkle_frontier()
        .extended(range_leaf_hashes)
        .map_err(StewardError::Content)?;
    if extended != *manifest.merkle_frontier() {
        return Err(StewardError::Content(format!(
            "series {series_hash} is not an exact Merkle append of parent {parent_hash}"
        )));
    }
    Ok(())
}

fn descriptor_bounds(descriptors: &[PackLeafDescriptor]) -> (Option<i64>, Option<i64>) {
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;
    for descriptor in descriptors {
        if let Some(value) = descriptor.min_event_time() {
            min = Some(min.map_or(value, |current| current.min(value)));
        }
        if let Some(value) = descriptor.max_event_time() {
            max = Some(max.map_or(value, |current| current.max(value)));
        }
    }
    (min, max)
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
/// the object's declared v3 physical span.
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
    require_buffered: bool,
) -> Result<(), StewardError> {
    if graph.blob_hashes.contains(&hash) && (!require_buffered || graph.bytes.contains_key(&hash)) {
        return Ok(());
    }
    if !require_buffered
        && remote
            .object_size(hash)
            .await?
            .is_some_and(|size| size >= tlogfs::large_files::LARGE_FILE_THRESHOLD as u64)
    {
        let _ = graph.objects.entry(hash).or_insert(FetchedObject::External);
        let _ = graph.blob_hashes.insert(hash);
        let _ = graph.external_blobs.insert(hash);
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
            .entry(hash)
            .or_insert_with(|| FetchedObject::Blob(bytes.clone()));
        let _ = graph.blob_hashes.insert(hash);
        let _ = graph.bytes.insert(hash, bytes);
        return Ok(());
    }
    // Not an inline row: check the exact external key. Never list the complete
    // blob partition merely to establish one referenced object's presence.
    if !remote
        .has_blob(hash)
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?
    {
        return Err(StewardError::Content(format!(
            "object {} is absent from the remote (inline and blob store)",
            hash.to_hex()
        )));
    }
    if require_buffered {
        return Err(StewardError::Content(format!(
            "structured leaf object {hash} is unavailable as buffered immutable content"
        )));
    }
    let _ = graph.objects.entry(hash).or_insert(FetchedObject::External);
    let _ = graph.blob_hashes.insert(hash);
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
    /// Local planning work used to decide this rebuild.
    pub local_cost: LocalPullCost,
}

/// Observable local work performed while planning one pull.
///
/// Ordinary incremental native-v2 pulls use the persistent manifest map and
/// prior series manifest, so `full_target_scan` is false and
/// `retained_leaf_hashes_scanned` is zero. Full clone/rebuild and explicit
/// diagnostics may still report a complete scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocalPullCost {
    /// Whether planning folded the complete destination pond.
    pub full_target_scan: bool,
    /// Fixed local manifest-root cursor files read.
    pub manifest_root_cursor_files_read: usize,
    /// Latest reserved-index rows returned by bounded recovery queries.
    pub manifest_root_rows_read: usize,
    /// Immutable Patricia-map node files read by point lookup.
    pub manifest_nodes_read: usize,
    /// Distinct destination manifest records loaded, including parent paths.
    pub manifest_records_loaded: usize,
    /// Prior `watertown.series.v3` manifest files read.
    pub series_manifests_read: usize,
    /// Retained per-leaf hashes enumerated by planning.
    pub retained_leaf_hashes_scanned: usize,
}

#[derive(Default)]
struct TargetPlanningState {
    nodes: HashMap<String, ManifestEntry>,
    directory_children: HashMap<String, Vec<ManifestRecordChild>>,
    series_leaves: HashMap<String, Vec<ObjectHash>>,
    series_manifests: HashMap<String, SeriesManifest>,
    index_bytes: Option<Vec<u8>>,
    local_cost: LocalPullCost,
}

async fn read_target_index_bytes(
    target: &Ship,
    pond_id: &str,
    expected_root: Option<ObjectHash>,
) -> Result<(Option<Vec<u8>>, usize, usize), StewardError> {
    let store = crate::local_content::LocalContentStore::new(target.pond_path());
    let cursor = if expected_root.is_some() {
        store.read_manifest_root_cursor(pond_id)?
    } else {
        None
    };
    let cursor_reads = usize::from(cursor.is_some());
    if let (Some(expected_root), Some(bytes)) = (expected_root, cursor)
        && decode_manifest_root(&bytes).is_ok_and(|root| root == expected_root)
    {
        return Ok((Some(bytes), cursor_reads, 0));
    }

    let bytes = crate::content_tree::index_root_pointer_bytes(
        target.data_persistence().table().clone(),
        pond_id,
    )
    .await?;
    if let Some(bytes) = &bytes {
        store.write_manifest_root_cursor(pond_id, bytes)?;
        if let Some(expected_root) = expected_root {
            let actual = decode_manifest_root(bytes).map_err(StewardError::Content)?;
            if actual != expected_root {
                return Err(StewardError::Content(format!(
                    "destination manifest root {actual} does not match the authenticated pull \
                     boundary {expected_root}; refusing an O(history) fallback"
                )));
            }
        }
    }
    let row_reads = usize::from(bytes.is_some());
    Ok((bytes, cursor_reads, row_reads))
}

fn directory_children_from_entries(
    nodes: &HashMap<String, ManifestEntry>,
) -> HashMap<String, Vec<ManifestRecordChild>> {
    let mut children: HashMap<String, Vec<ManifestRecordChild>> = HashMap::new();
    for entry in nodes.values() {
        if entry.parent_node_id.is_empty() {
            continue;
        }
        children
            .entry(entry.parent_node_id.clone())
            .or_default()
            .push(ManifestRecordChild::new(
                entry.node_id.clone(),
                entry.name.clone(),
                entry.entry_type,
            ));
    }
    for entries in children.values_mut() {
        entries.sort_by(|left, right| left.node_id.as_bytes().cmp(right.node_id.as_bytes()));
    }
    children
}

async fn full_target_planning_state(
    target: &mut Ship,
    pond_id: &str,
    foreign_pond_id: Option<uuid7::Uuid>,
) -> Result<TargetPlanningState, StewardError> {
    let (nodes, series_leaves) = if foreign_pond_id.is_some() {
        crate::content_tree::build_target_state_for_pond(target, pond_id).await?
    } else {
        crate::content_tree::build_target_state(target).await?
    };
    let (index_bytes, cursor_reads, row_reads) = if nodes.is_empty() {
        (None, 0, 0)
    } else {
        read_target_index_bytes(target, pond_id, None).await?
    };
    let retained_leaf_hashes_scanned = series_leaves.values().map(Vec::len).sum();
    Ok(TargetPlanningState {
        directory_children: directory_children_from_entries(&nodes),
        nodes,
        series_leaves,
        series_manifests: HashMap::new(),
        index_bytes,
        local_cost: LocalPullCost {
            full_target_scan: true,
            manifest_root_cursor_files_read: cursor_reads,
            manifest_root_rows_read: row_reads,
            retained_leaf_hashes_scanned,
            ..LocalPullCost::default()
        },
    })
}

fn incremental_boundary_manifest_root(graph: &FetchedGraph) -> Result<ObjectHash, StewardError> {
    if graph.manifest_complete {
        return Err(StewardError::Content(
            "complete graph has no incremental manifest boundary".to_string(),
        ));
    }
    let (boundary_hash, boundary) = graph.commits.last().ok_or_else(|| {
        StewardError::Content("incremental graph has no boundary commit".to_string())
    })?;
    if Some(*boundary_hash) == graph.tip {
        return Err(StewardError::Content(
            "incremental graph contains no prior boundary commit".to_string(),
        ));
    }
    Ok(boundary.manifest_root)
}

fn load_target_record<F>(
    editor: &mut ManifestMapEditor<F>,
    cache: &mut HashMap<String, Option<ManifestRecord>>,
    node_id: &str,
) -> Result<Option<ManifestRecord>, StewardError>
where
    F: FnMut(ObjectHash) -> Result<Vec<u8>, String>,
{
    if let Some(record) = cache.get(node_id) {
        return Ok(record.clone());
    }
    let record = editor.lookup(node_id).map_err(StewardError::Content)?;
    let _ = cache.insert(node_id.to_string(), record.clone());
    Ok(record)
}

fn decode_local_series_manifest(
    store: &crate::local_content::LocalContentStore,
    node_id: &str,
    hash: ObjectHash,
) -> Result<SeriesManifest, StewardError> {
    let bytes = store.read(hash).map_err(|error| {
        StewardError::Content(format!(
            "incremental pull cannot read prior series manifest {hash} for node {node_id}: \
             {error}; native-v2 does not fall back to rescanning retained series rows"
        ))
    })?;
    let manifest = SeriesManifest::decode(&bytes).map_err(|error| {
        StewardError::Content(format!(
            "decode prior series manifest {hash} for node {node_id}: {error}"
        ))
    })?;
    if manifest.hash() != hash {
        return Err(StewardError::Content(format!(
            "prior series manifest for node {node_id} hashes to {}, expected {hash}",
            manifest.hash()
        )));
    }
    Ok(manifest)
}

async fn sparse_target_planning_state(
    target: &Ship,
    graph: &FetchedGraph,
    pond_id: &str,
) -> Result<TargetPlanningState, StewardError> {
    let expected_root = incremental_boundary_manifest_root(graph)?;
    let (index_bytes, cursor_reads, row_reads) =
        read_target_index_bytes(target, pond_id, Some(expected_root)).await?;
    let index_bytes = index_bytes.ok_or_else(|| {
        StewardError::Content(
            "incremental pull has no destination .pond-node-index baseline; run an explicit \
                 full rebuild (or rebuild the graft) instead of rescanning local history"
                .to_string(),
        )
    })?;
    let prior_root = decode_manifest_root(&index_bytes).map_err(StewardError::Content)?;
    debug_assert_eq!(prior_root, expected_root);

    let local_store = crate::local_content::LocalContentStore::new(target.pond_path());
    let mut manifest_nodes_read = 0usize;
    let mut editor = ManifestMapEditor::new(Some(prior_root), |hash| {
        manifest_nodes_read += 1;
        local_store.read_string_error(hash).map_err(|error| {
            format!(
                "incremental pull cannot read local manifest-map node {hash}: {error}; \
                 run an explicit full rebuild instead of rescanning the Delta table"
            )
        })
    });
    let mut cache = HashMap::<String, Option<ManifestRecord>>::new();
    let mut records = HashMap::<String, ManifestRecord>::new();
    let mut directory_children = HashMap::<String, Vec<ManifestRecordChild>>::new();
    let mut series_manifests = HashMap::<String, SeriesManifest>::new();
    let changes = graph
        .manifest_changes
        .iter()
        .map(|change| (change.node_id().to_string(), change))
        .collect::<HashMap<_, _>>();
    let mut parents = VecDeque::new();

    for change in &graph.manifest_changes {
        let node_id = change.node_id();
        let current = load_target_record(&mut editor, &mut cache, node_id)?;
        if current != change.before {
            return Err(StewardError::Content(format!(
                "incremental publication baseline for node {node_id} does not match the \
                 destination manifest map"
            )));
        }
        if let Some(record) = current {
            if record.entry.entry_type == EntryType::DirectoryPhysical {
                let _ = directory_children.insert(node_id.to_string(), record.children.clone());
            }
            if matches!(
                record.entry.entry_type,
                EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
            ) && change
                .after
                .as_ref()
                .is_some_and(|after| after.entry.child_hash != record.entry.child_hash)
            {
                let manifest =
                    decode_local_series_manifest(&local_store, node_id, record.entry.child_hash)?;
                let _ = series_manifests.insert(node_id.to_string(), manifest);
            }
            if !record.entry.parent_node_id.is_empty() {
                parents.push_back(record.entry.parent_node_id.clone());
            }
            let _ = records.insert(node_id.to_string(), record);
        }
        if let Some(after) = &change.after
            && !after.entry.parent_node_id.is_empty()
        {
            parents.push_back(after.entry.parent_node_id.clone());
        }
    }
    parents.push_back(tinyfs::ROOT_UUID.to_string());

    while let Some(node_id) = parents.pop_front() {
        if records.contains_key(&node_id) {
            continue;
        }
        let current = load_target_record(&mut editor, &mut cache, &node_id)?;
        if let Some(record) = current {
            if record.entry.entry_type == EntryType::DirectoryPhysical {
                let _ = directory_children.insert(node_id.clone(), record.children.clone());
            }
            if !record.entry.parent_node_id.is_empty() {
                parents.push_back(record.entry.parent_node_id.clone());
            }
            let _ = records.insert(node_id, record);
        } else if let Some(after) = changes
            .get(&node_id)
            .and_then(|change| change.after.as_ref())
            && !after.entry.parent_node_id.is_empty()
        {
            parents.push_back(after.entry.parent_node_id.clone());
        }
    }
    drop(editor);

    let nodes = records
        .into_iter()
        .map(|(node_id, record)| (node_id, record.entry))
        .collect::<HashMap<_, _>>();
    Ok(TargetPlanningState {
        local_cost: LocalPullCost {
            manifest_root_cursor_files_read: cursor_reads,
            manifest_root_rows_read: row_reads,
            manifest_nodes_read,
            manifest_records_loaded: nodes.len(),
            series_manifests_read: series_manifests.len(),
            ..LocalPullCost::default()
        },
        nodes,
        directory_children,
        series_leaves: HashMap::new(),
        series_manifests,
        index_bytes: Some(index_bytes),
    })
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
    /// Create (adopting `node_id`) or append to a native `watertown.series.v3`
    /// logical series (`docs/logical-series-identity-design.md`, release
    /// blocker item 1). Unlike [`ApplyOp::File`], a v2 series carries no
    /// buffered version list here: apply resolves `manifest_hash` back into
    /// the graph's authenticated [`FetchedSeriesV2`], then fetches only pack
    /// objects whose spans intersect the missing logical suffix.
    SeriesV2 {
        parent: String,
        name: String,
        node_id: String,
        create: bool,
        entry_type: EntryType,
        /// The `watertown.series.v3` manifest object's hash -- this node's
        /// `child_hash` -- naming the verified [`FetchedSeriesV2`] in
        /// the graph's validated series-role map to materialize from.
        manifest_hash: ObjectHash,
        /// The first (0-based, whole-series) logical leaf index this
        /// operation must write. Packs and object spans wholly before this
        /// boundary are not read.
        leaves_from: u64,
        /// The source's aggregate mtime for this series
        /// ([`replicated_mtime`]), adopted verbatim on the *last* leaf this
        /// operation writes so the destination's own subsequent fold
        /// recomputes the identical aggregate `VersionMeta` (mtime is not
        /// part of the `watertown.series.v3` manifest hash, but is part of the
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
/// UTF-8, if a recipe fails to decode, if a series is not an exact append, or
/// if a write fails. After applying, the destination's incremental native-v2
/// fold must equal the tip's root tree hash and persistent manifest-map root.
pub async fn rebuild_pond(
    target: &mut Ship,
    remote: &dyn ContentSource,
    graph: &FetchedGraph,
) -> Result<RebuildOutcome, StewardError> {
    let root = graph
        .root_tree_hash()
        .ok_or_else(|| StewardError::Content("cannot rebuild from an empty graph".to_string()))?;
    if authenticated_zero_change_transition(graph)? {
        let pond_id = target.data_persistence().pond_id().to_string();
        if destination_matches_publication_for_pond(target, graph, &pond_id).await? {
            return Ok(RebuildOutcome::default());
        }
        return Err(StewardError::Content(
            "authenticated zero-change publication does not match the destination content roots"
                .to_string(),
        ));
    }
    if graph.manifest.is_empty() && graph.manifest_changes.is_empty() {
        return Err(StewardError::Content(
            "fetched graph has no node manifest".to_string(),
        ));
    }
    let tip_manifest_root = graph
        .commits
        .first()
        .map(|(_, c)| c.manifest_root)
        .ok_or_else(|| StewardError::Content("fetched graph has no tip commit".to_string()))?;

    let local_pond_id = target.data_persistence().pond_id().to_string();
    let target_state = if graph.manifest_complete {
        full_target_planning_state(target, &local_pond_id, None).await?
    } else {
        sparse_target_planning_state(target, graph, &local_pond_id).await?
    };
    let mut prepared_graph = graph.clone();
    align_series_fetches_to_target(remote, &mut prepared_graph, &target_state).await?;
    let effective_graph = graph_with_effective_manifest(&prepared_graph, &target_state.nodes)?;

    // Reject a manifest that is inconsistent with the fetched tree closure
    // before any mutation, so a hostile/corrupt remote cannot commit an
    // inconsistent tree that the post-apply fold would only catch after commit.
    if graph.manifest_complete {
        verify_manifest_matches_tree(&effective_graph)?;
    }

    let (ops, mut outcome) = plan_node_diff(
        &effective_graph,
        root,
        &target_state.nodes,
        &target_state.directory_children,
        &target_state.series_leaves,
        &target_state.series_manifests,
    )?;
    outcome.local_cost = target_state.local_cost;
    let mut pack_objects = prepare_pack_objects(&ops, &effective_graph, remote).await?;

    let root_node_id = src_root_id(&effective_graph)?.to_string();
    let mut tx = target
        .begin_write(&PondUserMetadata::new(vec!["pull".to_string()]))
        .await?;
    tx.expect_content_roots(root, tip_manifest_root);
    let apply_result = async {
        let root_wd = tx.root().await?;
        apply_ops(
            &root_node_id,
            root_wd,
            &ops,
            remote,
            &effective_graph,
            &mut pack_objects,
        )
        .await
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
/// graph must carry a manifest, references must resolve, and the bounded
/// native-v2 update must produce the tip root tree hash and manifest root.
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

async fn apply_graft_metadata(
    tx: &crate::guard::StewardTransactionGuard<'_>,
    graft: &PreparedGraft,
    foreign_id: &str,
    replace: bool,
) -> Result<(), StewardError> {
    use tinyfs::EntryType;

    let root = tx.root().await?;
    let _ = root.create_dir_all(&graft.parent).await?;
    let parent_wd = root.open_dir_path(&graft.parent).await?;
    let foreign_pond_id = uuid7::Uuid::from(
        *uuid::Uuid::parse_str(foreign_id)
            .map_err(|error| StewardError::Content(format!("parse foreign pond id: {error}")))?
            .as_bytes(),
    );
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
            .async_writer_path_with_type(&graft.pin_path, EntryType::FilePhysicalVersion)
            .await?;
        writer.write_all(graft.pin_yaml.as_bytes()).await?;
        writer.shutdown().await?;
    }
    Ok(())
}

fn authenticated_zero_change_transition(graph: &FetchedGraph) -> Result<bool, StewardError> {
    if graph.manifest_complete
        || !graph.manifest.is_empty()
        || !graph.manifest_changes.is_empty()
        || graph.commits.len() < 2
    {
        return Ok(false);
    }
    let (_, tip) = &graph.commits[0];
    let (_, boundary) = graph.commits.last().expect("checked at least two commits");
    if tip.root_tree_hash != boundary.root_tree_hash || tip.manifest_root != boundary.manifest_root
    {
        return Err(StewardError::Content(
            "publication has no manifest changes but its authenticated content roots changed"
                .to_string(),
        ));
    }
    Ok(true)
}

async fn destination_matches_publication_for_pond(
    target: &Ship,
    graph: &FetchedGraph,
    pond_id: &str,
) -> Result<bool, StewardError> {
    let tip = graph
        .commits
        .first()
        .map(|(_, commit)| commit)
        .ok_or_else(|| StewardError::Content("fetched graph has no tip commit".to_string()))?;
    destination_matches_commit_for_pond(target, tip, pond_id).await
}

async fn destination_matches_commit_for_pond(
    target: &Ship,
    commit: &Commit,
    pond_id: &str,
) -> Result<bool, StewardError> {
    let Some(index_bytes) = crate::content_tree::index_root_pointer_bytes(
        target.data_persistence().table().clone(),
        pond_id,
    )
    .await?
    else {
        return Ok(false);
    };
    let local_manifest_root = decode_manifest_root(&index_bytes).map_err(StewardError::Content)?;
    if local_manifest_root != commit.manifest_root {
        return Ok(false);
    }

    let store = crate::local_content::LocalContentStore::new(target.pond_path());
    let mut editor = ManifestMapEditor::new(Some(local_manifest_root), |hash| {
        store.read_string_error(hash)
    });
    let root = editor
        .lookup(tinyfs::ROOT_UUID)
        .map_err(StewardError::Content)?;
    let Some(root) = root else {
        return Err(StewardError::Content(
            "destination manifest map has no root record".to_string(),
        ));
    };
    Ok(root.entry.parent_node_id.is_empty()
        && root.entry.name.is_empty()
        && root.entry.entry_type == EntryType::DirectoryPhysical
        && root.entry.child_hash == commit.root_tree_hash)
}

/// Authenticate an exact-identity no-op against the destination's
/// authoritative reserved-index row and root manifest record.
pub async fn authenticate_destination_publication_head(
    target: &Ship,
    state: &PublicationState,
    commit: &Commit,
    pond_id: &str,
) -> Result<(), StewardError> {
    if state.snapshot_tip != commit.hash() || state.manifest_root != commit.manifest_root {
        return Err(StewardError::Content(
            "authenticated publication state disagrees with its tip commit".to_string(),
        ));
    }
    let index_bytes = crate::content_tree::index_root_pointer_bytes(
        target.data_persistence().table().clone(),
        pond_id,
    )
    .await?
    .ok_or_else(|| {
        StewardError::Content(format!(
            "destination pond {pond_id} has no authoritative manifest-root index"
        ))
    })?;
    let local_manifest_root = decode_manifest_root(&index_bytes).map_err(StewardError::Content)?;
    if local_manifest_root != commit.manifest_root {
        return Err(StewardError::Content(format!(
            "destination pond {pond_id} manifest root {local_manifest_root} does not match \
             authenticated publication root {}",
            commit.manifest_root
        )));
    }
    let store = crate::local_content::LocalContentStore::new(target.pond_path());
    let mut editor = ManifestMapEditor::new(Some(local_manifest_root), |hash| {
        store.read_string_error(hash)
    });
    let root = editor
        .lookup(tinyfs::ROOT_UUID)
        .map_err(StewardError::Content)?
        .ok_or_else(|| {
            StewardError::Content(format!(
                "destination pond {pond_id} manifest map has no root record"
            ))
        })?;
    if !root.entry.parent_node_id.is_empty()
        || !root.entry.name.is_empty()
        || root.entry.entry_type != EntryType::DirectoryPhysical
        || root.entry.child_hash != commit.root_tree_hash
    {
        return Err(StewardError::Content(format!(
            "destination pond {pond_id} roots do not match authenticated publication {}",
            state.snapshot_tip
        )));
    }
    Ok(())
}

/// Authenticate a graft no-op, including foreign roots, mount identity, and
/// the durable content pin.
pub async fn authenticate_graft_publication_head(
    target: &mut Ship,
    state: &PublicationState,
    commit: &Commit,
    name: &str,
    mount_path: &str,
) -> Result<(), StewardError> {
    let foreign_id = state.pond_id.to_string();
    authenticate_destination_publication_head(target, state, commit, &foreign_id).await?;
    let (parent, leaf) = crate::split_mount_path(mount_path).map_err(StewardError::Content)?;
    let expected_pin = crate::GraftPin {
        foreign_pond_id: foreign_id.clone(),
        mount_path: mount_path.to_string(),
        pinned_tip: state.snapshot_tip.to_hex(),
    };
    let pin_path = crate::GraftPin::pin_path(name);
    let tx = target
        .begin_read(&PondUserMetadata::new(vec![
            "pull".to_string(),
            "authenticate-graft-noop".to_string(),
            name.to_string(),
        ]))
        .await?;
    let validation = async {
        let root = tx.root().await?;
        let parent = root.open_dir_path(parent).await.map_err(|error| {
            StewardError::Content(format!(
                "graft mount parent for {mount_path:?} is absent: {error}"
            ))
        })?;
        let mount = parent.entry(leaf).await?.ok_or_else(|| {
            StewardError::Content(format!("graft mount {mount_path:?} is absent"))
        })?;
        if mount.pond_id.as_deref() != Some(foreign_id.as_str())
            || mount.child_node_id.to_string() != tinyfs::ROOT_UUID
            || mount.entry_type != EntryType::DirectoryPhysical
        {
            return Err(StewardError::Content(format!(
                "graft mount {mount_path:?} does not reference foreign pond {foreign_id}'s root"
            )));
        }
        let pin_bytes = root
            .read_file_path_to_vec(&pin_path)
            .await
            .map_err(|error| {
                StewardError::Content(format!("read graft pin {pin_path:?}: {error}"))
            })?;
        let pin = crate::GraftPin::from_yaml_bytes(&pin_bytes).map_err(|error| {
            StewardError::Content(format!("parse graft pin {pin_path:?}: {error}"))
        })?;
        if pin != expected_pin {
            return Err(StewardError::Content(format!(
                "graft pin {pin_path:?} does not match authenticated publication {}",
                state.snapshot_tip
            )));
        }
        Ok(())
    }
    .await;
    let close = tx.commit().await;
    validation?;
    let _ = close?;
    Ok(())
}

/// Authenticate that the destination's durable manifest-map transaction
/// already represents the fetched publication tip.
///
/// Used only to recover the mirror apply/ack crash window. The comparison is
/// against the verified remote commit's two content roots and the authoritative
/// reserved index row, not a mutable control-table acknowledgement or cache.
pub async fn destination_matches_publication(
    target: &Ship,
    graph: &FetchedGraph,
) -> Result<bool, StewardError> {
    let pond_id = target.data_persistence().pond_id().to_string();
    destination_matches_publication_for_pond(target, graph, &pond_id).await
}

/// Authenticate that a fetched incremental publication window continues the
/// exact durable consumer acknowledgement, including producer/ref identity,
/// publication-record lineage, generations, commit ancestry, and both content
/// roots at every published snapshot.
pub fn authenticate_publication_window(
    graph: &FetchedGraph,
    acknowledged: &PublicationState,
) -> Result<(), StewardError> {
    let current = graph.publication_state.as_ref().ok_or_else(|| {
        StewardError::Content("fetched graph has no publication state".to_string())
    })?;
    if current.pond_id != acknowledged.pond_id
        || current.ref_name != acknowledged.ref_name
        || current.format != acknowledged.format
        || current.generation <= acknowledged.generation
    {
        return Err(StewardError::Content(
            "fetched publication window does not continue the consumer acknowledgement".to_string(),
        ));
    }
    let (boundary_hash, boundary) = graph.publication_boundary.as_ref().ok_or_else(|| {
        StewardError::Content(
            "fetched publication window has no authenticated acknowledgement boundary".to_string(),
        )
    })?;
    if *boundary_hash != acknowledged.publication_record
        || boundary.snapshot_tip != acknowledged.snapshot_tip
        || boundary.manifest_root != acknowledged.manifest_root
        || boundary.pond_id != acknowledged.pond_id
        || boundary.ref_name != acknowledged.ref_name
    {
        return Err(StewardError::Content(
            "fetched publication boundary does not match the exact consumer acknowledgement"
                .to_string(),
        ));
    }
    let generation_delta =
        usize::try_from(current.generation - acknowledged.generation).map_err(|_| {
            StewardError::Content("publication generation delta is invalid".to_string())
        })?;
    if graph.publication_records.len() != generation_delta {
        return Err(StewardError::Content(format!(
            "publication lineage contains {} record(s), but generations {} to {} require \
             {generation_delta}",
            graph.publication_records.len(),
            acknowledged.generation,
            current.generation
        )));
    }

    let mut commit_indexes = Vec::new();
    let mut prior_commit_index = 0usize;
    for record in graph
        .publication_records
        .iter()
        .map(|(_, record)| record)
        .chain(std::iter::once(boundary))
    {
        let relative = graph.commits[prior_commit_index..]
            .iter()
            .position(|(hash, commit)| {
                *hash == record.snapshot_tip && commit.manifest_root == record.manifest_root
            })
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "publication record for snapshot {} is not bound to the fetched commit \
                     ancestry and manifest root",
                    record.snapshot_tip
                ))
            })?;
        prior_commit_index += relative;
        commit_indexes.push(prior_commit_index);
        prior_commit_index = prior_commit_index.checked_add(1).ok_or_else(|| {
            StewardError::Content("publication commit index overflow".to_string())
        })?;
    }
    if commit_indexes.first().copied() != Some(0) {
        return Err(StewardError::Content(
            "publication head record is not bound to the fetched tip commit".to_string(),
        ));
    }
    for (record_index, (_, record)) in graph.publication_records.iter().enumerate() {
        let start = commit_indexes[record_index];
        let parent = commit_indexes[record_index + 1];
        if parent <= start {
            return Err(StewardError::Content(
                "publication records do not advance through distinct commit snapshots".to_string(),
            ));
        }
        let expected = publication_delta_from_commits(
            graph.commits[start..parent]
                .iter()
                .rev()
                .map(|(_, commit)| commit),
        )?;
        require_record_matches_commit_delta(record, &expected)?;
    }
    Ok(())
}

/// Reconstruct and authenticate the structured publication boundary named
/// only by a durable snapshot pin.
///
/// This is used for graft recovery after the structured acknowledgement was
/// explicitly cleared. The active row's generation bounds the preceding fetch;
/// this function then binds the located immutable publication record, commit,
/// and manifest root before the pin may be used as an incremental boundary.
pub fn authenticate_pinned_publication_boundary(
    graph: &FetchedGraph,
    pinned_tip: ObjectHash,
) -> Result<PublicationState, StewardError> {
    let current = graph.publication_state.as_ref().ok_or_else(|| {
        StewardError::Content("fetched graph has no publication state".to_string())
    })?;
    let (record_hash, record) = graph.publication_boundary.as_ref().ok_or_else(|| {
        StewardError::Content(format!(
            "fetched publication window has no immutable record for pinned snapshot {pinned_tip}"
        ))
    })?;
    if record.snapshot_tip != pinned_tip {
        return Err(StewardError::Content(format!(
            "fetched publication boundary {} does not match pinned snapshot {pinned_tip}",
            record.snapshot_tip
        )));
    }
    let newer = i64::try_from(graph.publication_records.len()).map_err(|_| {
        StewardError::Content("publication window length does not fit i64".to_string())
    })?;
    let generation = current.generation.checked_sub(newer).ok_or_else(|| {
        StewardError::Content("pinned publication generation underflow".to_string())
    })?;
    let boundary = PublicationState::new(
        current.pond_id,
        &current.ref_name,
        record.snapshot_tip,
        record.manifest_root,
        *record_hash,
        generation,
        0,
    )
    .map_err(|error| StewardError::Content(error.to_string()))?;
    authenticate_publication_window(graph, &boundary)?;
    Ok(boundary)
}

/// Narrow an already authenticated publication window to the suffix after an
/// applied snapshot pin.
///
/// The pin must name a newer immutable publication record inside the exact
/// acknowledged window. Older, unrelated, or head pins are rejected.
pub fn narrow_authenticated_publication_window(
    graph: &FetchedGraph,
    acknowledged: &PublicationState,
    applied_tip: ObjectHash,
) -> Result<(PublicationState, FetchedGraph), StewardError> {
    authenticate_publication_window(graph, acknowledged)?;
    if applied_tip == acknowledged.snapshot_tip {
        return Ok((acknowledged.clone(), graph.clone()));
    }
    let record_index = graph
        .publication_records
        .iter()
        .position(|(_, record)| record.snapshot_tip == applied_tip)
        .ok_or_else(|| {
            StewardError::Content(format!(
                "graft pin {applied_tip} is not inside the authenticated publication window"
            ))
        })?;
    if record_index == 0 {
        return Err(StewardError::Content(format!(
            "graft pin {applied_tip} already names the current publication"
        )));
    }
    narrow_publication_window_at(graph, record_index)
}

#[derive(Default)]
struct AuthenticatedPublicationDelta {
    objects: BTreeSet<sync_store::content::ObjectDescriptor>,
    packs: BTreeSet<PackDescriptor>,
    changes: BTreeMap<String, ManifestChange>,
}

fn publication_delta_from_commits<'a>(
    commits: impl IntoIterator<Item = &'a Commit>,
) -> Result<AuthenticatedPublicationDelta, StewardError> {
    let mut delta = AuthenticatedPublicationDelta::default();
    for commit in commits {
        let _ = delta
            .objects
            .insert(sync_store::content::ObjectDescriptor::new(
                commit.hash(),
                sync_store::content::ContentObjectKind::Commit,
            ));
        delta
            .objects
            .extend(commit.introduced_objects.iter().copied());
        delta.packs.extend(commit.introduced_packs.iter().copied());
        for change in &commit.manifest_changes {
            let node_id = change.node_id().to_string();
            match delta.changes.get_mut(&node_id) {
                Some(existing) => {
                    if existing.after != change.before {
                        return Err(StewardError::Content(format!(
                            "published commit deltas are discontinuous at node {node_id}"
                        )));
                    }
                    existing.after = change.after.clone();
                    if existing.before == existing.after {
                        let _ = delta.changes.remove(&node_id);
                    }
                }
                None => {
                    let _ = delta.changes.insert(node_id, change.clone());
                }
            }
        }
    }
    Ok(delta)
}

fn require_record_matches_commit_delta(
    record: &PublicationRecord,
    expected: &AuthenticatedPublicationDelta,
) -> Result<(), StewardError> {
    let objects = record
        .introduced_objects
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if objects != expected.objects {
        return Err(StewardError::Content(format!(
            "publication record {} object inventory does not match its exact commit interval",
            record.hash()
        )));
    }
    let packs = record
        .introduced_packs
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if packs != expected.packs {
        return Err(StewardError::Content(format!(
            "publication record {} pack inventory does not match its exact commit interval",
            record.hash()
        )));
    }
    let changes = record
        .manifest_changes
        .iter()
        .cloned()
        .map(|change| (change.node_id().to_string(), change))
        .collect::<BTreeMap<_, _>>();
    if changes != expected.changes {
        return Err(StewardError::Content(format!(
            "publication record {} manifest changes do not match its exact commit interval",
            record.hash()
        )));
    }
    Ok(())
}

/// When the destination already represents an intermediate publication in an
/// authenticated fetched window, return that repaired frontier plus a graph
/// narrowed to only the still-unapplied suffix.
pub async fn recover_applied_publication(
    target: &Ship,
    graph: &FetchedGraph,
    acknowledged: &PublicationState,
) -> Result<Option<(PublicationState, FetchedGraph)>, StewardError> {
    authenticate_publication_window(graph, acknowledged)?;
    let current = graph.publication_state.as_ref().ok_or_else(|| {
        StewardError::Content("fetched graph has no publication state".to_string())
    })?;
    if target.control_table().pond_id_uuid() != current.pond_id {
        return Err(StewardError::Content(format!(
            "destination pond {} does not match publication source pond {}",
            target.control_table().pond_id_uuid(),
            current.pond_id
        )));
    }

    let pond_id = target.data_persistence().pond_id().to_string();
    for (record_index, (_record_hash, record)) in
        graph.publication_records.iter().enumerate().skip(1)
    {
        let Some(commit_index) = graph.commits.iter().position(|(hash, commit)| {
            *hash == record.snapshot_tip && commit.manifest_root == record.manifest_root
        }) else {
            return Err(StewardError::Content(format!(
                "intermediate publication {} is absent from the fetched commit ancestry",
                record.snapshot_tip
            )));
        };
        if !destination_matches_commit_for_pond(target, &graph.commits[commit_index].1, &pond_id)
            .await?
        {
            continue;
        }
        return narrow_publication_window_at(graph, record_index).map(Some);
    }
    Ok(None)
}

fn narrow_publication_window_at(
    graph: &FetchedGraph,
    record_index: usize,
) -> Result<(PublicationState, FetchedGraph), StewardError> {
    let current = graph.publication_state.as_ref().ok_or_else(|| {
        StewardError::Content("fetched graph has no publication state".to_string())
    })?;
    let (record_hash, record) = graph.publication_records.get(record_index).ok_or_else(|| {
        StewardError::Content(format!(
            "publication window has no record at index {record_index}"
        ))
    })?;
    let commit_index = graph
        .commits
        .iter()
        .position(|(hash, commit)| {
            *hash == record.snapshot_tip && commit.manifest_root == record.manifest_root
        })
        .ok_or_else(|| {
            StewardError::Content(format!(
                "publication {} is absent from the fetched commit ancestry",
                record.snapshot_tip
            ))
        })?;
    let generation = current
        .generation
        .checked_sub(i64::try_from(record_index).map_err(|_| {
            StewardError::Content("publication window index does not fit i64".to_string())
        })?)
        .ok_or_else(|| StewardError::Content("publication generation underflow".to_string()))?;
    let boundary = PublicationState::new(
        current.pond_id,
        &current.ref_name,
        record.snapshot_tip,
        record.manifest_root,
        *record_hash,
        generation,
        0,
    )
    .map_err(|error| StewardError::Content(error.to_string()))?;

    let mut remaining = graph.clone();
    remaining.commits.truncate(commit_index + 1);
    remaining.publication_records.truncate(record_index);
    remaining.publication_boundary = Some((*record_hash, record.clone()));
    remaining.manifest_changes = squash_publication_changes(&remaining.publication_records)?;
    remaining.manifest = remaining
        .manifest_changes
        .iter()
        .filter_map(|change| change.after.as_ref().map(|after| after.entry.clone()))
        .collect();
    remaining.publication_packs.clear();
    for (_, newer) in &remaining.publication_records {
        for descriptor in &newer.introduced_packs {
            remaining
                .publication_packs
                .entry(descriptor.series_hash)
                .or_default()
                .push(*descriptor);
        }
    }
    for packs in remaining.publication_packs.values_mut() {
        packs.sort_unstable();
        packs.dedup();
    }
    Ok((boundary, remaining))
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
    let tip_manifest_root = graph
        .commits
        .first()
        .map(|(_, c)| c.manifest_root)
        .ok_or_else(|| StewardError::Content("fetched graph has no tip commit".to_string()))?;
    let foreign_id = foreign_pond_id.to_string();
    if authenticated_zero_change_transition(graph)? {
        if !destination_matches_publication_for_pond(target, graph, &foreign_id).await? {
            return Err(StewardError::Content(
                "authenticated zero-change publication does not match the imported foreign roots"
                    .to_string(),
            ));
        }
        if let Some(graft) = &graft {
            let tx = target
                .begin_write(&PondUserMetadata::new(vec![
                    "pull".to_string(),
                    "advance-graft-pin".to_string(),
                ]))
                .await?;
            if let Err(error) = apply_graft_metadata(&tx, graft, &foreign_id, false).await {
                return Err(tx.abort_preserving(error).await);
            }
            _ = tx.commit().await?;
        }
        let foreign_seq = graph
            .commits
            .first()
            .map(|(_, commit)| commit.provenance.seq)
            .unwrap_or(0);
        target
            .data_persistence_mut()
            .sync_last_txn_seq(&foreign_id, foreign_seq);
        return Ok(RebuildOutcome::default());
    }
    if graph.manifest.is_empty() && graph.manifest_changes.is_empty() {
        return Err(StewardError::Content(
            "fetched graph has no node manifest".to_string(),
        ));
    }
    if replace && !graph.manifest_complete {
        return Err(StewardError::Content(
            "scoped graft replacement requires a complete source graph".to_string(),
        ));
    }
    let target_state = if graph.manifest_complete {
        full_target_planning_state(target, &foreign_id, Some(foreign_pond_id)).await?
    } else {
        sparse_target_planning_state(target, graph, &foreign_id).await?
    };
    let mut prepared_graph = graph.clone();
    align_series_fetches_to_target(remote, &mut prepared_graph, &target_state).await?;
    let effective_graph = graph_with_effective_manifest(&prepared_graph, &target_state.nodes)?;

    // Reject a manifest that is inconsistent with the fetched tree closure
    // before any mutation (see verify_manifest_matches_tree).
    if graph.manifest_complete {
        verify_manifest_matches_tree(&effective_graph)?;
    }

    let (ops, mut outcome) = if replace {
        plan_full_replacement(&effective_graph, root, &target_state.nodes)?
    } else {
        plan_node_diff(
            &effective_graph,
            root,
            &target_state.nodes,
            &target_state.directory_children,
            &target_state.series_leaves,
            &target_state.series_manifests,
        )?
    };
    outcome.local_cost = target_state.local_cost;
    let mut pack_objects = prepare_pack_objects(&ops, &effective_graph, remote).await?;

    let root_node_id = src_root_id(&effective_graph)?.to_string();
    let first_import = target_state.nodes.is_empty();
    let prior_index_bytes = (!replace)
        .then_some(target_state.index_bytes.clone())
        .flatten();
    let pond_path = target.pond_path().to_path_buf();
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
            apply_graft_metadata(&tx, graft, &foreign_id, replace).await?;
        }
        apply_ops(
            &root_node_id,
            root_wd.clone(),
            &ops,
            remote,
            &effective_graph,
            &mut pack_objects,
        )
        .await?;
        validate_and_stage_foreign_manifest(
            &tx,
            &root_wd,
            &foreign_id,
            &pond_path,
            prior_index_bytes,
            root,
            tip_manifest_root,
        )
        .await
    }
    .await;
    if let Err(error) = apply_result {
        return Err(tx.abort_preserving(error).await);
    }
    _ = tx.commit().await?;
    let foreign_index_bytes = sync_store::content::encode_manifest_root(tip_manifest_root);
    if let Err(error) = crate::local_content::LocalContentStore::new(&pond_path)
        .write_manifest_root_cursor(&foreign_id, &foreign_index_bytes)
    {
        log::warn!(
            "committed foreign manifest-root cursor could not be refreshed; the next pull will \
             recover it from the reserved index: {error}"
        );
    }

    // Advance only the foreign pond's seq frontier so the local allocator stays
    // contiguous. The source tip is the newest commit even when the ancestry
    // fetch stopped at the destination's prior tip.
    let foreign_seq = graph
        .commits
        .first()
        .map(|(_, c)| c.provenance.seq)
        .unwrap_or(0);
    target
        .data_persistence_mut()
        .sync_last_txn_seq(&foreign_id, foreign_seq);

    Ok(outcome)
}

async fn validate_and_stage_foreign_manifest(
    tx: &crate::guard::StewardTransactionGuard<'_>,
    foreign_root: &WD,
    foreign_pond_id: &str,
    pond_path: &std::path::Path,
    prior_index_bytes: Option<Vec<u8>>,
    expected_root_tree: ObjectHash,
    expected_manifest_root: ObjectHash,
) -> Result<(), StewardError> {
    let uncommitted = tx.state()?.uncommitted_live_rows().await?;
    let committed_table = tx.data_persistence()?.table().clone();
    let inputs = crate::content_tree::incremental_spine_inputs_v2(
        committed_table,
        prior_index_bytes.clone(),
        uncommitted,
        foreign_pond_id,
        pond_path,
    )
    .await?;
    if inputs.root_tree_hash != expected_root_tree || inputs.manifest_root != expected_manifest_root
    {
        return Err(StewardError::Content(format!(
            "imported foreign tree would produce root {} manifest {}, expected root {} manifest {}",
            inputs.root_tree_hash, inputs.manifest_root, expected_root_tree, expected_manifest_root
        )));
    }

    let local_store = crate::local_content::LocalContentStore::new(pond_path);
    local_store.put_batch(&inputs.object_bytes)?;
    local_store.put_batch(&inputs.pack_bytes)?;

    if prior_index_bytes.as_deref() == Some(inputs.index_bytes.as_slice()) {
        return Ok(());
    }
    let mut writer = if foreign_root.exists(tinyfs::INDEX_NODE_NAME).await {
        foreign_root.async_writer_reserved_index().await?
    } else {
        let node_id = NodeID::from_hex_string(tinyfs::INDEX_NODE_UUID)
            .map_err(|error| StewardError::Content(format!("reserved index node id: {error}")))?;
        foreign_root
            .create_file_with_id(tinyfs::INDEX_NODE_NAME, node_id)
            .await?
    };
    writer.write_all(&inputs.index_bytes).await?;
    writer.shutdown().await?;
    Ok(())
}

fn graph_with_effective_manifest(
    graph: &FetchedGraph,
    target_nodes: &HashMap<String, ManifestEntry>,
) -> Result<FetchedGraph, StewardError> {
    if graph.manifest_complete {
        return Ok(graph.clone());
    }
    let mut nodes = target_nodes.clone();
    for change in &graph.manifest_changes {
        let node_id = change.node_id();
        if let Some(before) = &change.before {
            let current = nodes.get(node_id).ok_or_else(|| {
                StewardError::Content(format!(
                    "incremental publication expects existing node {node_id}"
                ))
            })?;
            if current != &before.entry {
                return Err(StewardError::Content(format!(
                    "incremental publication baseline for node {node_id} does not match the \
                     consumer"
                )));
            }
        } else if nodes.contains_key(node_id) {
            return Err(StewardError::Content(format!(
                "incremental publication creates already-present node {node_id}"
            )));
        }
        match &change.after {
            Some(after) => {
                let _ = nodes.insert(node_id.to_string(), after.entry.clone());
            }
            None => {
                let _ = nodes.remove(node_id);
            }
        }
    }
    let mut effective = graph.clone();
    effective.manifest = nodes.into_values().collect();
    effective
        .manifest
        .sort_by(|left, right| left.node_id.as_bytes().cmp(right.node_id.as_bytes()));
    effective.manifest_complete = true;
    Ok(effective)
}

fn src_root_id(graph: &FetchedGraph) -> Result<&str, StewardError> {
    graph
        .manifest
        .iter()
        .find(|e| e.parent_node_id.is_empty() && e.name.is_empty())
        .map(|e| e.node_id.as_str())
        .ok_or_else(|| StewardError::Content("manifest has no root entry".to_string()))
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
        let tree_entries = graph.trees.get(&dir.child_hash).ok_or_else(|| {
            StewardError::Content(format!(
                "directory node {} references {} which is not a tree object in the closure",
                dir.node_id,
                dir.child_hash.to_hex()
            ))
        })?;
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
        &HashMap::new(),
    )?;
    deletes.append(&mut creates);
    Ok((deletes, outcome))
}

fn plan_node_diff(
    graph: &FetchedGraph,
    root: ObjectHash,
    target_nodes: &HashMap<String, ManifestEntry>,
    target_directory_children: &HashMap<String, Vec<ManifestRecordChild>>,
    target_series_leaves: &HashMap<String, Vec<ObjectHash>>,
    target_series_manifests: &HashMap<String, SeriesManifest>,
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
                target_directory_children
                    .get(parent_id)
                    .into_iter()
                    .flatten()
                    .map(|entry| entry.name.clone()),
            )
            .collect();
        emit_collision_safe_renames(parent_id, renames, reserved_names, &mut ops);

        for entry in kids {
            plan_one(
                entry,
                graph,
                target_nodes,
                target_series_leaves,
                target_series_manifests,
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
    target_series_leaves: &HashMap<String, Vec<ObjectHash>>,
    target_series_manifests: &HashMap<String, SeriesManifest>,
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
            });
        }
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
            if create {
                outcome.series += 1;
            }
            if needs_write {
                let series = series_v2(graph, entry.child_hash)?;
                let leaves_from = plan_series_v2_leaves(
                    entry,
                    series,
                    target_series_leaves,
                    target_series_manifests,
                    existing.map(|t| t.child_hash),
                )?;
                // Always emit the op on create (adopting the node even
                // if, defensively, it turned out to need no leaves), and
                // otherwise only when there is a real suffix to append.
                if create || leaves_from < series.manifest.leaf_count() {
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

/// Resolve a `watertown.series.v3` object to its verified [`FetchedSeriesV2`] state.
///
/// # Errors
///
/// Returns an error if the object at `series_hash` is not a verified series.
fn series_v2(
    graph: &FetchedGraph,
    series_hash: ObjectHash,
) -> Result<&FetchedSeriesV2, StewardError> {
    graph
        .series
        .get(&series_hash)
        .map(Box::as_ref)
        .ok_or_else(|| {
            StewardError::Content(format!(
                "series object {} missing from graph",
                series_hash.to_hex()
            ))
        })
}

/// Decide the suffix of a v2 logical series' leaves the target still needs
/// (release blocker item 1, `docs/logical-series-identity-design.md`).
///
/// A `watertown.series.v3` series' `child_hash` is its manifest hash, which is a pure
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
/// Returns an error if the destination's persisted prior manifest does not
/// equal the fetched suffix's authenticated base. Complete rebuilds may still
/// compare full retained leaf lists; ordinary incremental pulls never do.
fn plan_series_v2_leaves(
    entry: &ManifestEntry,
    series: &FetchedSeriesV2,
    target_series_leaves: &HashMap<String, Vec<ObjectHash>>,
    target_series_manifests: &HashMap<String, SeriesManifest>,
    existing_child_hash: Option<ObjectHash>,
) -> Result<u64, StewardError> {
    if let Some(child_hash) = existing_child_hash
        && child_hash == entry.child_hash
    {
        return Ok(series.manifest.leaf_count());
    }

    if existing_child_hash.is_none() {
        if series.base_series_hash.is_some() || series.leaf_start != 0 {
            return Err(StewardError::Content(format!(
                "new v2 series node {} was fetched as only a suffix; a fresh node requires the \
                 complete linked segment chain",
                entry.node_id
            )));
        }
        return Ok(0);
    }

    if let Some(held_manifest) = target_series_manifests.get(&entry.node_id) {
        let held_hash = existing_child_hash.expect("checked Some above");
        if held_manifest.hash() != held_hash {
            return Err(StewardError::Content(format!(
                "v2 series node {} prior manifest hashes to {}, expected {}",
                entry.node_id,
                held_manifest.hash(),
                held_hash
            )));
        }
        let held_count = held_manifest.leaf_count();
        if held_count < series.leaf_start || held_count > series.manifest.leaf_count() {
            return Err(StewardError::Content(format!(
                "v2 series node {} holds {} leaves, but fetched authenticated coverage begins at \
                 {} and ends at {}",
                entry.node_id,
                held_count,
                series.leaf_start,
                series.manifest.leaf_count()
            )));
        }
        let prefix_len = usize::try_from(held_count - series.leaf_start).map_err(|_| {
            StewardError::Content("series prefix length does not fit usize".to_string())
        })?;
        let base_frontier = match &series.base_manifest {
            Some(base) if base.leaf_count() == series.leaf_start => base.merkle_frontier().clone(),
            None if series.leaf_start == 0 => sync_store::content::MerkleFrontier::empty(),
            _ => {
                return Err(StewardError::Content(format!(
                    "v2 series node {} has no authenticated frontier at fetched leaf {}",
                    entry.node_id, series.leaf_start
                )));
            }
        };
        let reconstructed = base_frontier
            .extended(&series.leaf_hashes[..prefix_len])
            .map_err(StewardError::Content)?;
        if reconstructed != *held_manifest.merkle_frontier() {
            return Err(StewardError::Content(format!(
                "v2 series node {} retained frontier does not match the fetched series prefix",
                entry.node_id
            )));
        }
        if held_manifest.payload_kind() != series.manifest.payload_kind() {
            return Err(StewardError::Content(format!(
                "v2 series node {} retained payload kind differs from the fetched series",
                entry.node_id
            )));
        }
        let mut logical_count = series
            .base_manifest
            .as_ref()
            .map_or(0, SeriesManifest::logical_count);
        let mut min_event_time = series
            .base_manifest
            .as_ref()
            .and_then(SeriesManifest::min_event_time);
        let mut max_event_time = series
            .base_manifest
            .as_ref()
            .and_then(SeriesManifest::max_event_time);
        let mut logical_attributes = series
            .base_manifest
            .as_ref()
            .and_then(SeriesManifest::logical_attributes)
            .map(<[u8]>::to_vec);
        for descriptor in series
            .packs
            .iter()
            .flat_map(|(_, pack)| pack.leaf_descriptors())
            .take(prefix_len)
        {
            logical_count = logical_count
                .checked_add(descriptor.logical_count())
                .ok_or_else(|| {
                    StewardError::Content(
                        "series retained-prefix logical count overflows u64".to_string(),
                    )
                })?;
            if let Some(value) = descriptor.min_event_time() {
                min_event_time = Some(min_event_time.map_or(value, |current| current.min(value)));
            }
            if let Some(value) = descriptor.max_event_time() {
                max_event_time = Some(max_event_time.map_or(value, |current| current.max(value)));
            }
            logical_attributes = descriptor.logical_attributes().map(<[u8]>::to_vec);
        }
        if held_manifest.logical_count() != logical_count
            || held_manifest.min_event_time() != min_event_time
            || held_manifest.max_event_time() != max_event_time
            || held_manifest.logical_attributes() != logical_attributes.as_deref()
        {
            return Err(StewardError::Content(format!(
                "v2 series node {} retained manifest metadata does not match the authenticated \
                 fetched prefix",
                entry.node_id
            )));
        }
        return Ok(held_count);
    }

    let held = target_series_leaves.get(&entry.node_id).ok_or_else(|| {
        StewardError::Content(format!(
            "v2 series node {} changed without an authenticated prior manifest",
            entry.node_id
        ))
    })?;
    if series.leaf_start != 0
        || series.leaf_hashes.len() < held.len()
        || series.leaf_hashes[..held.len()] != *held
    {
        return Err(StewardError::Content(format!(
            "v2 series node {} diverged from its previously materialized logical leaves",
            entry.node_id
        )));
    }
    Ok(held.len() as u64)
}

/// Apply an ordered plan within an open transaction, adopting source node ids.
/// Small versions write from buffered bytes; large external versions stream
/// from the remote blob store straight into the writer, never buffered (D7).
/// A v2 series consumes its exact, prevalidated suffix-payload plan and
/// independently re-verifies each suffix leaf before writing it (see
/// [`materialize_series_v2`]). Inline payloads were fetched in one exact batch
/// before the transaction opened; cache misses stream only from external blob
/// storage and never fall back to object point queries.
async fn apply_ops(
    root_node_id: &str,
    root_wd: WD,
    ops: &[ApplyOp],
    remote: &dyn ContentSource,
    graph: &FetchedGraph,
    pack_objects: &mut PreparedPackObjects,
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
            } => {
                let pwd = parent_wd(&dir_wd, parent)?;
                let mut remaining = versions.iter();
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
                    remote,
                    pack_objects,
                    *leaves_from,
                    *replicated_mtime,
                )
                .await?;
            }
        }
    }
    if !pack_objects.uses.is_empty() || !pack_objects.cache.is_empty() {
        return Err(StewardError::Content(
            "physical pack object cache retained unconsumed planned uses".to_string(),
        ));
    }
    Ok(())
}

struct PackObjectPlan {
    uses: HashMap<ObjectHash, usize>,
    expected_lengths: HashMap<ObjectHash, u64>,
    inline_bytes: u64,
}

fn pack_object_plan(ops: &[ApplyOp], graph: &FetchedGraph) -> Result<PackObjectPlan, StewardError> {
    let mut uses = HashMap::new();
    let mut expected_lengths = HashMap::new();
    let mut inline_bytes = 0u64;
    for op in ops {
        let ApplyOp::SeriesV2 {
            manifest_hash,
            leaves_from,
            ..
        } = op
        else {
            continue;
        };
        let series = series_v2(graph, *manifest_hash)?;
        for (_, pack) in &series.packs {
            if *leaves_from >= pack.leaf_end() {
                continue;
            }
            let (_, logical_prefix) = suffix_start_in_pack(pack, *leaves_from)?;
            for span in pack
                .object_spans()
                .iter()
                .filter(|span| span.logical_end() > logical_prefix)
            {
                let hash = span.object_hash();
                let physical_len = span.physical_len();
                match expected_lengths.entry(hash) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let _ = entry.insert(physical_len);
                        if physical_len
                            < u64::try_from(tlogfs::large_files::LARGE_FILE_THRESHOLD)
                                .unwrap_or(u64::MAX)
                        {
                            inline_bytes =
                                inline_bytes.checked_add(physical_len).ok_or_else(|| {
                                    StewardError::Content(
                                        "inline physical pack object size sum overflows u64"
                                            .to_string(),
                                    )
                                })?;
                        }
                    }
                    std::collections::hash_map::Entry::Occupied(entry)
                        if *entry.get() != physical_len =>
                    {
                        return Err(StewardError::Content(format!(
                            "physical pack object {hash} has conflicting declared lengths {} and \
                             {physical_len}",
                            entry.get()
                        )));
                    }
                    std::collections::hash_map::Entry::Occupied(_) => {}
                }
                let count = uses.entry(hash).or_insert(0usize);
                *count = count.checked_add(1).ok_or_else(|| {
                    StewardError::Content(
                        "physical pack object use count overflows usize".to_string(),
                    )
                })?;
            }
        }
    }
    Ok(PackObjectPlan {
        uses,
        expected_lengths,
        inline_bytes,
    })
}

fn validate_inline_prefetch_size(inline_bytes: u64, limit: u64) -> Result<(), StewardError> {
    if inline_bytes > limit {
        return Err(StewardError::Content(format!(
            "exact suffix payload batch could retain {inline_bytes} inline bytes, exceeding the \
             {limit}-byte safety limit; refusing before reading remote payloads"
        )));
    }
    Ok(())
}

/// The exact physical pack-object work remaining after every destination
/// series prefix has been validated.
///
/// `uses` bounds transaction-wide deduplication and releases each object after
/// its last consumer. `cache` starts with every inline object returned by one
/// exact [`ContentSource::get_objects`] call. A planned hash absent from that
/// batch is treated only as a possible external blob; materialization streams
/// it through [`ContentSource::get_blob_reader`] and fails if it is absent
/// there too. It never probes [`ContentSource::get_object`].
struct PreparedPackObjects {
    uses: HashMap<ObjectHash, usize>,
    inline_expected: HashSet<ObjectHash>,
    cache: HashMap<ObjectHash, VerifiedPackObject>,
}

/// Prepare suffix payloads after planning has authenticated destination
/// prefixes, but before a destination write transaction is opened.
async fn prepare_pack_objects(
    ops: &[ApplyOp],
    graph: &FetchedGraph,
    remote: &dyn ContentSource,
) -> Result<PreparedPackObjects, StewardError> {
    let plan = pack_object_plan(ops, graph)?;
    validate_inline_prefetch_size(plan.inline_bytes, MAX_INLINE_PACK_PREFETCH_BYTES)?;
    let mut hashes = Vec::new();
    let threshold = u64::try_from(tlogfs::large_files::LARGE_FILE_THRESHOLD).unwrap_or(u64::MAX);
    for hash in plan.uses.keys().copied().collect::<BTreeSet<_>>() {
        let expected = plan.expected_lengths[&hash];
        if let Some(actual) = remote.object_size(hash).await? {
            if actual != expected {
                return Err(StewardError::Content(format!(
                    "physical object {hash} has receipt length {actual}, but its pack span \
                     declares {expected}"
                )));
            }
            if actual < threshold {
                hashes.push(hash);
            }
        }
    }
    let inline_expected = hashes.iter().copied().collect::<HashSet<_>>();
    let mut cache = HashMap::new();
    if !hashes.is_empty() {
        let inline = remote
            .get_objects(&hashes)
            .await
            .map_err(|e| StewardError::Content(format!("prefetch physical pack objects: {e}")))?;
        let mut actual_inline_bytes = 0u64;
        for (hash, bytes) in inline {
            let Some(expected_len) = plan.expected_lengths.get(&hash).copied() else {
                return Err(StewardError::Content(format!(
                    "physical pack object prefetch returned unexpected key {hash}"
                )));
            };
            verify(hash, &bytes)?;
            let actual_len = u64::try_from(bytes.len()).map_err(|_| {
                StewardError::Content(format!(
                    "physical pack object {hash} length does not fit in u64"
                ))
            })?;
            if actual_len != expected_len {
                return Err(StewardError::Content(format!(
                    "physical object {hash} has {actual_len} byte(s), but its v4 pack span declares \
                     {expected_len}"
                )));
            }
            actual_inline_bytes = actual_inline_bytes.checked_add(actual_len).ok_or_else(|| {
                StewardError::Content(
                    "returned inline physical pack object size sum overflows u64".to_string(),
                )
            })?;
            let _ = cache.insert(hash, VerifiedPackObject::Memory(bytes::Bytes::from(bytes)));
        }
        validate_inline_prefetch_size(actual_inline_bytes, MAX_INLINE_PACK_PREFETCH_BYTES)?;
    }
    Ok(PreparedPackObjects {
        uses: plan.uses,
        inline_expected,
        cache,
    })
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

enum VerifiedPackObject {
    Memory(bytes::Bytes),
    File { file: std::fs::File, len: u64 },
}

impl VerifiedPackObject {
    fn len(&self) -> u64 {
        match self {
            Self::Memory(bytes) => bytes.len() as u64,
            Self::File { len, .. } => *len,
        }
    }

    fn validate_len(&self, hash: ObjectHash, expected_len: u64) -> Result<(), StewardError> {
        if self.len() != expected_len {
            return Err(StewardError::Content(format!(
                "physical object {hash} has {} byte(s), but its v4 pack span declares {expected_len}",
                self.len()
            )));
        }
        Ok(())
    }
}

async fn ensure_pack_object<'a>(
    remote: &dyn ContentSource,
    span: &PackObjectSpan,
    prepared: &'a mut PreparedPackObjects,
) -> Result<&'a VerifiedPackObject, StewardError> {
    let hash = span.object_hash();
    let expected_len = span.physical_len();
    if let std::collections::hash_map::Entry::Vacant(entry) = prepared.cache.entry(hash) {
        if prepared.inline_expected.contains(&hash) {
            return Err(StewardError::Content(format!(
                "inline physical object {hash} was omitted from the exact payload fetch"
            )));
        }
        let (file, len) = spool_external_object(remote, hash).await?;
        let object = VerifiedPackObject::File { file, len };
        let _ = entry.insert(object);
    }
    let object = prepared
        .cache
        .get(&hash)
        .expect("pack object was found or inserted above");
    object.validate_len(hash, expected_len)?;
    Ok(object)
}

fn release_pack_object(
    hash: ObjectHash,
    prepared: &mut PreparedPackObjects,
) -> Result<(), StewardError> {
    let remaining = prepared.uses.get_mut(&hash).ok_or_else(|| {
        StewardError::Content(format!(
            "physical object {hash} was fetched without a planned use"
        ))
    })?;
    *remaining = remaining.checked_sub(1).ok_or_else(|| {
        StewardError::Content(format!("physical object {hash} use count underflow"))
    })?;
    if *remaining == 0 {
        let _ = prepared.uses.remove(&hash);
        let _ = prepared.cache.remove(&hash);
    }
    Ok(())
}

fn suffix_start_in_pack(pack: &PackIndex, leaves_from: u64) -> Result<(usize, u64), StewardError> {
    let skipped_leaves = leaves_from
        .saturating_sub(pack.leaf_start())
        .min(pack.leaf_end() - pack.leaf_start());
    let skipped = usize::try_from(skipped_leaves).map_err(|_| {
        StewardError::Content("pack suffix leaf offset does not fit in usize".to_string())
    })?;
    let logical_prefix =
        pack.leaf_descriptors()[..skipped]
            .iter()
            .try_fold(0u64, |sum, descriptor| {
                sum.checked_add(descriptor.logical_count()).ok_or_else(|| {
                    StewardError::Content(
                        "pack descriptor logical prefix overflows u64 during materialization"
                            .to_string(),
                    )
                })
            })?;
    Ok((skipped, logical_prefix))
}

/// Materialize a verified `watertown.series.v3` logical series into the destination
/// as native tlogfs rows (release blocker item 1,
/// `docs/logical-series-identity-design.md`).
///
/// Writes exactly one Oplog append per logical leaf
/// (`crates/tlogfs/src/series_identity.rs`), in leaf order, skipping every
/// leaf before `leaves_from` (already held by the target) and independently
/// re-verifying every leaf about to be written against its authenticated v3
/// descriptor hash *before* handing it to a writer. Physical objects are
/// prepared in one exact inline-object batch after prefix validation, and only
/// when their logical span intersects the missing suffix. Missing inline
/// values stream from external blob storage without any object point lookup.
async fn materialize_series_v2(
    pwd: &WD,
    name: &str,
    node_id: NodeID,
    create: bool,
    entry_type: EntryType,
    series: &FetchedSeriesV2,
    remote: &dyn ContentSource,
    pack_objects: &mut PreparedPackObjects,
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
                remote,
                pack_objects,
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
                remote,
                pack_objects,
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

/// Materialize a `watertown.series.v3` `FilePhysicalSeries` whose manifest declares
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
    remote: &dyn ContentSource,
    pack_objects: &mut PreparedPackObjects,
    leaves_from: u64,
    replicated_mtime: Option<i64>,
) -> Result<(), StewardError> {
    let total_leaves = series.manifest.leaf_count();
    if total_leaves == 0 {
        return materialize_empty_series(pwd, name, node_id, create, replicated_mtime).await;
    }
    let mut leaf_index = leaves_from;
    let mut node_created = false;

    for (pack_hash, pack) in &series.packs {
        if leaves_from >= pack.leaf_end() {
            continue;
        }
        let (first_descriptor, logical_prefix) = suffix_start_in_pack(pack, leaves_from)?;
        let mut descriptors = pack.leaf_descriptors()[first_descriptor..].iter();
        let mut hasher: Option<IncrementalFileLeafHasher> = None;
        let mut buffer: Option<Vec<u8>> = None;
        let mut descriptor: Option<&PackLeafDescriptor> = None;

        for span in pack
            .object_spans()
            .iter()
            .filter(|span| span.logical_end() > logical_prefix)
        {
            let skip = logical_prefix.saturating_sub(span.logical_start());
            let object = ensure_pack_object(remote, span, pack_objects).await?;
            match object {
                VerifiedPackObject::Memory(bytes) => {
                    let start = usize::try_from(skip).map_err(|_| {
                        StewardError::Content(format!(
                            "file pack {pack_hash} prefix skip does not fit in usize"
                        ))
                    })?;
                    feed_file_chunk(
                        bytes.get(start..).ok_or_else(|| {
                            StewardError::Content(format!(
                                "file pack {pack_hash} prefix skip {skip} exceeds physical object {}",
                                span.object_hash()
                            ))
                        })?,
                        &mut descriptors,
                        pwd,
                        name,
                        node_id,
                        create,
                        &mut node_created,
                        &mut leaf_index,
                        leaves_from,
                        series.leaf_start,
                        &series.leaf_hashes,
                        &mut hasher,
                        &mut buffer,
                        &mut descriptor,
                        replicated_mtime,
                        total_leaves,
                    )
                    .await?;
                }
                VerifiedPackObject::File { file, .. } => {
                    let mut file = tokio::fs::File::from_std(file.try_clone().map_err(|e| {
                        StewardError::Content(format!(
                            "clone verified physical object {}: {e}",
                            span.object_hash()
                        ))
                    })?);
                    let _ = file
                        .seek(std::io::SeekFrom::Start(skip))
                        .await
                        .map_err(|e| {
                            StewardError::Content(format!(
                                "seek verified physical object {}: {e}",
                                span.object_hash()
                            ))
                        })?;
                    let mut buf = vec![0u8; 256 * 1024];
                    loop {
                        let n = file.read(&mut buf).await.map_err(|e| {
                            StewardError::Content(format!(
                                "read verified physical object {}: {e}",
                                span.object_hash()
                            ))
                        })?;
                        if n == 0 {
                            break;
                        }
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
                            series.leaf_start,
                            &series.leaf_hashes,
                            &mut hasher,
                            &mut buffer,
                            &mut descriptor,
                            replicated_mtime,
                            total_leaves,
                        )
                        .await?;
                    }
                }
            }
            release_pack_object(span.object_hash(), pack_objects)?;
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
    leaf_hash_start: u64,
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
            let relative = leaf_index.checked_sub(leaf_hash_start).ok_or_else(|| {
                StewardError::Content(format!(
                    "file series leaf {} precedes fetched hash range starting at {leaf_hash_start}",
                    *leaf_index
                ))
            })?;
            let idx = usize::try_from(relative).map_err(|_| {
                StewardError::Content(
                    "file series relative leaf index does not fit in usize".to_string(),
                )
            })?;
            let expected = leaf_hashes.get(idx).copied().ok_or_else(|| {
                StewardError::Content(format!(
                    "file series leaf {} has no expected hash in fetched range \
                     [{leaf_hash_start}, {})",
                    *leaf_index,
                    leaf_hash_start.saturating_add(leaf_hashes.len() as u64)
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
    remote: &dyn ContentSource,
    pack_objects: &mut PreparedPackObjects,
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
    let mut leaf_index = leaves_from;
    let mut node_created = false;

    for (pack_hash, pack) in &series.packs {
        if leaves_from >= pack.leaf_end() {
            continue;
        }
        let descriptors = pack.leaf_descriptors();
        let (mut next_descriptor, logical_prefix) = suffix_start_in_pack(pack, leaves_from)?;
        let mut current_descriptor: Option<usize> = None;
        let mut current_batches: Vec<RecordBatch> = Vec::new();
        let mut current_rows: u64 = 0;
        let mut current_schema: Option<Arc<Schema>> = None;

        for span in pack
            .object_spans()
            .iter()
            .filter(|span| span.logical_end() > logical_prefix)
        {
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
            let object = ensure_pack_object(remote, span, pack_objects).await?;
            let (schema, batches) = match object {
                VerifiedPackObject::Memory(bytes) => {
                    decode_table_object(bytes.clone(), expected_fingerprint)
                        .await
                        .map_err(|e| {
                            StewardError::Content(format!(
                                "decode physical object {}: {e}",
                                span.object_hash()
                            ))
                        })?
                }
                VerifiedPackObject::File { file, .. } => {
                    let mut file = file.try_clone().map_err(|e| {
                        StewardError::Content(format!(
                            "clone verified physical object {}: {e}",
                            span.object_hash()
                        ))
                    })?;
                    let _ = std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(0)).map_err(
                        |e| {
                            StewardError::Content(format!(
                                "rewind verified physical object {}: {e}",
                                span.object_hash()
                            ))
                        },
                    )?;
                    decode_table_object(file, expected_fingerprint)
                        .await
                        .map_err(|e| {
                            StewardError::Content(format!(
                                "decode physical object {}: {e}",
                                span.object_hash()
                            ))
                        })?
                }
            };
            let decoded_rows = batches.iter().try_fold(0u64, |sum, batch| {
                sum.checked_add(batch.num_rows() as u64).ok_or_else(|| {
                    StewardError::Content(format!(
                        "physical object {} row count overflows u64",
                        span.object_hash()
                    ))
                })
            })?;
            let expected_rows = span.logical_end() - span.logical_start();
            if decoded_rows != expected_rows {
                return Err(StewardError::Content(format!(
                    "physical object {} decoded {decoded_rows} row(s), but its v4 pack span declares {}",
                    span.object_hash(),
                    expected_rows
                )));
            }
            let canonical_schema =
                sync_store::content::canonicalize_schema(&schema).map_err(StewardError::Content)?;
            let mut rows_to_skip = logical_prefix.saturating_sub(span.logical_start());
            for mut batch in batches {
                if rows_to_skip >= batch.num_rows() as u64 {
                    rows_to_skip -= batch.num_rows() as u64;
                    continue;
                }
                if rows_to_skip > 0 {
                    let skip = usize::try_from(rows_to_skip).map_err(|_| {
                        StewardError::Content(format!(
                            "table pack {pack_hash} prefix row count does not fit in usize"
                        ))
                    })?;
                    batch = batch.slice(skip, batch.num_rows() - skip);
                    rows_to_skip = 0;
                }
                let mut columns = Vec::with_capacity(batch.num_columns());
                for (column, field) in batch.columns().iter().zip(canonical_schema.fields()) {
                    columns.push(arrow_cast::cast(column, field.data_type()).map_err(|e| {
                        StewardError::Content(format!(
                            "normalize physical object {} column {:?}: {e}",
                            span.object_hash(),
                            field.name()
                        ))
                    })?);
                }
                let normalized = RecordBatch::try_new(Arc::clone(&canonical_schema), columns)
                    .map_err(|e| {
                        StewardError::Content(format!(
                            "normalize physical object {} schema: {e}",
                            span.object_hash()
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
                    series.leaf_start,
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
            release_pack_object(span.object_hash(), pack_objects)?;
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
    leaf_hash_start: u64,
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
            let relative = leaf_index.checked_sub(leaf_hash_start).ok_or_else(|| {
                StewardError::Content(format!(
                    "table series leaf {} precedes fetched hash range starting at \
                     {leaf_hash_start}",
                    *leaf_index
                ))
            })?;
            let idx = usize::try_from(relative).map_err(|_| {
                StewardError::Content(
                    "table series relative leaf index does not fit in usize".to_string(),
                )
            })?;
            let expected = leaf_hashes.get(idx).copied().ok_or_else(|| {
                StewardError::Content(format!(
                    "table series leaf {} has no expected hash in fetched range \
                     [{leaf_hash_start}, {})",
                    *leaf_index,
                    leaf_hash_start.saturating_add(leaf_hashes.len() as u64)
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
    if !graph.blob_hashes.contains(&hash) {
        return Err(StewardError::Content(format!(
            "blob object {} missing from graph",
            hash.to_hex()
        )));
    }
    match graph.bytes.get(&hash) {
        Some(bytes) => Ok(bytes.clone()),
        None if graph.external_blobs.contains(&hash) => Err(StewardError::Content(format!(
            "object {} is a large external blob and cannot be buffered here",
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
    if !graph.blob_hashes.contains(&hash) {
        return Err(StewardError::Content(format!(
            "blob object {} missing from graph",
            hash.to_hex()
        )));
    }
    match graph.bytes.get(&hash) {
        Some(bytes) => Ok(VersionSource::Inline(bytes.clone())),
        None if graph.external_blobs.contains(&hash) => Ok(VersionSource::External(hash)),
        None => Err(StewardError::Content(format!(
            "blob object {} has no buffered or external payload",
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
    use arrow_schema::{DataType, Field};
    use sync_store::ContentRemote;
    use sync_store::content::{MerkleFrontier, PackObjectSpan, generate_range_proof};
    use sync_store::testing::in_memory_remote_url;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use uuid::Uuid;

    #[test]
    fn publication_window_requires_strictly_advancing_snapshot_bindings() {
        let pond = Uuid::new_v4();
        let make_commit = |parent, seq| {
            Commit::new(
                sync_store::content::ContentModelVersion::PublicationV2,
                ObjectHash::of_bytes(format!("tree-{seq}").as_bytes()),
                parent,
                ObjectHash::of_bytes(format!("manifest-{seq}").as_bytes()),
                sync_store::content::Provenance {
                    pond_id: pond.to_string(),
                    seq,
                    time_micros: seq,
                    author: "test".to_string(),
                    request: "test".to_string(),
                },
            )
        };
        let a = make_commit(None, 1);
        let a_hash = a.hash();
        let b = make_commit(Some(a_hash), 2);
        let b_hash = b.hash();
        let c = make_commit(Some(b_hash), 3);
        let c_hash = c.hash();
        let boundary = PublicationRecord::new(
            pond,
            "main",
            a_hash,
            a.manifest_root,
            None,
            vec![sync_store::content::ObjectDescriptor::new(
                a_hash,
                sync_store::content::ContentObjectKind::Commit,
            )],
            vec![],
            vec![],
        )
        .unwrap();
        let duplicate = PublicationRecord::new(
            pond,
            "main",
            c_hash,
            c.manifest_root,
            Some(boundary.hash()),
            vec![],
            vec![],
            vec![],
        )
        .unwrap();
        let head = PublicationRecord::new(
            pond,
            "main",
            c_hash,
            c.manifest_root,
            Some(duplicate.hash()),
            vec![sync_store::content::ObjectDescriptor::new(
                c_hash,
                sync_store::content::ContentObjectKind::Commit,
            )],
            vec![],
            vec![],
        )
        .unwrap();
        let acknowledged =
            PublicationState::new(pond, "main", a_hash, a.manifest_root, boundary.hash(), 1, 1)
                .unwrap();
        let current =
            PublicationState::new(pond, "main", c_hash, c.manifest_root, head.hash(), 3, 3)
                .unwrap();
        let graph = FetchedGraph {
            tip: Some(c_hash),
            publication_state: Some(current),
            commits: vec![(c_hash, c), (b_hash, b), (a_hash, a)],
            publication_records: vec![(head.hash(), head), (duplicate.hash(), duplicate)],
            publication_boundary: Some((boundary.hash(), boundary)),
            ..FetchedGraph::default()
        };
        let error = authenticate_publication_window(&graph, &acknowledged)
            .expect_err("two generations must not bind the same commit");
        assert!(
            error
                .to_string()
                .contains("not bound to the fetched commit ancestry")
                || error.to_string().contains("distinct commit snapshots"),
            "{error}"
        );
    }

    #[test]
    fn publication_window_rejects_inventory_shifted_between_records() {
        let pond = Uuid::new_v4();
        let root = ObjectHash::of_bytes(b"tree");
        let manifest = ObjectHash::of_bytes(b"manifest");
        let make_commit = |parent, seq, object| {
            Commit::new_with_delta(
                sync_store::content::ContentModelVersion::PublicationV2,
                root,
                parent,
                manifest,
                vec![],
                vec![sync_store::content::ObjectDescriptor::new(
                    object,
                    sync_store::content::ContentObjectKind::RawBlob,
                )],
                vec![],
                sync_store::content::Provenance {
                    pond_id: pond.to_string(),
                    seq,
                    time_micros: seq,
                    author: "test".to_string(),
                    request: "test".to_string(),
                },
            )
            .unwrap()
        };
        let object_a = ObjectHash::of_bytes(b"a");
        let object_b = ObjectHash::of_bytes(b"b");
        let object_c = ObjectHash::of_bytes(b"c");
        let a = make_commit(None, 1, object_a);
        let a_hash = a.hash();
        let b = make_commit(Some(a_hash), 2, object_b);
        let b_hash = b.hash();
        let c = make_commit(Some(b_hash), 3, object_c);
        let c_hash = c.hash();
        let boundary = PublicationRecord::new(
            pond,
            "main",
            a_hash,
            manifest,
            None,
            vec![
                sync_store::content::ObjectDescriptor::new(
                    a_hash,
                    sync_store::content::ContentObjectKind::Commit,
                ),
                sync_store::content::ObjectDescriptor::new(
                    object_a,
                    sync_store::content::ContentObjectKind::RawBlob,
                ),
            ],
            vec![],
            vec![],
        )
        .unwrap();
        let b_record = PublicationRecord::new(
            pond,
            "main",
            b_hash,
            manifest,
            Some(boundary.hash()),
            vec![sync_store::content::ObjectDescriptor::new(
                b_hash,
                sync_store::content::ContentObjectKind::Commit,
            )],
            vec![],
            vec![],
        )
        .unwrap();
        let c_record = PublicationRecord::new(
            pond,
            "main",
            c_hash,
            manifest,
            Some(b_record.hash()),
            vec![
                sync_store::content::ObjectDescriptor::new(
                    c_hash,
                    sync_store::content::ContentObjectKind::Commit,
                ),
                sync_store::content::ObjectDescriptor::new(
                    object_b,
                    sync_store::content::ContentObjectKind::RawBlob,
                ),
                sync_store::content::ObjectDescriptor::new(
                    object_c,
                    sync_store::content::ContentObjectKind::RawBlob,
                ),
            ],
            vec![],
            vec![],
        )
        .unwrap();
        let acknowledged =
            PublicationState::new(pond, "main", a_hash, manifest, boundary.hash(), 1, 1).unwrap();
        let current =
            PublicationState::new(pond, "main", c_hash, manifest, c_record.hash(), 3, 3).unwrap();
        let graph = FetchedGraph {
            tip: Some(c_hash),
            publication_state: Some(current),
            commits: vec![(c_hash, c), (b_hash, b), (a_hash, a)],
            publication_records: vec![(c_record.hash(), c_record), (b_record.hash(), b_record)],
            publication_boundary: Some((boundary.hash(), boundary)),
            ..FetchedGraph::default()
        };
        let error = authenticate_publication_window(&graph, &acknowledged)
            .expect_err("aggregate-equivalent shifted inventory must fail");
        assert!(error.to_string().contains("object inventory"), "{error}");
    }

    #[test]
    fn pack_construction_rejects_object_crossing_schema_transition() {
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
            MerkleFrontier::from_leaves(&leaves),
        )
        .expect("manifest");
        let descriptors = vec![
            PackLeafDescriptor::new_with_leaf_hash_and_schema(
                leaves[0],
                1,
                Some(fingerprint_a),
                None,
                None,
                None,
            )
            .expect("descriptor a"),
            PackLeafDescriptor::new_with_leaf_hash_and_schema(
                leaves[1],
                1,
                Some(fingerprint_b),
                None,
                None,
                None,
            )
            .expect("descriptor b"),
        ];
        let err = PackIndex::new_with_spans(
            manifest.hash(),
            0,
            2,
            2,
            manifest.leaf_merkle_root(),
            generate_range_proof(&leaves, 0, 2).expect("proof"),
            vec![
                PackObjectSpan::new(ObjectHash::of_bytes(b"object"), 0, 2, 0, 100)
                    .expect("object span"),
            ],
            2,
            100,
            descriptors,
        )
        .expect_err("one physical object must not cross the transition");
        assert!(
            err.contains("crosses a schema-fingerprint transition"),
            "{err}"
        );
    }

    #[test]
    fn external_pack_object_size_mismatch_fails_closed() {
        let hash = ObjectHash::of_bytes(b"external payload");
        let object = VerifiedPackObject::File {
            file: tempfile::tempfile().expect("spool file"),
            len: 8,
        };
        let error = object
            .validate_len(hash, 9)
            .expect_err("declared span length mismatch must fail");
        assert!(error.to_string().contains("v4 pack span declares 9"));
    }

    #[test]
    fn inline_pack_prefetch_size_limit_is_explicit() {
        validate_inline_prefetch_size(
            MAX_INLINE_PACK_PREFETCH_BYTES,
            MAX_INLINE_PACK_PREFETCH_BYTES,
        )
        .expect("the exact limit is accepted");
        let error = validate_inline_prefetch_size(
            MAX_INLINE_PACK_PREFETCH_BYTES + 1,
            MAX_INLINE_PACK_PREFETCH_BYTES,
        )
        .expect_err("one byte over the limit must fail");
        assert!(
            error
                .to_string()
                .contains("refusing before reading remote payloads")
        );
    }

    fn cost_series_node_id() -> NodeID {
        NodeID::from_hex_string("00000000-0000-7700-8000-000000000777")
            .expect("cost series node id")
    }

    async fn seed_cost_series(ship: &mut Ship, leaf_count: usize) {
        assert!(leaf_count > 0);
        let tx = ship
            .begin_write(&PondUserMetadata::new(vec!["seed-cost-series".to_string()]))
            .await
            .expect("begin seed transaction");
        let root = tx.root().await.expect("seed root");
        let first = vec![b'a'];
        let mut writer = root
            .create_file_with_id("observations.series", cost_series_node_id())
            .await
            .expect("create seeded series");
        writer.set_mtime(0);
        writer.write_all(&first).await.expect("write first leaf");
        writer.shutdown().await.expect("close first leaf");

        let file_id = root
            .get_node_path("/observations.series")
            .await
            .expect("seeded series path")
            .id();
        let mut cumulative = first.clone();
        let mut hash_state = tlogfs::bao_outboard::IncrementalHashState::new();
        hash_state.ingest(&first);
        let mut outboard =
            tlogfs::bao_outboard::SeriesOutboard::from_first_version_state(&hash_state, 1);
        for index in 1..leaf_count {
            let byte = vec![b'a' + u8::try_from(index % 26).expect("modulo fits")];
            let pending_start = cumulative.len() / tlogfs::bao_outboard::BLOCK_SIZE
                * tlogfs::bao_outboard::BLOCK_SIZE;
            outboard = tlogfs::bao_outboard::SeriesOutboard::append_version(
                &outboard,
                &cumulative[pending_start..],
                &byte,
            );
            cumulative.extend_from_slice(&byte);
            tx.state()
                .expect("seed state")
                .store_file_content_ref(
                    file_id,
                    tlogfs::file_writer::ContentRef::Small(byte),
                    tlogfs::file_writer::FileMetadata::Data,
                    Some(i64::try_from(index + 1).expect("version fits")),
                    Some(outboard.to_bytes()),
                    None,
                    Some(i64::try_from(index).expect("mtime fits")),
                    None,
                )
                .await
                .expect("store seeded leaf");
        }
        _ = tx.commit().await.expect("commit seeded series");
    }

    async fn append_cost_series(ship: &mut Ship, byte: u8) {
        ship.write_transaction(
            &PondUserMetadata::new(vec!["append-cost-series".to_string()]),
            async move |transaction| {
                let root = transaction.root().await?;
                let mut writer = root
                    .async_writer_path_with_type(
                        "/observations.series",
                        EntryType::FilePhysicalSeries,
                    )
                    .await?;
                writer.write_all(&[byte]).await?;
                writer.shutdown().await?;
                Ok(())
            },
        )
        .await
        .expect("append cost series");
    }

    async fn write_cost_file(ship: &mut Ship) {
        ship.write_transaction(
            &PondUserMetadata::new(vec!["write-cost-file".to_string()]),
            async move |transaction| {
                let root = transaction.root().await?;
                let mut writer = root
                    .async_writer_path_with_type("/changed.txt", EntryType::FilePhysicalVersion)
                    .await?;
                writer.write_all(b"changed").await?;
                writer.shutdown().await?;
                Ok(())
            },
        )
        .await
        .expect("write cost file");
    }

    async fn cost_fixture(
        label: &str,
        leaf_count: usize,
    ) -> (
        tempfile::TempDir,
        tempfile::TempDir,
        Ship,
        Ship,
        ContentRemote,
    ) {
        let producer_dir = tempdir().expect("producer tempdir");
        let consumer_dir = tempdir().expect("consumer tempdir");
        let mut producer = Ship::create_pond(producer_dir.path().join("producer"), label)
            .await
            .expect("create producer");
        let mut consumer = Ship::create_pond(consumer_dir.path().join("consumer"), label)
            .await
            .expect("create consumer");
        seed_cost_series(&mut producer, leaf_count).await;
        seed_cost_series(&mut consumer, leaf_count).await;
        assert_eq!(
            crate::compute_content_tree(&producer)
                .await
                .expect("producer root")
                .root_tree_hash,
            crate::compute_content_tree(&consumer)
                .await
                .expect("consumer root")
                .root_tree_hash
        );
        let url = in_memory_remote_url(&format!("{label}-{}", Uuid::new_v4()));
        let mut remote = ContentRemote::create_at_url(
            &url,
            producer.control_table().pond_id_uuid(),
            HashMap::new(),
        )
        .await
        .expect("create remote");
        let _ = crate::push_content_to_remote(&producer, &mut remote, "main")
            .await
            .expect("publish baseline");
        (producer_dir, consumer_dir, producer, consumer, remote)
    }

    #[tokio::test]
    async fn unrelated_file_apply_does_not_scan_thousand_leaf_series() {
        let (_producer_dir, _consumer_dir, mut producer, mut consumer, mut remote) =
            cost_fixture("local-cost-unrelated", 1_000).await;
        let baseline = remote
            .current_publication("main")
            .await
            .expect("read baseline")
            .expect("baseline state");
        write_cost_file(&mut producer).await;
        let _ = crate::push_content_to_remote(&producer, &mut remote, "main")
            .await
            .expect("publish file change");
        let graph = fetch_object_graph_since(&remote, "main", Some(baseline.snapshot_tip))
            .await
            .expect("fetch file delta");
        assert!(
            graph
                .objects
                .values()
                .all(|object| !matches!(object, FetchedObject::SeriesV2(_)))
        );
        let outcome = rebuild_pond(&mut consumer, &remote, &graph)
            .await
            .expect("apply file delta");
        assert!(!outcome.local_cost.full_target_scan);
        assert_eq!(outcome.local_cost.manifest_root_cursor_files_read, 1);
        assert_eq!(outcome.local_cost.manifest_root_rows_read, 0);
        assert_eq!(outcome.local_cost.series_manifests_read, 0);
        assert_eq!(outcome.local_cost.retained_leaf_hashes_scanned, 0);
        assert!(outcome.local_cost.manifest_records_loaded <= 3);
        assert!(outcome.local_cost.manifest_nodes_read <= 8);
    }

    async fn one_leaf_append_cost(prior_leaves: usize) -> LocalPullCost {
        let label = format!("local-cost-series-{prior_leaves}");
        let (_producer_dir, _consumer_dir, mut producer, mut consumer, mut remote) =
            cost_fixture(&label, prior_leaves).await;
        let baseline = remote
            .current_publication("main")
            .await
            .expect("read baseline")
            .expect("baseline state");
        append_cost_series(&mut producer, b'z').await;
        let _ = crate::push_content_to_remote(&producer, &mut remote, "main")
            .await
            .expect("publish suffix");
        let graph = fetch_object_graph_since(&remote, "main", Some(baseline.snapshot_tip))
            .await
            .expect("fetch suffix");
        let series = graph
            .objects
            .values()
            .find_map(|object| match object {
                FetchedObject::SeriesV2(series) => Some(series),
                _ => None,
            })
            .expect("fetched series");
        assert_eq!(series.leaf_start, prior_leaves as u64);
        assert_eq!(series.leaf_hashes.len(), 1);
        rebuild_pond(&mut consumer, &remote, &graph)
            .await
            .expect("apply suffix")
            .local_cost
    }

    #[tokio::test]
    async fn missing_manifest_cursor_recovers_with_one_latest_index_row() {
        let (_producer_dir, _consumer_dir, mut producer, mut consumer, mut remote) =
            cost_fixture("local-cost-cursor-recovery", 100).await;
        let baseline = remote
            .current_publication("main")
            .await
            .expect("read baseline")
            .expect("baseline state");
        let cursor = crate::get_data_path(consumer.pond_path())
            .join("_content/v2/state/manifest-root")
            .join(format!("pond={}", consumer.control_table().pond_id_uuid()));
        std::fs::remove_file(cursor).expect("remove local manifest cursor");

        append_cost_series(&mut producer, b'z').await;
        let _ = crate::push_content_to_remote(&producer, &mut remote, "main")
            .await
            .expect("publish suffix");
        let graph = fetch_object_graph_since(&remote, "main", Some(baseline.snapshot_tip))
            .await
            .expect("fetch suffix");
        let cost = rebuild_pond(&mut consumer, &remote, &graph)
            .await
            .expect("apply suffix")
            .local_cost;
        assert!(!cost.full_target_scan);
        assert_eq!(cost.manifest_root_cursor_files_read, 0);
        assert_eq!(cost.manifest_root_rows_read, 1);
        assert_eq!(cost.retained_leaf_hashes_scanned, 0);
    }

    #[tokio::test]
    async fn one_leaf_append_local_cost_is_fixed_after_1_100_1000_leaves() {
        let one = one_leaf_append_cost(1).await;
        let hundred = one_leaf_append_cost(100).await;
        let thousand = one_leaf_append_cost(1_000).await;
        for cost in [one, hundred, thousand] {
            assert!(!cost.full_target_scan, "{cost:?}");
            assert_eq!(cost.manifest_root_cursor_files_read, 1, "{cost:?}");
            assert_eq!(cost.manifest_root_rows_read, 0, "{cost:?}");
            assert_eq!(cost.series_manifests_read, 1, "{cost:?}");
            assert_eq!(cost.retained_leaf_hashes_scanned, 0, "{cost:?}");
            assert!(cost.manifest_records_loaded <= 3, "{cost:?}");
            assert!(cost.manifest_nodes_read <= 8, "{cost:?}");
        }
        assert_eq!(one, hundred);
        assert_eq!(one, thousand);
    }
}
