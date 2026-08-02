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

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use crate::content_source::ContentSource;
use sync_store::content::{
    Commit, ManifestEntry, ObjectHash, TreeEntry, VersionMeta, decode_manifest, decode_recipe,
    decode_series, decode_tree,
};
use tinyfs::{EntryType, NodeID, WD};
use tlogfs::PondUserMetadata;
use tokio::io::AsyncWriteExt;

use crate::{Ship, StewardError};

/// A fetched content object, in the structured form a rebuild needs, alongside
/// its exact bytes (kept so the rebuild can write file content and re-verify).
#[derive(Debug, Clone)]
pub enum FetchedObject {
    /// A directory: its decoded, canonical-order entries.
    Tree(Vec<TreeEntry>),
    /// A leaf blob: a file version's bytes, a symlink target, or recipe bytes.
    Blob(Vec<u8>),
    /// A multi-version series: its ordered version blob hashes.
    Series(Vec<ObjectHash>),
    /// A large leaf blob that lives out-of-row in the remote blob store and is
    /// deliberately *not* buffered (Decision D7).  Its bytes are streamed from
    /// the remote straight into the local writer at rebuild time, keyed by this
    /// object's hash; only its presence is recorded here.
    External,
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
        fetch_tree(remote, root, &mut graph).await?;
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
                    fetch_series(remote, entry.child_hash, graph).await?;
                }
                EntryType::FilePhysicalVersion
                | EntryType::TablePhysicalVersion
                | EntryType::Symlink
                | EntryType::DirectoryDynamic
                | EntryType::FileDynamic
                | EntryType::TableDynamic => {
                    fetch_blob(remote, entry.child_hash, graph).await?;
                }
            }
        }
    }
    Ok(())
}

/// Fetch a series object and its version blobs.
async fn fetch_series(
    remote: &dyn ContentSource,
    series_hash: ObjectHash,
    graph: &mut FetchedGraph,
) -> Result<(), StewardError> {
    if graph.objects.contains_key(&series_hash) {
        return Ok(());
    }
    let bytes = fetch_verified(remote, series_hash).await?;
    let versions =
        decode_series(&bytes).map_err(|e| StewardError::Content(format!("decode series: {e}")))?;
    let _ = graph
        .objects
        .insert(series_hash, FetchedObject::Series(versions.clone()));
    let _ = graph.bytes.insert(series_hash, bytes);
    for version in versions {
        fetch_blob(remote, version, graph).await?;
    }
    Ok(())
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

    let (target_nodes, target_series) = crate::content_tree::build_target_state(target).await?;

    // Reject a manifest that is inconsistent with the fetched tree closure
    // before any mutation, so a hostile/corrupt remote cannot commit an
    // inconsistent tree that the post-apply fold would only catch after commit.
    verify_manifest_matches_tree(graph)?;

    let (ops, outcome) = plan_node_diff(graph, root, &target_nodes, &target_series)?;

    let root_node_id = src_root_id(graph)?.to_string();
    target
        .write_transaction(
            &PondUserMetadata::new(vec!["pull".to_string()]),
            async move |fs| {
                let root_wd = fs.root().await?;
                apply_ops(&root_node_id, root_wd, &ops, remote).await?;
                Ok(())
            },
        )
        .await?;

    let report = crate::compute_content_tree(target).await?;
    if report.root_tree_hash != root {
        return Err(StewardError::Content(format!(
            "rebuilt pond folds to {} but the tip root tree is {}",
            report.root_tree_hash.to_hex(),
            root.to_hex()
        )));
    }

    let pond_id = target.data_persistence().pond_id().to_string();
    let table = target.data_persistence().table().clone();
    let roots = crate::content_tree::compute_commit_roots_for_table(table, &pond_id).await?;
    if roots.node_manifest_hash != tip_manifest_hash {
        return Err(StewardError::Content(format!(
            "rebuilt node manifest hashes to {} but the tip commit's manifest is {}",
            roots.node_manifest_hash.to_hex(),
            tip_manifest_hash.to_hex()
        )));
    }
    if roots.node_manifest_root != tip_manifest_root {
        return Err(StewardError::Content(format!(
            "rebuilt node manifest Merkle root is {} but the tip commit's root is {}",
            roots.node_manifest_root.to_hex(),
            tip_manifest_root.to_hex()
        )));
    }

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
    let (target_nodes, target_series) =
        crate::content_tree::build_target_state_for_pond(target, &foreign_id).await?;

    // Reject a manifest that is inconsistent with the fetched tree closure
    // before any mutation (see verify_manifest_matches_tree).
    verify_manifest_matches_tree(graph)?;

    let (ops, outcome) = plan_node_diff(graph, root, &target_nodes, &target_series)?;

    let root_node_id = src_root_id(graph)?.to_string();
    let first_import = target_nodes.is_empty();
    target
        .write_transaction(
            &PondUserMetadata::new(vec!["pull".to_string(), "import".to_string()]),
            async move |fs| {
                if first_import {
                    fs.initialize_foreign_root(foreign_pond_id).await?;
                }
                let foreign_node = fs.foreign_root_node(foreign_pond_id).await?;
                let foreign_np = tinyfs::NodePath {
                    node: foreign_node,
                    path: "/".into(),
                };
                let root_wd = fs.wd(&foreign_np, foreign_np.clone()).await?;
                apply_ops(&root_node_id, root_wd, &ops, remote).await?;
                Ok(())
            },
        )
        .await?;

    let table = target.data_persistence().table().clone();
    let report =
        crate::content_tree::compute_content_tree_for_table(table.clone(), &foreign_id).await?;
    if report.root_tree_hash != root {
        return Err(StewardError::Content(format!(
            "imported foreign tree folds to {} but the tip root tree is {}",
            report.root_tree_hash.to_hex(),
            root.to_hex()
        )));
    }

    let roots = crate::content_tree::compute_commit_roots_for_table(table, &foreign_id).await?;
    if roots.node_manifest_hash != tip_manifest_hash {
        return Err(StewardError::Content(format!(
            "imported node manifest hashes to {} but the tip commit's manifest is {}",
            roots.node_manifest_hash.to_hex(),
            tip_manifest_hash.to_hex()
        )));
    }
    if roots.node_manifest_root != tip_manifest_root {
        return Err(StewardError::Content(format!(
            "imported node manifest Merkle root is {} but the tip commit's root is {}",
            roots.node_manifest_root.to_hex(),
            tip_manifest_root.to_hex()
        )));
    }

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
fn emit_collision_safe_renames(parent: &str, intents: Vec<RenameIntent>, ops: &mut Vec<ApplyOp>) {
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
        let temp = format!(".pull-rename-tmp-{}", intent.node_id);
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
fn plan_node_diff(
    graph: &FetchedGraph,
    root: ObjectHash,
    target_nodes: &HashMap<String, ManifestEntry>,
    target_series: &HashMap<String, Vec<ObjectHash>>,
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
        emit_collision_safe_renames(parent_id, renames, &mut ops);

        for entry in kids {
            plan_one(
                entry,
                graph,
                target_nodes,
                target_series,
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
    target_series: &HashMap<String, Vec<ObjectHash>>,
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
            let versions = if content_changed {
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
            let (versions, collapse_first) =
                plan_series_versions(entry, graph, target_series, existing.map(|t| t.child_hash))?;
            ops.push(ApplyOp::File {
                parent: entry.parent_node_id.clone(),
                name: entry.name.clone(),
                node_id: entry.node_id.clone(),
                create,
                entry_type: entry.entry_type,
                versions,
                collapse_first,
            });
        }
        EntryType::Symlink => {
            if create {
                outcome.symlinks += 1;
            }
            if create || content_changed {
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
            if create || content_changed {
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

/// Decide which series version blobs to write and whether the first replaces the
/// versions already held.
///
/// The common case is append-only: the versions the target holds are a prefix of
/// the incoming list, and only the appended suffix is written (Section 8.5.3).
/// But a source-side compaction (`pond maintain --collapse-versions`) legitimately
/// *replaces* many superseded versions with a single merged version, so the
/// incoming list is no longer a prefix-extension of what a caught-up mirror holds.
/// That case is replicated by rewriting the full incoming list with the first
/// version marked to collapse the held ones (`collapse_first = true`), using the
/// source's own merged bytes -- the pre-collapse versions are gone from the source
/// and cannot be re-fetched, so the mirror must adopt the merged version directly.
fn plan_series_versions(
    entry: &ManifestEntry,
    graph: &FetchedGraph,
    target_series: &HashMap<String, Vec<ObjectHash>>,
    existing_child_hash: Option<ObjectHash>,
) -> Result<(Vec<PlannedVersion>, bool), StewardError> {
    let incoming = series_versions(graph, entry.child_hash)?;

    // The manifest entry carries one VersionMeta per live version, in the same
    // order as the series object's hashes. A mismatch means the two objects
    // disagree about the node's shape, so the metadata cannot be aligned with
    // the bytes it describes; rebuilding from it would silently attach the
    // wrong bounds to a version.
    if !entry.versions.is_empty() && entry.versions.len() != incoming.len() {
        return Err(StewardError::Content(format!(
            "series node {} has {} version(s) but its entry carries {} metadata record(s)",
            entry.node_id,
            incoming.len(),
            entry.versions.len()
        )));
    }
    let meta_at = |index: usize| entry.versions.get(index);

    let held = match existing_child_hash {
        None => &[][..],
        Some(child_hash) if child_hash == entry.child_hash => return Ok((Vec::new(), false)),
        Some(_) => target_series
            .get(&entry.node_id)
            .map(Vec::as_slice)
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "series node {} changed but its current versions are unknown",
                    entry.node_id
                ))
            })?,
    };

    // Append-only fast path: the incoming list extends what is already held, so
    // write only the missing suffix.
    if incoming.len() >= held.len() && incoming[..held.len()] == *held {
        let suffix = incoming[held.len()..]
            .iter()
            .enumerate()
            .map(|(offset, hash)| planned_version(graph, *hash, meta_at(held.len() + offset)))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok((suffix, false));
    }

    // Divergent history: the held versions are no longer a prefix of the source's
    // live series (a compaction replaced them). Rewrite the full incoming list;
    // the first version collapses everything the target held.
    let full = incoming
        .iter()
        .enumerate()
        .map(|(index, hash)| planned_version(graph, *hash, meta_at(index)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((full, true))
}

/// Apply an ordered plan within an open transaction, adopting source node ids.
/// Small versions write from buffered bytes; large external versions stream from
/// the remote blob store straight into the writer, never buffered (D7).
async fn apply_ops(
    root_node_id: &str,
    root_wd: WD,
    ops: &[ApplyOp],
    remote: &dyn ContentSource,
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
                        write_version(writer, first, *entry_type, remote).await?;
                    }
                } else if *collapse_first && let Some(first) = remaining.next() {
                    // Replicate a source-side compaction: the first version
                    // starts a fresh baseline and supersedes every version the
                    // target already held, so its fold matches the source.
                    let writer = pwd
                        .async_writer_path_collapsing_with_type(name, *entry_type)
                        .await?;
                    write_version(writer, first, *entry_type, remote).await?;
                }
                for version in remaining {
                    let writer = pwd.async_writer_path_with_type(name, *entry_type).await?;
                    write_version(writer, version, *entry_type, remote).await?;
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
        }
    }
    Ok(())
}

/// Write one file/series version through `writer`, then finalize it.  An inline
/// version copies buffered bytes; an external version streams from the remote
/// blob store in bounded chunks, re-hashing to enforce content addressing so a
/// large blob never lands in a single buffer (D7).
async fn write_version(
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
    finalize_writer(writer, entry_type, &version.meta).await
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
async fn finalize_writer(
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

/// Resolve a series object to its ordered versions (blob hash plus metadata).
fn series_versions(
    graph: &FetchedGraph,
    series_hash: ObjectHash,
) -> Result<&[ObjectHash], StewardError> {
    match graph.objects.get(&series_hash) {
        Some(FetchedObject::Series(versions)) => Ok(versions),
        Some(_) => Err(StewardError::Content(format!(
            "expected a series at {} but found a non-series object",
            series_hash.to_hex()
        ))),
        None => Err(StewardError::Content(format!(
            "series object {} missing from graph",
            series_hash.to_hex()
        ))),
    }
}
