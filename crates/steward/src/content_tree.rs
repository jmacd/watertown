// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Read-side content-tree computation (the SPACE layer over live state).
//!
//! This module reads a pond's live filesystem and folds it into a single
//! `root_tree_hash` using the content-addressed object model from
//! [`sync_store::content`].  It is the read-only counterpart to the
//! commit-time fold described in `docs/content-addressed-pond-design.md`
//! Section 5: it proves the object model against real ponds and answers the
//! comparison question (Goal 2) -- two ponds (or two subtrees) are identical
//! iff their tree hashes match -- without persisting anything.
//!
//! # How it reads live state
//!
//! Like [`crate::fsck`], it scans the data table once.  A directory's *latest*
//! `OplogEntry` row stores its complete live entry set (Arrow IPC of
//! [`tlogfs::DirectoryEntry`]), so the current tree is reconstructed directly
//! from the latest row per node with no operation replay.  The fold then runs
//! bottom-up from the local pond's root.
//!
//! # `child_hash` by node kind (design Section 9)
//!
//! | Node kind                                   | `child_hash`                          |
//! |---------------------------------------------|---------------------------------------|
//! | physical directory                          | recursive [`tree_hash`]               |
//! | physical file / table (single version)      | the version blob hash (`blake3`)      |
//! | physical series (multi-version)             | [`series_hash`] over version blobs    |
//! | symlink                                     | `blake3(target bytes)`                |
//! | dynamic dir / file / `table:dynamic`        | [`recipe_hash`] (factory + config)    |
//!
//! Dynamic nodes hash their stored definition (factory type plus config), not
//! their computed output, and their generated children are not folded in.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use datafusion::execution::context::SessionContext;

use sync_store::content::{
    Commit, ManifestEntry, ObjectHash, Provenance, TreeEntry, decode_manifest, encode_manifest,
    encode_recipe, encode_series, encode_tree, manifest_hash, node_merkle_rebuild_root,
    recipe_hash, series_hash,
};
use tinyfs::{EntryType, ROOT_UUID};
use tlogfs::schema::{CollapseRange, OplogEntry, decode_directory_entries, live_series_versions};

use crate::control_table::CommitSpine;
use crate::{Ship, StewardError};

/// Reconcile a pond's transparency-log tiles with its committed leaf sequence
/// and re-emit the checkpoint (design Decision D5/D9).
///
/// The authoritative leaf sequence is the pond-resident commit-log node's
/// ordered `commit_object` bytes; the tile log is a derived, re-materializable
/// export.  The writer drives its next leaf position from the committed leaf
/// count, replaying every leaf the export is missing in commit order, so a
/// dropped append self-heals on the next commit.
///
/// Failures are logged and swallowed: the transparency log is a derived
/// publishing artifact and must not unwind an already-committed transaction.
/// Shared by the guard's write commit and [`crate::Ship::compact`].
pub(crate) async fn materialize_tlog(
    pond_path: &std::path::Path,
    table: deltalake::DeltaTable,
    pond_id: uuid::Uuid,
) {
    let dir = crate::get_tlog_path(pond_path);
    let origin = format!("watertown/{pond_id}");
    let log = sync_store::TileLog::new(dir, origin);

    // Decision D9: the authoritative leaf sequence is the pond-resident commit
    // log node, not the disposable control table.  Each log-node version holds
    // one encoded commit object; the tile export is reconciled against them.
    let leaves = match read_log_leaves(table, &pond_id.to_string()).await {
        Ok(l) => l,
        Err(e) => {
            log::error!("failed to read transparency-log leaf sequence: {e}");
            return;
        }
    };

    let exported = match log.size() {
        Ok(n) => n as usize,
        Err(e) => {
            log::error!("failed to read transparency-log checkpoint size: {e}");
            return;
        }
    };

    if exported >= leaves.len() {
        return;
    }

    let missing: Vec<Vec<u8>> = leaves[exported..].to_vec();

    match log.append_leaf_data(missing) {
        Ok(checkpoint) => log::debug!(
            "transparency log checkpoint emitted (size={}, root={})",
            checkpoint.size,
            checkpoint.root.to_hex()
        ),
        Err(e) => log::error!("failed to materialize transparency-log tiles: {e}"),
    }
}

/// Result of a [`compute_content_tree`] run.
#[derive(Debug, Clone)]
pub struct ContentTreeReport {
    /// The content hash of the local pond's root directory tree.  Equal roots
    /// mean byte-identical content across the whole pond.
    pub root_tree_hash: ObjectHash,
    /// Number of distinct nodes folded into the root.
    pub nodes_hashed: usize,
}

/// The materialized content objects reachable from a pond's root tree.
///
/// Produced by [`materialize_content_objects`].  Per Decision D7 the objects
/// split by where their bytes live: small objects (trees, series manifests,
/// symlinks, recipes, and small blobs) carry their bytes inline and become
/// `objects` rows in a push; large blobs carry only their hash and transfer
/// via the external `_large_files` path.  Both are keyed by the same BLAKE3
/// hash, so reachability and dedup are uniform.
///
/// The node manifest (Section 4.5) is also included inline, since the commit
/// references it by hash and a consumer must fetch it to adopt the source's
/// node_ids.  Commit objects are NOT included here -- they are produced by the
/// commit path and added by the push layer on top of this closure.
#[derive(Debug, Clone, Default)]
pub struct MaterializedObjects {
    /// Objects whose bytes are carried inline, keyed by content hash.  These are
    /// pure content (trees, series, symlinks, recipes, small blobs) and so
    /// dedup across ponds; identity-bearing objects are kept out (see
    /// `manifest`).
    pub inline: BTreeMap<ObjectHash, Vec<u8>>,
    /// Large-blob hashes whose bytes transfer via the external path.
    pub external_blobs: BTreeSet<ObjectHash>,
    /// The node manifest object: its hash and bytes (Section 4.5).  Kept
    /// separate from `inline` because it carries the source's node_ids, so it
    /// is pond-specific and must not be counted as shareable content -- two
    /// ponds with identical content still have different manifests.  `None`
    /// only on a default-constructed value; a real fold always produces one.
    pub manifest: Option<(ObjectHash, Vec<u8>)>,
}

impl MaterializedObjects {
    /// Record an inline object (idempotent: re-recording a hash is a no-op).
    fn put_inline(&mut self, hash: ObjectHash, bytes: Vec<u8>) {
        let _ = self.inline.entry(hash).or_insert(bytes);
    }

    /// Record a large blob to transfer externally by hash.
    fn put_external(&mut self, hash: ObjectHash) {
        let _ = self.external_blobs.insert(hash);
    }

    /// Total number of distinct objects (inline, external, and the manifest).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inline.len() + self.external_blobs.len() + usize::from(self.manifest.is_some())
    }

    /// True when no objects were materialized.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inline.is_empty() && self.external_blobs.is_empty() && self.manifest.is_none()
    }
}

/// One child entry of a directory, captured during the fold so that a later
/// comparison can descend by `child_hash` without re-reading the data table.
#[derive(Debug, Clone)]
pub(crate) struct ChildRef {
    /// The entry name within its parent directory.
    pub name: String,
    /// The entry kind (drives how the child contributes its `child_hash`).
    pub entry_type: EntryType,
    /// The content hash this child contributes to its parent's tree hash.
    pub child_hash: ObjectHash,
    /// The child node's own `node_id`, captured so the node manifest can record
    /// identity alongside the content tree (Section 4.5).
    pub child_node_id: String,
    /// The child directory's node key, present only when the child is a
    /// physical directory (the only kind a diff can descend into).
    pub child_dir_key: Option<NodeKey>,
}

/// An in-memory index of a pond's content tree: the root hash plus, for every
/// physical directory node, its sorted child entries with their child hashes.
///
/// Built once per pond by [`build_content_tree_for_table`]; consumed by the
/// content-tree comparison in [`crate::content_diff`], which walks two indices
/// top-down and prunes any subtree whose `child_hash` already matches.
pub(crate) struct ContentTreeIndex {
    /// The local pond's root directory tree hash.
    pub root_tree_hash: ObjectHash,
    /// The node key of the local pond's root directory.
    pub root_key: NodeKey,
    /// Per physical-directory child lists, in name order.
    pub dirs: HashMap<NodeKey, Vec<ChildRef>>,
    /// Per series node, its ordered version blob hashes (ascending version).
    /// Lets an incremental rebuild compute the suffix it must append to a
    /// series it already holds (design Section 8.5.3).
    pub series_versions: HashMap<NodeKey, Vec<ObjectHash>>,
    /// Number of distinct nodes folded into the root.
    pub nodes_hashed: usize,
}

/// Composite identity of a node within the data table: `(pond_id, node_id)`.
///
/// Keying by both keeps cross-pond imports correct, because every pond's root
/// shares the same well-known `node_id` and would otherwise collide.
pub(crate) type NodeKey = (String, String);

/// The latest-version facts about one node needed to hash it.
struct NodeFacts {
    /// Directory entry bytes (for directories) or the node's content (for
    /// inline files, symlink targets, and dynamic config).  `None` when the
    /// content is externalized (large file) or empty.
    content: Option<Vec<u8>>,
    /// The recorded `blake3` of this version, if any.
    blake3: Option<String>,
    /// The factory type for a dynamic node (`None` for physical nodes).  Folded
    /// into the recipe hash so the content commits to the factory, not just its
    /// config (Decision D4).
    factory: Option<String>,
}

/// One version of a series: its blob hash plus the inline bytes when the
/// version is small enough to be stored in-row.  `content` is `None` for an
/// externalized (large) version, whose bytes transfer via the external path.
struct VersionBlob {
    hash: ObjectHash,
    content: Option<Vec<u8>>,
}

/// Compute the local pond's `root_tree_hash` from its live filesystem state.
///
/// Reads the data table once, reconstructs the current tree, and folds it
/// bottom-up.  Pure and side-effect free.
///
/// # Errors
///
/// Returns an error if the data table cannot be read, if the local pond has no
/// root directory row, if a referenced child node is missing, or if directory
/// content cannot be decoded.
pub async fn compute_content_tree(ship: &Ship) -> Result<ContentTreeReport, StewardError> {
    let local_pond_id = ship.data_persistence().pond_id().to_string();
    let table = ship.data_persistence().table().clone();
    compute_content_tree_for_table(table, &local_pond_id).await
}

/// Compute a pond's `root_tree_hash` directly from a `DeltaTable` handle.
///
/// This is the table-level entry point used by the commit path, where no
/// active transaction is held: it opens a fresh `SessionContext`, reads the
/// data table once, reconstructs the current tree, and folds it bottom-up.
/// Pure and side-effect free.
///
/// # Errors
///
/// Returns an error if the data table cannot be read, if the named pond has no
/// root directory row, if a referenced child node is missing, or if directory
/// content cannot be decoded.
pub async fn compute_content_tree_for_table(
    table: deltalake::DeltaTable,
    local_pond_id: &str,
) -> Result<ContentTreeReport, StewardError> {
    let index = build_content_tree_for_table(table, local_pond_id).await?;
    Ok(ContentTreeReport {
        root_tree_hash: index.root_tree_hash,
        nodes_hashed: index.nodes_hashed,
    })
}

/// Build the full content-tree index for a pond from a `DeltaTable` handle.
///
/// Reads the data table once, reconstructs the current tree, and folds it
/// bottom-up while capturing every physical directory's child list (so a later
/// comparison can descend by `child_hash`).  Pure and side-effect free.
///
/// # Errors
///
/// Returns an error if the data table cannot be read, if the named pond has no
/// root directory row, if a referenced child node is missing, or if directory
/// content cannot be decoded.
pub(crate) async fn build_content_tree_for_table(
    table: deltalake::DeltaTable,
    local_pond_id: &str,
) -> Result<ContentTreeIndex, StewardError> {
    // Hash/index paths never need blob bytes: file rows fold in via `blake3`,
    // and the only content the fold decodes (directories, symlinks, dynamic
    // node configs) has no `blake3`, so the narrow scan fetches exactly it.
    let rows = scan_live_rows(table, false).await?;
    fold_rows(rows, local_pond_id, None)
}

/// Build the node manifest for a pond's content tree from an already-built
/// index: one [`ManifestEntry`] per node, recording the source's `node_id`
/// alongside its parent, name, type, and content address (Section 4.5).
///
/// Every non-root node appears exactly once as a child of its parent directory;
/// the root has no parent, so it is added explicitly with an empty parent and
/// name.  The manifest is the one place node identity is recorded, so a
/// consumer can adopt these ids and mirror the source row-for-row (Decision
/// D8).
pub(crate) fn node_manifest_entries(index: &ContentTreeIndex) -> Vec<ManifestEntry> {
    let local_pond = &index.root_key.0;
    let mut entries = Vec::with_capacity(index.nodes_hashed.max(1));
    entries.push(ManifestEntry::new(
        index.root_key.1.clone(),
        String::new(),
        String::new(),
        EntryType::DirectoryPhysical,
        index.root_tree_hash,
    ));
    for (dir_key, children) in &index.dirs {
        if &dir_key.0 != local_pond {
            continue;
        }
        let parent_node_id = &dir_key.1;
        for child in children {
            entries.push(ManifestEntry::new(
                child.child_node_id.clone(),
                parent_node_id.clone(),
                child.name.clone(),
                child.entry_type,
                child.child_hash,
            ));
        }
    }
    entries
}

/// Compute the node-manifest commitments a commit references: the node
/// manifest object's content hash (`node_manifest_hash`) and the node-keyed
/// Merkle root over that manifest (`node_manifest_root`).  Both come from a
/// single fold of the data table, so they are mutually consistent.
///
/// # Errors
///
/// Returns an error if the data table cannot be read or folded, or if the
/// manifest cannot be encoded or hashed (a duplicate `node_id`).
pub(crate) async fn compute_commit_roots_for_table(
    table: deltalake::DeltaTable,
    local_pond_id: &str,
) -> Result<CommitRoots, StewardError> {
    let index = build_content_tree_for_table(table, local_pond_id).await?;
    let manifest = node_manifest_entries(&index);
    let node_manifest_hash = manifest_hash(&manifest).map_err(StewardError::Content)?;
    let node_manifest_root = node_merkle_rebuild_root(&manifest).map_err(StewardError::Content)?;
    Ok(CommitRoots {
        node_manifest_hash,
        node_manifest_root,
    })
}

/// The node-manifest commitments a commit object references, recomputed from a
/// pond's live state (used by the pull path to verify a rebuilt mirror).
pub(crate) struct CommitRoots {
    pub node_manifest_hash: ObjectHash,
    pub node_manifest_root: ObjectHash,
}

/// Build the full node-manifest bytes for the current in-transaction live state
/// (design `docs/incremental-content-tree-design.md` Section 4, Approach A /
/// Phase 2).
///
/// `committed_table` is the pre-commit Delta table (its rows are read with the
/// same narrow projection as every read-side fold); `uncommitted` are this
/// transaction's pending records plus synthesized modified-directory rows (from
/// [`tlogfs::persistence::State::uncommitted_live_rows`]).  The two are merged
/// and ordered so the latest version per node wins, then folded exactly like
/// the post-commit path -- the reserved index node is excluded from the fold,
/// so the manifest never lists itself.  Phase 2 writes the complete manifest as
/// one index-node version per commit; Phase 4 will make it a touched-only delta.
///
/// # Errors
///
/// Returns an error if the committed table cannot be scanned, the merged rows
/// cannot be folded, or the manifest cannot be encoded (a duplicate `node_id`).
///
/// The reserved-node write's inputs, all folded from the same in-transaction
/// live snapshot in a single scan: the encoded node manifest (index-node
/// content) plus the content roots (`root_tree_hash`, `node_manifest_hash`,
/// `node_manifest_root`) that the commit object needs.
///
/// Folding once here lets the guard write the index node and the authoritative
/// commit-log leaf atomically in the same transaction without re-scanning the
/// data table (design `docs/incremental-content-tree-design.md` Section 10,
/// step 4a).  The two reserved nodes are excluded from the fold, so writing
/// them never perturbs these roots.
pub(crate) struct SpineInputs {
    pub manifest_bytes: Vec<u8>,
    pub root_tree_hash: ObjectHash,
    pub node_manifest_hash: ObjectHash,
    pub node_manifest_root: ObjectHash,
}

pub(crate) async fn in_txn_spine_inputs(
    committed_table: deltalake::DeltaTable,
    uncommitted: Vec<OplogEntry>,
    local_pond_id: &str,
) -> Result<SpineInputs, StewardError> {
    let mut rows = scan_live_rows(committed_table, false).await?;
    rows.extend(uncommitted);
    // fold_rows takes the latest version per node from ascending-version order;
    // pending and synthesized rows carry higher (or `i64::MAX`) versions, so
    // ordering by version per node makes them win over their committed rows.
    rows.sort_by(|a, b| {
        a.pond_id
            .cmp(&b.pond_id)
            .then_with(|| a.node_id.to_string().cmp(&b.node_id.to_string()))
            .then_with(|| a.version.cmp(&b.version))
    });
    let index = fold_rows(rows, local_pond_id, None)?;
    let manifest = node_manifest_entries(&index);
    let node_manifest_hash = manifest_hash(&manifest).map_err(StewardError::Content)?;
    let node_manifest_root = node_merkle_rebuild_root(&manifest).map_err(StewardError::Content)?;
    let manifest_bytes = encode_manifest(&manifest).map_err(StewardError::Content)?;
    Ok(SpineInputs {
        manifest_bytes,
        root_tree_hash: index.root_tree_hash,
        node_manifest_hash,
        node_manifest_root,
    })
}

/// One node's live child listing, as recomputed incrementally.  A directory's
/// `tree_hash` is `encode_tree` over `(name, entry_type, child_hash)` for its
/// content children, so those three fields are all the incremental fold needs.
#[derive(Clone)]
struct ChildLite {
    node_id: String,
    name: String,
    entry_type: EntryType,
}

/// Whether the expensive full-fold verification oracle runs on every
/// content-changing commit.
///
/// The oracle recomputes both commit roots with a full `O(n)`
/// [`in_txn_spine_inputs`] fold and asserts they match the `O(change)`
/// [`incremental_spine_inputs`] result (step 4b).  It is always on in debug
/// builds.  In release builds it is opt-in via the `POND_VERIFY_FOLD`
/// environment variable -- set to any value other than empty, `0`, or `false`
/// -- so a high-value pond can validate every commit without a debug rebuild.
/// The environment is read once and cached for the process lifetime.
pub(crate) fn fold_verification_enabled() -> bool {
    if cfg!(debug_assertions) {
        return true;
    }
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("POND_VERIFY_FOLD")
            .ok()
            .map(|v| {
                let v = v.trim();
                !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false")
            })
            .unwrap_or(false);
        if enabled {
            log::warn!(
                "POND_VERIFY_FOLD is set: every write transaction recomputes both \
                 commit roots with a full O(n) fold to cross-check the incremental \
                 O(change) result. This adds per-commit overhead; unset it to disable."
            );
        }
        enabled
    })
}

/// Compute the two commit roots incrementally along the touched path only,
/// using the pond's previously committed node manifest as the child-hash
/// baseline (design `docs/incremental-content-tree-design.md` Section 10,
/// step 4b).
///
/// The prior manifest records every node's `child_hash`, parent, name, and
/// type, so it fully describes the committed tree.  This transaction's
/// changeset (`uncommitted`) replaces the listings of modified directories and
/// the content hashes of touched leaves; every directory on the root-to-change
/// path then has its `tree_hash` recomputed bottom-up, while untouched subtrees
/// keep their cached `child_hash`.  The result is byte-identical to a full
/// [`fold_rows`] of the post-commit live state, which the guard verifies against
/// this on every commit when [`fold_verification_enabled`] is true (always in
/// debug builds; opt-in via `POND_VERIFY_FOLD` in release builds).
///
/// `prior_manifest_bytes` is `None` only at genesis (no index node yet), when
/// there is no baseline to build on and the full fold in [`in_txn_spine_inputs`]
/// runs instead.
///
/// # Errors
///
/// Returns an error if the prior manifest cannot be decoded, a touched series'
/// committed versions cannot be read, a referenced child has no known hash, or
/// a tree/manifest cannot be encoded.
pub(crate) async fn incremental_spine_inputs(
    committed_table: deltalake::DeltaTable,
    prior_manifest_bytes: Option<Vec<u8>>,
    uncommitted: Vec<OplogEntry>,
    local_pond_id: &str,
) -> Result<SpineInputs, StewardError> {
    let Some(prior_bytes) = prior_manifest_bytes else {
        // Genesis: no committed manifest exists to build on, so fold the whole
        // (small) initial tree once.
        return in_txn_spine_inputs(committed_table, uncommitted, local_pond_id).await;
    };
    let prior = decode_manifest(&prior_bytes).map_err(StewardError::Content)?;

    // Baseline drawn from the prior manifest: every node's current child_hash,
    // type, parent, and each directory's content-child listing.
    let mut child_hash: HashMap<String, ObjectHash> = HashMap::new();
    let mut etype_of: HashMap<String, EntryType> = HashMap::new();
    let mut parent_of: HashMap<String, String> = HashMap::new();
    let mut dir_children: HashMap<String, Vec<ChildLite>> = HashMap::new();
    for e in &prior {
        let _ = child_hash.insert(e.node_id.clone(), e.child_hash);
        let _ = etype_of.insert(e.node_id.clone(), e.entry_type);
        if e.node_id == ROOT_UUID {
            continue;
        }
        let _ = parent_of.insert(e.node_id.clone(), e.parent_node_id.clone());
        dir_children
            .entry(e.parent_node_id.clone())
            .or_default()
            .push(ChildLite {
                node_id: e.node_id.clone(),
                name: e.name.clone(),
                entry_type: e.entry_type,
            });
    }

    // Split this transaction's changeset into the latest directory snapshot,
    // the latest leaf row, and the accumulated series version blobs per node.
    let mut dir_rows: HashMap<String, (i64, Vec<u8>)> = HashMap::new();
    let mut leaf_latest: HashMap<String, OplogEntry> = HashMap::new();
    let mut series_new: HashMap<String, BTreeMap<i64, ObjectHash>> = HashMap::new();
    let mut series_ranges: HashMap<String, Vec<(i64, CollapseRange)>> = HashMap::new();
    let mut changed: BTreeSet<String> = BTreeSet::new();
    for row in uncommitted {
        // Foreign-pond rows (cross-pond mount subtrees) are never folded into
        // this pond's tree: the fold skips a mount point and everything under
        // it, so the changeset must ignore those rows too.
        if row.pond_id != local_pond_id {
            continue;
        }
        let node = row.node_id.to_string();
        let _ = changed.insert(node.clone());
        let _ = etype_of.insert(node.clone(), row.file_type);
        match row.file_type {
            EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
                let hash = row_blob_hash(&row.blake3, row.content.as_deref());
                let _ = series_new
                    .entry(node.clone())
                    .or_default()
                    .insert(row.version, hash);
                series_ranges
                    .entry(node)
                    .or_default()
                    .push((row.version, CollapseRange::of(&row)));
            }
            EntryType::DirectoryPhysical => {
                let content = row.content.clone().unwrap_or_default();
                let slot = dir_rows
                    .entry(node)
                    .or_insert((row.version, content.clone()));
                if row.version >= slot.0 {
                    *slot = (row.version, content);
                }
            }
            _ => {
                let win = leaf_latest
                    .get(&node)
                    .is_none_or(|prev| row.version >= prev.version);
                if win {
                    let _ = leaf_latest.insert(node, row);
                }
            }
        }
    }

    // New content hash of every touched leaf, dispatching on kind exactly as
    // `hash_child` does.
    for (node, row) in &leaf_latest {
        let hash = match row.file_type {
            EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
                row_blob_hash(&row.blake3, row.content.as_deref())
            }
            EntryType::Symlink => ObjectHash::of_bytes(row.content.as_deref().unwrap_or(&[])),
            EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic => {
                let factory = row.factory.as_deref().ok_or_else(|| {
                    StewardError::DeltaLake(format!(
                        "dynamic node {node} is missing its factory type"
                    ))
                })?;
                recipe_hash(factory, row.content.as_deref().unwrap_or(&[]))
            }
            other => {
                return Err(StewardError::DeltaLake(format!(
                    "unexpected leaf entry type {other:?} for node {node}"
                )));
            }
        };
        let _ = child_hash.insert(node.clone(), hash);
    }

    // New content hash of every touched series: its committed version blobs
    // followed by this transaction's appended versions, pruned by range
    // containment and ordered oldest content first, exactly as [`fold_rows`]
    // does, then hashed as one series object.
    for (node, appended) in &series_new {
        let (mut versions, mut ranges) =
            read_series_committed(committed_table.clone(), local_pond_id, node).await?;
        for (version, hash) in appended {
            let _ = versions.insert(*version, *hash);
        }
        // This transaction's rows shadow the committed rows of the same version.
        if let Some(new_ranges) = series_ranges.get(node) {
            ranges.retain(|(v, _)| !new_ranges.iter().any(|(nv, _)| nv == v));
            ranges.extend(new_ranges.iter().copied());
        }
        let ordered: Vec<ObjectHash> = live_series_versions(&ranges)
            .into_iter()
            .filter_map(|version| versions.get(&version).copied())
            .collect();
        let _ = child_hash.insert(node.clone(), series_hash(&ordered));
    }

    // Replace the listing of every modified directory, skipping the entries the
    // fold also skips: cross-pond mounts and the two reserved nodes.
    for (node, (_, content)) in &dir_rows {
        let entries = decode_directory_entries(content)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
        let mut kids: Vec<ChildLite> = Vec::with_capacity(entries.len());
        for de in entries {
            let child_pond = de
                .pond_id
                .clone()
                .unwrap_or_else(|| local_pond_id.to_string());
            if child_pond != local_pond_id {
                continue;
            }
            let cid = de.child_node_id.to_string();
            if cid == tinyfs::INDEX_NODE_UUID || cid == tinyfs::LOG_NODE_UUID {
                continue;
            }
            let _ = parent_of.insert(cid.clone(), node.clone());
            let _ = etype_of.insert(cid.clone(), de.entry_type);
            kids.push(ChildLite {
                node_id: cid,
                name: de.name,
                entry_type: de.entry_type,
            });
        }
        let _ = dir_children.insert(node.clone(), kids);
    }

    // Every directory on a root-to-change path must be re-hashed: a directory
    // whose own listing changed, plus every ancestor of any touched node.
    let mut dirty: BTreeSet<String> = BTreeSet::new();
    for node in &changed {
        if dir_rows.contains_key(node) {
            let _ = dirty.insert(node.clone());
        }
        let mut cursor = parent_of.get(node).cloned();
        while let Some(dir) = cursor {
            let newly = dirty.insert(dir.clone());
            cursor = parent_of.get(&dir).cloned();
            if !newly {
                // This directory (and therefore its ancestors) is already dirty.
                break;
            }
        }
    }
    // A content-changing commit always alters the root tree.
    let _ = dirty.insert(ROOT_UUID.to_string());

    // Recompute dirty directories deepest-first, so each parent reads the fresh
    // child_hash of any dirty child before it is itself hashed.
    let mut depth_memo: HashMap<String, usize> = HashMap::new();
    let mut order: Vec<String> = dirty.iter().cloned().collect();
    order.sort_by_key(|d| std::cmp::Reverse(node_depth(d, &parent_of, &mut depth_memo)));
    for dir in order {
        let kids = dir_children.get(&dir).cloned().unwrap_or_default();
        let mut tree_entries: Vec<TreeEntry> = Vec::with_capacity(kids.len());
        for kid in kids {
            let ch = child_hash.get(&kid.node_id).ok_or_else(|| {
                StewardError::DeltaLake(format!(
                    "incremental fold: child {} of directory {dir} has no known hash",
                    kid.node_id
                ))
            })?;
            tree_entries.push(TreeEntry::new(kid.name, kid.entry_type, *ch));
        }
        let encoded = encode_tree(&tree_entries).map_err(StewardError::Content)?;
        let _ = child_hash.insert(dir, ObjectHash::of_bytes(&encoded));
    }

    let root_tree_hash = *child_hash.get(ROOT_UUID).ok_or_else(|| {
        StewardError::DeltaLake("incremental fold produced no root tree hash".to_string())
    })?;

    // Rebuild the manifest by walking the live tree from the root, so deleted
    // (now-unreachable) subtrees drop out and only live nodes are recorded.
    let mut manifest: Vec<ManifestEntry> = Vec::with_capacity(prior.len());
    manifest.push(ManifestEntry::new(
        ROOT_UUID.to_string(),
        String::new(),
        String::new(),
        EntryType::DirectoryPhysical,
        root_tree_hash,
    ));
    let mut stack = vec![ROOT_UUID.to_string()];
    let mut seen: HashSet<String> = HashSet::new();
    while let Some(dir) = stack.pop() {
        if !seen.insert(dir.clone()) {
            continue;
        }
        for kid in dir_children.get(&dir).cloned().unwrap_or_default() {
            let ch = child_hash.get(&kid.node_id).ok_or_else(|| {
                StewardError::DeltaLake(format!(
                    "incremental fold: live child {} has no known hash",
                    kid.node_id
                ))
            })?;
            manifest.push(ManifestEntry::new(
                kid.node_id.clone(),
                dir.clone(),
                kid.name,
                kid.entry_type,
                *ch,
            ));
            if kid.entry_type == EntryType::DirectoryPhysical {
                stack.push(kid.node_id);
            }
        }
    }

    let node_manifest_hash = manifest_hash(&manifest).map_err(StewardError::Content)?;
    let node_manifest_root = node_merkle_rebuild_root(&manifest).map_err(StewardError::Content)?;
    let manifest_bytes = encode_manifest(&manifest).map_err(StewardError::Content)?;
    Ok(SpineInputs {
        manifest_bytes,
        root_tree_hash,
        node_manifest_hash,
        node_manifest_root,
    })
}

/// Depth of a node below the root (root = 0), memoized across a fold.  A node
/// whose parent chain does not reach the root (a detached fragment) is treated
/// as maximally deep so it is recomputed before any real ancestor.
fn node_depth(
    node: &str,
    parent_of: &HashMap<String, String>,
    memo: &mut HashMap<String, usize>,
) -> usize {
    if node == ROOT_UUID {
        return 0;
    }
    if let Some(d) = memo.get(node) {
        return *d;
    }
    let depth = match parent_of.get(node) {
        Some(parent) => node_depth(parent, parent_of, memo).saturating_add(1),
        None => usize::MAX,
    };
    let _ = memo.insert(node.to_string(), depth);
    depth
}

/// Read a series node's committed version blob hashes from `table`, keyed by
/// version, together with the [`CollapseRange`] of every committed row.  Unlike
/// [`fold_rows`], the pruning is left to the caller so this transaction's
/// appended rows can be folded in first.
///
/// Used by the incremental fold to rebuild a touched series' hash from its
/// committed history plus this transaction's appended versions.
///
/// # Errors
///
/// Returns an error if the series rows cannot be read or deserialized.
async fn read_series_committed(
    table: deltalake::DeltaTable,
    pond_id: &str,
    node_id: &str,
) -> Result<(BTreeMap<i64, ObjectHash>, Vec<(i64, CollapseRange)>), StewardError> {
    let ctx = SessionContext::new();
    let _previous = ctx
        .register_table("series_live", Arc::new(table))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let sql = format!(
        "SELECT version, blake3, content, collapsed_from, collapsed_through FROM series_live \
         WHERE pond_id = '{pond_id}' AND node_id = '{node_id}' ORDER BY version",
    );
    let batches = ctx
        .sql(&sql)
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let mut rows: Vec<SeriesVersionRow> = Vec::new();
    for batch in &batches {
        let parsed: Vec<SeriesVersionRow> = serde_arrow::from_record_batch(batch)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
        rows.extend(parsed);
    }
    let ranges: Vec<(i64, CollapseRange)> = rows
        .iter()
        .map(|r| {
            (
                r.version,
                CollapseRange::new(r.version, r.collapsed_from, r.collapsed_through),
            )
        })
        .collect();
    let mut versions: BTreeMap<i64, ObjectHash> = BTreeMap::new();
    for row in rows {
        let _ = versions.insert(
            row.version,
            row_blob_hash(&row.blake3, row.content.as_deref()),
        );
    }
    Ok((versions, ranges))
}

/// One committed series version row: its version and the fields
/// [`row_blob_hash`] needs, plus the collapse range columns.
#[derive(serde::Deserialize)]
struct SeriesVersionRow {
    version: i64,
    blake3: Option<String>,
    content: Option<Vec<u8>>,
    collapsed_from: Option<i64>,
    collapsed_through: Option<i64>,
}

/// Read the reserved commit-log node's leaves from `table` in commit order:
/// each element is the raw encoded `commit_object` bytes of one leaf, ordered
/// by ascending series version (design Decision D9).  Returns an empty vector
/// when the log node does not exist yet (genesis, before the first
/// content-changing commit).
///
/// The log node is a raw byte series; each version stores exactly one leaf, so
/// leaves are read at version granularity rather than through the merged series
/// read (which would concatenate every leaf).
pub(crate) async fn read_log_leaves(
    table: deltalake::DeltaTable,
    pond_id: &str,
) -> Result<Vec<Vec<u8>>, StewardError> {
    let ctx = SessionContext::new();
    let _previous = ctx
        .register_table("log_live", Arc::new(table))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let sql = format!(
        "SELECT version, content FROM log_live \
         WHERE pond_id = '{pond_id}' AND node_id = '{log}' ORDER BY version",
        log = tinyfs::LOG_NODE_UUID,
    );
    let batches = ctx
        .sql(&sql)
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let mut leaves: Vec<Vec<u8>> = Vec::new();
    for batch in &batches {
        let parsed: Vec<LogLeaf> = serde_arrow::from_record_batch(batch)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
        for row in parsed {
            let bytes = row.content.ok_or_else(|| {
                StewardError::Content(format!(
                    "commit-log leaf at version {} has no content",
                    row.version
                ))
            })?;
            leaves.push(bytes);
        }
    }
    Ok(leaves)
}

/// One commit-log leaf row: its series version plus the inline commit-object
/// bytes.  Matches the `version, content` projection in [`read_log_leaves`].
#[derive(serde::Deserialize)]
struct LogLeaf {
    version: i64,
    content: Option<Vec<u8>>,
}

/// The current tip of the commit-log node -- the hash of its last leaf's commit
/// object -- to use as the `parent_commit_hash` of the next commit.  `None`
/// when the log node is empty (genesis).
pub(crate) async fn log_tip_commit_hash(
    table: deltalake::DeltaTable,
    pond_id: &str,
) -> Result<Option<ObjectHash>, StewardError> {
    let leaves = read_log_leaves(table, pond_id).await?;
    let Some(last) = leaves.last() else {
        return Ok(None);
    };
    let commit = Commit::decode(last)
        .map_err(|e| StewardError::Content(format!("decode commit-log tip: {e}")))?;
    Ok(Some(commit.hash()))
}

/// Read the commit spines from a pond's log node, keyed by transaction `seq`.
///
/// Each log leaf is a `commit_object` whose provenance carries the `seq` of the
/// transaction that stamped it; this decodes every leaf into a [`CommitSpine`]
/// (the four hex fields the control table caches) so a control-table rebuild
/// can restore the spine from the authoritative, pond-resident log rather than
/// leaving it empty.  Returns an empty map for a pond with no content-changing
/// commits.
///
/// # Errors
///
/// Returns an error if the log node cannot be read or a leaf cannot be decoded.
pub(crate) async fn read_log_spines(
    table: deltalake::DeltaTable,
    pond_id: &str,
) -> Result<HashMap<i64, CommitSpine>, StewardError> {
    let leaves = read_log_leaves(table, pond_id).await?;
    let mut spines = HashMap::with_capacity(leaves.len());
    for bytes in leaves {
        let commit = Commit::decode(&bytes)
            .map_err(|e| StewardError::Content(format!("decode commit-log leaf: {e}")))?;
        let spine = CommitSpine {
            root_tree_hash: commit.root_tree_hash.to_hex(),
            parent_commit_hash: commit.parent_commit_hash.map(|h| h.to_hex()),
            commit_hash: commit.hash().to_hex(),
            commit_object: hex::encode(&bytes),
        };
        let _ = spines.insert(commit.provenance.seq, spine);
    }
    Ok(spines)
}

/// Build a commit spine from precomputed roots and an explicit parent, without
/// consulting the control table.  Used by the guard to stamp the
/// pond-resident, authoritative commit-log leaf in-transaction (Decision D9);
/// the parent comes from the log node's tip, not the control-table cache.
pub(crate) fn build_commit_spine(
    parent_commit_hash: Option<ObjectHash>,
    root_tree_hash: ObjectHash,
    node_manifest_hash: ObjectHash,
    node_manifest_root: ObjectHash,
    pond_id_str: &str,
    txn_seq: i64,
    request: String,
) -> CommitSpine {
    let provenance = Provenance {
        pond_id: pond_id_str.to_string(),
        seq: txn_seq,
        time_micros: chrono::Utc::now().timestamp_micros(),
        author: String::new(),
        request,
    };
    let commit = Commit::new(
        root_tree_hash,
        parent_commit_hash,
        node_manifest_hash,
        node_manifest_root,
        provenance,
    );
    CommitSpine {
        root_tree_hash: root_tree_hash.to_hex(),
        parent_commit_hash: parent_commit_hash.map(|h| h.to_hex()),
        commit_hash: commit.hash().to_hex(),
        commit_object: hex::encode(commit.encode()),
    }
}

/// Build a target pond's current node state for an incremental rebuild: a map
/// from `node_id` to its [`ManifestEntry`], and a map from each series
/// `node_id` to its ordered version blob hashes.
///
/// The maps are keyed by `node_id` alone (not the full `NodeKey`) because an
/// incremental pull operates within a single mirror pond; the diff against the
/// fetched source manifest is `node_id`-keyed (Decision D8).
///
/// # Errors
///
/// Returns an error if the data table cannot be read or folded.
pub(crate) async fn build_target_state(
    ship: &Ship,
) -> Result<
    (
        HashMap<String, ManifestEntry>,
        HashMap<String, Vec<ObjectHash>>,
    ),
    StewardError,
> {
    let local_pond_id = ship.data_persistence().pond_id().to_string();
    build_target_state_for_pond(ship, &local_pond_id).await
}

/// Build the target's current node state for a named pond, keyed by `node_id`,
/// for a cross-pond import: the foreign pond's rows live under their own
/// `pond_id` partition, so the diff against the source manifest is computed
/// over that pond_id rather than the local one.  Returns empty maps when the
/// foreign pond has no root row yet (a first import has nothing to diff).
///
/// # Errors
///
/// Returns an error if the data table cannot be read or folded for a non-empty
/// foreign pond.
pub(crate) async fn build_target_state_for_pond(
    ship: &Ship,
    pond_id: &str,
) -> Result<
    (
        HashMap<String, ManifestEntry>,
        HashMap<String, Vec<ObjectHash>>,
    ),
    StewardError,
> {
    let table = ship.data_persistence().table().clone();
    let index = match build_content_tree_for_table(table, pond_id).await {
        Ok(index) => index,
        // A foreign pond with no root row yet: first import, empty target.
        Err(StewardError::DeltaLake(msg)) if msg.contains("no root directory row") => {
            return Ok((HashMap::new(), HashMap::new()));
        }
        Err(e) => return Err(e),
    };
    let by_id = node_manifest_entries(&index)
        .into_iter()
        .map(|e| (e.node_id.clone(), e))
        .collect();
    // Filter to the requested pond BEFORE dropping the pond_id component.  The
    // fold scans the whole data table and keys `series_versions` by
    // (pond_id, node_id); under D8 the source's node_ids are adopted verbatim,
    // so a mirror/import can hold the same series node_id under two different
    // pond_ids.  Collapsing to node_id-only without this filter lets a foreign
    // pond's version list win nondeterministically, corrupting the append-only
    // prefix used by incremental pull.  Mirrors the pond filter in
    // node_manifest_entries.
    let series = index
        .series_versions
        .into_iter()
        .filter(|((pond, _node_id), _versions)| pond == pond_id)
        .map(|((_pond, node_id), versions)| (node_id, versions))
        .collect();
    Ok((by_id, series))
}

/// Materialize the content objects reachable from a pond's root tree.
///
/// Reads the data table once and folds it exactly like the hash path, but also
/// captures each object's bytes: encoded tree objects, series manifests, and
/// small blob/symlink/recipe bytes inline, with large blobs recorded by hash
/// for external transfer (Decision D7).  Commit objects are added separately by
/// the push layer.  Pure and side-effect free.
///
/// # Errors
///
/// Returns an error if the data table cannot be read, if the named pond has no
/// root directory row, if a referenced child node is missing, or if directory
/// content cannot be decoded.
pub async fn materialize_content_objects(ship: &Ship) -> Result<MaterializedObjects, StewardError> {
    let local_pond_id = ship.data_persistence().pond_id().to_string();
    let table = ship.data_persistence().table().clone();
    let mut materialized = MaterializedObjects::default();
    // Materialization must read every blob so it can be transferred, so this
    // is the one caller that scans with content (`want_content = true`).
    let rows = scan_live_rows(table, true).await?;
    let index = fold_rows(rows, &local_pond_id, Some(&mut materialized))?;
    // The node manifest travels with the closure so a consumer can adopt the
    // source's node_ids (Section 4.5).  It is kept separate from the pure
    // content objects because it is pond-specific (it carries node_ids); the
    // commit references it by hash.
    let manifest = node_manifest_entries(&index);
    let manifest_bytes = encode_manifest(&manifest).map_err(StewardError::Content)?;
    materialized.manifest = Some((ObjectHash::of_bytes(&manifest_bytes), manifest_bytes));
    Ok(materialized)
}

/// Scan a pond's live rows once for the content fold (and, in the per-commit
/// path, the partition checksums computed from the same rows).
///
/// When `want_content` is false -- the per-commit hot path and every read-side
/// fold -- inline file `content` and `bao_outboard` bytes are NOT read from
/// parquet.  A file row already carries a `blake3` that stands in for its
/// content, and the only rows whose small `content` the fold decodes
/// (directories, symlinks, and dynamic-node configs) carry no `blake3`; those
/// are fetched by a second query filtered to `blake3 IS NULL`.  This keeps a
/// commit's read volume proportional to structural metadata, not to the inline
/// blob bytes of the whole pond (design "Incremental Content Tree", Tier 0).
///
/// When `want_content` is true -- materialization for push -- every row's
/// content is read so blobs can be transferred.
///
/// # Errors
///
/// Returns an error if the data table cannot be registered, queried, or
/// deserialized.
async fn scan_live_rows(
    table: deltalake::DeltaTable,
    want_content: bool,
) -> Result<Vec<OplogEntry>, StewardError> {
    let ctx = SessionContext::new();
    let _previous = ctx
        .register_table("content_live", Arc::new(table))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    scan_live_rows_ctx(&ctx, want_content).await
}

/// Column list matching [`OplogEntry`]'s Arrow schema field order, but with the
/// two large byte columns replaced by typed NULL literals so the parquet reader
/// never materializes them while the batch still deserializes into a full
/// [`OplogEntry`] (with `content`/`bao_outboard` left `None`).
const NARROW_META_SQL: &str = "SELECT part_id, node_id, file_type, timestamp, version, \
     arrow_cast(NULL, 'Binary') AS content, blake3, size, min_event_time, max_event_time, \
     min_override, max_override, extended_attributes, factory, format, txn_seq, pond_id, \
     arrow_cast(NULL, 'Binary') AS bao_outboard, collapsed_through, collapsed_from \
     FROM content_live ORDER BY pond_id, part_id, node_id, version";

/// Content of exactly the structural rows the fold decodes -- those without a
/// `blake3` (directories, symlinks, dynamic nodes).
const NARROW_CONTENT_SQL: &str =
    "SELECT pond_id, node_id, version, content FROM content_live WHERE blake3 IS NULL";

/// Just the structural `content` of a `blake3 IS NULL` row, keyed for splicing
/// back into its metadata row.
#[derive(serde::Deserialize)]
struct StructuralContent {
    pond_id: String,
    node_id: String,
    version: i64,
    content: Option<Vec<u8>>,
}

/// [`scan_live_rows`] against a session with `content_live` already registered
/// (split out so it can be exercised over an in-memory table in tests).
async fn scan_live_rows_ctx(
    ctx: &SessionContext,
    want_content: bool,
) -> Result<Vec<OplogEntry>, StewardError> {
    let sql = if want_content {
        "SELECT * FROM content_live ORDER BY pond_id, part_id, node_id, version"
    } else {
        NARROW_META_SQL
    };
    let batches = ctx
        .sql(sql)
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let mut rows: Vec<OplogEntry> = Vec::new();
    for batch in &batches {
        let parsed: Vec<OplogEntry> = serde_arrow::from_record_batch(batch)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
        rows.extend(parsed);
    }

    if want_content {
        return Ok(rows);
    }

    // Splice the small content of structural (blake3-free) rows back in.
    let content_batches = ctx
        .sql(NARROW_CONTENT_SQL)
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let mut by_key: HashMap<(String, String, i64), Vec<u8>> = HashMap::new();
    for batch in &content_batches {
        let parsed: Vec<StructuralContent> = serde_arrow::from_record_batch(batch)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
        for c in parsed {
            if let Some(bytes) = c.content {
                let _ = by_key.insert((c.pond_id, c.node_id, c.version), bytes);
            }
        }
    }
    for row in &mut rows {
        if row.blake3.is_none() {
            let key = (row.pond_id.clone(), row.node_id.to_string(), row.version);
            if let Some(bytes) = by_key.remove(&key) {
                row.content = Some(bytes);
            }
        }
    }
    Ok(rows)
}

/// Fold already-scanned live rows into a [`ContentTreeIndex`].
///
/// When `sink` is `Some`, every folded object's bytes are recorded into it
/// (split inline vs external per Decision D7); when `None`, only hashes are
/// computed.  Either way the returned [`ContentTreeIndex`] is identical, so the
/// child-hash rules live in exactly one implementation.  The rows must arrive
/// in ascending `version` order per node (the scan's `ORDER BY`), so later rows
/// overwrite earlier ones for the latest-version snapshot.
fn fold_rows(
    rows: Vec<OplogEntry>,
    local_pond_id: &str,
    sink: Option<&mut MaterializedObjects>,
) -> Result<ContentTreeIndex, StewardError> {
    // Latest-version facts per node, and per-version blobs for series.
    let mut latest: HashMap<NodeKey, NodeFacts> = HashMap::new();
    let mut series_versions: HashMap<NodeKey, BTreeMap<i64, VersionBlob>> = HashMap::new();
    // Collapse range of every series row.  A compaction leaves the superseded
    // per-version rows in the table beside a merged row covering them; the
    // superseded versions are pruned after the scan.
    let mut series_ranges: HashMap<NodeKey, Vec<(i64, CollapseRange)>> = HashMap::new();

    for row in rows {
        let key = (row.pond_id.clone(), row.node_id.to_string());

        if matches!(
            row.file_type,
            EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
        ) {
            let hash = row_blob_hash(&row.blake3, row.content.as_deref());
            let _ = series_versions.entry(key.clone()).or_default().insert(
                row.version,
                VersionBlob {
                    hash,
                    content: row.content.clone(),
                },
            );
            series_ranges
                .entry(key.clone())
                .or_default()
                .push((row.version, CollapseRange::of(&row)));
        }

        let _ = latest.insert(
            key,
            NodeFacts {
                content: row.content,
                blake3: row.blake3,
                factory: row.factory,
            },
        );
    }

    // Drop versions superseded by a compaction.  The live series read path keeps
    // exactly the rows no *other* row's collapse range contains (see
    // tlogfs::schema::live_series_entries, used by
    // OpLogPersistence::async_file_reader_series); the content fold must match it
    // exactly.  Folding in phantom superseded blobs makes a pulled mirror
    // reconstruct duplicated data whose fold still equals the source's; dropping
    // a live row is worse, because the mirror then never learns it needs that
    // blob yet both sides still agree the trees match.
    //
    // The result is a *sequence in byte order*, not a version-keyed map: a
    // merged run carries a fresh highest version while standing for content in
    // the middle of the stream, so iterating by version would order the runs
    // after the loose tail. That ordering is not cosmetic -- a destination
    // reconstructs a pulled series by writing these blobs in order, and
    // plan_series_versions compares them as a prefix to find the suffix it must
    // append.
    let mut series_live: HashMap<NodeKey, Vec<VersionBlob>> = HashMap::new();
    for (key, versions) in &mut series_versions {
        let ordered = match series_ranges.get(key) {
            Some(ranges) => live_series_versions(ranges),
            None => versions.keys().copied().collect(),
        };
        let blobs: Vec<VersionBlob> = ordered
            .into_iter()
            .filter_map(|version| versions.remove(&version))
            .collect();
        let _ = series_live.insert(key.clone(), blobs);
    }
    let series_versions = series_live;

    let root_key = (local_pond_id.to_string(), ROOT_UUID.to_string());
    if !latest.contains_key(&root_key) {
        return Err(StewardError::DeltaLake(
            "local pond has no root directory row".to_string(),
        ));
    }

    let mut memo: HashMap<NodeKey, ObjectHash> = HashMap::new();
    let mut in_progress: Vec<NodeKey> = Vec::new();
    let mut dirs: HashMap<NodeKey, Vec<ChildRef>> = HashMap::new();
    let root_tree_hash = hash_directory(
        &root_key,
        &latest,
        &series_versions,
        &mut memo,
        &mut in_progress,
        &mut dirs,
        sink,
    )?;

    let series_version_hashes = series_versions
        .into_iter()
        .map(|(key, versions)| (key, versions.iter().map(|v| v.hash).collect()))
        .collect();

    Ok(ContentTreeIndex {
        root_tree_hash,
        root_key,
        dirs,
        series_versions: series_version_hashes,
        nodes_hashed: memo.len(),
    })
}

/// Compute the blob hash of a file version: the recorded `blake3` when present
/// and well-formed, else a hash of the inline content (empty if externalized).
fn row_blob_hash(blake3: &Option<String>, content: Option<&[u8]>) -> ObjectHash {
    if let Some(hex) = blake3
        && let Ok(h) = ObjectHash::from_hex(hex)
    {
        return h;
    }
    ObjectHash::of_bytes(content.unwrap_or(&[]))
}

/// Fold one directory (by key) into its recursive [`tree_hash`], recording its
/// child list into `dirs` for later comparison.  When `sink` is `Some`, the
/// encoded tree object bytes (and, via `hash_child`, descendant object bytes)
/// are recorded for materialization.
#[allow(clippy::too_many_arguments)]
fn hash_directory(
    key: &NodeKey,
    latest: &HashMap<NodeKey, NodeFacts>,
    series_versions: &HashMap<NodeKey, Vec<VersionBlob>>,
    memo: &mut HashMap<NodeKey, ObjectHash>,
    in_progress: &mut Vec<NodeKey>,
    dirs: &mut HashMap<NodeKey, Vec<ChildRef>>,
    mut sink: Option<&mut MaterializedObjects>,
) -> Result<ObjectHash, StewardError> {
    if let Some(h) = memo.get(key) {
        return Ok(*h);
    }
    if in_progress.contains(key) {
        return Err(StewardError::DeltaLake(format!(
            "directory cycle detected at node {}/{}",
            key.0, key.1
        )));
    }

    let facts = latest.get(key).ok_or_else(|| {
        StewardError::DeltaLake(format!("missing directory node {}/{}", key.0, key.1))
    })?;
    let entries = decode_directory_entries(facts.content.as_deref().unwrap_or(&[]))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;

    in_progress.push(key.clone());

    let mut tree_entries: Vec<TreeEntry> = Vec::with_capacity(entries.len());
    let mut children: Vec<ChildRef> = Vec::with_capacity(entries.len());
    for entry in entries {
        // A child belongs to its parent's pond unless it carries an explicit
        // foreign pond_id (a cross-pond import mount point).
        let child_pond = entry.pond_id.clone().unwrap_or_else(|| key.0.clone());
        let child_key = (child_pond, entry.child_node_id.to_string());
        // A cross-pond mount point is a graft by reference, not this pond's
        // content: its subtree lives in the foreign pond's own content tree and
        // the push filters rows to this pond_id.  Omit it from the fold entirely
        // -- it contributes no tree entry and no child object -- so the content
        // tree is exactly this pond's own data.  This keeps the producer's
        // published tree consistent with what any consumer reconstructs (which
        // never receives the foreign subtree), and it is what blocks transitive
        // re-replication of a foreign mount across a multi-hop import (C imports
        // B imports A: C must not see A through B).  Because the omission happens
        // here, the node manifest excludes these mounts too (it is built from
        // the same child lists).
        if child_key.0 != key.0 {
            continue;
        }
        // The reserved node-manifest index node is a child of root but is
        // deliberately excluded from the content-tree fold and the node
        // manifest: its content is derived from the very hashes it stores, so
        // folding it in would be self-referential.  Skipping it here keeps
        // root's tree_hash and the manifest independent of the index node's
        // presence, exactly like a cross-pond mount (design
        // `docs/incremental-content-tree-design.md` Section 3).
        if child_key.1 == tinyfs::INDEX_NODE_UUID || child_key.1 == tinyfs::LOG_NODE_UUID {
            continue;
        }
        let child_hash = hash_child(
            &child_key,
            entry.entry_type,
            latest,
            series_versions,
            memo,
            in_progress,
            dirs,
            sink.as_deref_mut(),
        )?;
        let child_node_id = child_key.1.clone();
        let child_dir_key = if entry.entry_type == EntryType::DirectoryPhysical {
            Some(child_key)
        } else {
            None
        };
        children.push(ChildRef {
            name: entry.name.clone(),
            entry_type: entry.entry_type,
            child_hash,
            child_node_id,
            child_dir_key,
        });
        tree_entries.push(TreeEntry::new(entry.name, entry.entry_type, child_hash));
    }

    let _ = in_progress.pop();

    let encoded = encode_tree(&tree_entries).map_err(StewardError::DeltaLake)?;
    let hash = ObjectHash::of_bytes(&encoded);
    if let Some(sink) = sink {
        sink.put_inline(hash, encoded);
    }
    let _ = memo.insert(key.clone(), hash);
    let _ = dirs.insert(key.clone(), children);
    Ok(hash)
}

/// Compute the `child_hash` an entry of the given kind contributes to its
/// parent, dispatching on the entry type per design Section 9.  When `sink` is
/// `Some`, the child's object bytes are recorded for materialization.
#[allow(clippy::too_many_arguments)]
fn hash_child(
    key: &NodeKey,
    entry_type: EntryType,
    latest: &HashMap<NodeKey, NodeFacts>,
    series_versions: &HashMap<NodeKey, Vec<VersionBlob>>,
    memo: &mut HashMap<NodeKey, ObjectHash>,
    in_progress: &mut Vec<NodeKey>,
    dirs: &mut HashMap<NodeKey, Vec<ChildRef>>,
    sink: Option<&mut MaterializedObjects>,
) -> Result<ObjectHash, StewardError> {
    match entry_type {
        EntryType::DirectoryPhysical => {
            hash_directory(key, latest, series_versions, memo, in_progress, dirs, sink)
        }
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
            let versions = series_versions.get(key).ok_or_else(|| {
                StewardError::DeltaLake(format!("missing series node {}/{}", key.0, key.1))
            })?;
            let ordered: Vec<ObjectHash> = versions.iter().map(|v| v.hash).collect();
            let series = series_hash(&ordered);
            if let Some(sink) = sink {
                // The series manifest object, plus each version blob: small
                // versions inline, large (externalized) versions by hash (D7).
                sink.put_inline(series, encode_series(&ordered));
                for v in versions.iter() {
                    record_blob(sink, v.hash, v.content.as_deref());
                }
            }
            Ok(series)
        }
        // Symlinks hash their target bytes; dynamic nodes hash their recipe
        // (factory type plus config), so the content commits to the factory and
        // a consumer can reconstruct which factory to instantiate (D4).
        EntryType::Symlink => {
            let facts = leaf_facts(key, latest)?;
            let bytes = facts.content.as_deref().unwrap_or(&[]);
            let hash = ObjectHash::of_bytes(bytes);
            if let Some(sink) = sink {
                // Symlink targets are small; always inline.
                sink.put_inline(hash, bytes.to_vec());
            }
            Ok(hash)
        }
        EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic => {
            let facts = leaf_facts(key, latest)?;
            let factory = facts.factory.as_deref().ok_or_else(|| {
                StewardError::DeltaLake(format!(
                    "dynamic node {}/{} is missing its factory type",
                    key.0, key.1
                ))
            })?;
            let config = facts.content.as_deref().unwrap_or(&[]);
            let hash = recipe_hash(factory, config);
            if let Some(sink) = sink {
                // Recipes (factory + config) are small; always inline.
                sink.put_inline(hash, encode_recipe(factory, config));
            }
            Ok(hash)
        }
        // Single-version physical file or table: the version blob hash.
        EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
            let facts = leaf_facts(key, latest)?;
            let hash = row_blob_hash(&facts.blake3, facts.content.as_deref());
            if let Some(sink) = sink {
                record_blob(sink, hash, facts.content.as_deref());
            }
            Ok(hash)
        }
    }
}

/// Record a file/version blob into the materialization sink: inline when the
/// bytes are in-row (small), external by hash when the content is `None`
/// (an externalized large file -- Decision D7).
fn record_blob(sink: &mut MaterializedObjects, hash: ObjectHash, content: Option<&[u8]>) {
    match content {
        Some(bytes) => sink.put_inline(hash, bytes.to_vec()),
        None => sink.put_external(hash),
    }
}

/// Look up a non-directory node's latest facts, erroring if it is missing.
fn leaf_facts<'a>(
    key: &NodeKey,
    latest: &'a HashMap<NodeKey, NodeFacts>,
) -> Result<&'a NodeFacts, StewardError> {
    latest
        .get(key)
        .ok_or_else(|| StewardError::DeltaLake(format!("missing node {}/{}", key.0, key.1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build an in-memory `content_live` table from OplogEntry rows so the narrow
    // scan can be exercised without a Delta table.
    fn register_rows(entries: &[OplogEntry]) -> SessionContext {
        use tlogfs::schema::ForArrow;
        let batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &entries)
            .expect("encode OplogEntry rows");
        let schema = batch.schema();
        let mem =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
        let ctx = SessionContext::new();
        let _ = ctx
            .register_table("content_live", Arc::new(mem))
            .expect("register");
        ctx
    }

    /// The narrow scan projects an explicit column list, and serde_arrow fills a
    /// missing `Option` field with `None` instead of failing. Adding a field to
    /// `OplogEntry` without adding it here therefore fails *silently*, which is
    /// how `collapsed_from` was first missed: every merged run then folded as
    /// `[0, hi]` and superseded live rows it never covered.
    #[test]
    fn narrow_scan_projects_every_oplog_entry_field() {
        use tlogfs::schema::ForArrow;
        for field in OplogEntry::for_arrow() {
            let name = field.name();
            assert!(
                NARROW_META_SQL.contains(name.as_str()),
                "NARROW_META_SQL is missing OplogEntry field `{name}`; a field                  absent from the projection deserializes as None and corrupts                  the fold silently"
            );
        }
    }

    /// Corruption #4: the content fold must prune series versions by *range
    /// containment*, never by the highest `collapsed_through` sentinel.
    ///
    /// Once collapse is tiered, a run created early carries a low version and a
    /// low range, while a later merge of a *newer* window can have a
    /// `collapsed_through` above that run's version number. The old rule --
    /// "drop every version <= max(collapsed_through)" -- then discards a live
    /// run holding the only copy of the versions it absorbed.
    ///
    /// The failure is silent in the worst way: both ponds in a pull apply the
    /// same rule, so their tree hashes still agree and the guard reports
    /// convergence, while the destination never learns it needs that blob.
    #[test]
    fn fold_prunes_series_by_range_not_by_max_watermark() {
        use tinyfs::{DirectoryEntry, EntryType, FileID};
        use tlogfs::schema::encode_directory_entries;

        let pond = tinyfs::local_pond_uuid();
        let dir_id = FileID::root_for(pond);
        let series_id =
            FileID::new_in_partition(dir_id.part_id(), EntryType::FilePhysicalSeries, pond);

        // Run A absorbed versions 1..=10 and was allocated version 26.
        // A later window, versions 21..=31, merged as version 36 -- so the
        // highest sentinel (31) sits *above* run A's version number (26).
        let series_row = |version: i64, from: Option<i64>, through: Option<i64>| {
            let mut row = OplogEntry::new_small_file(
                series_id,
                version,
                version,
                format!("blob-for-version-{version}").into_bytes(),
                1,
            );
            row.collapsed_from = from;
            row.collapsed_through = through;
            row
        };
        let run_a = series_row(26, Some(1), Some(10));
        let loose_11 = series_row(11, None, None);
        let run_b = series_row(36, Some(21), Some(31));
        let loose_40 = series_row(40, None, None);
        // A row run A genuinely covers: it must be pruned.
        let superseded_5 = series_row(5, None, None);

        let dir_content = encode_directory_entries(&[DirectoryEntry::new(
            "series".to_string(),
            series_id.node_id(),
            EntryType::FilePhysicalSeries,
            1,
        )])
        .expect("encode directory");
        let dir_row = OplogEntry::new_inline(dir_id, 1, 1, dir_content, 1);

        let rows = vec![
            dir_row,
            run_a.clone(),
            loose_11.clone(),
            run_b.clone(),
            loose_40.clone(),
            superseded_5.clone(),
        ];
        let index = fold_rows(rows, &pond.to_string(), None).expect("fold");

        let key = (pond.to_string(), series_id.node_id().to_string());
        let folded = index
            .series_versions
            .get(&key)
            .expect("the series is folded into the tree");

        let expect = |row: &OplogEntry| row_blob_hash(&row.blake3, row.content.as_deref());
        assert_eq!(
            folded,
            &vec![
                expect(&run_a),
                expect(&loose_11),
                expect(&run_b),
                expect(&loose_40)
            ],
            "run A (version 26) is live -- no other row's range contains \
             [1,10] -- and must be folded, in range order, ahead of the loose \
             versions that follow it in the byte stream"
        );
        assert!(
            !folded.contains(&expect(&superseded_5)),
            "version 5 lies inside run A's range and must be pruned"
        );
    }

    #[tokio::test]
    async fn narrow_scan_drops_blob_content_but_keeps_structural() {
        use tinyfs::{EntryType, FileID};
        let pond = tinyfs::local_pond_uuid();

        // Directory: blake3 None, small structural content the fold decodes.
        let dir_id = FileID::new_physical_dir_id(pond);
        let dir_content = b"structural-directory-bytes".to_vec();
        let dir_row = OplogEntry::new_inline(dir_id, 1, 1, dir_content.clone(), 1);

        // Small file: blake3 Some, content redundant with the hash.
        let file_id =
            FileID::new_in_partition(dir_id.part_id(), EntryType::FilePhysicalVersion, pond);
        let file_content = b"redundant-blob-bytes".to_vec();
        let file_row = OplogEntry::new_small_file(file_id, 2, 1, file_content.clone(), 1);

        let ctx = register_rows(&[dir_row.clone(), file_row.clone()]);

        // Narrow scan: file blob content is dropped, structural content spliced.
        let narrow = scan_live_rows_ctx(&ctx, false).await.expect("narrow scan");
        let narrow_file = narrow
            .iter()
            .find(|r| r.blake3.is_some())
            .expect("file row present");
        assert!(
            narrow_file.content.is_none(),
            "blake3-bearing file content must not be read by the narrow scan"
        );
        assert!(narrow_file.bao_outboard.is_none());
        let narrow_dir = narrow
            .iter()
            .find(|r| r.blake3.is_none())
            .expect("dir row present");
        assert_eq!(
            narrow_dir.content.as_deref(),
            Some(dir_content.as_slice()),
            "structural (blake3-free) content must be spliced back for the fold"
        );

        // Full scan: every row keeps its content for materialization.
        let full = scan_live_rows_ctx(&ctx, true).await.expect("full scan");
        let full_file = full
            .iter()
            .find(|r| r.blake3.is_some())
            .expect("file row present");
        assert_eq!(full_file.content.as_deref(), Some(file_content.as_slice()));
    }
}
