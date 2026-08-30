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
    Commit, ContentModelVersion, ManifestEntry, ObjectHash, PayloadKind, Provenance,
    SeriesManifest, TreeEntry, VersionMeta, decode_manifest, encode_canonical_attributes,
    encode_manifest, encode_recipe, encode_tree, manifest_hash, merkle_root,
    node_merkle_rebuild_root, recipe_hash,
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
    /// mean identical content across the whole pond -- identical bytes *and*
    /// the node metadata the directory entries commit to, since a version's
    /// metadata is data about that version.  A replica has an equal root; two
    /// ponds written independently from the same bytes do not, because their
    /// versions were created at different times.
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
    /// Everything [`publish_initial_series_packs`] needs to mint one
    /// whole-range "initial" identity pack per `watertown.series.v1` series folded
    /// in this materialization, without re-walking the pond. Not itself a
    /// pushed object (a `PackIndex` is derived storage metadata excluded
    /// from the content tree), so it is not counted by [`Self::len`]/
    /// [`Self::is_empty`].
    pub(crate) series_material: Vec<SeriesPackMaterial>,
}

/// One v2 series node's manifest plus its ordered live versions, captured
/// during materialization so a later, separate step can mint an "initial"
/// pack (`docs/logical-series-identity-design.md`) from the exact same
/// content the fold already read, without a second pass over the pond.
///
/// See [`publish_initial_series_packs`] for how this is consumed and why:
/// the dual reader (`crate::content_pull::fetch_series_v2`) requires an
/// exact pack cover before it will trust any `watertown.series.v1` content, so a
/// freshly-folded v2 series is otherwise unfetchable the moment it is
/// pushed.
#[derive(Debug, Clone)]
pub(crate) struct SeriesPackMaterial {
    /// The series' own content address -- the `watertown.series.v1` manifest hash,
    /// and the key packs are published under.
    pub(crate) series_hash: ObjectHash,
    /// `FilePhysicalSeries` or `TablePhysicalSeries`; nothing else is ever
    /// recorded here.
    pub(crate) entry_type: EntryType,
    /// The already-built, already-verified manifest this series folded to.
    pub(crate) manifest: SeriesManifest,
    /// This series' live versions, oldest first -- exactly the slice
    /// [`build_series_manifest`] itself folded, including metadata-only
    /// (no-leaf) versions, which a pack builder must skip identically.
    pub(crate) versions: Vec<SeriesVersionData>,
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
    /// Node metadata for the child's live versions, oldest first: one entry for
    /// a single-version node, one per version for a series, none for a
    /// directory (whose state is its subtree).
    pub versions: Vec<VersionMeta>,
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
    /// Per series node, its ordered *logical leaf* hashes (ascending
    /// version, skipping leafless metadata-only rows) --
    /// `docs/logical-series-identity-design.md` v2 identity, distinct from
    /// `series_versions`' physical blob identity above. A v2 materializer
    /// (`steward::content_pull`'s `rebuild_pond`/`import_pond`) diffs a
    /// fetched, verified [`sync_store::content::SeriesManifest`]'s ordered
    /// leaf hashes against this per-node list to find the suffix of leaves
    /// it must still write, exactly as `series_versions` lets a v1
    /// incremental rebuild find its own suffix -- but at logical-leaf
    /// granularity, which is stable across a re-encode (physical blob
    /// identity is not: re-encoding a table leaf's Parquet bytes changes
    /// `blob_hash` even when the decoded rows are unchanged).
    pub series_leaf_hashes: HashMap<NodeKey, Vec<ObjectHash>>,
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
    /// The node metadata of this node's latest version, carried on the parent
    /// directory's entry so a replica can restore it.
    meta: VersionMeta,
}

/// One version of a series, carrying both its physical blob identity
/// (materialization/replica-divergence bookkeeping, keyed on raw bytes) and,
/// when present, its persisted v2 logical-leaf identity
/// (`docs/logical-series-identity-design.md`). Shared by the full fold
/// ([`fold_rows`]/[`hash_child`]) and the incremental fold
/// ([`incremental_spine_inputs`]/[`read_series_committed`]) so both compute
/// the [`SeriesManifest`] identically -- see [`build_series_manifest`].
#[derive(Debug, Clone)]
pub(crate) struct SeriesVersionData {
    /// This row's Delta table `version` number. Needed (only by
    /// pack-maintenance's bounded repack) to fetch this one version's
    /// inline content lazily and individually via
    /// [`read_series_version_inline_content`], rather than the whole
    /// series' content column being read into memory in one batch (see
    /// that function's doc comment).
    pub(crate) version: i64,
    /// The physical blob hash of this version's raw bytes. Unrelated to the
    /// v2 logical identity, but still needed to materialize/publish this
    /// version's blob (initial pack publication/fetch keeps physical blobs
    /// available even once the series' own identity is the manifest hash)
    /// and for the physical-byte replica-divergence bookkeeping in
    /// [`ContentTreeIndex::series_versions`], which is unaffected by this
    /// module's v1-to-v2 change.
    pub(crate) blob_hash: ObjectHash,
    /// Inline bytes when small; `None` when externalized (large file) --
    /// only read by the full fold's materialization sink.
    pub(crate) content: Option<Vec<u8>>,
    /// This version's own node metadata, canonicalized exactly as every
    /// other node kind is (used for the tree-entry-level `VersionMeta`, a
    /// concern distinct from the v2 manifest's own canonical-attributes
    /// requirement below).
    pub(crate) meta: VersionMeta,
    /// This version's raw (un-canonicalized) `extended_attributes` JSON as
    /// persisted on the row. Needed to compute `watertown.series.v1`'s
    /// `logical_attributes` via
    /// [`sync_store::content::encode_canonical_attributes`], whose
    /// canonical-JSON convention is distinct from this module's own
    /// [`canonical_attributes`].
    pub(crate) raw_extended_attributes: Option<String>,
    /// The persisted v2 logical leaf hash (`None` for an empty,
    /// metadata-only append -- the nonempty-leaf invariant means there is no
    /// leaf to identify, not an error).
    pub(crate) logical_leaf_hash: Option<ObjectHash>,
    /// The persisted logical count (rows for a table leaf, bytes for a file
    /// leaf); `Some` iff `logical_leaf_hash` is `Some`.
    pub(crate) logical_count: Option<i64>,
    /// The persisted table schema fingerprint (`TablePhysicalSeries` only;
    /// always `None` for a file series).
    pub(crate) schema_fingerprint: Option<ObjectHash>,
    /// This version's persisted physical byte size (the row's `size`
    /// column). Needed, without reading any payload bytes, to compute an
    /// initial pack's `physical_byte_count` directly from persisted rows
    /// (see [`build_initial_pack_index`]).
    pub(crate) blob_size: u64,
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
    entries.push(ManifestEntry::bare(
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
                child.versions.clone(),
            ));
        }
    }
    entries
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

/// Canonical full-fold view of one pond partition, including the state needed
/// to explain a root mismatch at node and series-version granularity.
pub(crate) struct FoldedContentState {
    pub manifest: Vec<ManifestEntry>,
    pub series_versions: HashMap<String, Vec<ObjectHash>>,
    pub root_tree_hash: ObjectHash,
    pub node_manifest_hash: ObjectHash,
    pub node_manifest_root: ObjectHash,
}

pub(crate) async fn in_txn_content_state(
    committed_table: deltalake::DeltaTable,
    uncommitted: Vec<OplogEntry>,
    pond_id: &str,
) -> Result<FoldedContentState, StewardError> {
    let mut rows = scan_live_rows(committed_table, false).await?;
    rows.extend(uncommitted);
    rows.sort_by(|a, b| {
        a.pond_id
            .cmp(&b.pond_id)
            .then_with(|| a.node_id.to_string().cmp(&b.node_id.to_string()))
            .then_with(|| a.version.cmp(&b.version))
    });
    let index = fold_rows(rows, pond_id, None)?;
    let manifest = node_manifest_entries(&index);
    let node_manifest_hash = manifest_hash(&manifest).map_err(StewardError::Content)?;
    let node_manifest_root = node_merkle_rebuild_root(&manifest).map_err(StewardError::Content)?;
    let series_versions = index
        .series_versions
        .iter()
        .filter(|((row_pond_id, _), _)| row_pond_id == pond_id)
        .map(|((_, node_id), versions)| (node_id.clone(), versions.clone()))
        .collect();
    Ok(FoldedContentState {
        manifest,
        series_versions,
        root_tree_hash: index.root_tree_hash,
        node_manifest_hash,
        node_manifest_root,
    })
}

pub(crate) async fn in_txn_spine_inputs(
    committed_table: deltalake::DeltaTable,
    uncommitted: Vec<OplogEntry>,
    local_pond_id: &str,
) -> Result<SpineInputs, StewardError> {
    let state = in_txn_content_state(committed_table, uncommitted, local_pond_id).await?;
    let manifest_bytes = encode_manifest(&state.manifest).map_err(StewardError::Content)?;
    Ok(SpineInputs {
        manifest_bytes,
        root_tree_hash: state.root_tree_hash,
        node_manifest_hash: state.node_manifest_hash,
        node_manifest_root: state.node_manifest_root,
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
    // Node metadata per node, seeded from the prior manifest and replaced for
    // every node this transaction touched.  Carried alongside `child_hash`
    // because a metadata-only change leaves the content hash untouched.
    let mut child_versions: HashMap<String, Vec<VersionMeta>> = HashMap::new();
    let mut etype_of: HashMap<String, EntryType> = HashMap::new();
    let mut parent_of: HashMap<String, String> = HashMap::new();
    let mut dir_children: HashMap<String, Vec<ChildLite>> = HashMap::new();
    for e in &prior {
        let _ = child_hash.insert(e.node_id.clone(), e.child_hash);
        let _ = child_versions.insert(e.node_id.clone(), e.versions.clone());
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
    let mut series_new: HashMap<String, BTreeMap<i64, SeriesVersionData>> = HashMap::new();
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
                let data = series_version_data(
                    row.version,
                    row.timestamp,
                    &row.blake3,
                    row.content.clone(),
                    row.min_event_time,
                    row.max_event_time,
                    row.extended_attributes.as_ref(),
                    &row.logical_leaf_hash,
                    row.logical_count,
                    &row.series_schema_fingerprint,
                    row.size,
                    &node,
                )?;
                let _ = series_new
                    .entry(node.clone())
                    .or_default()
                    .insert(row.version, data);
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
        let _ = child_versions.insert(
            node.clone(),
            vec![version_meta(
                row.timestamp,
                row.min_event_time,
                row.max_event_time,
                row.extended_attributes.as_ref(),
            )],
        );
    }

    // New content hash of every touched series: its committed version blobs
    // followed by this transaction's appended versions, pruned by range
    // containment and ordered oldest content first, exactly as [`fold_rows`]
    // does, then folded into one watertown.series.v1 manifest.
    for (node, appended) in &series_new {
        let (mut versions, mut ranges) =
            read_series_committed(committed_table.clone(), local_pond_id, node).await?;
        for (version, data) in appended {
            let _ = versions.insert(*version, data.clone());
        }
        // This transaction's rows shadow the committed rows of the same version.
        if let Some(new_ranges) = series_ranges.get(node) {
            ranges.retain(|(v, _)| !new_ranges.iter().any(|(nv, _)| nv == v));
            ranges.extend(new_ranges.iter().copied());
        }
        let ordered: Vec<SeriesVersionData> = live_series_versions(&ranges)
            .into_iter()
            .filter_map(|version| versions.get(&version).cloned())
            .collect();
        let entry_type = *etype_of.get(node).ok_or_else(|| {
            StewardError::DeltaLake(format!(
                "incremental fold: series node {node} has no known entry type"
            ))
        })?;
        let (manifest, meta) = build_series_manifest(entry_type, &ordered)?;
        let _ = child_hash.insert(node.clone(), manifest.hash());
        let _ = child_versions.insert(node.clone(), vec![meta]);
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
            let versions = child_versions
                .get(&kid.node_id)
                .cloned()
                .unwrap_or_default();
            tree_entries.push(TreeEntry::new(kid.name, kid.entry_type, *ch, versions));
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
    manifest.push(ManifestEntry::bare(
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
                child_versions
                    .get(&kid.node_id)
                    .cloned()
                    .unwrap_or_default(),
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
) -> Result<(BTreeMap<i64, SeriesVersionData>, Vec<(i64, CollapseRange)>), StewardError> {
    let ctx = SessionContext::new();
    let _previous = ctx
        .register_table("series_live", Arc::new(table))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let sql = format!(
        "SELECT version, timestamp, blake3, content, collapsed_from, collapsed_through, \
         min_event_time, max_event_time, extended_attributes, logical_leaf_hash, \
         logical_count, series_schema_fingerprint, size FROM series_live \
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
    let node_desc = format!("{pond_id}/{node_id}");
    let mut versions: BTreeMap<i64, SeriesVersionData> = BTreeMap::new();
    for row in rows {
        let version = row.version;
        let data = series_version_data(
            version,
            row.timestamp,
            &row.blake3,
            row.content,
            row.min_event_time,
            row.max_event_time,
            row.extended_attributes.as_ref(),
            &row.logical_leaf_hash,
            row.logical_count,
            &row.series_schema_fingerprint,
            row.size,
            &node_desc,
        )?;
        let _ = versions.insert(version, data);
    }
    Ok((versions, ranges))
}

/// Fetch **one** already-known-live series version's inline content, by
/// `(pond_id, node_id, version)`, straight from that one Oplog row --
/// never the whole series' `content` column read into memory in one batch
/// the way [`read_series_committed`] does.
///
/// Returns `None` when that version's row carries no inline `content` --
/// it was externalized to `_large_files`, so its bytes must instead be
/// streamed from there by [`SeriesVersionData::blob_hash`] -- never as a
/// signal that the version itself is missing (callers already know it
/// exists from a prior metadata-only ordered read, e.g.
/// [`read_series_live_metadata_ordered`], and use `None` exactly to decide
/// "stream externally" vs "use these inline bytes").
///
/// This is `crate::pack_maintenance`'s bounded per-leaf content source: a
/// real repack fetches metadata for every live version up front (bounded,
/// tiny), then calls this once per leaf, immediately before that leaf's
/// bytes are streamed into the pack, so at most one leaf's inline content
/// is ever held in memory at a time -- never a whole series' worth
/// (finding 2, `docs/logical-series-identity-design.md`'s pack-maintenance
/// memory-boundedness requirement).
///
/// # Errors
///
/// Returns an error if the query fails, if the row cannot be deserialized,
/// or if more than one row shares this `(pond_id, node_id, version)` key
/// (a corrupt/duplicated commit -- never silently resolved by picking one).
pub(crate) async fn read_series_version_inline_content(
    table: deltalake::DeltaTable,
    pond_id: &str,
    node_id: &str,
    version: i64,
) -> Result<Option<Vec<u8>>, StewardError> {
    let ctx = SessionContext::new();
    let _previous = ctx
        .register_table("series_live", Arc::new(table))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let sql = format!(
        "SELECT content FROM series_live WHERE pond_id = '{pond_id}' AND node_id = '{node_id}' \
         AND version = {version}",
    );
    let batches = ctx
        .sql(&sql)
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    #[derive(serde::Deserialize)]
    struct InlineContentRow {
        content: Option<Vec<u8>>,
    }
    let mut rows: Vec<InlineContentRow> = Vec::new();
    for batch in &batches {
        let parsed: Vec<InlineContentRow> = serde_arrow::from_record_batch(batch)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
        rows.extend(parsed);
    }
    if rows.len() > 1 {
        return Err(StewardError::DeltaLake(format!(
            "node {pond_id}/{node_id} version {version}: {} rows share one (pond_id, node_id, \
             version) key (expected at most one)",
            rows.len()
        )));
    }
    Ok(rows.into_iter().next().and_then(|r| r.content))
}

/// Read one series node's current *live* versions' identity/bookkeeping
/// metadata only, oldest first -- everything [`build_series_manifest`] and
/// pack-maintenance candidacy need (`logical_leaf_hash`, `logical_count`,
/// `series_schema_fingerprint`, event-time bounds, extended attributes,
/// persisted `size`) *without* selecting or deserializing the row's
/// (potentially large) inline `content` column at all.
///
/// This is the metadata-only counterpart to [`read_series_committed`]:
/// `crate::pack_maintenance`'s discovery (shared by dry-run and a real run)
/// uses this so surveying every over-threshold series in a pond never reads,
/// decodes, or buffers a single byte of any series' actual payload -- a real
/// repack instead fetches one leaf's inline content at a time, only for the
/// one leaf it is about to stream, via
/// [`read_series_version_inline_content`].
///
/// Every [`SeriesVersionData::content`] returned here is `None` regardless of
/// whether the row's content was actually inline or externalized: nothing
/// this feeds ([`build_series_manifest`], [`current_pack_fanout`]) reads
/// `content` at all, so leaving it unset is correct, not merely convenient.
///
/// # Errors
///
/// Returns an error if the series rows cannot be read or deserialized.
pub(crate) async fn read_series_live_metadata_ordered(
    table: deltalake::DeltaTable,
    pond_id: &str,
    node_id: &str,
) -> Result<Vec<SeriesVersionData>, StewardError> {
    let ctx = SessionContext::new();
    let _previous = ctx
        .register_table("series_live", Arc::new(table))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let sql = format!(
        "SELECT version, timestamp, blake3, collapsed_from, collapsed_through, \
         min_event_time, max_event_time, extended_attributes, logical_leaf_hash, \
         logical_count, series_schema_fingerprint, size FROM series_live \
         WHERE pond_id = '{pond_id}' AND node_id = '{node_id}' ORDER BY version",
    );
    let batches = ctx
        .sql(&sql)
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let mut rows: Vec<SeriesVersionMetaRow> = Vec::new();
    for batch in &batches {
        let parsed: Vec<SeriesVersionMetaRow> = serde_arrow::from_record_batch(batch)
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
    let node_desc = format!("{pond_id}/{node_id}");
    let mut versions: BTreeMap<i64, SeriesVersionData> = BTreeMap::new();
    for row in rows {
        let version = row.version;
        let data = series_version_data(
            version,
            row.timestamp,
            &row.blake3,
            None,
            row.min_event_time,
            row.max_event_time,
            row.extended_attributes.as_ref(),
            &row.logical_leaf_hash,
            row.logical_count,
            &row.series_schema_fingerprint,
            row.size,
            &node_desc,
        )?;
        let _ = versions.insert(version, data);
    }
    Ok(live_series_versions(&ranges)
        .into_iter()
        .filter_map(|version| versions.get(&version).cloned())
        .collect())
}

/// One committed series version row's identity/bookkeeping metadata, exactly
/// [`SeriesVersionRow`] minus the inline `content` column -- see
/// [`read_series_live_metadata_ordered`].
#[derive(serde::Deserialize)]
struct SeriesVersionMetaRow {
    version: i64,
    timestamp: i64,
    blake3: Option<String>,
    collapsed_from: Option<i64>,
    collapsed_through: Option<i64>,
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    extended_attributes: Option<String>,
    logical_leaf_hash: Option<String>,
    logical_count: Option<i64>,
    series_schema_fingerprint: Option<String>,
    size: Option<i64>,
}

/// One committed series version row: its version and the fields
/// [`row_blob_hash`] needs, plus the collapse range columns, the node
/// metadata a replica cannot recompute, and the v2 logical-series identity
/// columns [`series_version_data`] parses.
#[derive(serde::Deserialize)]
struct SeriesVersionRow {
    version: i64,
    timestamp: i64,
    blake3: Option<String>,
    content: Option<Vec<u8>>,
    collapsed_from: Option<i64>,
    collapsed_through: Option<i64>,
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    extended_attributes: Option<String>,
    logical_leaf_hash: Option<String>,
    logical_count: Option<i64>,
    series_schema_fingerprint: Option<String>,
    size: Option<i64>,
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
    log_tip_hash(last)
}

fn log_tip_hash(bytes: &[u8]) -> Result<Option<ObjectHash>, StewardError> {
    // A migration freeze must authenticate an old source tip without
    // interpreting its legacy commit/tree schema. Old writers still need to
    // be stopped separately because they do not honor the freeze marker.
    if bytes.starts_with(b"dp.commit.3\n") {
        return Ok(Some(ObjectHash::of_bytes(bytes)));
    }
    let commit = Commit::decode(bytes)
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
        ContentModelVersion::LogicalSeriesV2,
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
/// from `node_id` to its [`ManifestEntry`]; a map from each series `node_id`
/// to its ordered version blob hashes (v1 physical-blob identity); and a map
/// from each series `node_id` to its ordered v2 logical leaf hashes (`docs/
/// logical-series-identity-design.md`), used by native v2 materialization to
/// find the suffix of leaves a source's fetched manifest still needs to
/// write.
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
        HashMap<String, Vec<ObjectHash>>,
    ),
    StewardError,
> {
    let table = ship.data_persistence().table().clone();
    let index = match build_content_tree_for_table(table, pond_id).await {
        Ok(index) => index,
        // A foreign pond with no root row yet: first import, empty target.
        Err(StewardError::DeltaLake(msg)) if msg.contains("no root directory row") => {
            return Ok((HashMap::new(), HashMap::new(), HashMap::new()));
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
    let series_leaves = index
        .series_leaf_hashes
        .into_iter()
        .filter(|((pond, _node_id), _leaves)| pond == pond_id)
        .map(|((_pond, node_id), leaves)| (node_id, leaves))
        .collect();
    Ok((by_id, series, series_leaves))
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

/// Build the "initial" whole-range identity pack index for one folded v2
/// series, directly from its already-persisted rows -- no payload bytes are
/// read, decoded, concatenated, or re-encoded, and no new physical object is
/// minted.
///
/// This replaces the earlier production path that rebuilt/reuploaded an
/// entire series as one in-memory object under a `u64::MAX` layout cap
/// (quadratic remote storage for a series that grows by repeated small
/// appends, since every push re-encoded and republished everything again).
/// The native `Oplog` already stores, per append, the physical blob hash
/// ([`SeriesVersionData::blob_hash`]) the ordinary content push already
/// publishes (inline or external, [`super::content_tree::hash_child`]'s
/// `record_blob` call) alongside the persisted logical leaf hash/count/
/// schema/bounds/attrs
/// (`docs/logical-series-identity-design.md`'s persisted-leaf invariant).
/// The pack index is therefore built by pairing each leaf-bearing version's
/// already-published physical object with its own persisted per-leaf
/// metadata -- the exact "one-object-per-append" physical stream, never
/// merged or re-split.
///
/// A series with `leaf_count() == 0` needs no cover
/// ([`sync_store::content::select_exact_cover`] already special-cases an
/// empty series) and returns `Ok(None)`.
///
/// Deterministic and idempotent: called with the same persisted
/// [`SeriesPackMaterial`], this always produces byte-identical
/// [`PackIndex`] encodings, so it is safe to call repeatedly (on every push,
/// or on-demand when a local `pond://` source is asked for a series it has
/// not yet advertised) without ever diverging. Shared by the remote
/// publication path ([`publish_initial_series_packs`]) and
/// [`crate::content_source::LocalPondSource`]'s on-demand materialization
/// of `data/_packs/series=<hex>` for an unpushed local pond.
///
/// # Errors
///
/// Returns an error if a leaf-bearing version's canonical logical
/// attributes cannot be re-encoded, if constructing the full-range Merkle
/// proof or the [`PackIndex`] itself is rejected (which would mean this
/// pond's own persisted rows disagree with the manifest it just folded from
/// those same rows -- an internal bug, not user error), or if the freshly
/// built pack fails its own self-check against `material.manifest`.
pub(crate) fn build_initial_pack_index(
    material: &SeriesPackMaterial,
) -> Result<Option<sync_store::content::PackIndex>, StewardError> {
    if !matches!(
        material.entry_type,
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
    ) {
        return Err(StewardError::DeltaLake(format!(
            "series pack material carries an unexpected entry type {:?}",
            material.entry_type
        )));
    }
    if material.manifest.leaf_count() == 0 {
        return Ok(None);
    }
    let leaf_versions: Vec<&SeriesVersionData> = material
        .versions
        .iter()
        .filter(|v| v.logical_leaf_hash.is_some())
        .collect();

    let mut whole_series_leaf_hashes = Vec::with_capacity(leaf_versions.len());
    let mut physical_object_hashes = Vec::with_capacity(leaf_versions.len());
    let mut leaf_descriptors = Vec::with_capacity(leaf_versions.len());
    let mut physical_byte_count: u64 = 0;
    for v in &leaf_versions {
        let leaf_hash = v.logical_leaf_hash.ok_or_else(|| {
            StewardError::Content(
                "internal: a version filtered for Some(logical_leaf_hash) above lost it -- \
                 corrupt SeriesPackMaterial"
                    .to_string(),
            )
        })?;
        let logical_count = v.logical_count.ok_or_else(|| {
            StewardError::Content(
                "leaf-bearing series version has a logical_leaf_hash but no logical_count"
                    .to_string(),
            )
        })?;
        let logical_count = u64::try_from(logical_count).map_err(|_| {
            StewardError::Content("series version logical_count is negative".to_string())
        })?;
        let attrs = canonical_leaf_attributes(v)?;
        let descriptor = match material.manifest.revision() {
            sync_store::content::SeriesManifestRevision::V1 => {
                if material.entry_type == EntryType::TablePhysicalSeries
                    && v.schema_fingerprint != material.manifest.schema_fingerprint()
                {
                    return Err(StewardError::Content(format!(
                        "v1 table manifest's homogeneous schema fingerprint {:?} does not match \
                         leaf fingerprint {:?}",
                        material.manifest.schema_fingerprint(),
                        v.schema_fingerprint
                    )));
                }
                sync_store::content::PackLeafDescriptor::new(
                    logical_count,
                    v.meta.min_event_time,
                    v.meta.max_event_time,
                    attrs,
                )
            }
            sync_store::content::SeriesManifestRevision::V2 => {
                sync_store::content::PackLeafDescriptor::new_with_schema(
                    logical_count,
                    v.schema_fingerprint,
                    v.meta.min_event_time,
                    v.meta.max_event_time,
                    attrs,
                )
            }
        }
        .map_err(StewardError::Content)?;
        whole_series_leaf_hashes.push(leaf_hash);
        physical_object_hashes.push(v.blob_hash);
        leaf_descriptors.push(descriptor);
        physical_byte_count = physical_byte_count
            .checked_add(v.blob_size)
            .ok_or_else(|| {
                StewardError::Content("series physical_byte_count aggregate overflow".to_string())
            })?;
    }

    let total_leaf_count = whole_series_leaf_hashes.len() as u64;
    let range_proof = sync_store::content::generate_range_proof(
        &whole_series_leaf_hashes,
        0,
        leaf_versions.len(),
    )
    .map_err(StewardError::Content)?;
    let range_root = material.manifest.leaf_merkle_root();

    let pack = match material.manifest.revision() {
        sync_store::content::SeriesManifestRevision::V1 => sync_store::content::PackIndex::new(
            material.series_hash,
            0,
            total_leaf_count,
            total_leaf_count,
            range_root,
            range_proof,
            physical_object_hashes,
            material.manifest.logical_count(),
            physical_byte_count,
            leaf_descriptors,
        ),
        sync_store::content::SeriesManifestRevision::V2 => sync_store::content::PackIndex::new_v2(
            material.series_hash,
            0,
            total_leaf_count,
            total_leaf_count,
            range_root,
            range_proof,
            physical_object_hashes,
            material.manifest.logical_count(),
            physical_byte_count,
            leaf_descriptors,
        ),
    }
    .map_err(StewardError::Content)?;

    // Self-check before ever handing this pack to a publisher: a pack built
    // from this pond's own just-folded rows must verify against the
    // manifest those same rows just folded to, or the persisted state and
    // the fold disagree -- an internal bug that must not be published.
    sync_store::content::verify_pack_against_manifest(
        material.series_hash,
        &material.manifest,
        &pack,
        &whole_series_leaf_hashes,
    )
    .map_err(StewardError::Content)?;

    Ok(Some(pack))
}

/// Build and publish one whole-range "initial" identity pack for every v2
/// series captured in `materialized.series_material`
/// (`docs/logical-series-identity-design.md`).
///
/// A freshly-folded `watertown.series.v1` manifest is otherwise unfetchable the
/// moment it is pushed: the dual reader
/// (`crate::content_pull::fetch_series_v2`) requires an exact pack cover
/// before it will trust any series content, and nothing else in this
/// codebase publishes one. Each pack is minted directly from persisted rows
/// by [`build_initial_pack_index`] -- no payload bytes are read, decoded, or
/// re-encoded, and no new physical object is written; the pack instead
/// advertises the exact per-version physical objects the ordinary content
/// push already published (inline or external). Call this only after that
/// ordinary push has durably landed those objects on `remote` (blobs-first,
/// index-last applies across the whole push, not only within one pack).
///
/// `known_present` names the physical object hashes this same push already
/// durably wrote (the just-committed inline objects and streamed external
/// blobs) -- passed through to
/// [`sync_store::ContentRemote::publish_pack_with_known_present`] so
/// publishing these packs never re-probes an object this push just proved
/// present with its own write, only objects it did not itself just write
/// (item 3, `docs/logical-series-identity-design.md`).
///
/// # Errors
///
/// Returns an error if [`build_initial_pack_index`] fails for any series, or
/// if publishing the pack to `remote` fails (including when a physical
/// object the pack names is not actually present on `remote` yet -- which
/// would mean this was called before the ordinary push completed).
pub(crate) async fn publish_initial_series_packs(
    remote: &sync_store::ContentRemote,
    series_material: &[SeriesPackMaterial],
    known_present: &HashSet<ObjectHash>,
) -> Result<usize, StewardError> {
    let mut published = 0usize;
    for material in series_material {
        let Some(pack) = build_initial_pack_index(material)? else {
            continue;
        };
        let _ = remote
            .publish_pack_with_known_present(material.series_hash, &pack, &[], known_present)
            .await
            .map_err(|e| StewardError::Content(format!("publish initial series pack: {e}")))?;
        published += 1;
    }
    Ok(published)
}

/// This version's `logical_attributes`, re-encoded canonically (see
/// [`encode_canonical_attributes`]), or `None` when the version carries no
/// extended attributes -- matching [`sync_store::content::FileLeafInput`]/
/// [`TableLeafInput`]'s absent-vs-empty convention.
///
/// Also reused by `crate::pack_maintenance`'s repack paths (`pub(crate)`)
/// so a repack's own [`PackLeafDescriptor`](sync_store::content::PackLeafDescriptor)s
/// use the identical canonicalization the initial pack publication path
/// does.
pub(crate) fn canonical_leaf_attributes(
    version: &SeriesVersionData,
) -> Result<Option<Vec<u8>>, StewardError> {
    match &version.raw_extended_attributes {
        Some(json) => Ok(Some(encode_canonical_attributes(json).map_err(|e| {
            StewardError::Content(format!("logical attributes: {e}"))
        })?)),
        None => Ok(None),
    }
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
     extended_attributes, factory, format, txn_seq, pond_id, \
     arrow_cast(NULL, 'Binary') AS bao_outboard, collapsed_through, collapsed_from, \
     logical_leaf_hash, logical_count, series_schema_fingerprint \
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
    let mut series_versions: HashMap<NodeKey, BTreeMap<i64, SeriesVersionData>> = HashMap::new();
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
            let node_desc = format!("{}/{}", key.0, key.1);
            let data = series_version_data(
                row.version,
                row.timestamp,
                &row.blake3,
                row.content.clone(),
                row.min_event_time,
                row.max_event_time,
                row.extended_attributes.as_ref(),
                &row.logical_leaf_hash,
                row.logical_count,
                &row.series_schema_fingerprint,
                row.size,
                &node_desc,
            )?;
            let _ = series_versions
                .entry(key.clone())
                .or_default()
                .insert(row.version, data);
            series_ranges
                .entry(key.clone())
                .or_default()
                .push((row.version, CollapseRange::of(&row)));
        }

        let _ = latest.insert(
            key,
            NodeFacts {
                meta: version_meta(
                    row.timestamp,
                    row.min_event_time,
                    row.max_event_time,
                    row.extended_attributes.as_ref(),
                ),
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
    let mut series_live: HashMap<NodeKey, Vec<SeriesVersionData>> = HashMap::new();
    for (key, versions) in &mut series_versions {
        let ordered = match series_ranges.get(key) {
            Some(ranges) => live_series_versions(ranges),
            None => versions.keys().copied().collect(),
        };
        let items: Vec<SeriesVersionData> = ordered
            .into_iter()
            .filter_map(|version| versions.remove(&version))
            .collect();
        let _ = series_live.insert(key.clone(), items);
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

    let series_leaf_hashes = series_versions
        .iter()
        .map(|(key, versions)| {
            (
                key.clone(),
                versions
                    .iter()
                    .filter_map(|v| v.logical_leaf_hash)
                    .collect(),
            )
        })
        .collect();
    let series_version_hashes = series_versions
        .into_iter()
        .map(|(key, versions)| (key, versions.iter().map(|v| v.blob_hash).collect()))
        .collect();

    Ok(ContentTreeIndex {
        root_tree_hash,
        root_key,
        dirs,
        series_versions: series_version_hashes,
        series_leaf_hashes,
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

/// Canonicalize an extended-attributes JSON object so two ponds holding the
/// same attributes serialize them identically.
///
/// The stored form comes from `serde_json` over a `HashMap`, whose key order is
/// nondeterministic. That is harmless while the string is only ever read back
/// locally, but a directory entry *hashes* it: if a source and its mirror
/// emitted different key orders for equal attributes, their `tree_hash` would
/// never converge and every pull would rewrite the series forever. Re-encoding
/// through a `BTreeMap` sorts the keys and makes the encoding canonical.
///
/// Anything that does not parse as a flat JSON object is passed through
/// verbatim -- an unrecognized shape is still node state and must not be lost.
fn canonical_attributes(attrs: Option<&String>) -> Option<String> {
    let raw = attrs?;
    match serde_json::from_str::<BTreeMap<String, String>>(raw) {
        Ok(sorted) => serde_json::to_string(&sorted)
            .ok()
            .or_else(|| Some(raw.clone())),
        Err(_) => Some(raw.clone()),
    }
}

/// Extract the node metadata a replica cannot recompute from content bytes.
fn version_meta(
    timestamp: i64,
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    extended_attributes: Option<&String>,
) -> VersionMeta {
    VersionMeta {
        timestamp: Some(timestamp),
        min_event_time,
        max_event_time,
        extended_attributes: canonical_attributes(extended_attributes),
    }
}

/// Parse a persisted hex `ObjectHash` column, erroring loudly on malformed
/// hex rather than silently treating it as absent.
///
/// `logical_leaf_hash` and `series_schema_fingerprint` are written by exactly
/// one code path (`tlogfs::series_identity::stamp_logical_leaf`) that always
/// produces valid hex, so a decode failure here means the persisted row is
/// corrupt -- a `None` fallback would silently drop a leaf from the series
/// identity, which is exactly the class of bug
/// `docs/logical-series-identity-design.md` requires being loud about.
fn parse_optional_object_hash(
    hex: Option<&str>,
    field: &str,
    node_desc: &str,
) -> Result<Option<ObjectHash>, StewardError> {
    hex.map(|s| {
        ObjectHash::from_hex(s).map_err(|e| {
            StewardError::DeltaLake(format!("node {node_desc}: invalid {field} hex {s:?}: {e}"))
        })
    })
    .transpose()
}

/// Build one series version's [`SeriesVersionData`] from its row's scalar
/// fields. Shared by the full fold ([`fold_rows`]) and the incremental fold
/// ([`incremental_spine_inputs`], [`read_series_committed`]) so a row's v2
/// fields are parsed identically everywhere.
#[allow(clippy::too_many_arguments)]
fn series_version_data(
    version: i64,
    timestamp: i64,
    blake3: &Option<String>,
    content: Option<Vec<u8>>,
    min_event_time: Option<i64>,
    max_event_time: Option<i64>,
    extended_attributes: Option<&String>,
    logical_leaf_hash: &Option<String>,
    logical_count: Option<i64>,
    series_schema_fingerprint: &Option<String>,
    size: Option<i64>,
    node_desc: &str,
) -> Result<SeriesVersionData, StewardError> {
    let blob_hash = row_blob_hash(blake3, content.as_deref());
    let logical_leaf_hash =
        parse_optional_object_hash(logical_leaf_hash.as_deref(), "logical_leaf_hash", node_desc)?;
    let schema_fingerprint = parse_optional_object_hash(
        series_schema_fingerprint.as_deref(),
        "series_schema_fingerprint",
        node_desc,
    )?;
    // The row's own persisted `size` is authoritative for physical byte
    // count (needed by initial pack construction without reading payload
    // bytes); fall back to the inline content length only when `size` was
    // never recorded (older rows), never silently to zero for content that
    // is actually present.
    let blob_size = match size {
        Some(s) if s >= 0 => s as u64,
        Some(negative) => {
            return Err(StewardError::DeltaLake(format!(
                "node {node_desc}: negative persisted size {negative}"
            )));
        }
        None => content.as_deref().map(<[u8]>::len).unwrap_or(0) as u64,
    };
    Ok(SeriesVersionData {
        version,
        blob_hash,
        meta: version_meta(
            timestamp,
            min_event_time,
            max_event_time,
            extended_attributes,
        ),
        raw_extended_attributes: extended_attributes.cloned(),
        logical_leaf_hash,
        logical_count,
        schema_fingerprint,
        blob_size,
        content,
    })
}

/// Build a series node's `watertown.series.v1` [`SeriesManifest`] and its single
/// aggregate [`VersionMeta`] from its live versions in fold order (oldest
/// first).
///
/// A version with no persisted `logical_leaf_hash` contributes no leaf
/// (skipped when building the ordered leaf sequence and the
/// `logical_count`/event-bounds aggregates) ONLY when it is truly empty
/// (`blob_size == 0`) -- a genuine metadata-only touch. A NONEMPTY leafless
/// live version is corruption (BLOCKER 3, `docs/logical-series-identity-design.md`:
/// every public write path stamps `stamp_logical_leaf` before a nonempty
/// series row can commit) and is rejected with an error rather than
/// silently dropped, since silently skipping it would erase a real logical
/// leaf from the series' identity without a trace. Either way, a leafless
/// A leaf-bearing table version must carry its own
/// `series_schema_fingerprint`; table versions are not required to agree.
/// A metadata-only touch contributes no leaf and therefore no schema to the
/// manifest.
///
/// The returned `VersionMeta` -- the *one* series-level metadata record a v2
/// series tree entry carries (`docs/logical-series-identity-design.md`,
/// rather than one per physical version as v1 did) -- takes its timestamp
/// and attributes from the latest version that contributed a leaf (the
/// latest *logical* append), alongside the aggregate event bounds.
///
/// # Errors
///
/// Returns an error if `entry_type` is not a series type, if a nonempty
/// version has no `logical_leaf_hash` (corrupt row), if a leaf-bearing
/// version is missing its `logical_count`, if the aggregate `logical_count`
/// overflows `u64`, if a leaf-bearing table version has no schema
/// fingerprint, or if the assembled manifest fails
/// [`SeriesManifest::new_v2`]'s invariants.
pub(crate) fn build_series_manifest(
    entry_type: EntryType,
    versions: &[SeriesVersionData],
) -> Result<(SeriesManifest, VersionMeta), StewardError> {
    let payload_kind = match entry_type {
        EntryType::FilePhysicalSeries => PayloadKind::File,
        EntryType::TablePhysicalSeries => PayloadKind::Table,
        other => {
            return Err(StewardError::DeltaLake(format!(
                "build_series_manifest called for non-series entry type {other:?}"
            )));
        }
    };

    let mut leaf_hashes: Vec<ObjectHash> = Vec::new();
    let mut logical_count: u64 = 0;
    let mut min_event_time: Option<i64> = None;
    let mut max_event_time: Option<i64> = None;
    let mut latest_meta: Option<VersionMeta> = None;
    let mut latest_raw_attrs: Option<String> = None;

    for v in versions {
        let Some(leaf_hash) = v.logical_leaf_hash else {
            // A leafless live version is only legitimate when it is truly
            // empty (zero physical bytes): a metadata-only touch that
            // carries no content of its own. A nonempty leafless row means
            // `stamp_logical_leaf` was bypassed or failed silently upstream
            // -- BLOCKER 3 requires this be treated as corruption here, not
            // silently skipped, since silently skipping it would drop a
            // real logical leaf from the series' identity without a trace.
            if v.blob_size > 0 {
                return Err(StewardError::DeltaLake(format!(
                    "series version at timestamp {:?} is nonempty ({} bytes) but has no \
                     logical_leaf_hash -- corrupt row (persisted-leaf invariant violated)",
                    v.meta.timestamp, v.blob_size
                )));
            }
            continue;
        };
        match payload_kind {
            PayloadKind::Table if v.schema_fingerprint.is_none() => {
                return Err(StewardError::DeltaLake(
                    "leaf-bearing table series version has no series_schema_fingerprint"
                        .to_string(),
                ));
            }
            PayloadKind::File if v.schema_fingerprint.is_some() => {
                return Err(StewardError::DeltaLake(
                    "leaf-bearing file series version must not carry a series_schema_fingerprint"
                        .to_string(),
                ));
            }
            _ => {}
        }
        leaf_hashes.push(leaf_hash);
        let count = v.logical_count.ok_or_else(|| {
            StewardError::DeltaLake(
                "series version has a logical_leaf_hash but no logical_count".to_string(),
            )
        })?;
        let count = u64::try_from(count).map_err(|_| {
            StewardError::DeltaLake("series version has a negative logical_count".to_string())
        })?;
        logical_count = logical_count.checked_add(count).ok_or_else(|| {
            StewardError::DeltaLake("series logical_count aggregate overflow".to_string())
        })?;
        if let Some(min) = v.meta.min_event_time {
            min_event_time = Some(min_event_time.map_or(min, |cur: i64| cur.min(min)));
        }
        if let Some(max) = v.meta.max_event_time {
            max_event_time = Some(max_event_time.map_or(max, |cur: i64| cur.max(max)));
        }
        latest_meta = Some(v.meta.clone());
        latest_raw_attrs = v.raw_extended_attributes.clone();
    }

    let leaf_merkle_root = merkle_root(&leaf_hashes);
    let logical_attributes = match &latest_raw_attrs {
        Some(json) => Some(
            encode_canonical_attributes(json)
                .map_err(|e| StewardError::DeltaLake(format!("logical attributes: {e}")))?,
        ),
        None => None,
    };

    let manifest = SeriesManifest::new_v2(
        payload_kind,
        logical_count,
        leaf_hashes.len() as u64,
        min_event_time,
        max_event_time,
        logical_attributes,
        leaf_merkle_root,
    )
    .map_err(StewardError::DeltaLake)?;

    let meta = VersionMeta {
        timestamp: latest_meta
            .as_ref()
            .and_then(|m| m.timestamp)
            .or_else(|| versions.last().and_then(|v| v.meta.timestamp)),
        min_event_time,
        max_event_time,
        extended_attributes: latest_meta.and_then(|m| m.extended_attributes),
    };

    Ok((manifest, meta))
}

/// Fold one directory (by key) into its recursive [`tree_hash`], recording its
/// child list into `dirs` for later comparison.  When `sink` is `Some`, the
/// encoded tree object bytes (and, via `hash_child`, descendant object bytes)
/// are recorded for materialization.
#[allow(clippy::too_many_arguments)]
fn hash_directory(
    key: &NodeKey,
    latest: &HashMap<NodeKey, NodeFacts>,
    series_versions: &HashMap<NodeKey, Vec<SeriesVersionData>>,
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
        let (child_hash, versions) = hash_child(
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
            versions: versions.clone(),
        });
        tree_entries.push(TreeEntry::new(
            entry.name,
            entry.entry_type,
            child_hash,
            versions,
        ));
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
    series_versions: &HashMap<NodeKey, Vec<SeriesVersionData>>,
    memo: &mut HashMap<NodeKey, ObjectHash>,
    in_progress: &mut Vec<NodeKey>,
    dirs: &mut HashMap<NodeKey, Vec<ChildRef>>,
    sink: Option<&mut MaterializedObjects>,
) -> Result<(ObjectHash, Vec<VersionMeta>), StewardError> {
    match entry_type {
        EntryType::DirectoryPhysical => {
            // A directory carries no metadata of its own: its state is the
            // subtree its tree_hash already commits to.
            let hash = hash_directory(key, latest, series_versions, memo, in_progress, dirs, sink)?;
            Ok((hash, Vec::new()))
        }
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
            let versions = series_versions.get(key).ok_or_else(|| {
                StewardError::DeltaLake(format!("missing series node {}/{}", key.0, key.1))
            })?;
            let (manifest, meta) = build_series_manifest(entry_type, versions)?;
            let hash = manifest.hash();
            if let Some(sink) = sink {
                // The v2 watertown.series.v1 manifest object, plus each version's
                // physical blob: small versions inline, large (externalized)
                // versions by hash (D7). Physical blobs stay available for
                // initial pack publication/fetch even though the series'
                // identity is now the manifest hash, not a hash over these
                // blobs (`docs/logical-series-identity-design.md`).
                sink.put_inline(hash, manifest.encode());
                for v in versions.iter() {
                    record_blob(sink, v.blob_hash, v.content.as_deref());
                }
                // Capture what `publish_initial_series_packs` needs to mint
                // this series' whole-range identity pack, so the dual
                // reader can fetch it the moment it is pushed.
                // `build_initial_pack_index` (the sole reader of
                // `SeriesPackMaterial::versions`) never reads a version's
                // inline `content` -- it was already recorded above via
                // `record_blob` for materialization -- so drop it here
                // rather than cloning every inline series payload a second
                // time into this side table (release blocker item 3): an
                // in-memory `push`/`pond://` fold would otherwise briefly
                // hold two copies of all inline series content at once.
                sink.series_material.push(SeriesPackMaterial {
                    series_hash: hash,
                    entry_type,
                    manifest: manifest.clone(),
                    versions: versions
                        .iter()
                        .cloned()
                        .map(|v| SeriesVersionData { content: None, ..v })
                        .collect(),
                });
            }
            Ok((hash, vec![meta]))
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
            Ok((hash, vec![facts.meta.clone()]))
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
            Ok((hash, vec![facts.meta.clone()]))
        }
        // Single-version physical file or table: the version blob hash.
        EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
            let facts = leaf_facts(key, latest)?;
            let hash = row_blob_hash(&facts.blake3, facts.content.as_deref());
            if let Some(sink) = sink {
                record_blob(sink, hash, facts.content.as_deref());
            }
            Ok((hash, vec![facts.meta.clone()]))
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

    #[test]
    fn legacy_commit_tip_uses_the_raw_object_hash_without_decoding() {
        let legacy = b"dp.commit.3\nintentionally-not-a-current-commit";
        assert_eq!(
            log_tip_hash(legacy).expect("legacy tip"),
            Some(ObjectHash::of_bytes(legacy))
        );
    }

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
            let content = format!("blob-for-version-{version}").into_bytes();
            let mut row =
                OplogEntry::new_small_file(series_id, version, version, content.clone(), 1);
            row.collapsed_from = from;
            row.collapsed_through = through;
            // BLOCKER 3: every nonempty live row must carry a valid logical
            // leaf hash/count -- these synthetic rows emulate the (now
            // production-unreachable, see `schema::tests` for why) tiered
            // collapse row shape purely to test `fold_rows`'s range-pruning
            // logic, so they must still satisfy the invariant
            // `build_series_manifest` now enforces.
            let hash = sync_store::content::file_leaf_hash(&content, None, None, None)
                .expect("file leaf hash");
            row.logical_leaf_hash = Some(hash.to_hex());
            row.logical_count = Some(content.len() as i64);
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

    #[tokio::test]
    async fn native_fold_emits_decodable_series_manifest_and_append_changes_root() {
        use tinyfs::{DirectoryEntry, EntryType, FileID};
        use tlogfs::schema::{ExtendedAttributes, encode_directory_entries};

        let pond = tinyfs::local_pond_uuid();
        let dir_id = FileID::root_for(pond);
        let series_id =
            FileID::new_in_partition(dir_id.part_id(), EntryType::FilePhysicalSeries, pond);

        let dir_content = encode_directory_entries(&[DirectoryEntry::new(
            "series".to_string(),
            series_id.node_id(),
            EntryType::FilePhysicalSeries,
            1,
        )])
        .expect("encode directory");
        let dir_row = OplogEntry::new_inline(dir_id, 1, 1, dir_content, 1);

        let mut attrs_v1 = ExtendedAttributes::default();
        _ = attrs_v1.set_timestamp_column("ts");
        let mut v1 =
            OplogEntry::new_file_series(series_id, 100, 1, b"aaaa".to_vec(), 10, 20, attrs_v1, 1);
        tlogfs::series_identity::stamp_logical_leaf(std::path::Path::new("/unused"), &mut v1)
            .await
            .expect("stamp v1");

        // Fold with only the first version: proves the manifest is
        // decodable and correct with a single leaf.
        let index_one = fold_rows(vec![dir_row.clone(), v1.clone()], &pond.to_string(), None)
            .expect("fold one version");
        let mut sink_one = MaterializedObjects::default();
        let index_one_sunk = fold_rows(
            vec![dir_row.clone(), v1.clone()],
            &pond.to_string(),
            Some(&mut sink_one),
        )
        .expect("fold one version with sink");
        assert_eq!(index_one.root_tree_hash, index_one_sunk.root_tree_hash);

        let root_key = (pond.to_string(), ROOT_UUID.to_string());
        let one_series_child = index_one_sunk
            .dirs
            .get(&root_key)
            .expect("root children")
            .iter()
            .find(|c| c.name == "series")
            .expect("series child ref");
        let manifest_bytes_one = sink_one
            .inline
            .get(&one_series_child.child_hash)
            .expect("manifest object materialized under its own hash");
        let manifest_one = SeriesManifest::decode(manifest_bytes_one)
            .expect("decode watertown.series.v1 manifest");
        assert_eq!(manifest_one.payload_kind(), PayloadKind::File);
        assert_eq!(manifest_one.logical_count(), 4, "4 bytes in v1");
        assert_eq!(manifest_one.leaf_count(), 1);
        assert_eq!(manifest_one.min_event_time(), Some(10));
        assert_eq!(manifest_one.max_event_time(), Some(20));
        assert_eq!(one_series_child.versions.len(), 1);
        assert_eq!(one_series_child.versions[0].timestamp, Some(100));

        // Now append a second version and fold again.
        let mut attrs_v2 = ExtendedAttributes::default();
        _ = attrs_v2.set_timestamp_column("ts");
        let mut v2 =
            OplogEntry::new_file_series(series_id, 200, 2, b"bbbbbb".to_vec(), 30, 40, attrs_v2, 2);
        tlogfs::series_identity::stamp_logical_leaf(std::path::Path::new("/unused"), &mut v2)
            .await
            .expect("stamp v2");

        let mut sink_two = MaterializedObjects::default();
        let index_two = fold_rows(
            vec![dir_row, v1, v2],
            &pond.to_string(),
            Some(&mut sink_two),
        )
        .expect("fold two versions with sink");

        assert_ne!(
            index_one_sunk.root_tree_hash, index_two.root_tree_hash,
            "appending a second logical leaf must change the root tree hash"
        );

        let two_series_child = index_two
            .dirs
            .get(&root_key)
            .expect("root children")
            .iter()
            .find(|c| c.name == "series")
            .expect("series child ref");
        assert_ne!(
            one_series_child.child_hash, two_series_child.child_hash,
            "the series manifest hash itself must change with the append"
        );
        let manifest_bytes_two = sink_two
            .inline
            .get(&two_series_child.child_hash)
            .expect("manifest object materialized under its own hash");
        let manifest_two = SeriesManifest::decode(manifest_bytes_two)
            .expect("decode watertown.series.v1 manifest");
        assert_eq!(manifest_two.logical_count(), 10, "4 + 6 bytes across both");
        assert_eq!(manifest_two.leaf_count(), 2);
        assert_eq!(
            manifest_two.min_event_time(),
            Some(10),
            "aggregate min must come from the earliest version"
        );
        assert_eq!(
            manifest_two.max_event_time(),
            Some(40),
            "aggregate max must come from the latest version"
        );

        // Design doc: one series-level VersionMeta per tree entry, not one
        // per physical row -- and it reflects the latest logical append.
        assert_eq!(two_series_child.versions.len(), 1);
        assert_eq!(
            two_series_child.versions[0].timestamp,
            Some(200),
            "aggregate VersionMeta.timestamp must be the latest append's timestamp"
        );
        assert_eq!(two_series_child.versions[0].min_event_time, Some(10));
        assert_eq!(two_series_child.versions[0].max_event_time, Some(40));
    }

    /// `build_series_manifest`'s aggregation must be a pure, repeatable
    /// function of its input rows: same versions in, byte-identical manifest
    /// and metadata out, every time. And per the design doc, an empty
    /// (metadata-only, no logical leaf) version in the middle of a series
    /// must be transparent to aggregation -- it contributes no leaf, no
    /// count, and must not become the "latest" version merely by appearing
    /// last in a slice; only leaf-bearing versions can be "the latest
    /// logical append".
    #[test]
    fn build_series_manifest_aggregation_is_deterministic_and_uses_latest_leaf_bearing_version() {
        let leaf_a = ObjectHash::of_bytes(b"leaf-a");
        let leaf_b = ObjectHash::of_bytes(b"leaf-b");

        let v_a = SeriesVersionData {
            version: 1,
            blob_hash: ObjectHash::of_bytes(b"blob-a"),
            content: None,
            meta: VersionMeta {
                timestamp: Some(100),
                min_event_time: Some(10),
                max_event_time: Some(20),
                extended_attributes: Some("{\"a\":\"1\"}".to_string()),
            },
            raw_extended_attributes: Some("{\"a\":\"1\"}".to_string()),
            logical_leaf_hash: Some(leaf_a),
            logical_count: Some(4),
            schema_fingerprint: None,
            blob_size: 4,
        };
        // A metadata-only version between the two real leaves: no leaf hash,
        // so it must not perturb the aggregate bounds or become "latest".
        let v_metadata_only = SeriesVersionData {
            version: 2,
            blob_hash: ObjectHash::of_bytes(b"blob-meta"),
            content: None,
            meta: VersionMeta {
                timestamp: Some(150),
                min_event_time: Some(9_999),
                max_event_time: Some(9_999),
                extended_attributes: Some("{\"a\":\"should-not-win\"}".to_string()),
            },
            raw_extended_attributes: Some("{\"a\":\"should-not-win\"}".to_string()),
            logical_leaf_hash: None,
            logical_count: None,
            schema_fingerprint: None,
            blob_size: 0,
        };
        let v_b = SeriesVersionData {
            version: 3,
            blob_hash: ObjectHash::of_bytes(b"blob-b"),
            content: None,
            meta: VersionMeta {
                timestamp: Some(200),
                min_event_time: Some(30),
                max_event_time: Some(40),
                extended_attributes: Some("{\"a\":\"2\"}".to_string()),
            },
            raw_extended_attributes: Some("{\"a\":\"2\"}".to_string()),
            logical_leaf_hash: Some(leaf_b),
            logical_count: Some(6),
            schema_fingerprint: None,
            blob_size: 6,
        };
        let versions = vec![v_a, v_metadata_only, v_b];

        let (manifest_1, meta_1) =
            build_series_manifest(EntryType::FilePhysicalSeries, &versions).expect("fold 1");
        let (manifest_2, meta_2) =
            build_series_manifest(EntryType::FilePhysicalSeries, &versions).expect("fold 2");

        assert_eq!(
            manifest_1.hash(),
            manifest_2.hash(),
            "aggregation must be deterministic across repeated calls"
        );
        assert_eq!(manifest_1.logical_count(), 10);
        assert_eq!(manifest_1.leaf_count(), 2);
        assert_eq!(manifest_1.min_event_time(), Some(10));
        assert_eq!(manifest_1.max_event_time(), Some(40));
        assert_eq!(
            meta_1.timestamp,
            Some(200),
            "latest LEAF-BEARING version's timestamp wins, not the metadata-only row's"
        );
        assert_eq!(meta_1.min_event_time, Some(10));
        assert_eq!(meta_1.max_event_time, Some(40));
        assert_eq!(meta_1.timestamp, meta_2.timestamp);
        assert_eq!(meta_1.extended_attributes, meta_2.extended_attributes);
    }

    /// A genuinely empty table series has no leaf schema to record. The v2
    /// manifest represents that state without inventing a global schema.
    #[test]
    fn build_series_manifest_represents_a_genuinely_empty_table_series() {
        let v_empty = SeriesVersionData {
            version: 4,
            blob_hash: ObjectHash::of_bytes(b""),
            content: Some(Vec::new()),
            meta: VersionMeta {
                timestamp: Some(100),
                min_event_time: None,
                max_event_time: None,
                extended_attributes: None,
            },
            raw_extended_attributes: None,
            logical_leaf_hash: None,
            logical_count: None,
            schema_fingerprint: None,
            blob_size: 0,
        };
        let versions = vec![v_empty];

        let (manifest, meta) =
            build_series_manifest(EntryType::TablePhysicalSeries, &versions).expect("fold");
        assert_eq!(manifest.payload_kind(), PayloadKind::Table);
        assert_eq!(
            manifest.revision(),
            sync_store::content::SeriesManifestRevision::V2
        );
        assert_eq!(manifest.schema_fingerprint(), None);
        assert_eq!(manifest.leaf_count(), 0);
        assert_eq!(manifest.logical_count(), 0);
        assert_eq!(meta.timestamp, Some(100));
    }

    #[test]
    fn build_series_manifest_accepts_heterogeneous_table_leaf_schemas() {
        let schema_a = ObjectHash::of_bytes(b"schema-a");
        let schema_b = ObjectHash::of_bytes(b"schema-b");
        let versions = vec![
            SeriesVersionData {
                version: 1,
                blob_hash: ObjectHash::of_bytes(b"blob-a"),
                content: None,
                meta: VersionMeta {
                    timestamp: Some(100),
                    min_event_time: Some(1),
                    max_event_time: Some(2),
                    extended_attributes: None,
                },
                raw_extended_attributes: None,
                logical_leaf_hash: Some(ObjectHash::of_bytes(b"leaf-a")),
                logical_count: Some(3),
                schema_fingerprint: Some(schema_a),
                blob_size: 10,
            },
            SeriesVersionData {
                version: 2,
                blob_hash: ObjectHash::of_bytes(b"blob-b"),
                content: None,
                meta: VersionMeta {
                    timestamp: Some(200),
                    min_event_time: Some(3),
                    max_event_time: Some(4),
                    extended_attributes: None,
                },
                raw_extended_attributes: None,
                logical_leaf_hash: Some(ObjectHash::of_bytes(b"leaf-b")),
                logical_count: Some(5),
                schema_fingerprint: Some(schema_b),
                blob_size: 20,
            },
        ];

        let (manifest, _) =
            build_series_manifest(EntryType::TablePhysicalSeries, &versions).expect("fold");
        assert_eq!(manifest.leaf_count(), 2);
        assert_eq!(manifest.logical_count(), 8);
        assert_eq!(manifest.schema_fingerprint(), None);

        let material = SeriesPackMaterial {
            series_hash: manifest.hash(),
            entry_type: EntryType::TablePhysicalSeries,
            manifest,
            versions,
        };
        let pack = build_initial_pack_index(&material)
            .expect("build initial pack")
            .expect("nonempty pack");
        assert_eq!(
            pack.leaf_descriptors()[0].schema_fingerprint(),
            Some(schema_a)
        );
        assert_eq!(
            pack.leaf_descriptors()[1].schema_fingerprint(),
            Some(schema_b)
        );
    }

    #[test]
    fn build_series_manifest_rejects_table_leaf_without_schema() {
        let version = SeriesVersionData {
            version: 1,
            blob_hash: ObjectHash::of_bytes(b"blob"),
            content: None,
            meta: VersionMeta {
                timestamp: Some(100),
                min_event_time: None,
                max_event_time: None,
                extended_attributes: None,
            },
            raw_extended_attributes: None,
            logical_leaf_hash: Some(ObjectHash::of_bytes(b"leaf")),
            logical_count: Some(1),
            schema_fingerprint: None,
            blob_size: 10,
        };
        let err = build_series_manifest(EntryType::TablePhysicalSeries, &[version])
            .expect_err("schema-less table leaf must fail");
        assert!(err.to_string().contains("series_schema_fingerprint"));
    }

    /// BLOCKER 3: a nonempty leafless live row (`blob_size > 0` but no
    /// `logical_leaf_hash`) must be treated as corruption and rejected, not
    /// silently skipped -- silently skipping it would erase a real logical
    /// leaf from the series' identity without a trace. Only a truly empty
    /// row (`blob_size == 0`, covered above) may remain leafless.
    #[test]
    fn build_series_manifest_rejects_nonempty_leafless_live_row_as_corruption() {
        let leaf_a = ObjectHash::of_bytes(b"leaf-a");
        let v_a = SeriesVersionData {
            version: 5,
            blob_hash: ObjectHash::of_bytes(b"blob-a"),
            content: None,
            meta: VersionMeta {
                timestamp: Some(100),
                min_event_time: Some(10),
                max_event_time: Some(20),
                extended_attributes: None,
            },
            raw_extended_attributes: None,
            logical_leaf_hash: Some(leaf_a),
            logical_count: Some(4),
            schema_fingerprint: None,
            blob_size: 4,
        };
        // Corrupt: physically nonempty (blob_size > 0) but never stamped --
        // this must never happen via any public write path after BLOCKER 3,
        // but the fold must still refuse to silently accept it if it does.
        let v_corrupt = SeriesVersionData {
            version: 6,
            blob_hash: ObjectHash::of_bytes(b"blob-corrupt"),
            content: None,
            meta: VersionMeta {
                timestamp: Some(150),
                min_event_time: None,
                max_event_time: None,
                extended_attributes: None,
            },
            raw_extended_attributes: None,
            logical_leaf_hash: None,
            logical_count: None,
            schema_fingerprint: None,
            blob_size: 128,
        };
        let versions = vec![v_a, v_corrupt];

        let err = build_series_manifest(EntryType::FilePhysicalSeries, &versions)
            .expect_err("nonempty leafless row must be rejected, not silently skipped");
        let message = err.to_string();
        assert!(
            message.contains("logical_leaf_hash") || message.contains("corrupt"),
            "error should describe the missing-leaf corruption, got: {message}"
        );
    }
}
