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
    Commit, ContentModelVersion, ContentObjectKind, ManifestChange, ManifestEntry,
    ManifestMapEditor, ManifestRecord, ManifestRecordChild, MerkleFrontier, ObjectDescriptor,
    ObjectHash, PackDescriptor, PayloadKind, Provenance, SeriesManifest, TreeEntry, VersionMeta,
    build_manifest_map, decode_manifest_root, encode_canonical_attributes, encode_manifest_root,
    encode_recipe, encode_tree, generate_append_range_proof, recipe_hash,
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
    pub inline: BTreeMap<ObjectHash, MaterializedInlineObject>,
    /// Large-blob hashes whose bytes transfer via the external path.
    pub external_blobs: BTreeSet<ObjectHash>,
    /// The node manifest object: its hash and bytes (Section 4.5).  Kept
    /// separate from `inline` because it carries the source's node_ids, so it
    /// is pond-specific and must not be counted as shareable content -- two
    /// ponds with identical content still have different manifests.  `None`
    /// only on a default-constructed value; a real fold always produces one.
    pub manifest: Option<(ObjectHash, Vec<u8>)>,
    /// Persistent identity-map root for this snapshot.
    pub manifest_root: Option<ObjectHash>,
    /// Complete identity records, populated only by explicit full
    /// materialization (initial publication, capsule build, or diagnostics).
    pub manifest_records: Vec<ManifestRecord>,
    /// Everything an explicit full materialization needs to mint one
    /// whole-range root pack per `watertown.series.v3` series, without
    /// re-walking the pond. Not itself a
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
/// Initial publication uses this to make the current complete series
/// independently fetchable. Ordinary append commits do not construct this
/// full material; they emit a linked suffix segment from only the new rows.
#[derive(Debug, Clone)]
pub(crate) struct SeriesPackMaterial {
    /// The series' own content address -- the `watertown.series.v3` manifest hash,
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

/// One inline content object with its kind carried from the typed fold site.
///
/// Payload bytes are never inspected to infer this value: arbitrary raw files
/// may legitimately begin with any Watertown wire-format magic prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedInlineObject {
    pub kinds: BTreeSet<ContentObjectKind>,
    pub bytes: Vec<u8>,
}

impl std::ops::Deref for MaterializedInlineObject {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl MaterializedObjects {
    /// Record an inline object (idempotent: re-recording a hash is a no-op).
    fn put_inline(
        &mut self,
        kind: ContentObjectKind,
        hash: ObjectHash,
        bytes: Vec<u8>,
    ) -> Result<(), StewardError> {
        match self.inline.entry(hash) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                let _ = entry.insert(MaterializedInlineObject {
                    kinds: BTreeSet::from([kind]),
                    bytes,
                });
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if entry.get().bytes != bytes {
                    return Err(StewardError::Content(format!(
                        "content hash {hash} was materialized with conflicting bytes"
                    )));
                }
                let _ = entry.get_mut().kinds.insert(kind);
            }
        }
        Ok(())
    }

    /// Record a large blob to transfer externally by hash.
    fn put_external(&mut self, hash: ObjectHash) {
        let _ = self.external_blobs.insert(hash);
    }

    /// Total number of distinct objects (inline, external, and the manifest).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inline.len() + self.external_blobs.len() + usize::from(self.manifest_root.is_some())
    }

    /// True when no objects were materialized.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inline.is_empty() && self.external_blobs.is_empty() && self.manifest_root.is_none()
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
    /// Per series node, ordered physical blob hashes retained for full-fold
    /// diagnostics and synthetic collapse-ordering tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub series_versions: HashMap<NodeKey, Vec<ObjectHash>>,
    /// Per series node, its ordered *logical leaf* hashes (ascending
    /// version, skipping leafless metadata-only rows) --
    /// `docs/logical-series-identity-design.md` v2 identity, distinct from
    /// `series_versions`' physical blob identity above. Full clone/rebuild and
    /// explicit diagnostics may compare this complete list. Ordinary
    /// incremental pulls instead compare the prior persisted
    /// [`sync_store::content::SeriesManifest`] frontier/count with the fetched
    /// suffix base and never build this retained-leaf list.
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
/// ([`incremental_spine_inputs_v2`]) so both compute the [`SeriesManifest`]
/// identically -- see [`build_series_manifest`].
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
    /// full-fold physical diagnostics.
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
    /// persisted on the row. Needed to compute `watertown.series.v3`'s
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

/// Build the persistent manifest-map records for an already-folded snapshot.
pub(crate) fn node_manifest_records(
    index: &ContentTreeIndex,
) -> Result<Vec<ManifestRecord>, StewardError> {
    let mut entries = node_manifest_entries(index)
        .into_iter()
        .map(|entry| (entry.node_id.clone(), entry))
        .collect::<HashMap<_, _>>();
    let mut records = Vec::with_capacity(entries.len());
    let local_pond = &index.root_key.0;
    for (node_id, entry) in entries.drain() {
        let children = if entry.entry_type == EntryType::DirectoryPhysical {
            index
                .dirs
                .get(&(local_pond.clone(), node_id.clone()))
                .into_iter()
                .flatten()
                .map(|child| {
                    ManifestRecordChild::new(
                        child.child_node_id.clone(),
                        child.name.clone(),
                        child.entry_type,
                    )
                })
                .collect()
        } else {
            Vec::new()
        };
        records.push(ManifestRecord::new(entry, children).map_err(StewardError::Content)?);
    }
    records.sort_by(|left, right| left.node_id().as_bytes().cmp(right.node_id().as_bytes()));
    Ok(records)
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
/// content) plus the content roots (`root_tree_hash`, persistent
/// `manifest_root`) and bounded publication delta that the commit needs.
///
/// Folding once here lets the guard write the index node and the authoritative
/// commit-log leaf atomically in the same transaction without re-scanning the
/// data table (design `docs/incremental-content-tree-design.md` Section 10,
/// step 4a).  The two reserved nodes are excluded from the fold, so writing
/// them never perturbs these roots.
pub(crate) struct SpineInputs {
    pub index_bytes: Vec<u8>,
    pub root_tree_hash: ObjectHash,
    pub manifest_root: ObjectHash,
    pub manifest_changes: Vec<ManifestChange>,
    pub introduced_objects: Vec<ObjectDescriptor>,
    pub introduced_packs: Vec<PackDescriptor>,
    pub object_bytes: BTreeMap<ObjectHash, Vec<u8>>,
    pub pack_bytes: BTreeMap<ObjectHash, Vec<u8>>,
}

/// Canonical full-fold view of one pond partition for explicit diagnostics and
/// optional incremental-fold cross-checking.
pub(crate) struct FoldedContentState {
    pub manifest_records: Vec<ManifestRecord>,
    pub root_tree_hash: ObjectHash,
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
    let manifest_records = node_manifest_records(&index)?;
    Ok(FoldedContentState {
        manifest_records,
        root_tree_hash: index.root_tree_hash,
    })
}

pub(crate) async fn in_txn_spine_inputs(
    committed_table: deltalake::DeltaTable,
    uncommitted: Vec<OplogEntry>,
    local_pond_id: &str,
) -> Result<SpineInputs, StewardError> {
    let mut rows = scan_live_rows(committed_table, true).await?;
    rows.extend(uncommitted);
    rows.sort_by(|a, b| {
        a.pond_id
            .cmp(&b.pond_id)
            .then_with(|| a.node_id.to_string().cmp(&b.node_id.to_string()))
            .then_with(|| a.version.cmp(&b.version))
    });
    let mut materialized = MaterializedObjects::default();
    let index = fold_rows(rows, local_pond_id, Some(&mut materialized))?;
    let records = node_manifest_records(&index)?;
    let (manifest_root, manifest_objects) =
        build_manifest_map(&records).map_err(StewardError::Content)?;
    let manifest_changes = records
        .into_iter()
        .map(|record| ManifestChange::new(None, Some(record)).map_err(StewardError::Content))
        .collect::<Result<Vec<_>, _>>()?;
    let mut introduced_objects = materialized
        .inline
        .iter()
        .flat_map(|(hash, object)| {
            object
                .kinds
                .iter()
                .copied()
                .map(|kind| ObjectDescriptor::new(*hash, kind))
        })
        .collect::<Vec<_>>();
    let mut object_bytes = materialized
        .inline
        .into_iter()
        .map(|(hash, object)| (hash, object.bytes))
        .collect::<BTreeMap<_, _>>();
    introduced_objects.extend(
        materialized
            .external_blobs
            .iter()
            .copied()
            .map(|hash| ObjectDescriptor::new(hash, ContentObjectKind::RawBlob)),
    );
    for (hash, bytes) in manifest_objects {
        let _ = object_bytes.insert(hash, bytes);
        introduced_objects.push(ObjectDescriptor::new(hash, ContentObjectKind::ManifestNode));
    }
    let mut pack_bytes = BTreeMap::new();
    let mut introduced_packs = Vec::new();
    for material in &materialized.series_material {
        if let Some(pack) = build_initial_pack_index(material)? {
            let bytes = pack.encode();
            let pack_hash = ObjectHash::of_bytes(&bytes);
            let _ = pack_bytes.insert(pack_hash, bytes);
            introduced_packs.push(PackDescriptor::new(material.series_hash, pack_hash));
        }
    }
    introduced_objects.sort_unstable();
    introduced_objects.dedup();
    introduced_packs.sort_unstable();
    introduced_packs.dedup();
    Ok(SpineInputs {
        index_bytes: encode_manifest_root(manifest_root),
        root_tree_hash: index.root_tree_hash,
        manifest_root,
        manifest_changes,
        introduced_objects,
        introduced_packs,
        object_bytes,
        pack_bytes,
    })
}

/// One node's live child listing, as recomputed incrementally.  A directory's
/// `tree_hash` is `encode_tree` over `(name, entry_type, child_hash)` for its
/// content children, so those three fields are all the incremental fold needs.
#[derive(Clone)]
#[cfg(any())]
#[allow(dead_code)]
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
/// [`incremental_spine_inputs`] result (step 4b). It is an explicit diagnostic,
/// enabled in any build only by `POND_VERIFY_FOLD` (any value other than empty,
/// `0`, or `false`). Ordinary debug/test execution must retain production's
/// bounded local-cost shape rather than hiding a full fold behind build mode.
/// The environment is read once and cached for the process lifetime.
pub(crate) fn fold_verification_enabled() -> bool {
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

/// Compute a native-v2 commit delta from the prior persistent manifest root
/// and this transaction's touched rows.
///
/// Point lookups load only identities on changed/ancestor paths. Unchanged
/// Patricia subtrees and content objects remain addressed by their prior
/// hashes and are never enumerated.
pub(crate) async fn incremental_spine_inputs_v2(
    committed_table: deltalake::DeltaTable,
    prior_index_bytes: Option<Vec<u8>>,
    uncommitted: Vec<OplogEntry>,
    local_pond_id: &str,
    pond_path: &std::path::Path,
) -> Result<SpineInputs, StewardError> {
    let Some(prior_index_bytes) = prior_index_bytes else {
        return in_txn_spine_inputs(committed_table, uncommitted, local_pond_id).await;
    };
    let prior_root = decode_manifest_root(&prior_index_bytes).map_err(StewardError::Content)?;
    let local_store = crate::local_content::LocalContentStore::new(pond_path);
    let loader_store = local_store.clone();
    let mut editor = ManifestMapEditor::new(Some(prior_root), move |hash| {
        loader_store.read_string_error(hash)
    });
    let mut prior_cache: HashMap<String, Option<ManifestRecord>> = HashMap::new();
    let mut mutations: HashMap<String, Option<ManifestRecord>> = HashMap::new();
    let mut object_bytes = BTreeMap::new();
    let mut introduced_objects = Vec::new();
    let mut pack_bytes = BTreeMap::new();
    let mut introduced_packs = Vec::new();

    let mut dir_rows: HashMap<String, (i64, Vec<u8>)> = HashMap::new();
    let mut leaf_latest: HashMap<String, OplogEntry> = HashMap::new();
    let mut series_new: HashMap<String, BTreeMap<i64, SeriesVersionData>> = HashMap::new();
    for row in uncommitted {
        if row.pond_id != local_pond_id {
            continue;
        }
        let node = row.node_id.to_string();
        match row.file_type {
            EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
                let range = CollapseRange::of(&row);
                if range.merged {
                    return Err(StewardError::DeltaLake(format!(
                        "native series commit for node {node} contains a collapsed range \
                         [{}, {}]; logical series updates must be append-only",
                        range.lo, range.hi
                    )));
                }
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
                if leaf_latest
                    .get(&node)
                    .is_none_or(|prior| row.version >= prior.version)
                {
                    let _ = leaf_latest.insert(node, row);
                }
            }
        }
    }

    for (node_id, row) in &leaf_latest {
        let prior = lookup_prior(&mut editor, &mut prior_cache, node_id)?;
        let (child_hash, versions, object) = match row.file_type {
            EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
                let hash = row_blob_hash(&row.blake3, row.content.as_deref());
                (
                    hash,
                    vec![version_meta(
                        row.timestamp,
                        row.min_event_time,
                        row.max_event_time,
                        row.extended_attributes.as_ref(),
                    )],
                    row.content
                        .clone()
                        .map(|bytes| (ContentObjectKind::RawBlob, bytes)),
                )
            }
            EntryType::Symlink => {
                let bytes = row.content.clone().unwrap_or_default();
                (
                    ObjectHash::of_bytes(&bytes),
                    vec![version_meta(
                        row.timestamp,
                        row.min_event_time,
                        row.max_event_time,
                        row.extended_attributes.as_ref(),
                    )],
                    Some((ContentObjectKind::RawBlob, bytes)),
                )
            }
            EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic => {
                let factory = row.factory.as_deref().ok_or_else(|| {
                    StewardError::DeltaLake(format!(
                        "dynamic node {node_id} is missing its factory type"
                    ))
                })?;
                let bytes = encode_recipe(factory, row.content.as_deref().unwrap_or(&[]));
                (
                    ObjectHash::of_bytes(&bytes),
                    vec![version_meta(
                        row.timestamp,
                        row.min_event_time,
                        row.max_event_time,
                        row.extended_attributes.as_ref(),
                    )],
                    Some((ContentObjectKind::Recipe, bytes)),
                )
            }
            other => {
                return Err(StewardError::DeltaLake(format!(
                    "unexpected changed leaf type {other:?} for node {node_id}"
                )));
            }
        };
        if let Some((kind, bytes)) = object {
            insert_local_object(
                &mut object_bytes,
                &mut introduced_objects,
                kind,
                child_hash,
                bytes,
            )?;
        } else {
            introduced_objects.push(ObjectDescriptor::new(
                child_hash,
                ContentObjectKind::RawBlob,
            ));
        }
        let mut entry = prior.as_ref().map_or_else(
            || {
                ManifestEntry::new(
                    node_id.clone(),
                    String::new(),
                    String::new(),
                    row.file_type,
                    child_hash,
                    versions.clone(),
                )
            },
            |record| record.entry.clone(),
        );
        entry.entry_type = row.file_type;
        entry.child_hash = child_hash;
        entry.versions = versions;
        let record = ManifestRecord::new(entry, Vec::new()).map_err(StewardError::Content)?;
        let _ = mutations.insert(node_id.clone(), Some(record));
    }

    for (node_id, appended) in &series_new {
        let prior = lookup_prior(&mut editor, &mut prior_cache, node_id)?;
        let entry_type = prior
            .as_ref()
            .map(|record| record.entry.entry_type)
            .or_else(|| {
                appended.values().next().map(|_| {
                    if appended
                        .values()
                        .any(|version| version.schema_fingerprint.is_some())
                    {
                        EntryType::TablePhysicalSeries
                    } else {
                        EntryType::FilePhysicalSeries
                    }
                })
            })
            .ok_or_else(|| {
                StewardError::DeltaLake(format!("changed series node {node_id} has no entry type"))
            })?;
        let prior_manifest = prior
            .as_ref()
            .map(|record| {
                let hash = record.entry.child_hash;
                let bytes = local_store.read(hash)?;
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
            })
            .transpose()?;
        let appended_versions = appended.values().cloned().collect::<Vec<_>>();
        let (manifest, meta) = append_series_manifest(
            entry_type,
            prior_manifest.as_ref(),
            prior
                .as_ref()
                .and_then(|record| record.entry.versions.first()),
            &appended_versions,
        )?;
        let series_hash = manifest.hash();
        insert_local_object(
            &mut object_bytes,
            &mut introduced_objects,
            ContentObjectKind::SeriesManifest,
            series_hash,
            manifest.encode(),
        )?;
        for version in appended.values() {
            if version.blob_size == 0 {
                continue;
            }
            match &version.content {
                Some(bytes) => insert_local_object(
                    &mut object_bytes,
                    &mut introduced_objects,
                    ContentObjectKind::RawBlob,
                    version.blob_hash,
                    bytes.clone(),
                )?,
                None => introduced_objects.push(ObjectDescriptor::new(
                    version.blob_hash,
                    ContentObjectKind::RawBlob,
                )),
            }
        }
        let prior_leaf_count = prior_manifest
            .as_ref()
            .map_or(0, SeriesManifest::leaf_count);
        let appended_leaf_count = appended_versions
            .iter()
            .filter(|version| version.logical_leaf_hash.is_some())
            .count();
        if appended_leaf_count > 0 {
            let parent_series_hash = prior_manifest
                .as_ref()
                .filter(|manifest| manifest.leaf_count() > 0)
                .map(|_| {
                    prior
                        .as_ref()
                        .expect("a prior manifest implies a prior record")
                        .entry
                        .child_hash
                });
            let prefix = prior_manifest
                .as_ref()
                .map_or_else(MerkleFrontier::empty, |manifest| {
                    manifest.merkle_frontier().clone()
                });
            let pack = build_series_segment_pack(
                series_hash,
                entry_type,
                &manifest,
                parent_series_hash,
                prior_leaf_count,
                &prefix,
                &appended_versions,
            )?;
            let bytes = pack.encode();
            let pack_hash = ObjectHash::of_bytes(&bytes);
            let _ = pack_bytes.insert(pack_hash, bytes);
            introduced_packs.push(PackDescriptor::new(series_hash, pack_hash));
        }
        let mut entry = prior.as_ref().map_or_else(
            || {
                ManifestEntry::new(
                    node_id.clone(),
                    String::new(),
                    String::new(),
                    entry_type,
                    series_hash,
                    vec![meta.clone()],
                )
            },
            |record| record.entry.clone(),
        );
        entry.entry_type = entry_type;
        entry.child_hash = series_hash;
        entry.versions = vec![meta];
        let record = ManifestRecord::new(entry, Vec::new()).map_err(StewardError::Content)?;
        let _ = mutations.insert(node_id.clone(), Some(record));
    }

    let mut new_parent: HashMap<String, (String, String, EntryType)> = HashMap::new();
    let mut decoded_directories: HashMap<String, Vec<ManifestRecordChild>> = HashMap::new();
    for (directory_id, (_, content)) in &dir_rows {
        let mut children = Vec::new();
        for child in decode_directory_entries(content)
            .map_err(|error| StewardError::DeltaLake(error.to_string()))?
        {
            let child_pond = child
                .pond_id
                .clone()
                .unwrap_or_else(|| local_pond_id.to_string());
            if child_pond != local_pond_id {
                continue;
            }
            let child_id = child.child_node_id.to_string();
            if child_id == tinyfs::INDEX_NODE_UUID || child_id == tinyfs::LOG_NODE_UUID {
                continue;
            }
            if new_parent
                .insert(
                    child_id.clone(),
                    (directory_id.clone(), child.name.clone(), child.entry_type),
                )
                .is_some()
            {
                return Err(StewardError::DeltaLake(format!(
                    "node {child_id} appears in more than one modified directory"
                )));
            }
            children.push(ManifestRecordChild::new(
                child_id,
                child.name,
                child.entry_type,
            ));
        }
        let _ = decoded_directories.insert(directory_id.clone(), children);
    }

    for (directory_id, children) in &decoded_directories {
        let current = current_record(&mut editor, &mut prior_cache, &mutations, directory_id)?;
        let mut entry = current.as_ref().map_or_else(
            || {
                ManifestEntry::bare(
                    directory_id.clone(),
                    String::new(),
                    String::new(),
                    EntryType::DirectoryPhysical,
                    ObjectHash::of_bytes(&[]),
                )
            },
            |record| record.entry.clone(),
        );
        entry.entry_type = EntryType::DirectoryPhysical;
        let record = ManifestRecord::new(entry, children.clone()).map_err(StewardError::Content)?;
        let _ = mutations.insert(directory_id.clone(), Some(record));

        for child in children {
            let current =
                current_record(&mut editor, &mut prior_cache, &mutations, &child.node_id)?;
            let current = match current {
                Some(current) => current,
                None if child.entry_type == EntryType::DirectoryPhysical => {
                    let bytes = encode_tree(&[]).map_err(StewardError::Content)?;
                    let hash = ObjectHash::of_bytes(&bytes);
                    insert_local_object(
                        &mut object_bytes,
                        &mut introduced_objects,
                        ContentObjectKind::Tree,
                        hash,
                        bytes,
                    )?;
                    ManifestRecord::new(
                        ManifestEntry::bare(
                            child.node_id.clone(),
                            directory_id.clone(),
                            child.name.clone(),
                            child.entry_type,
                            hash,
                        ),
                        Vec::new(),
                    )
                    .map_err(StewardError::Content)?
                }
                None => {
                    return Err(StewardError::DeltaLake(format!(
                        "modified directory {directory_id} references unknown child {}",
                        child.node_id
                    )));
                }
            };
            let mut updated = current;
            updated.entry.parent_node_id = directory_id.clone();
            updated.entry.name = child.name.clone();
            updated.entry.entry_type = child.entry_type;
            let _ = mutations.insert(child.node_id.clone(), Some(updated));
        }
    }

    for (directory_id, children) in &decoded_directories {
        let prior = lookup_prior(&mut editor, &mut prior_cache, directory_id)?;
        let new_ids = children
            .iter()
            .map(|child| child.node_id.as_str())
            .collect::<HashSet<_>>();
        for prior_child in prior.into_iter().flat_map(|record| record.children) {
            if !new_ids.contains(prior_child.node_id.as_str())
                && !new_parent.contains_key(&prior_child.node_id)
            {
                mark_deleted_subtree(
                    &prior_child.node_id,
                    &mut editor,
                    &mut prior_cache,
                    &mut mutations,
                )?;
            }
        }
    }

    let mutation_ids = mutations.keys().cloned().collect::<Vec<_>>();
    let mut dirty_directories = BTreeSet::new();
    for node_id in mutation_ids {
        let before = lookup_prior(&mut editor, &mut prior_cache, &node_id)?;
        let after = mutations.get(&node_id).cloned().flatten();
        for record in before.iter().chain(after.iter()) {
            if record.entry.entry_type == EntryType::DirectoryPhysical {
                let _ = dirty_directories.insert(record.entry.node_id.clone());
            }
            let mut parent = record.entry.parent_node_id.clone();
            let mut seen = HashSet::new();
            while !parent.is_empty() && seen.insert(parent.clone()) {
                let _ = dirty_directories.insert(parent.clone());
                parent = current_record(&mut editor, &mut prior_cache, &mutations, &parent)?
                    .or_else(|| prior_cache.get(&parent).cloned().flatten())
                    .map(|record| record.entry.parent_node_id)
                    .unwrap_or_default();
            }
        }
    }
    let _ = dirty_directories.insert(ROOT_UUID.to_string());

    let mut depth_cache = HashMap::new();
    let mut dirty_order = dirty_directories.into_iter().collect::<Vec<_>>();
    dirty_order.sort_by_key(|node_id| {
        std::cmp::Reverse(manifest_record_depth(
            node_id,
            &mut editor,
            &mut prior_cache,
            &mutations,
            &mut depth_cache,
        ))
    });
    for directory_id in dirty_order {
        let Some(mut directory) =
            current_record(&mut editor, &mut prior_cache, &mutations, &directory_id)?
        else {
            continue;
        };
        if directory.entry.entry_type != EntryType::DirectoryPhysical {
            continue;
        }
        let mut tree_entries = Vec::with_capacity(directory.children.len());
        for child in &directory.children {
            let child_record =
                current_record(&mut editor, &mut prior_cache, &mutations, &child.node_id)?
                    .ok_or_else(|| {
                        StewardError::DeltaLake(format!(
                            "directory {directory_id} references deleted child {}",
                            child.node_id
                        ))
                    })?;
            if child_record.entry.parent_node_id != directory_id
                || child_record.entry.name != child.name
                || child_record.entry.entry_type != child.entry_type
            {
                return Err(StewardError::DeltaLake(format!(
                    "directory {directory_id} child identity {} disagrees with its manifest record",
                    child.node_id
                )));
            }
            tree_entries.push(TreeEntry::new(
                child.name.clone(),
                child.entry_type,
                child_record.entry.child_hash,
                child_record.entry.versions.clone(),
            ));
        }
        let bytes = encode_tree(&tree_entries).map_err(StewardError::Content)?;
        let tree_hash = ObjectHash::of_bytes(&bytes);
        insert_local_object(
            &mut object_bytes,
            &mut introduced_objects,
            ContentObjectKind::Tree,
            tree_hash,
            bytes,
        )?;
        directory.entry.child_hash = tree_hash;
        directory.entry.versions.clear();
        let _ = mutations.insert(directory_id, Some(directory));
    }

    let root_tree_hash = current_record(&mut editor, &mut prior_cache, &mutations, ROOT_UUID)?
        .ok_or_else(|| StewardError::DeltaLake("manifest update deleted the root".to_string()))?
        .entry
        .child_hash;

    let mut manifest_changes = Vec::new();
    let mut ordered_mutations = mutations.into_iter().collect::<Vec<_>>();
    ordered_mutations.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    for (node_id, after) in ordered_mutations {
        let before = lookup_prior(&mut editor, &mut prior_cache, &node_id)?;
        if before == after {
            continue;
        }
        manifest_changes
            .push(ManifestChange::new(before, after.clone()).map_err(StewardError::Content)?);
        match after {
            Some(record) => editor.upsert(record).map_err(StewardError::Content)?,
            None => {
                let removed = editor.remove(&node_id).map_err(StewardError::Content)?;
                if !removed {
                    return Err(StewardError::DeltaLake(format!(
                        "manifest change deletes absent node {node_id}"
                    )));
                }
            }
        }
    }
    let (manifest_root, manifest_objects) = editor.finish().map_err(StewardError::Content)?;
    for (hash, bytes) in manifest_objects {
        insert_local_object(
            &mut object_bytes,
            &mut introduced_objects,
            ContentObjectKind::ManifestNode,
            hash,
            bytes,
        )?;
    }
    introduced_objects.sort_unstable();
    introduced_objects.dedup();
    introduced_packs.sort_unstable();
    introduced_packs.dedup();

    Ok(SpineInputs {
        index_bytes: encode_manifest_root(manifest_root),
        root_tree_hash,
        manifest_root,
        manifest_changes,
        introduced_objects,
        introduced_packs,
        object_bytes,
        pack_bytes,
    })
}

fn lookup_prior<F>(
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

fn current_record<F>(
    editor: &mut ManifestMapEditor<F>,
    cache: &mut HashMap<String, Option<ManifestRecord>>,
    mutations: &HashMap<String, Option<ManifestRecord>>,
    node_id: &str,
) -> Result<Option<ManifestRecord>, StewardError>
where
    F: FnMut(ObjectHash) -> Result<Vec<u8>, String>,
{
    match mutations.get(node_id) {
        Some(record) => Ok(record.clone()),
        None => lookup_prior(editor, cache, node_id),
    }
}

fn mark_deleted_subtree<F>(
    node_id: &str,
    editor: &mut ManifestMapEditor<F>,
    cache: &mut HashMap<String, Option<ManifestRecord>>,
    mutations: &mut HashMap<String, Option<ManifestRecord>>,
) -> Result<(), StewardError>
where
    F: FnMut(ObjectHash) -> Result<Vec<u8>, String>,
{
    let Some(record) = current_record(editor, cache, mutations, node_id)? else {
        return Ok(());
    };
    for child in record.children.clone() {
        mark_deleted_subtree(&child.node_id, editor, cache, mutations)?;
    }
    let _ = mutations.insert(node_id.to_string(), None);
    Ok(())
}

fn manifest_record_depth<F>(
    node_id: &str,
    editor: &mut ManifestMapEditor<F>,
    cache: &mut HashMap<String, Option<ManifestRecord>>,
    mutations: &HashMap<String, Option<ManifestRecord>>,
    depth_cache: &mut HashMap<String, usize>,
) -> usize
where
    F: FnMut(ObjectHash) -> Result<Vec<u8>, String>,
{
    if node_id == ROOT_UUID {
        return 0;
    }
    if let Some(depth) = depth_cache.get(node_id) {
        return *depth;
    }
    let depth = current_record(editor, cache, mutations, node_id)
        .ok()
        .flatten()
        .and_then(|record| {
            (!record.entry.parent_node_id.is_empty()).then_some(record.entry.parent_node_id)
        })
        .map_or(usize::MAX / 2, |parent| {
            manifest_record_depth(&parent, editor, cache, mutations, depth_cache).saturating_add(1)
        });
    let _ = depth_cache.insert(node_id.to_string(), depth);
    depth
}

fn insert_local_object(
    object_bytes: &mut BTreeMap<ObjectHash, Vec<u8>>,
    descriptors: &mut Vec<ObjectDescriptor>,
    kind: ContentObjectKind,
    hash: ObjectHash,
    bytes: Vec<u8>,
) -> Result<(), StewardError> {
    let actual = ObjectHash::of_bytes(&bytes);
    if actual != hash {
        return Err(StewardError::Content(format!(
            "local {} object hashes to {}, expected {}",
            kind.as_str(),
            actual,
            hash
        )));
    }
    if let Some(existing) = object_bytes.insert(hash, bytes.clone())
        && existing != bytes
    {
        return Err(StewardError::Content(format!(
            "two local objects claim hash {} with different bytes",
            hash
        )));
    }
    descriptors.push(ObjectDescriptor::new(hash, kind));
    Ok(())
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
/// this on every commit when the explicit `POND_VERIFY_FOLD` diagnostic is set.
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
#[cfg(any())]
#[allow(dead_code)]
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
    // does, then folded into one watertown.series.v3 manifest.
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

    let records = manifest
        .iter()
        .cloned()
        .map(|entry| {
            let children = dir_children
                .get(&entry.node_id)
                .into_iter()
                .flatten()
                .map(|child| {
                    ManifestRecordChild::new(
                        child.node_id.clone(),
                        child.name.clone(),
                        child.entry_type,
                    )
                })
                .collect();
            ManifestRecord::new(entry, children).map_err(StewardError::Content)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (manifest_root, manifest_objects) =
        build_manifest_map(&records).map_err(StewardError::Content)?;
    Ok(SpineInputs {
        index_bytes: encode_manifest_root(manifest_root),
        root_tree_hash,
        manifest_root,
        manifest_changes: Vec::new(),
        introduced_objects: manifest_objects
            .keys()
            .copied()
            .map(|hash| ObjectDescriptor::new(hash, ContentObjectKind::ManifestNode))
            .collect(),
        introduced_packs: Vec::new(),
        object_bytes: manifest_objects,
        pack_bytes: BTreeMap::new(),
    })
}

/// Depth of a node below the root (root = 0), memoized across a fold.  A node
/// whose parent chain does not reach the root (a detached fragment) is treated
/// as maximally deep so it is recomputed before any real ancestor.
#[cfg(any())]
#[allow(dead_code)]
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

/// Fetch **one** already-known-live series version's inline content, by
/// `(pond_id, node_id, version)`, straight from that one Oplog row --
/// never the whole series' `content` column read into memory in one batch.
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

/// One committed series version row's identity/bookkeeping metadata, excluding
/// the inline `content` column -- see [`read_series_live_metadata_ordered`].
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

/// Read exactly the latest committed reserved manifest-index pointer for one
/// pond partition.
///
/// This avoids opening the index as a `FilePhysicalSeries`, whose generic read
/// path would load every retained collapsed row before selecting the live one.
pub(crate) async fn index_root_pointer_bytes(
    table: deltalake::DeltaTable,
    pond_id: &str,
) -> Result<Option<Vec<u8>>, StewardError> {
    let ctx = SessionContext::new();
    let _ = ctx
        .register_table("index_tip", Arc::new(table))
        .map_err(|error| StewardError::DeltaLake(error.to_string()))?;
    let sql = format!(
        "SELECT version, content FROM index_tip WHERE pond_id = '{pond_id}' \
         AND node_id = '{index}' ORDER BY version DESC LIMIT 1",
        index = tinyfs::INDEX_NODE_UUID,
    );
    let batches = ctx
        .sql(&sql)
        .await
        .map_err(|error| StewardError::DeltaLake(error.to_string()))?
        .collect()
        .await
        .map_err(|error| StewardError::DeltaLake(error.to_string()))?;
    let mut rows = Vec::new();
    for batch in &batches {
        rows.extend(
            serde_arrow::from_record_batch::<Vec<LogLeaf>>(batch)
                .map_err(|error| StewardError::DeltaLake(error.to_string()))?,
        );
    }
    match rows.as_slice() {
        [] => Ok(None),
        [row] => row.content.clone().map(Some).ok_or_else(|| {
            StewardError::Content(format!(
                "manifest-index pointer at version {} has no content",
                row.version
            ))
        }),
        _ => Err(StewardError::Content(
            "bounded manifest-index query returned multiple rows".to_string(),
        )),
    }
}

/// The current tip of the commit-log node -- the hash of its last leaf's commit
/// object -- to use as the `parent_commit_hash` of the next commit.  `None`
/// when the log node is empty (genesis).
pub(crate) async fn log_tip_commit_hash(
    table: deltalake::DeltaTable,
    pond_id: &str,
) -> Result<Option<ObjectHash>, StewardError> {
    let ctx = SessionContext::new();
    let _ = ctx
        .register_table("log_tip", Arc::new(table))
        .map_err(|error| StewardError::DeltaLake(error.to_string()))?;
    let sql = format!(
        "SELECT version, content FROM log_tip WHERE pond_id = '{pond_id}' AND node_id = '{log}' \
         ORDER BY version DESC LIMIT 1",
        log = tinyfs::LOG_NODE_UUID,
    );
    let batches = ctx
        .sql(&sql)
        .await
        .map_err(|error| StewardError::DeltaLake(error.to_string()))?
        .collect()
        .await
        .map_err(|error| StewardError::DeltaLake(error.to_string()))?;
    let mut rows = Vec::new();
    for batch in &batches {
        rows.extend(
            serde_arrow::from_record_batch::<Vec<LogLeaf>>(batch)
                .map_err(|error| StewardError::DeltaLake(error.to_string()))?,
        );
    }
    match rows.as_slice() {
        [] => Ok(None),
        [row] => {
            let bytes = row.content.as_deref().ok_or_else(|| {
                StewardError::Content(format!(
                    "commit-log tip at version {} has no content",
                    row.version
                ))
            })?;
            log_tip_hash(bytes)
        }
        _ => Err(StewardError::Content(
            "bounded commit-log tip query returned multiple rows".to_string(),
        )),
    }
}

fn log_tip_hash(bytes: &[u8]) -> Result<Option<ObjectHash>, StewardError> {
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
    manifest_root: ObjectHash,
    manifest_changes: Vec<ManifestChange>,
    introduced_objects: Vec<ObjectDescriptor>,
    introduced_packs: Vec<PackDescriptor>,
    pond_id_str: &str,
    txn_seq: i64,
    request: String,
) -> Result<CommitSpine, StewardError> {
    let provenance = Provenance {
        pond_id: pond_id_str.to_string(),
        seq: txn_seq,
        time_micros: chrono::Utc::now().timestamp_micros(),
        author: String::new(),
        request,
    };
    let commit = Commit::new_with_delta(
        ContentModelVersion::PublicationV2,
        root_tree_hash,
        parent_commit_hash,
        manifest_root,
        manifest_changes,
        introduced_objects,
        introduced_packs,
        provenance,
    )
    .map_err(StewardError::Content)?;
    Ok(CommitSpine {
        root_tree_hash: root_tree_hash.to_hex(),
        parent_commit_hash: parent_commit_hash.map(|h| h.to_hex()),
        commit_hash: commit.hash().to_hex(),
        commit_object: hex::encode(commit.encode()),
    })
}

/// Build a target pond's complete current node state for a full clone/rebuild
/// or explicit diagnostic: a map from `node_id` to its [`ManifestEntry`] and a
/// map from each series `node_id` to every ordered v2 logical leaf hash.
///
/// Ordinary incremental native-v2 pulls must not call this O(history) helper;
/// they point-load changed records and prior series manifests from the
/// persistent local manifest map.
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

/// Build the complete target state for a named foreign pond during a full
/// import/rebuild. Returns empty maps when the foreign pond has no root row yet.
/// Incremental graft pulls use the foreign pond's reserved manifest root and
/// do not call this helper.
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
    let series_leaves = index
        .series_leaf_hashes
        .into_iter()
        .filter(|((pond, _node_id), _leaves)| pond == pond_id)
        .map(|((_pond, node_id), leaves)| (node_id, leaves))
        .collect();
    Ok((by_id, series_leaves))
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
    let records = node_manifest_records(&index)?;
    let (manifest_root, nodes) = build_manifest_map(&records).map_err(StewardError::Content)?;
    for (hash, bytes) in nodes {
        materialized.put_inline(ContentObjectKind::ManifestNode, hash, bytes)?;
    }
    materialized.manifest_root = Some(manifest_root);
    materialized.manifest_records = records;
    Ok(materialized)
}

/// Build a whole-range root pack index for one fully folded current
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
/// [`PackIndex`] encodings. Initial publication and explicit full
/// materialization may call it repeatedly without diverging; ordinary
/// appends use [`build_series_segment_pack`] over only their new suffix.
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
    build_series_segment_pack(
        material.series_hash,
        material.entry_type,
        &material.manifest,
        None,
        0,
        &MerkleFrontier::empty(),
        &material.versions,
    )
    .map(Some)
}

/// Build one canonical linked segment ending at `manifest`.
///
/// `versions` contains only the newly appended rows for ordinary commits.
/// The prior manifest's compact frontier supplies the complete historical
/// prefix proof, so descriptor construction, physical spans, and proof bytes
/// are proportional to this suffix plus at most 64 frontier hashes.
#[allow(clippy::too_many_arguments)]
fn build_series_segment_pack(
    series_hash: ObjectHash,
    entry_type: EntryType,
    manifest: &SeriesManifest,
    parent_series_hash: Option<ObjectHash>,
    leaf_start: u64,
    prefix_frontier: &MerkleFrontier,
    versions: &[SeriesVersionData],
) -> Result<sync_store::content::PackIndex, StewardError> {
    if !matches!(
        entry_type,
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
    ) {
        return Err(StewardError::DeltaLake(format!(
            "series pack material carries an unexpected entry type {entry_type:?}"
        )));
    }
    if prefix_frontier.leaf_count() != leaf_start {
        return Err(StewardError::Content(format!(
            "series segment starts at leaf {leaf_start} but its prefix frontier contains {} leaves",
            prefix_frontier.leaf_count()
        )));
    }
    let leaf_versions = versions
        .iter()
        .filter(|version| version.logical_leaf_hash.is_some())
        .collect::<Vec<_>>();
    if leaf_versions.is_empty() {
        return Err(StewardError::Content(
            "cannot build a series segment without a logical leaf".to_string(),
        ));
    }

    let mut range_leaf_hashes = Vec::with_capacity(leaf_versions.len());
    let mut object_spans = Vec::with_capacity(leaf_versions.len());
    let mut leaf_descriptors = Vec::with_capacity(leaf_versions.len());
    let mut logical_cursor: u64 = 0;
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
        let descriptor = sync_store::content::PackLeafDescriptor::new_with_leaf_hash_and_schema(
            leaf_hash,
            logical_count,
            v.schema_fingerprint,
            v.meta.min_event_time,
            v.meta.max_event_time,
            attrs,
        )
        .map_err(StewardError::Content)?;
        range_leaf_hashes.push(leaf_hash);
        let logical_end = logical_cursor.checked_add(logical_count).ok_or_else(|| {
            StewardError::Content("series logical object span overflow".to_string())
        })?;
        let physical_end = physical_byte_count
            .checked_add(v.blob_size)
            .ok_or_else(|| {
                StewardError::Content("series physical_byte_count aggregate overflow".to_string())
            })?;
        object_spans.push(
            sync_store::content::PackObjectSpan::new(
                v.blob_hash,
                logical_cursor,
                logical_end,
                physical_byte_count,
                physical_end,
            )
            .map_err(StewardError::Content)?,
        );
        logical_cursor = logical_end;
        physical_byte_count = physical_end;
        leaf_descriptors.push(descriptor);
    }

    let range_leaf_count = u64::try_from(range_leaf_hashes.len())
        .map_err(|_| StewardError::Content("series segment leaf count exceeds u64".to_string()))?;
    let leaf_end = leaf_start
        .checked_add(range_leaf_count)
        .ok_or_else(|| StewardError::Content("series segment leaf range overflow".to_string()))?;
    if leaf_end != manifest.leaf_count() {
        return Err(StewardError::Content(format!(
            "series segment [{leaf_start}, {leaf_end}) does not end at manifest leaf count {}",
            manifest.leaf_count()
        )));
    }
    let range_proof = generate_append_range_proof(prefix_frontier, range_leaf_hashes.len())
        .map_err(StewardError::Content)?;
    let range_root = manifest.leaf_merkle_root();

    let pack = sync_store::content::PackIndex::new_segment_with_spans(
        series_hash,
        parent_series_hash,
        leaf_start,
        leaf_end,
        manifest.leaf_count(),
        range_root,
        range_proof,
        object_spans,
        logical_cursor,
        physical_byte_count,
        leaf_descriptors,
    )
    .map_err(StewardError::Content)?;
    pack.validate_series_segment()
        .map_err(StewardError::Content)?;

    // Self-check before ever handing this pack to a publisher: a pack built
    // from this pond's own just-folded rows must verify against the
    // manifest those same rows just folded to, or the persisted state and
    // the fold disagree -- an internal bug that must not be published.
    sync_store::content::verify_pack_against_manifest(
        series_hash,
        manifest,
        &pack,
        &range_leaf_hashes,
    )
    .map_err(StewardError::Content)?;

    Ok(pack)
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
/// ([`incremental_spine_inputs_v2`], [`read_series_live_metadata_ordered`]) so
/// a row's logical-series fields are parsed identically everywhere.
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

fn append_series_manifest(
    entry_type: EntryType,
    prior: Option<&SeriesManifest>,
    prior_meta: Option<&VersionMeta>,
    appended: &[SeriesVersionData],
) -> Result<(SeriesManifest, VersionMeta), StewardError> {
    let payload_kind = match entry_type {
        EntryType::FilePhysicalSeries => PayloadKind::File,
        EntryType::TablePhysicalSeries => PayloadKind::Table,
        other => {
            return Err(StewardError::DeltaLake(format!(
                "append_series_manifest called for non-series entry type {other:?}"
            )));
        }
    };
    if let Some(prior) = prior
        && prior.payload_kind() != payload_kind
    {
        return Err(StewardError::DeltaLake(format!(
            "series changed payload kind from {:?} to {payload_kind:?}",
            prior.payload_kind()
        )));
    }

    let mut frontier = prior.map_or_else(MerkleFrontier::empty, |manifest| {
        manifest.merkle_frontier().clone()
    });
    let mut logical_count = prior.map_or(0, SeriesManifest::logical_count);
    let mut min_event_time = prior.and_then(SeriesManifest::min_event_time);
    let mut max_event_time = prior.and_then(SeriesManifest::max_event_time);
    let mut logical_attributes = prior
        .and_then(SeriesManifest::logical_attributes)
        .map(ToOwned::to_owned);
    let mut latest_meta = prior_meta.cloned();
    let mut appended_leaf = false;

    for version in appended {
        let Some(leaf_hash) = version.logical_leaf_hash else {
            if version.blob_size > 0 {
                return Err(StewardError::DeltaLake(format!(
                    "series version at timestamp {:?} is nonempty ({} bytes) but has no \
                     logical_leaf_hash -- corrupt row (persisted-leaf invariant violated)",
                    version.meta.timestamp, version.blob_size
                )));
            }
            continue;
        };
        match payload_kind {
            PayloadKind::Table if version.schema_fingerprint.is_none() => {
                return Err(StewardError::DeltaLake(
                    "leaf-bearing table series version has no series_schema_fingerprint"
                        .to_string(),
                ));
            }
            PayloadKind::File if version.schema_fingerprint.is_some() => {
                return Err(StewardError::DeltaLake(
                    "leaf-bearing file series version must not carry a series_schema_fingerprint"
                        .to_string(),
                ));
            }
            _ => {}
        }
        let count = version.logical_count.ok_or_else(|| {
            StewardError::DeltaLake(
                "series version has a logical_leaf_hash but no logical_count".to_string(),
            )
        })?;
        let count = u64::try_from(count).map_err(|_| {
            StewardError::DeltaLake("series version has a negative logical_count".to_string())
        })?;
        if count == 0 {
            return Err(StewardError::DeltaLake(
                "series version logical_count must be positive".to_string(),
            ));
        }
        frontier
            .append(leaf_hash)
            .map_err(StewardError::DeltaLake)?;
        logical_count = logical_count.checked_add(count).ok_or_else(|| {
            StewardError::DeltaLake("series logical_count aggregate overflow".to_string())
        })?;
        if let Some(min) = version.meta.min_event_time {
            min_event_time = Some(min_event_time.map_or(min, |current| current.min(min)));
        }
        if let Some(max) = version.meta.max_event_time {
            max_event_time = Some(max_event_time.map_or(max, |current| current.max(max)));
        }
        logical_attributes = canonical_leaf_attributes(version)?;
        latest_meta = Some(version.meta.clone());
        appended_leaf = true;
    }

    let manifest = SeriesManifest::new(
        payload_kind,
        logical_count,
        frontier.leaf_count(),
        min_event_time,
        max_event_time,
        logical_attributes,
        frontier,
    )
    .map_err(StewardError::DeltaLake)?;
    let meta = VersionMeta {
        timestamp: latest_meta
            .as_ref()
            .and_then(|metadata| metadata.timestamp)
            .or_else(|| appended.last().and_then(|version| version.meta.timestamp)),
        min_event_time,
        max_event_time,
        extended_attributes: if appended_leaf {
            latest_meta.and_then(|metadata| metadata.extended_attributes)
        } else {
            prior_meta.and_then(|metadata| metadata.extended_attributes.clone())
        },
    };
    Ok((manifest, meta))
}

/// Build a series node's `watertown.series.v3` [`SeriesManifest`] and its single
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
/// [`SeriesManifest::new`]'s invariants.
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

    let merkle_frontier = MerkleFrontier::from_leaves(&leaf_hashes);
    let logical_attributes = match &latest_raw_attrs {
        Some(json) => Some(
            encode_canonical_attributes(json)
                .map_err(|e| StewardError::DeltaLake(format!("logical attributes: {e}")))?,
        ),
        None => None,
    };

    let manifest = SeriesManifest::new(
        payload_kind,
        logical_count,
        leaf_hashes.len() as u64,
        min_event_time,
        max_event_time,
        logical_attributes,
        merkle_frontier,
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
        sink.put_inline(ContentObjectKind::Tree, hash, encoded)?;
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
                // The watertown.series.v3 manifest object, plus each version's
                // physical blob: small versions inline, large (externalized)
                // versions by hash (D7). Physical blobs stay available for
                // initial pack publication/fetch even though the series'
                // identity is now the manifest hash, not a hash over these
                // blobs (`docs/logical-series-identity-design.md`).
                sink.put_inline(ContentObjectKind::SeriesManifest, hash, manifest.encode())?;
                for v in versions.iter() {
                    record_blob(sink, v.blob_hash, v.content.as_deref())?;
                }
                // Capture what initial publication needs to mint this
                // series' whole-range root pack.
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
                sink.put_inline(ContentObjectKind::RawBlob, hash, bytes.to_vec())?;
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
                sink.put_inline(
                    ContentObjectKind::Recipe,
                    hash,
                    encode_recipe(factory, config),
                )?;
            }
            Ok((hash, vec![facts.meta.clone()]))
        }
        // Single-version physical file or table: the version blob hash.
        EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
            let facts = leaf_facts(key, latest)?;
            let hash = row_blob_hash(&facts.blake3, facts.content.as_deref());
            if let Some(sink) = sink {
                record_blob(sink, hash, facts.content.as_deref())?;
            }
            Ok((hash, vec![facts.meta.clone()]))
        }
    }
}

/// Record a file/version blob into the materialization sink: inline when the
/// bytes are in-row (small), external by hash when the content is `None`
/// (an externalized large file -- Decision D7).
fn record_blob(
    sink: &mut MaterializedObjects,
    hash: ObjectHash,
    content: Option<&[u8]>,
) -> Result<(), StewardError> {
    match content {
        Some(bytes) => sink.put_inline(ContentObjectKind::RawBlob, hash, bytes.to_vec())?,
        None => sink.put_external(hash),
    }
    Ok(())
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
    fn obsolete_commit_tip_is_rejected() {
        let obsolete = b"dp.commit.3\nintentionally-not-a-current-commit";
        assert!(log_tip_hash(obsolete).is_err());
    }

    #[test]
    fn producer_one_leaf_segment_metadata_is_bounded_after_1_100_1000_leaves() {
        fn build(prior_count: usize) -> sync_store::content::PackIndex {
            let prior_leaves = (0..prior_count)
                .map(|index| ObjectHash::of_bytes(format!("prior-{index}").as_bytes()))
                .collect::<Vec<_>>();
            let prior_frontier = MerkleFrontier::from_leaves(&prior_leaves);
            let prior_manifest = SeriesManifest::new(
                PayloadKind::File,
                prior_count as u64 * 8,
                prior_count as u64,
                None,
                None,
                None,
                prior_frontier.clone(),
            )
            .expect("prior manifest");
            let appended = SeriesVersionData {
                version: prior_count as i64 + 1,
                blob_hash: ObjectHash::of_bytes(b"new-blob"),
                content: None,
                meta: VersionMeta {
                    timestamp: Some(1),
                    min_event_time: None,
                    max_event_time: None,
                    extended_attributes: None,
                },
                raw_extended_attributes: None,
                logical_leaf_hash: Some(ObjectHash::of_bytes(b"new-leaf")),
                logical_count: Some(8),
                schema_fingerprint: None,
                blob_size: 8,
            };
            let (manifest, _) = append_series_manifest(
                EntryType::FilePhysicalSeries,
                Some(&prior_manifest),
                Some(&VersionMeta {
                    timestamp: Some(0),
                    min_event_time: None,
                    max_event_time: None,
                    extended_attributes: None,
                }),
                std::slice::from_ref(&appended),
            )
            .expect("append manifest");
            build_series_segment_pack(
                manifest.hash(),
                EntryType::FilePhysicalSeries,
                &manifest,
                Some(prior_manifest.hash()),
                prior_count as u64,
                &prior_frontier,
                &[appended],
            )
            .expect("build suffix segment")
        }

        let packs = [build(1), build(100), build(1_000)];
        for (prior, pack) in [1u64, 100, 1_000].into_iter().zip(&packs) {
            assert_eq!(pack.leaf_start(), prior);
            assert_eq!(pack.leaf_end(), prior + 1);
            assert_eq!(pack.leaf_descriptors().len(), 1);
            assert_eq!(pack.physical_object_hashes().len(), 1);
        }
        let sizes = packs.map(|pack| pack.encode().len() as u64);
        assert!(
            sizes.iter().max().unwrap() - sizes.iter().min().unwrap() <= 4 * 1024,
            "only bounded Merkle proof overhead may vary with prior leaf count: {sizes:?}"
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
            .expect("decode watertown.series.v3 manifest");
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
            .expect("decode watertown.series.v3 manifest");
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
        assert_eq!(
            pack.leaf_descriptors()[0].logical_leaf_hash(),
            ObjectHash::of_bytes(b"leaf-a")
        );
        assert_eq!(
            pack.object_spans()
                .iter()
                .map(|span| {
                    (
                        span.logical_start(),
                        span.logical_end(),
                        span.physical_start(),
                        span.physical_end(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![(0, 3, 0, 10), (3, 8, 10, 30)]
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
