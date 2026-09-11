# Unified Pond Storage Architecture (Decision D9)

Status: **target architecture.** Steps 4a (spine relocation), 4b (incremental
commit fold), 5 (`commit_object` node-keyed Merkle root), 5b (checksum
subsumption), and 6 (validation) have landed; the D9 sequence is complete. This
document describes where the system ends up, and why the design is
coherent. It is the architectural companion to the phased implementation plan in
`docs/incremental-content-tree-design.md` (see its Section 10 and progress
table); read this for the "what and why," read that for the "how and when."

Native-v2 publication further refines the INDEX representation: it is now a
fixed-size pointer to a pond-local immutable, path-compressed Patricia map,
rather than one flat manifest value. Remote publication is specified by
`low-cost-correctness-review.md` Section 16.

## 1. The one invariant

> **The pond is one `data/` Delta Lake instance. Everything durable and shared
> lives in the pond. `control/` is not part of the pond: it is a local
> concurrency gate plus a cache, disposable and reconstructable from the pond.
> Authority flows one way: `pond -> control`, never the reverse.**

Concretely: nothing may exist *only* in `control/` that cannot be rebuilt from
`data/`, with the single, deliberate exception of **this replica's local
operator state** (which remote it pushes to, how far it has pushed) -- and that
state is, by definition, not pond content and must not travel with the pond.

Every design choice below is a consequence of holding this invariant.

## 2. On-disk layout

```
{POND}/
|- data/                     THE POND -- one Delta Lake instance, the only source of truth
|  |- _delta_log/            Delta transaction log (commit history, pond_txn metadata)
|  |- _large_files/blake3=*  external blobs > 64 KiB (no Delta schema fits raw bytes)
|  |- _content/v2/objects/   pond-local immutable commit/tree/manifest cache
|  |- _content/v2/state/     fixed local manifest-root cursors (recoverable)
|  |- _packs/                 explicit local whole-range pack maintenance sidecars
|  |- part_id=<uuid>/*.parquet
|  |    filesystem rows: one partition per directory; user files + dirs
|  |
|  |- INDEX node   (tinyfs::INDEX_NODE_UUID,  ".pond-node-index")
|  |    DERIVED cache. Fixed-size persistent manifest-map root pointer.
|  |    Fold-excluded, hidden from enumeration. Updated incrementally on pull;
|  |    rebuilt only by full clone/rebuild.
|  |
|  +- LOG node     (tinyfs::LOG_NODE_UUID,    ".pond-commit-log")
|       AUTHORITATIVE history. Append-only series; one version per
|       content-changing commit = one encoded commit_object (embeds
|       root_tree_hash + parent_commit_hash + provenance). Fold-excluded,
|       hidden from enumeration, never collapsed. IS the transparency-log
|       leaf sequence, pond-resident.
|
|- control/                  NOT THE POND -- disposable, local-only
|  |- write lock             single-writer concurrency gate (advisory OS lock)
|  |- audit log              Begin / DataCommitted / Failed / Completed per txn
|  |- spine cache            root_tree_hash / parent / commit_hash / commit_object
|  |                         (a copy of the LOG tail; NOT authoritative)
|  +- operator settings      remote configs/modes and structured publication
|                            acknowledgements (format, tip, manifest root,
|                            record head, generation)
|                            (local to this replica; the only non-rebuildable state)
|
|- tlog/                     DERIVED export -- C2SP tlog-tiles + checkpoint over the LOG node
+- git/                      DERIVED cache -- bare repo mirror
```

Only `data/` is authoritative and shared. `control/`, `tlog/`, and `git/` are
all reconstructable; a replica that loses any of them can rebuild it from
`data/` (operator settings excepted, see Section 6).

## 3. The two reserved nodes

All commit machinery lives in two reserved fixed-`FileID` nodes under the root.
Both are `FilePhysicalSeries` raw-byte nodes, both are excluded from the
content-tree fold, and both are hidden from directory enumeration. They differ
only in authority and transfer semantics.

| | content | authoritative? | transferred on pull? | recovered by |
|---|---|---|---|---|
| **INDEX** node | persistent manifest-map root pointer | derived | no (recomputed locally) | incremental pull delta or explicit full fold |
| **LOG** node | append-only `commit_object` per commit | **authoritative** | yes | it *is* the history |

They are fold-excluded for the same reason: their contents are *derived from*
(INDEX) or *reference* (LOG, via `root_tree_hash`) the very root they would
otherwise be hashed into. Folding them in would be self-referential -- the same
argument that excluded the index node in Phase 2.

They are two nodes rather than one because their economics differ. INDEX is a
small pointer into rebuildable pond-local immutable map nodes and is **not
shipped as filesystem content**. LOG is small, append-only, and carries
provenance that exists nowhere else, so it remains authoritative local
history; native-v2 publication transfers only the commit delta since the
acknowledged remote tip.

The generic collapsed-row primitive is not a user-series feature in native-v2.
`FilePhysicalSeries` and `TablePhysicalSeries` writes are append-only; the
public collapsing path rejects them. Only INDEX replaces its prior fixed-size
pointer, and local maintenance may delete those excluded obsolete rows.

### Why the LOG node is what makes control disposable

The commit object embeds **provenance**: sequence number, commit time, author,
and the request (CLI args) that produced the commit. Before D9 that provenance
was written only into `control/`'s `DataCommittedMetadata`, so a control loss
meant the spine could not be reconstructed faithfully -- author and request were
gone, and `pond rebuild-control` had to record them as `"unknown"`. That was the
architectural "slip": a pond-derived, durable fact lived only in the disposable
layer.

Relocating the commit object into the pond-resident LOG node closes the slip.
Provenance now lands in `data/`, atomically, in the same Delta transaction as
the data it describes. The pond is **self-describing at every committed
version**, and the control spine is demoted to a redundant cache.

## 4. What a commit does (end state)

A single `pond` invocation is one transaction with one `TransactionGuard`. On a
content-changing write, before the Delta transaction finalizes, the steward:

1. **Folds the changeset** incrementally along touched content and identity
   paths, with an optional full-fold oracle. This yields `root_tree_hash`, the
   persistent manifest-map root, changed map nodes, changed content objects,
   and per-node before/after records.
2. **Writes the immutable local objects**, then writes the collapsing INDEX
   node version containing only the fixed-size map-root pointer.
3. **Reads the LOG tip** (parent commit hash) from the committed table and
   **builds the `watertown.commit.v2` object** (`root_tree_hash` + parent +
   `manifest_root` + bounded manifest/object/pack delta + provenance).
4. **Appends the LOG node** version = that commit object.
5. Commits INDEX + LOG + data rows **in one Delta transaction**.

After the Delta commit lands, the steward writes the control audit row and the
spine cache (both disposable), then reconciles the derived `tlog/` tile export
against the LOG node. None of the post-commit steps are authoritative; a crash
between the Delta commit and the control write leaves the pond correct and the
caches self-healing on the next open or commit.

This eliminates the pre-D9 non-atomic window ("commit the data, then separately
record the spine in control").

### Compaction adds no commit

Compaction (`Ship::compact`) rewrites parquet via Delta `optimize`. It is
**content-preserving** -- the invariant check guarantees `root_tree_hash` is
byte-identical to the parent's -- so it is transparent to the content graph and
appends **no LOG leaf**, exactly as `git gc`/repack adds no commits. Push and
pull resolve the tip from the last content-changing commit, whose root already
matches. This is why compaction never needed to solve the "inject a row into an
`optimize` commit" atomicity problem: there is nothing to inject.

The LOG node is therefore excluded from collapse/compaction candidacy: every one
of its versions is a permanent transparency-log leaf and must never be merged
away.

## 5. Where 4b, 5, and 5b take us

Step 4a made the architecture correct. The remaining steps make it efficient and
remove the last pond-derived data from the authoritative-in-control position.

- **4b -- incremental commit fold.** Both roots (`root_tree_hash` and the
  manifest-map root) are computed from the changeset along touched paths
  instead of a full scan. The INDEX node stores the current root while
  immutable Patricia nodes under `data/_content/v2/objects` preserve unchanged
  subtrees by hash. Commit cost goes from `O(n)` to `O(change)`. The
  in-transaction incremental fold can be explicitly cross-checked against a
  full `O(n)` fold by setting `POND_VERIFY_FOLD` in any build. It is never
  enabled merely by using a debug binary, so ordinary cost tests and operator
  commands retain the production path. **This is a performance change, not an authority change** --
  but it is what makes the INDEX node a genuine durable incremental cache rather
  than a per-commit full rewrite.
- **4c -- incremental pull planning.** An acknowledged mirror or foreign graft
  reads its destination INDEX root and point-loads only changed manifest records
  plus required parent paths. A changed series loads its prior
  `watertown.series.v3` manifest and compares the stored leaf count/root/bounded
  frontier with the fetched suffix's parent state; it never folds the complete
  destination table or hashes retained leaves. Missing local map state fails
  loudly and requires an explicit full rebuild.
- **5 -- persistent manifest map in `commit_object`.** *(Done.)*
  `watertown.commit.v2` names the persistent manifest root and carries only the
  transaction's canonical before/after identity changes plus introduced
  object/pack descriptors. The former monolithic `node_manifest_hash` field is
  not part of the native-v2 commit or transfer path.
- **5b -- checksum subsumption.** *(Done.)* A partition *is* a directory; that
  directory's `tree_hash` in the content tree *is* its content checksum.
  `fsck` and the compaction invariant now compare content-tree hashes
  (`root_tree_hash` per pond, per-directory `tree_hash` per partition), and the
  Tier-0 `row_leaf_digest` partition checksums are retired from the active
  commit path. The per-transaction partition checksums -- previously the last
  pond-derived datum that `control/` held and `rebuild-control` could not
  recover -- are **no longer computed**, because the same guarantee comes from
  the pond-resident tree hashes. The `DataCommittedMetadata.partition_checksums`
  field has been removed outright, together with the dead bundle-based
  replication stack (the `sync-remote` and `sync-steward` crates and the
  `steward` `remote_adapter`); production replication is entirely
  content-addressed. The live control-table schema formerly in `sync-steward`
  now lives in `steward::inner_control`.

The endpoint: **`control/` holds nothing pond-derived that isn't rebuildable
from `data/`.** The audit log comes from Delta `pond_txn` history; the spine
comes from the LOG node; the checksums come from INDEX tree hashes. The invariant
in Section 1 becomes fully realized rather than aspirational.

- **6 -- validation.** Incremental-vs-rebuild equivalence for both roots, plus a
  "discard `control/`, rebuild from the pond" test that asserts the
  reconstructed spine matches byte-for-byte (now possible because provenance is
  in the LOG node). Keep `tlog_materialize_test`, `content_pull_test`, and
  `testsuite/tests/719-tlog-verify.sh` green.

## 6. What legitimately stays local (and why that is coherent)

`control/` is disposable, not empty. After the full sequence it retains exactly
three things, none of which contradict the invariant:

1. **The write lock.** An ephemeral, single-host concurrency gate. It describes a
   momentary intent, not durable state; there is nothing to reconstruct.
2. **A cache of pond-derived facts.** The audit log and the spine cache are
   copies of what `data/` already proves. They exist for fast local queries
   (`pond log`, tip lookup) and are rebuildable at any time -- `pond
   rebuild-control` reconstructs the audit skeleton from Delta history and
   replays the commit spine from the authoritative LOG node, so `pond log`,
   tip lookups, and content-addressed push all work after a rebuild. Phase 6's
   `rebuild_control_preserves_content_roots` confirms the `fsck` content root,
   the content-tree root, and the full commit-object spine survive a control
   discard + rebuild byte-for-byte.
3. **Local operator state.** Which remotes this replica is attached to, their
   modes, and the `last_pushed_seq` / `last_pulled_seq` watermarks. This is the
   one class of state that is *not* rebuildable from the pond -- and that is
   correct: it is not pond content. It describes *this replica's relationship to
   other replicas*, is deliberately never shipped with a backup (so pushing a
   pond never leaks local watermarks), and is re-established with `pond remote
   add` after a rebuild. A pond is a shared artifact; its replication topology is
   local.

This is the coherence test for any future addition: if a new datum is *content*
or *history*, it belongs in `data/` (in the tree, the INDEX node, or the LOG
node). If it is *this replica talking to other replicas*, it belongs in
`control/`. Nothing else may live only in `control/`.

## 7. Replication, restated in these terms

- **Push** ships content by hash (blobs, trees, the commit chain from the LOG
  node) plus external `_large_files` blobs by hash. It never ships `control/` or
  the INDEX node.
- **Pull** fetches the commit chain and content closure, reconstructs `data/`,
  and **rebuilds the INDEX node locally** (never trusting a shipped index). The
  LOG node is populated from the fetched commits -- it *is* the transferred
  history.
- **`tlog/`** is a derived SHA-256 tile export of the LOG node, reconciled after
  each commit; a lost or lagging export self-heals against the LOG node.

Two replicas are identical iff their `root_tree_hash` matches -- a pure,
lineage-independent content check that needs neither `control/` nor matching
`node_id`s, only the pond.

## 8. Glossary of authority

| Datum | Authoritative home | Disposable copies |
|---|---|---|
| User files & directories | `data/` filesystem rows | -- |
| Large blobs (> 64 KiB) | `data/_large_files/` (by hash) | remote chunk table |
| `root_tree_hash`, node manifest | recomputable from `data/` rows; cached in INDEX node | -- |
| Commit spine + provenance | **LOG node** (`data/`) | `control/` spine cache |
| Transaction audit log | Delta `pond_txn` history (`data/`) | `control/` audit rows |
| Partition content checksum | content-tree `tree_hash` (recomputed from `data/`; cached in INDEX node) | `control/` checksums (retired -- field removed) |
| Transparency-log leaves | **LOG node** (`data/`) | `tlog/` tile export |
| Remote topology & watermarks | `control/` (local only) | -- |
| Write lock | `control/` (ephemeral) | -- |
