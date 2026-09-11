# Incremental Content Tree: O(change) Commits

> **Status:** Active design, implementation in progress. **Supersedes**
> [`content-addressed-pond-design.md`](./archive/content-addressed-pond-design.md),
> which remains a valid description of the object model and transparency log but
> describes per-commit hashing costs that this note replaces. Where the two
> disagree about *how* the commit spine is computed and stored, **this document
> wins**. The object model (blobs, trees, commits, the transparency log file
> layout) is carried forward unchanged.
>
> **Native-v2 update:** `low-cost-correctness-review.md` Section 16 supersedes
> this document's flat INDEX-node encoding and legacy remote layout. The local
> INDEX node now stores only a persistent Patricia-map root pointer;
> `watertown.commit.v2` carries that root and the transaction's bounded
> identity/object delta. Remote payloads are raw immutable
> `_content/v2/objects` with receipts and a dedicated `_publication/` Delta
> active-row table. The historical phase narrative below remains useful
> rationale, not the current wire/storage specification.

---

## 0. Implementation progress

Phases track the plan in Section 8; each is independently reviewable.

| Phase | Scope | Status |
|---|---|---|
| Tier 0 | Single narrow post-commit scan (no inline blob bytes) + checksum row-leaf reset | **Done** |
| 2 | Reserved index node (now a fixed-size persistent-map root pointer) | **Done, superseded encoding** |
| 3 | Persistent path-compressed node-identity Merkle map | **Done in native v2** |
| D9 | Unified single-pond storage: authoritative LOG + derived INDEX; control demoted (Section 10) | Design accepted |
| 4a | Spine relocation + atomic commit (LOG node, in-txn `commit_object`) | **Done** |
| 4b | Incremental commit fold (content root from changeset along touched path) | **Done** |
| 5 | `watertown.commit.v2` names the persistent manifest root and bounded delta | **Done** |
| 5b | Checksum subsumption (per-directory `tree_hash` replaces `row_leaf_digest`) | **Done** |
| 6 | Validation (equivalence + rebuild-from-pond; keep tlog/pull/719 green; presubmit) | **Done** |

### Remote pull ancestry cost invariant

There is no remote object or commit index in native v2. A cold open reads the
checkpointed, retention-bounded `_publication/` table. An acknowledged
consumer follows immutable per-push publication records only until its known
record/tip boundary. A structured acknowledgement fixes the exact read count
from the generation delta and requires its immutable record at that boundary;
a tip-only walk is capped by the active generation. Fetch then reads changed
manifest records and changed objects by exact raw keys and never lists a
cumulative payload namespace. An initial clone explicitly traverses the
requested manifest map. Tests compare identical
consumer changes after 1, 100, and 1,000 prior updates and assert equal request
counts with only fixed encoding variance in bytes.

### Local incremental pull cost invariant

An acknowledged destination does not fold its complete local Delta table to
plan an ordinary pull. Its reserved `.pond-node-index` stores the current
persistent Patricia-map root; imported foreign ponds carry the same reserved
pointer under their own pond partition. A fixed local cursor normally supplies
that pointer without opening the series; after cache loss one latest-row query
recovers and rewrites the cursor. Planning point-loads only changed
node records plus the parent paths needed for ordering, deletion, and
collision-safe rename handling. A missing/mismatched root or map object fails
loudly and requires an explicit full rebuild; there is no silent full-scan
fallback.

For a changed series, the prior manifest record names the local
`watertown.series.v3` object. Fetch starts at the current canonical locator and
extends backward only until it covers that node's exact stored leaf frontier;
a new node instead reaches leaf zero (preferring a consolidated locator when
available). The retained frontier may fall inside a differently segmented
pack. Its Merkle frontier and aggregate metadata are authenticated without
enumerating retained leaf rows. Materialization appends only the verified
suffix; file-series physical hashing resumes from its persisted Bao frontier
and at most one bounded tail block.

Mirror retries and exact-identity no-ops authenticate the destination with one
bounded latest-row query against the authoritative reserved manifest pointer,
never from the best-effort cursor alone, and verify its root record against the
fetched tip commit. If the data
transaction already applied that tip but the source-bound acknowledgement did
not commit, the retry repairs only the acknowledgement. An authenticated
fast-forward whose before/after content roots are identical likewise advances
mirror acknowledgement without a data transaction; graft import atomically
advances its source-tip pin. Graft pulls fetch one exact selected publication
state, authenticate that same graph to the structured acknowledgement, and
only then narrow it to an applied pin. A cleared acknowledgement requires the
pin's immutable publication record, commit, and roots to be reconstructed
within the active generation bound.

Cost tests apply an unrelated file change beside an unchanged 1,000-leaf
series and one-leaf appends after 1, 100, and 1,000 retained leaves. They expose
whether a full target scan ran and count local manifest-node files, manifest
root cursor/latest-row reads, manifest records, prior series manifests, and
retained leaf hashes read. Ordinary incremental results are fixed plus changed
paths, with zero retained leaf hashes scanned.

**Tier 0 (done).** Landed on branch `jmacd/65`. The two full-table `SELECT *`
post-commit scans (content-tree fold + partition checksums) are now one shared
**narrow** scan that never reads inline file `content`/`bao_outboard` bytes; the
partition-checksum row leaf was redefined to exclude those redundant bytes (a
coordinated Decision D2 reset). `commit_object`, `root_tree_hash`, the node
manifest, and every transparency-log byte are unchanged -- only the
partition-checksum *values* reset. Code:

- `content_tree::scan_live_rows` / `scan_live_rows_ctx` -- narrow projection
  (`arrow_cast(NULL, 'Binary')` for the two byte columns) plus a `WHERE blake3
  IS NULL` query that splices back the small `content` of the only rows the fold
  decodes (directories, symlinks, dynamic-node configs).
- `content_tree::fold_rows` -- split out of the old `scan_and_fold`; fold logic
  unchanged.
- `content_tree::compute_commit_snapshot` -- one scan yielding both content
  roots **and** the partition checksums; `assemble_commit_spine` does only the
  parent lookup. The guard's main commit path now scans once instead of twice.
- `remote_adapter::row_leaf_digest` -- the single leaf definition, shared by the
  per-commit checksum and `fsck`; `partition_checksums_from_rows` computes from
  already-scanned rows.
- Unit tests cover the narrow-scan splice and full-vs-narrow leaf equivalence;
  full workspace suite, clippy, and fmt are green.

See Section 7 for the rationale and Section 6 for the cost table.

**Phase 2 (done).** The reserved node-manifest index node is implemented
(Approach A: written in-transaction, pre-commit). One well-known `FileID`
(`tinyfs::INDEX_NODE_UUID = 00000000-0000-7700-8000-000000000000`, the `0x7`
low nibble in byte 6 encoding `FilePhysicalSeries`) holds the full node manifest
as a raw-byte series node named `.pond-node-index` under the data-FS root. Each
write transaction folds the in-transaction live state (committed rows plus this
transaction's pending records and synthesized modified-directory snapshots) into
the complete manifest and appends it as one index-node version; a *collapsing*
write keeps the node at a single live version, so it never becomes a
user-visible compaction candidate. The reserved node is excluded from the fold
(so `root_tree_hash` and the manifest are unchanged by its presence) and hidden
from all user-facing directory enumeration. Phase 2 still keeps the post-commit
full-rescan fold for the committed roots; Phase 4 retires that and switches the
index node from a full manifest to a touched-only delta. Code:

- `tinyfs::INDEX_NODE_UUID` / `index_node_uuid()` / `NodeID::is_index()`
  (`node.rs`); enumeration filter in `WD::get_entries`/`entries` (`wd.rs`).
- `tlogfs::persistence::State::uncommitted_live_rows` -- non-mutating pending +
  synthesized modified-directory rows for the in-transaction fold.
- `content_tree::in_txn_manifest_bytes` -- merges committed + uncommitted rows,
  orders latest-version-wins, folds, and encodes the manifest; `hash_directory`
  skips the reserved node id (fold exclusion).
- `guard::write_index_node` -- called in `commit()` before `data_tx.commit()`
  for write transactions; `create_file_with_id` on first commit, collapsing
  series write thereafter. Unit test `ship::test_index_node_phase2`.

**Phase 3 (done).** The node-keyed Merkle of Section 4.2 is implemented as a
standalone `sync-store` primitive, not yet wired into `commit_object` (that is
Phase 5). It is a sparse binary Merkle tree of fixed depth 256 keyed by
`blake3(node_id)`, with an `EMPTY[d]` ladder collapsing the empty positions so
neither implementation materializes empty subtrees. Two implementations produce
byte-identical roots, mirroring the tlog tile writer/rebuilder pairing:

- `content::node_merkle::NodeMerkle` -- the incremental updater. It stores every
  populated node hash, so `set`/`remove` recompute only the changed leaf's
  root-to-leaf path and `root()` is `O(1)`. `set_entry` derives a leaf value
  digest from a `ManifestEntry` (entry type, length-prefixed name and parent,
  child hash) so a rename, reparent, retype, or content change all move the
  root; the `node_id` is the key.
- `content::node_merkle::rebuild_root` (re-exported as
  `node_merkle_rebuild_root`) -- the `O(n)` whole-set rebuilder / oracle; sorts
  the `(key, value)` pairs and folds them recursively, sharing the same
  leaf/interior/empty hashing rules. It rejects duplicate `node_id`s, the same
  guard `encode_manifest` applies.

The `equivalence_over_random_mutations` unit test drives the incremental updater
through 4000 random insert/update/delete operations over a small key space and
asserts its root stays byte-identical to a from-scratch rebuild of the live set
after every mutation; scripted rename/reparent/retype/content and
insert-then-remove tests pin the leaf semantics. Phase 4 will drive this updater
from the transaction changeset and feed its root into `commit_object` (Phase 5).

---

## 1. Why this document exists

The content-addressed design computes a `root_tree_hash` and a
`node_manifest_hash` for every commit; both are the leaf of the transparency
log and the tip of the content tree. The original note idealized an
incremental fold but the shipped commit path does not implement it, and one of
the two hashes could not be made incremental as specified at all. The result is
that **every commit costs `O(#nodes)`** regardless of how small the change is.

This document specifies a commit path whose cost is proportional to the
**change**, not the pond size, while keeping the content-addressed object model
and the transparency log byte-for-byte compatible in layout.

The single organizing idea:

> **All derived per-node hashes live in one reserved, delta-versioned index
> node inside the data filesystem. Directories are never rewritten for a
> descendant change, both content roots (`root_tree_hash` and the manifest
> root) update along the touched path only, and the transparency log is
> unaffected.**

---

## 2. What is wrong today (measured)

Three independent `O(#nodes)` costs run on every commit:

1. **`content_tree::scan_and_fold`** issues
   `SELECT * FROM content_live ORDER BY pond_id, part_id, node_id, version`
   -- a full read of every row and version, including inline file `content`
   bytes (up to the 64 KB large-file threshold) that the fold never uses --
   then folds the whole tree bottom-up.
2. **`remote_adapter::compute_live_checksums_for_table`** issues a second full
   `SELECT *` over the same rows in the same order, for partition checksums.
3. **`manifest_hash`** (`sync-store/content/manifest.rs`) is a **flat** hash
   over the entire sorted list of node entries, re-encoded and re-hashed every
   commit inside `compute_commit_roots_for_table`. It is part of `commit_object`
   (Decision D8), so it is mandatory and cannot be skipped.

Costs 1 and 2 are two full table scans; cost 3 is a full re-hash. A single-file
edit in a million-node pond pays all three at full size.

### Why storing tree hashes in directory records is not the fix

The recursive fold means that when a deep file changes, every ancestor
directory's `tree_hash` genuinely changes. If those hashes were stored *inside*
each directory's own record, every deep write would append a new version of
every ancestor directory -- across as many partitions as there are ancestors --
which is the worst case for a log-structured store (many small files, fast
metadata growth). The current filesystem deliberately avoids this: a directory
row is rewritten **only when its own child listing changes**
(`flush_directory_operations` skips any directory not marked `modified`), so
adding a deep file rewrites only the immediate parent. **This property must be
preserved.** The derived ancestor hashes have to live somewhere that is *not*
the directory records.

### Why the control filesystem is not that place

The control filesystem (`{POND}/control`) is **disposable**: it supervises the
pond and is not replicated to the remote. Any durable, pond-derived content
must live in the data filesystem so it survives and replicates. Derived hashes
are pond content; they belong in the data FS.

---

## 3. The reserved index node

Introduce one node with a **well-known, reserved `FileID`** (the same mechanism
that gives the root directory its fixed id, `ROOT_UUID`). It lives in the data
filesystem, so it is durable and replicated. It holds the **persisted node
manifest**: one entry per node,

```
node_id -> ( parent_node_id, name, entry_type, child_hash )
```

for every node in the pond, plus tombstones for deleted nodes. This is a
superset of the per-directory `tree_hash` table the original design called for
(Section 5.2 there): file leaf hashes are included too, so the fold and the
manifest read exactly one structure and never touch file rows for a child hash.

Four properties define it:

- **Delta-versioned.** Each commit appends **one** new version containing only
  that commit's touched entries and tombstones -- the touched ancestor chain
  becomes *rows in one node version*, never files across partitions. Reads
  merge versions latest-wins, exactly the existing series merge-on-read;
  accumulated versions are collapsed by the existing `collapsed_through`
  compaction sentinel.
- **Durable and replicated.** It is an ordinary data-FS node, so it is backed
  up and synced with everything else.
- **Excluded from the fold.** Because its content is derived from the very
  hashes it stores, including it in `root_tree_hash` would be self-referential.
  It is skipped by the fold and by the manifest, the same way unresolved
  cross-pond mounts are skipped. It is therefore not attested by
  `root_tree_hash` and not transferred on content-addressed pull -- a consumer
  **rebuilds** it from the tree it receives. It is a cache with a durable
  backing, not a source of truth.
- **Single writer per commit.** One node, one new version per transaction: the
  minimum possible storage operation.

### Tombstones

Series data has no native deletion. A deleted node is recorded as an explicit
tombstone entry in the delta so the merged view drops it. Compaction discards
tombstones whose target is absent from the baseline.

---

## 4. The two content roots, computed incrementally

Both roots the commit needs are now derived from the index node plus the
transaction's in-memory changeset. The transaction already knows exactly which
nodes it touched (`pending_records`, modified directories), so nothing is
re-scanned to discover the change.

### 4.1 `root_tree_hash` -- incremental fold

```
1. Merge the index node's latest state into a node_id -> child_hash view.
2. touched = union of ancestor chains of the changed nodes.
3. For each touched directory, bottom-up, recompute
      tree_hash(dir) = fold(sorted (name, entry_type, child_hash) of its children)
   reading unchanged children's hashes from the merged view.
4. root_tree_hash = the recomputed root.
```

Work is proportional to the touched paths' breadth, not the pond. Unchanged
subtrees are never visited.

### 4.2 `node_manifest_root`: adding a node-keyed Merkle root

The `commit_object` gains a **node-keyed Merkle root** over the manifest
entries alongside the existing flat `node_manifest_hash` -- a sparse Merkle tree
/ Merkle search tree keyed by `node_id`, whose root recomputes only along the
touched leaves' paths on a point update (`O(change * log n)`), instead of
re-hashing all entries (`O(#nodes)`).

The flat `node_manifest_hash` is **kept, not replaced**: it is the
content-address the pull path uses to fetch and verify the monolithic manifest
object (`blake3(manifest_bytes) == node_manifest_hash`). The Merkle root is a
separate commitment that enables incremental verification and, later,
incremental manifest transfer. Replacing the flat hash would break the
monolithic-manifest transfer, so both travel in the commit until a later phase
transfers the manifest as a content-addressed tree.

The existing `sync-store/checksum::Merkle` is **not** reused for the root: it is
a rebuild-only, sorted-array construction (`O(n log n)`, sorts every leaf) and
its root differs from a sparse-tree root for the same set. Instead we define
**one** node-keyed construction with two implementations that must agree
byte-for-byte:

- an **incremental updater** used on the commit hot path, and
- a whole-set **`O(n)` rebuilder** used to build or repair the structure and as
  a test oracle,

with an equivalence test asserting identical roots across long random mutation
sequences. This mirrors the transparency-log tile writer, which already pairs an
incremental writer with an `O(n)` rebuilder and asserts byte-identical output.

### 4.3 `commit_object` and the format reset

```
commit_object = BLAKE3( root_tree_hash,
                        parent_commit_hash,
                        node_manifest_hash,   // flat: manifest object fetch key
                        node_manifest_root,   // new: node-keyed Merkle root
                        provenance )
```

Adding the Merkle-root field changes `commit_object`'s bytes, and therefore the
**value** of every transparency-log leaf. This is a permitted format change
under Decision D2 (the project resets rather than migrates; there are no legacy
ponds), and it requires a coordinated reset of any peers that sync. The
transparency-log **file layout is unchanged**: the leaf is still
`SHA-256(commit_object)`, the tiles and checkpoint are byte-identical in
structure, only the leaf values differ.

---

## 5. The transparency log is unaffected in shape

The log's authoritative leaf sequence is still the ordered `DataCommitted`
records in the control table, each storing `commit_object`. The C2SP
`tlog-tiles` under `{POND}/tlog` remain a derived, re-materializable export
(Decision D5). Three sequences advance one-per-commit in lockstep:

| Per commit | Lives in | Role |
|---|---|---|
| `commit_object` (LOG node version) | **data FS (the pond, replicated)** | **authoritative leaf log** |
| `DataCommitted` mirror | control table (disposable) | rebuildable cache of the tip |
| tile + checkpoint | `{POND}/tlog` | derived export for external witnesses |
| index node version | data FS (replicated) | cache making both roots `O(change)` |

> **Architecture revision (Section 10).** The authoritative `commit_object`
> sequence has moved out of the disposable control table into a pond-resident,
> append-only **LOG node** in the single pond Delta instance. The control table
> keeps only a rebuildable cache. See Section 10 for the unified two-reserved-node
> model; the paragraphs above and the "Why the control filesystem is not that
> place" note in Section 2 are the motivation.

`pond tlog show` / `pond tlog verify` are unchanged in shape. Only the computation
and storage of the two content roots move, and the authoritative leaf sequence is
now read from the pond LOG node rather than the control table.

---

## 6. Cost analysis

Per commit, for a change touching `k` nodes across paths of depth `d` in a pond
of `n` nodes:

| Cost | Before | After |
|---|---|---|
| Content-tree fold | `O(n)` full scan + fold | `O(k*d)` touched paths |
| Partition checksum scan | `O(n)` second full scan | folded into one shared scan (Tier 0) |
| Manifest hash | `O(n)` flat re-hash | `O(k*log n)` Merkle path updates |
| Data-table reads for child hashes | inline `content` bytes dragged in | none: hashes read from the index node |
| New storage operations | directory + node rows | + **one** index-node version (delta) |

The dominant `O(n)` terms all become proportional to the change. The only new
per-commit storage operation is a single small delta version of the index node.

---

## 7. Tier 0: an independent, immediate win

Before any of the incremental machinery, one change stands alone and ships
first:

- Replace the two `SELECT *` post-commit scans with a **narrow metadata
  projection** that never reads inline file `content` or `bao_outboard` bytes,
  and **unify them into one shared pass** feeding both the partition-checksum
  computation and the content-tree fold.

A file row's inline `content` is redundant with its `blake3` (which is
`blake3(content)` for physical versions), and `bao_outboard` is a
verified-streaming tree derived from the same content; both are dropped from the
scan. The only rows whose small `content` the fold still needs -- directories,
symlinks, and dynamic-node configs -- carry no `blake3`, so they are fetched by
a second narrow query filtered to `blake3 IS NULL`.

Because the partition-checksum leaf previously hashed the **whole** serialized
row (including those inline bytes), dropping them requires **redefining the row
leaf hash** to exclude `content`/`bao_outboard` and rely on `blake3` instead.
This changes the partition-checksum *values* -- a coordinated reset under
Decision D2 (the checksum lives in the disposable control table and replicated
bundles; there are no legacy ponds) -- but it does **not** touch `commit_object`,
`root_tree_hash`, the node manifest, or any transparency-log bytes: those are
computed by the fold, which is unaffected. The single leaf definition is shared
by the per-commit checksum and `fsck`, so both stay consistent.

This cuts per-commit read volume substantially (no inline blob bytes) and halves
the scan count, while leaving `commit_object` and the content roots byte-for-byte
identical. It is a safe first pull request while the incremental path lands
behind it.

**Status: implemented.** `content_tree::scan_live_rows` (narrow scan + splice),
`content_tree::fold_rows`, `content_tree::compute_commit_snapshot` (one scan ->
both roots + checksums), `remote_adapter::row_leaf_digest` (shared with `fsck`),
and `remote_adapter::partition_checksums_from_rows`.

---

## 8. Implementation plan

Roughly in dependency order; each phase is independently reviewable.

1. **Tier 0 -- narrow scan + unification + checksum leaf reset.** Project only
   the metadata the fold and checksum need (no inline `content`/`bao_outboard`;
   structural rows re-fetched via `blake3 IS NULL`); run one shared post-commit
   scan instead of two; redefine the checksum row leaf to exclude the inline
   bytes (a D2 checksum reset, `commit_object` unchanged). (`content_tree.rs`,
   `remote_adapter.rs`, `fsck.rs`.) **Done.**
2. **Reserved index node.** Allocate the well-known `FileID`; write a
   delta-versioned manifest node (all nodes + tombstones); reuse series
   merge-on-read and `collapsed_through` compaction; exclude it from the fold.
   **Done.**
3. **Incremental node-keyed Merkle.** One construction, incremental updater plus
   `O(n)` rebuilder oracle, byte-identical equivalence test. **Done.**
4. **Incremental commit fold.** Compute both roots from the changeset plus the
   index node along the touched path; feed `commit_object`; retire the
   post-commit full-rescan fold, keeping the whole-tree fold as rebuilder and
   oracle. **Now scoped under the Section 10 unified design** (in-transaction
   roots enable the in-transaction LOG-node spine).
5. **`commit_object` node-keyed Merkle root.** Add `node_manifest_root`
   alongside the flat `node_manifest_hash` (kept as the manifest object's fetch
   key); update encode/decode and bump the commit magic; keep
   `SHA-256(commit_object)` as the leaf; the pull path verifies both the flat
   hash and the Merkle root against the tip commit. Incremental manifest
   transfer is deferred to a later delta-INDEX phase. **Done.**
6. **Validation.** Equivalence tests for both roots (incremental vs rebuild)
   across random mutation sequences; keep `tlog_materialize_test`,
   `content_pull_test`, and `testsuite/tests/719-tlog-verify.sh` green;
   presubmit (`cargo fmt --all`, `cargo clippy --workspace --all-features -- -D
   warnings`, `cargo test --workspace`).

See Section 10 for the revised, unified implementation sequence that folds the
spine relocation, control demotion, and checksum subsumption into these phases.

---

## 9. Decisions record (relative to the superseded document)

- **Supersedes D3.** "Persist tree hashes; recompute only the touched ancestor
  chain" is retained in spirit but relocated: the persisted structure is the
  reserved delta-versioned index node in the data FS, not a per-directory field
  and not a separate control-side table. The incremental fold this enables is
  now mandatory, not idealized.
- **Adjusts D1.** Rows stay keyed by `(pond_id, node_id, version)` and the index
  node reuses that same versioned-node lifecycle (ordinary retention/vacuum, no
  new GC), so D1's rejection of at-rest content-addressing still holds; the
  index node is simply another versioned node under that same regime.
- **New: reserved index node.** All derived per-node hashes (the persisted node
  manifest, all nodes) live in one reserved well-known `FileID` in the data FS,
  delta-versioned, excluded from the fold, rebuilt on pull.
- **New: node-keyed Merkle manifest root.** `commit_object` carries an
  incremental node-keyed Merkle root in place of the flat `manifest_hash`; this
  is a D2 format reset that changes transparency-log leaf values but not the log
  file layout.
- **New (D9): unified single-pond storage.** The pond is one `data/` Delta
  instance holding two reserved fixed-`FileID` nodes -- an authoritative
  append-only **LOG** node (the `commit_object` spine / transparency-log leaves,
  transferred on pull) and the derived **INDEX** node (manifest + incremental
  Merkle/child-hash caches, fold-excluded, rebuilt on pull). `{POND}/control` is
  demoted to a disposable, rebuildable cache plus the single-writer gate;
  partition checksums are subsumed by the content tree's per-directory
  `tree_hash`. This supersedes the earlier placement of the authoritative spine
  in the control table. See Section 10.
- **Unchanged:** the object model (blob/tree/commit/series/recipe hashing),
  D4 recipe hashes, D6 single delta-managed remote, D7 external large blobs,
  D8 node-id adoption via the manifest, D5 checkpoint-every-commit tiles, and
  the deferred items (signing, writable fork).

---

## 10. Unified single-pond storage (Decision D9)

> The architectural end state -- including steps 4b/5/5b and the coherence
> argument for a disposable control filesystem -- is written up separately in
> `docs/unified-pond-storage-architecture.md`. This section records the decision
> and the phased mechanics.

The pond is **one `data/` Delta Lake instance**. Steward (`{POND}/control`) is
**not part of the pond**: it provides local concurrency control and gates access,
is disposable, and is fully reconstructable from the pond. Authority flows one
way -- `pond -> control` -- so nothing may exist only in control that cannot be
rebuilt from the pond.

Two reserved fixed-`FileID` nodes carry all commit machinery, distinguished only
by their transfer semantics:

```
  THE POND  (single data/ Delta instance)
  |- filesystem rows                       (partitions = directories)
  |- _large_files/blake3=*                 external blobs (no Delta schema fits raw bytes)
  |
  |- INDEX node   (INDEX_NODE_UUID)              DERIVED, fold-excluded, REBUILT on pull
  |     - node manifest: node_id -> parent,name,type,child_hash
  |     - incremental caches: child-hash map + NodeMerkle nodes
  |     - per-directory tree_hash == that partition's content checksum
  |
  +- LOG node     (new reserved FileID)          AUTHORITATIVE, fold-excluded, TRANSFERRED
        - append-only: one version per commit = commit_object
          (embeds root_tree_hash + parent + provenance)
        - IS the transparency-log leaf sequence, pond-resident

  NOT THE POND
  |- control/    disposable: single-writer gate + cache of the tip
  |                reconstruct := read LOG tail + fold current state
  +- {POND}/tlog  derived SHA-256 tile export of the LOG node
```

|  | authoritative? | transferred on pull? | recovered by |
|---|---|---|---|
| INDEX node | derived | no (rebuilt) | re-fold the rows |
| LOG node | authoritative | yes | it *is* the history |

Both are fold-excluded because their contents are derived from (INDEX) or
reference (LOG, via `root_tree_hash`) the very root they would otherwise be
hashed into -- self-reference, the same reason the index node was excluded in
Phase 2. They are kept as two `FileID`s rather than one because INDEX is `O(n)`
and rebuildable (do not ship it -- bandwidth) while LOG is small and
authoritative (ship it).

### Consequences

- **Atomic commit.** LOG version + INDEX version + data rows land in one Delta
  transaction, eliminating today's non-atomic "data commit, then control record"
  window.
- **Control demoted.** `DataCommittedMetadata` (roots, spine) is no longer
  authoritative; the control table keeps only a rebuildable tip cache.
- **Checksums subsumed.** A partition is a directory; its `tree_hash` in the
  INDEX node is its content checksum. Replication compares content-tree hashes;
  the Tier-0 `row_leaf_digest` partition checksums are retired onto content-tree
  hashes (consistent with "the fsck Merkle is a vestige").
- **`_large_files` stays external** (raw blobs fit no Delta schema); the remote
  backup chunks them into a Delta table, but the local pond references them by
  hash.

### Revised implementation sequence

Refines Section 8 steps 4-6 without changing their spirit; each step is
independently reviewable and keeps the whole suite green.

1. **Spine relocation + atomic commit.** *(Done.)* Compute `root_tree_hash`
   and the manifest hash **in-transaction** (reuse the Phase 2 in-txn fold),
   build `commit_object`, and append it to the new reserved **LOG** node in the
   same Delta transaction. Demote control's `DataCommitted` to a rebuildable
   cache and read the authoritative leaf sequence (`pond tlog *`,
   materialization) from the LOG node. Still `O(n)` fold; correctness/
   architecture only.
   - The LOG node (`tinyfs::LOG_NODE_UUID`, name `.pond-commit-log`) is a
     non-collapsing `FilePhysicalSeries`; each version holds one encoded
     `commit_object` and is a permanent transparency-log leaf. It is excluded
     from the fold, hidden from enumeration, and excluded from collapse
     candidacy (`list_collapsible_series`) so leaves are never compacted.
   - The parent of each commit is the LOG node's current tip
     (`log_tip_commit_hash`), read from the committed table before the new leaf
     is appended -- not the control table.
   - **Compaction appends no LOG leaf.** Compaction is content-preserving
     (`root_tree_hash` equals the parent's), so it is transparent to the
     content graph, exactly like `git gc`/repack adds no commits. Push/pull
     resolve the tip from the last content-changing commit, whose root already
     matches. This dissolves the atomicity question for the `optimize`-based
     compaction path (which cannot inject a data row into its own Delta commit).
   - The post-commit fold is kept as a validation oracle for this step (the
     in-transaction `root_tree_hash` must equal the post-commit fold); step 4b
     retires that second scan.
2. **Incremental INDEX (Section 8 step 4).** *(Done, content root.)* Make the
   in-transaction fold incremental along the touched path: the previously
   committed node manifest (read from the index node, which reassembles a large
   manifest transparently) is the child-hash baseline, this transaction's
   changeset replaces modified directory listings and touched leaf/series
   hashes, and every directory on a root-to-change path has its `tree_hash`
   recomputed bottom-up while untouched subtrees keep their cached `child_hash`
   (`crate::content_tree::incremental_spine_inputs`). The second post-commit
   `fold_rows` is retired; the post-commit scan now yields only the partition
   checksums (checksum subsumption is step 4/5b below). `fold_rows` survives as
   a `#[cfg(debug_assertions)]` oracle asserted against the incremental roots on
   every commit, so the whole test suite validates equivalence. The manifest is
   still rebuilt and re-hashed with the flat `node_manifest_hash` each commit;
   the node-keyed Merkle root and its persisted NodeMerkle cache land with the
   `commit_object` change (step 3), where `commit_object` adopts the root
   alongside the flat hash.
3. **`commit_object` node-keyed Merkle root (Section 8 step 5).** *(Done.)* Add
   `node_manifest_root` (the NodeMerkle root) to `commit_object` alongside the
   flat `node_manifest_hash`, which is retained as the manifest object's fetch
   key; the pull path verifies both against the tip commit. Incremental manifest
   transfer via the Merkle root is deferred to a later delta-INDEX phase.
4. **Checksum subsumption.** *(Done.)* `fsck` now computes the structural
   root from each pond's content tree -- a directory *is* a partition, and its
   recursive `tree_hash` (fold-excluding the reserved INDEX/LOG nodes) is its
   content checksum; the cross-pond root is a `tree_hash` over the per-pond
   `root_tree_hash`es. The compaction invariant (`Ship::compact`) asserts the
   pond's `root_tree_hash` is byte-identical pre/post instead of comparing
   per-partition `row_leaf_digest` Merkles, and the commit path
   (`StewardTransactionGuard`, root-init) no longer computes partition
   checksums. The `DataCommittedMetadata.partition_checksums` field, the dead
   bundle-based replication stack (`sync-remote` and `sync-steward` crates, the
   `steward` `remote_adapter`, `row_leaf_digest`, and
   `compute_live_checksums_for_table`), and the `sync-tests` crate have all been
   removed; production replication is entirely content-addressed. The live
   control-table schema formerly in `sync-steward` now lives in
   `steward::inner_control`.
5. **Validation (Section 8 step 6).** *(Done.)* Incremental-vs-rebuild
   equivalence for both roots is asserted on every write transaction by the
   `StewardTransactionGuard` fold oracle (`root_tree_hash`,
   `node_manifest_hash`, `node_manifest_root`, and the manifest bytes). The
   oracle is an explicit `POND_VERIFY_FOLD` diagnostic in every build (see
   `content_tree::fold_verification_enabled`); debug mode alone never adds the
   hidden `O(n)` pass. A new
   `content_tree_test::incremental_roots_match_full_fold_over_diverse_mutations`
   drives create / nested-dir / overwrite / rename / delete through that oracle.
   A new `rebuild_control_test::rebuild_control_preserves_content_roots` discards
   the control table, rebuilds it from the data FS alone, and confirms the
   `fsck` content root and `compute_content_tree` root are byte-identical --
   proving the pond content is reconstructable from data without the disposable
   control table. `tlog_materialize_test`, `content_pull_test`, and the full
   workspace suite (plus fmt and `clippy -D warnings`) are green;
   `testsuite/tests/719-tlog-verify.sh` is unaffected (it compares tip commit
   hashes, not fsck partition digests).
