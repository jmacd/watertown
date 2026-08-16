# Logical Series Identity and Physical Packs

> **Status:** Proposed. This is a wire-format change and must not be enabled
> until mixed-version replication and migration tests pass.

## Problem

Today a series node's `child_hash` is `blake3(dp.series.1 || ordered parquet
blob hashes)`. Series collapse concatenates live parquet runs into a new blob
and supersedes the old runs. Although the logical rows are unchanged, the blob
list, series object, directory hashes, pond root, and commit tip all change.
Replication therefore treats physical maintenance as a logical edit.

Delta `OPTIMIZE` is not the problem: it rewrites Delta files without changing
live oplog rows. The identity error is specifically the series-collapse model,
where physical run layout is stored as logical series history.

## Invariants

1. Appending, deleting, or changing a logical observation changes series
   identity.
2. Repacking the same observations does not change series identity, metadata,
   directory hashes, or the pond root.
3. Every pack is independently verifiable against the logical range it serves.
4. A reader can recover after losing all local packs by fetching advertised
   packs from a remote.
5. Replicas may choose different valid pack layouts for the same logical
   series.
6. No v1 reader may silently interpret v2 identity.

## Model

Separate each series into two layers.

### Logical manifest

The logical manifest is immutable, append-oriented history. Each leaf commits
to a canonical Arrow representation of a bounded row group:

```text
logical_leaf = blake3(
  schema_fingerprint ||
  canonical_rows ||
  min_event_time ||
  max_event_time ||
  logical_attributes
)
```

The series identity is a domain-separated Merkle root over ordered logical
leaves plus the schema fingerprint and logical row count. It is the only
series value used by `ManifestEntry.child_hash` and directory folding.
Physical parquet encoding, compression, file boundaries, and pack location do
not contribute.

Canonical rows require a frozen specification before implementation: Arrow
type encoding, null representation, dictionary normalization, timestamp
timezone handling, floating-point NaN normalization, and row ordering must be
byte-identical across writers.

### Physical pack index

A pack index maps a contiguous logical-leaf range to one or more content-
addressed parquet objects:

```text
pack = {
  logical_start,
  logical_end,
  logical_range_root,
  parquet_objects,
  row_count,
  byte_count
}
```

Pack indexes are derived storage metadata. They are excluded from the logical
content tree, like Delta file layout. A pack is accepted only when decoding it
recomputes the declared logical range root. Multiple overlapping pack choices
may coexist; readers choose any verified covering set.

Collapse writes a replacement pack, verifies its logical range root, publishes
the new pack index atomically, and only then makes superseded packs reclaimable.
It never edits the logical manifest.

## Wire compatibility

Introduce `dp.series.2` for logical manifests and a separately tagged
`dp.series-pack.1` object. A tree entry remains structurally unchanged: its
`child_hash` names either a v1 series object or a v2 logical manifest, detected
by object magic after fetch.

Commit encoding must advance from `dp.commit.3` to `dp.commit.4` and include a
content-model version. This deliberately prevents old binaries from accepting
v2 roots. New binaries read both commit versions during migration but publish
only the configured version. Remotes retain v1 objects until no published v1
tip references them.

## Migration

Migration is explicit per pond and resumable:

1. Freeze collapse and record the source v1 tip.
2. Read each live v1 series in logical row order and produce v2 logical leaves
   plus an initial verified pack index.
3. Build the complete v2 manifest and root in a preview transaction.
4. Verify row counts, schema, event-time bounds, attributes, and canonical
   logical roots against a second full scan.
5. Commit one migration transaction and publish a `dp.commit.4` tip.
6. Keep the v1 tip and its reachable objects pinned through the rollback
   window; rollback republishes that tip rather than translating backward.
7. Re-enable collapse using pack-only maintenance.

Migration cannot preserve the old root because v1 commits to parquet bytes and
v2 commits to canonical logical rows. It produces one intentional root change;
all later physical repacks must preserve that new root.

## Delivery gates

1. Freeze canonical row encoding with cross-process golden vectors.
2. Implement v2 logical-manifest and pack codecs without changing writers.
3. Add a dual reader and mixed v1/v2 fetch tests.
4. Prototype pack verification and prove repeated collapse preserves the v2
   series root while changing physical blob layout.
5. Add interrupted-migration, rollback, remote refetch, out-of-order pack, and
   corrupt-pack tests.
6. Enable v2 writing behind a pond setting; make it the default only after
   deployed readers understand `dp.commit.4`.

The prototype succeeds only if an append changes the logical root, a repack
does not, and a clean replica reconstructs identical rows using a different
verified pack selection.
