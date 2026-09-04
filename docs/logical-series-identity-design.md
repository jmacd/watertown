# Logical Series Identity and Physical Packs

Watertown series identity is logical and packing-independent. The current
native object formats are `watertown.series.v2` and
`watertown.series-pack.v2`; no prior series or pack format is recognized.

## Invariants

1. Logical content identity does not depend on Parquet encoding, row-group
   layout, compression, Bao layout, or pack boundaries.
2. Leaf order is significant.
3. Table schema is immutable per logical leaf and may evolve between leaves.
4. Packs are derived physical metadata and never contribute to a series hash.
5. A pack is trusted only after its physical content has reconstructed the
   claimed logical leaves and its range proof reaches the independently
   fetched series root.
6. Unknown or obsolete wire magic is a hard error.

## Stable logical hashing domains

These domains are current and intentionally retained:

```text
watertown.series-schema.v1\n
watertown.series-rows.v1\n
watertown.series-leaf.v1\n
watertown.series-merkle.v1\n
```

The `.v1` suffixes identify stable logical algorithms. They are not obsolete
native object formats and must not be renamed during storage-format cleanup.

### Schema identity

Canonical Arrow schemas are normalized so physical dictionary encoding does
not alter logical type identity. Field order, names, nullability, canonical
logical types, timestamp units/timezones, decimal precision/scale, and
supported schema metadata are framed under
`watertown.series-schema.v1`.

Unsupported or ambiguous Arrow types fail closed.

### Row identity

Canonical table rows are encoded under `watertown.series-rows.v1`. Scalar
presence, fixed-width values, variable-width lengths, normalized NaNs,
timestamps, decimals, and row order are explicitly framed.

### Leaf identity

Each nonempty file or table append is one logical leaf under
`watertown.series-leaf.v1`.

A table leaf commits to:

- payload kind;
- schema fingerprint;
- logical row count;
- canonical row payload length and bytes;
- independent minimum and maximum event-time bounds;
- canonical logical attributes.

A file leaf commits to the equivalent fields without a schema fingerprint,
using exact file bytes as the payload.

Empty logical leaves are not representable. Empty singleton nodes are
represented structurally by the capsule/native node rather than by inventing
a zero-length leaf.

### Merkle identity

Ordered leaf hashes are folded under `watertown.series-merkle.v1`. Range
proofs use `watertown.series-range-proof.v1` and bind a contiguous leaf range
to the complete series root.

## `watertown.series.v2`

The current series manifest contains:

```text
magic: watertown.series.v2\n
payload_kind
logical_count
leaf_count
bounds_flags and optional aggregate bounds
canonical logical attributes
leaf_merkle_root
```

It has no series-global schema fingerprint. Table schemas are committed per
leaf. The BLAKE3 hash of the exact manifest bytes is the series content
address stored in the parent tree entry.

The decoder requires the exact current magic, rejects truncation and trailing
bytes, validates canonical attributes, requires logical and leaf counts to be
zero together, and checks empty/nonempty Merkle-root consistency.

## `watertown.series-pack.v2`

A current pack index contains:

```text
magic: watertown.series-pack.v2\n
series_hash
leaf_start, leaf_end, total_leaf_count
range_root
range_proof
ordered physical_object_hashes
logical_count
physical_byte_count
ordered leaf descriptors
```

Each descriptor contains a positive logical count, an optional schema
fingerprint, optional event-time bounds, and canonical logical attributes.
Table descriptors require a schema fingerprint; file descriptors forbid one.

Pack verification requires:

1. `series_hash` equals the hash under which the manifest was fetched;
2. the range is in bounds and has exactly one descriptor per leaf;
3. descriptor logical counts equal the pack logical count;
4. physical objects decode into exactly the descriptor ranges;
5. reconstructed leaf hashes match the pack range;
6. the range proof reaches both the pack's declared root and the manifest's
   independently fetched leaf Merkle root.

Exact-cover selection is deterministic: minimize pack count, then break ties
lexicographically by pack hash.

## Writer path

The tlogfs write choke point computes and persists every leaf hash, logical
count, and table schema fingerprint. Steward folds ordered live leaf hashes
into a `watertown.series.v2` manifest and publishes a whole-range initial
`watertown.series-pack.v2`. Later repacking may replace or add pack indexes
without modifying Oplog rows, the series manifest, tree/commit roots, Delta
version, or transaction sequence.

`StorageFormat::Inline` and `StorageFormat::FullDir` remain valid schema
values. They are unrelated to removed content-object compatibility.

## Reader path

Readers decode only `watertown.series.v2`, discover current pack
advertisements, select an exact cover, verify every pack and physical object,
and reconstruct logical leaves in order. File bytes and table rows may cross
physical object boundaries within a pack, but a physical table object may not
cross a schema transition.

The target compares persisted logical leaf hashes to find an append suffix.
Non-prefix divergence is corruption or an unsupported history rewrite, not a
signal to invoke an older reader.

## Compatibility boundary

`watertown.commit.v1` explicitly selects this content model. There is no
mixed-format writer, dual reader, in-place migration, or fallback dispatch.
A pond or remote containing another commit, series, pack, or recipe encoding
must be recovered through an independently supported current-format snapshot
or reinitialized.
