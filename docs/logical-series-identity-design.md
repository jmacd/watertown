# Logical Series Identity and Physical Packs

Watertown series identity is logical and packing-independent. The current
native object formats are `watertown.series.v3` and
`watertown.series-pack.v4`; no prior series or pack format is recognized.

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

## `watertown.series.v3`

The current series manifest contains:

```text
magic: watertown.series.v3\n
payload_kind
logical_count
leaf_count
bounds_flags and optional aggregate bounds
canonical logical attributes
leaf_merkle_root
compact Merkle frontier peaks
```

It has no series-global schema fingerprint. Table schemas are committed per
leaf. The BLAKE3 hash of the exact manifest bytes is the series content
address stored in the parent tree entry.

The frontier is the canonical set of perfect left-to-right Merkle subtrees
selected by the set bits of `leaf_count`. It is derivable from the complete
leaf sequence, contributes no new logical choice, and is bounded to at most 64
hashes. A writer appends new leaf hashes to the prior manifest's frontier to
compute the new root and suffix proof without scanning prior leaves.

The decoder requires the exact current magic, rejects truncation and trailing
bytes, validates canonical attributes, requires logical and leaf counts to be
zero together, and checks empty/nonempty Merkle-root consistency.

## `watertown.series-pack.v4`

A current pack index contains:

```text
magic: watertown.series-pack.v4\n
series_hash
optional parent_series_hash
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

A canonical root segment has no parent and covers `[0, total_leaf_count)`. A
canonical append segment names the immutable prior series manifest and covers
exactly `[parent.leaf_count, total_leaf_count)`. The reader verifies that
appending the segment descriptor hashes to the parent's compact frontier
reproduces the current manifest exactly, including root, counts, bounds, and
latest logical attributes.

## Writer path

The tlogfs write choke point computes and persists every leaf hash, logical
count, and table schema fingerprint. A first publication may fold the complete
current sequence into one root segment. An ordinary append reads the prior
`watertown.series.v3` manifest, extends its bounded frontier with only the new
leaf hashes, and publishes one `watertown.series-pack.v4` suffix segment whose
descriptors and physical objects cover only that append range.

Each immutable series state has a fixed-key locator to its content-addressed
segment pack. The segment links to the prior series hash; neither the locator
nor historical packs are rewritten. Publication records carry only the
segment descriptors introduced by that push. Ordinary push never lists a pack
prefix or constructs a cumulative pack inventory.

User `FilePhysicalSeries` and `TablePhysicalSeries` writes are append-only.
The former public collapsing path now fails loudly and directs operators to a
verified `pondcapsule.4` reset for replacement semantics. The generic collapse
sentinel remains reachable only for the reserved `.pond-node-index`, whose
fixed-size manifest-root pointer replaces its prior pointer.

Remote keys are:

```text
_content/v2/packs/blake3=<pack_hash>
_content/v2/packs/by-series/blake3=<series_hash>
_content/v2/packs/consolidated/by-series/blake3=<series_hash>
```

The local explicit-maintenance sidecar is version-isolated under
`data/_packs/v4/series=<series_hash>/pack=<pack_hash>`.

`pond maintain --collapse-versions N` may build a content-addressed whole-range
pack under local `data/_packs`; it never rewrites or reclaims the source Oplog
rows. `pond backup publish-consolidated NAME` is the only production path that
uploads those verified objects and pack to one named push/both backup and then
installs the separate fixed-key consolidated locator. The command requires the
remote pond identity and current publication state to match the exact local
snapshot, meters every remote operation, and commits limiter usage on success
or failure. Fresh readers prefer that locator, so consolidation terminates
traversal without mutating or deleting the historical segment chain.
Incremental readers deliberately keep using ordinary append locators so an
existing prefix never turns into a whole-range metadata read after
maintenance.

`StorageFormat::Inline` and `StorageFormat::FullDir` remain valid schema
values. They are unrelated to removed content-object compatibility.

## Reader path

Readers decode only `watertown.series.v3` and perform fixed-key locator reads;
ordinary fetch never lists advertisements. A fresh clone starts at the current
series hash and walks only that series' immutable segment chain until a root or
consolidated segment. It verifies every segment against the independently
fetched manifest for that segment state, then recomputes the current complete
leaf Merkle root from all collected descriptors.

An incremental consumer starts with segment states introduced after its
acknowledged publication, then extends metadata traversal backward only as far
as each changed destination node requires. A new node must reach a root or
consolidated segment. An existing node point-loads its exact prior
`watertown.series.v3` object from the persistent manifest map and authenticates
that leaf count, Merkle frontier, counts, bounds, and attributes at the
corresponding point in the fetched chain. The boundary may fall inside a valid
segment whose pack layout differs from the producer that originally supplied
the retained prefix. Retained leaf rows are never enumerated or re-hashed, and
payload objects wholly before the authenticated frontier are not fetched. The
existing file-series writer resumes from its stored Bao frontier plus at most
one bounded partial block.
File bytes and table rows may cross physical object boundaries within a pack,
but a physical table object may not cross a schema transition.

Any prior-manifest mismatch is corruption or an unsupported history rewrite,
not a signal to scan the complete local series or invoke an older reader.

## Compatibility boundary

`watertown.commit.v2` explicitly selects this content model and the persistent
node-identity Merkle map. There is no
mixed-format writer, dual reader, in-place migration, or fallback dispatch.
A pond or remote containing another commit, series, pack, or recipe encoding
must be recovered through an independently supported current-format snapshot
or reinitialized.
