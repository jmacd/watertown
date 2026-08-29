# Logical Series Identity and Physical Packs

> **Status:** Implemented for native writers, remote-pull materialization,
> atomic pack publication, and pack-only local maintenance (delivery gates
> 1-7 below are done, including `rebuild_pond` / `import_pond`
> materialization of a verified v2 series into a destination pond -- see
> "Native v2 materialization (gate 4/7)" below). Adoption on an existing
> pond is a **destructive reset**, not an online migration -- see "Reset
> instead of migration" below. `pond maintain --collapse-versions` against
> a v2 pond now performs real, bounded, content-addressed repacking of
> over-threshold series (never rewriting Oplog rows, the manifest, or the
> logical root) plus its usual reclaim pass -- see "Pack-only local
> maintenance (implemented)" below. One limitation remains open: this
> bounds only the pack layout a remote/`pond://` reader selects, not a
> local, in-process read against the pond's own Oplog/Delta table, which
> still scans every live physical-version row for a series.

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

The logical manifest is immutable, append-oriented history. Its identity is
project-owned canonical bytes hashed with BLAKE3; Arrow IPC, Parquet, and Bao
outboards are not logical encodings.

There are two logical payload kinds:

- A `TablePhysicalSeries` leaf contains an ordered group of observation rows.
  Series order is append order, then original row order within each append.
  It is never inferred by sorting event time: duplicate timestamps and
  out-of-order observations are valid content.
- A `FilePhysicalSeries` leaf contains an exact appended byte range. The
  logical series is the concatenation of those ranges in append order.

Logical leaves are nonempty: a table leaf contains at least one row and a file
leaf at least one byte. Empty writes create no leaf. Consequently a manifest's
logical count and leaf count are either both zero or both nonzero.

Each leaf preimage is unambiguously framed:

```text
logical_leaf = blake3(
  "watertown.series-leaf.v1\n" ||
  u8 payload_kind ||
  u32 schema_fingerprint_length || schema_fingerprint ||
  u64 logical_count ||
  u64 canonical_payload_length || canonical_payload ||
  u8 bounds_flags || [i64 min_event_time] || [i64 max_event_time] ||
  u32 logical_attributes_length || canonical_logical_attributes
)
```

All integers are little-endian. Every variable field is length-prefixed.
Absent and empty values are distinct. Logical attributes are canonical JSON:
object keys sorted by UTF-8 bytes, no insignificant whitespace, project-owned
string escaping, and numbers restricted to signed or unsigned 64-bit integers.
Floating-point JSON attributes and integers outside that range are rejected.

For table leaves, the schema fingerprint and canonical row payload are defined
by the canonical-row specification below. For file leaves the schema
fingerprint is empty, `logical_count` is the byte count, and the canonical
payload is the exact bytes.

Ordered leaf hashes form an RFC-6962-shaped BLAKE3 Merkle tree: leaf and
interior preimages have distinct domain tags, and an unpaired rightmost node
is promoted rather than duplicated. A `watertown.series.v1` root object records the
payload kind, schema fingerprint, logical row/byte count, leaf count, aggregate
event-time bounds and logical attributes, and the leaf Merkle root. The
BLAKE3 hash of that encoded root object is both the series identity and the
fetchable object address used by `ManifestEntry.child_hash`.

Physical parquet encoding, compression, row-group boundaries, pack filenames,
Bao outboards, and pack selection do not contribute.

#### Canonical table rows

The canonical row format is row-major and schema-driven. Column order, field
names, nullability, and logical Arrow types are part of the schema
fingerprint. Field and schema metadata maps are sorted by UTF-8 key bytes.
Dictionary arrays are expanded to their logical values before encoding.
Chunking and array buffer layout do not contribute.

Each scalar starts with a null marker. A present fixed-width scalar uses its
specified little-endian logical representation; variable-width values use a
`u64` byte length followed by bytes. Timestamps are signed counts in their
declared Arrow unit. Their timezone annotation remains part of the schema, so
changing it is a schema change rather than a physical rewrite. Floating-point
NaNs are normalized to `0x7fc00000` for `Float32` and
`0x7ff8000000000000` for `Float64`; positive and negative zero remain
distinct. UTF-8 is not Unicode-normalized.

The v1 canonical encoder accepts Boolean; signed and unsigned integers;
Float32 and Float64; Utf8 and LargeUtf8; Binary and LargeBinary; Date32;
Timestamp in every Arrow unit, with its optional timezone; and Decimal128.
Dictionary arrays normalize to their value type and values. Nested and all
other Arrow types are rejected before v2 writing rather than stringified or
silently omitted. A later encoding version must specify their recursive
framing before accepting them.

The permanent wire constants are:

- schema magic `watertown.series-schema.v1\n`;
- row magic `watertown.series-rows.v1\n`;
- leaf magic `watertown.series-leaf.v1\n`;
- payload kinds table `0`, file `1`;
- scalar markers absent `0`, present `1`;
- bounds bits minimum `0x01`, maximum `0x02`;
- type tags Boolean `0`, Int8 through Int64 `1..4`, UInt8 through UInt64
  `5..8`, Float32 `9`, Float64 `10`, Utf8 `11`, LargeUtf8 `12`, Binary `13`,
  LargeBinary `14`, Date32 `15`, Timestamp `16`, Decimal128 `17`.
- ordered-Merkle domain `watertown.series-merkle.v1\n`, with empty `0`, leaf `1`,
  and interior `2` tags;
- range-proof magic `watertown.series-range-proof.v1\n`;
- manifest magic `watertown.series.v1\n`;
- pack-index magic `watertown.series-pack.v1\n`.

Cross-process golden vectors freeze schema bytes, scalar bytes, leaf hashes,
Merkle roots, and `watertown.series.v1` object hashes. Arrow library upgrades must
pass those vectors.

### Physical pack index

A pack index maps a contiguous logical-leaf range to one or more content-
addressed parquet objects:

```text
pack = {
  series_hash,
  leaf_start,
  leaf_end,              // exclusive
  total_leaf_count,
  range_root,            // reconstructed whole-series root
  range_proof,
  physical_objects,
  logical_count,
  physical_byte_count,
  leaf_descriptors       // one per logical leaf in [leaf_start, leaf_end)
}
```

`leaf_descriptors` is the accepted per-leaf descriptor model: exactly one
descriptor for every logical leaf in `[leaf_start, leaf_end)`, in leaf order.
Each descriptor is

```text
leaf_descriptor = {
  logical_count,         // this leaf's row (table) or byte (file) count; > 0
  bounds_flags,
  [min_event_time],      // independently optional
  [max_event_time],      // independently optional
  [canonical_logical_attributes]  // optional canonical JSON object bytes
}
```

carrying exactly the same per-leaf metadata shape as a `logical_leaf`
preimage (row/byte count, independently optional min/max event-time bounds,
optional canonical logical attributes), minted fresh here rather than reused,
since a descriptor is derived storage metadata describing a leaf, not the
leaf's own hashed identity. Absent and empty-`Some` are distinct exactly as
elsewhere: `logical_count` must be strictly positive (an empty leaf is not a
supported model; a leaf with no logical content simply should not exist), and
`Some(&[])` canonical attributes is rejected in favor of `None`.

`physical_objects` is an ordered stream of content-addressed physical objects
with no relationship to leaf boundaries: a pack names however many physical
objects it needs, and `leaf_descriptors` names however many leaves it covers,
independently. The decoded stream represents exactly the declared leaf range,
with no unrepresented prefix or suffix. A reader recovers each leaf's content
by:

1. decoding every physical object in `physical_objects` order and
   concatenating their decoded logical content (rows for a table pack, bytes
   for a file pack) into one ordered logical stream;
2. walking `leaf_descriptors` in order and slicing that stream using each
   descriptor's `logical_count`, in order, to recover exactly that leaf's
   rows or bytes.

The same physical object hash may appear more than once: repeated
content-addressed bytes are still repeated logical stream segments. Storage
deduplicates the object itself, while readers preserve every occurrence in
the ordered index.

This is what permits a single leaf's content to cross a physical object
boundary (for example, a table leaf whose rows were split across two Parquet
files by an unrelated repack), and a single physical object to hold any
number of leaves, from zero (impossible only because a pack must still name
at least one object) up to the pack's entire leaf range. Decoding physical
objects and performing this partition into leaves is the dual reader's job
(delivery gate 4); this gate only defines, validates, and hashes the
descriptor data the reader will need -- it never inspects Parquet, Bao, or
physical bytes itself.

`leaf_descriptors` also carries per-leaf metadata queries (event-time bounds,
attributes) without requiring a reader to decode every physical object first:
a caller checking whether a pack might contain a given event-time range, for
example, can consult descriptor bounds directly. Nothing about
`verify_pack_against_manifest`'s membership proof changes: the range
proof/root check is still computed purely from leaf hashes and is completely
independent of `leaf_descriptors`' content, so a corrupt or mismatched
descriptor cannot forge series membership -- it can only make the pack's own
declared metadata internally inconsistent (rejected by the checks below), or
change the pack's own content hash (since descriptors are part of the hashed
pack bytes).

The manifest wire order is payload kind, length-prefixed schema fingerprint,
logical count, leaf count, bounds flags and present bounds, length-prefixed
canonical logical attributes, then the leaf Merkle root. The range proof is an
ordered list of `(u64 start, u64 count, 32-byte hash)` nodes. The pack wire
order is exactly the field order above, with the proof length-prefixed, the
physical objects preceded by a `u32` count, and `leaf_descriptors` preceded by
a `u32` count with each descriptor framed as `u64 logical_count`, `u8
bounds_flags`, the present bound(s), then length-prefixed canonical
attributes. All object decoders reject unknown tags, invalid ranges,
noncanonical proof shapes, truncation, and trailing bytes.

Pack indexes are derived storage metadata. They are excluded from the logical
content tree, like Delta file layout. A pack is accepted only when:

1. its physical object bytes pass their normal BLAKE3/Bao verification;
2. decoding and canonicalizing it reproduces the declared ordered leaf hashes
   and logical range root;
3. its range proof binds that root to the leaf Merkle root in `watertown.series.v1`;
4. counts, schema, bounds, and logical attributes agree with the decoded data;
5. `leaf_descriptors.len()` equals `leaf_end - leaf_start` exactly, and the
   descriptors' `logical_count`s sum (checked, rejecting overflow) to exactly
   the pack's own declared `logical_count`.

Multiple overlapping pack choices may coexist; readers may choose any
independently verified exact cover. A valid pack from another series cannot be
substituted merely because it is internally self-consistent.

Because packs are excluded from logical identity, they cannot be discovered
through the commit/tree object closure. Remotes therefore expose a separate
pack-advertisement namespace keyed by the `watertown.series.v1` object hash. Updating
that namespace does not create a logical commit. Advertisements are
append-only until referenced packs pass their retention window; a clean
replica lists candidates, chooses a verified exact cover, and fetches their
content-addressed objects. `file://`, object-store, and `pond://` sources must
implement the same discovery operation before v2 writing is enabled.

Collapse writes a replacement pack, verifies its logical range root, publishes
the new pack index atomically, and only then makes superseded packs reclaimable.
It never edits the logical manifest.

#### Pack discovery and advertisement (gate 3)

The pack-advertisement namespace is one object-store key per pack, sibling to
(not inside) a remote's Delta-managed `objects`/`refs` partitions and its
existing `_blobs/blob=<64-hex>` physical-blob namespace:

```text
_packs/series=<64-hex series_hash>/pack=<64-hex pack_hash>
```

`pack_hash` is the pack index's own content address (`blake3` of its encoded
bytes), so the key is self-verifying: a fetch recomputes it and rejects a
mismatch rather than trusting the object store's own naming. Scoping the key
by `series_hash` first means listing every candidate for one series, and
fetching one pack by its exact key, are both a single operation -- never an
ambiguous lookup across every series a remote holds. A fetched pack index is
additionally decoded and its own `series_hash` field is checked against the
directory it was found under, rejecting a pack advertised (by mistake or by
attack) under the wrong series' key.

**Publication order is physical objects first, pack index last.** Publishing
a pack uploads every physical object it names that is not already present
(discovered with one bulk listing of `_blobs/`, not a `HEAD` per object, the
same shape as the existing blob-presence check), then re-lists to confirm
every declared physical object is now present, and only then writes the pack
index to its content-addressed key. A pack index therefore never becomes
reachable while any object it names is missing: a publisher that fails or is
interrupted partway through leaves no advertisement at all rather than a
dangling one, and there is no separate two-phase commit or lock to make that
true -- it follows directly from checking presence immediately before the one
write that makes the index visible.

**Advertisements are append-only; v1 has no retention metadata.** Nothing in
this gate deletes or overwrites a pack index with different bytes (its key
*is* its content hash, so a same-key write is always byte-identical and
therefore idempotent); GC of superseded packs is future work. Two independent
publishers -- including two replicas choosing different valid layouts for the
same series -- may publish concurrently without coordination: each writes
only its own objects and its own pack-index key, so neither can corrupt or
race the other's advertisement. A consumer that lists mid-publish simply does
not yet see a still-incomplete pack (by the ordering above, that pack is not
listed until its index key exists at all); it never sees a corrupt one.

**Exact-cover selection is deterministic.** Given a series' total leaf count
and a set of candidate `(pack_hash, decoded PackIndex)` pairs already checked
against that series (right `series_hash`, right `total_leaf_count`), the
selector finds a subset of candidate ranges that exactly tiles
`[0, total_leaf_count)` with no gap and no overlap, using the fewest packs;
ties are broken by the lexicographically smaller pack hash. It rejects a
candidate for the wrong series or the wrong total outright, and reports an
error rather than a partial answer when no candidate covers leaf `0`, a gap
prevents any chain from reaching `total_leaf_count`, or the series is
otherwise uncoverable. The search is a dynamic program over the range
endpoints candidates actually name (coordinate-compressed), not over
`total_leaf_count` itself, so it stays linear in the number of packs offered
rather than exponential in the number of ways to combine them, and rejects an
empty candidate set for a nonempty series exactly as it rejects any other gap.

**Uniform read contract, still disabled for writing.** `ContentSource`
(the trait already used by the fetch/import path) gained the same two
methods -- list a series' candidate pack hashes, fetch one pack index by
series and hash -- for every backend: `ContentRemote` (the `file://`/cloud
object-store implementation, listing and fetching under `_packs/` exactly as
above) and `LocalPondSource` (a `pond://` producer clone, reading a persistent
`data/_packs/` directory beside `data/_large_files/`).
A v1 pond, or a v2 series with nothing published yet, returns an empty list
from either backend -- not an error -- since absence of advertisements is the
ordinary case until a writer exists. This gate did not itself wire pack
discovery into `watertown.series.v1` writers or collapse -- the selection algorithm
and the discovery contract existed and were tested in isolation, ready for
the reader gate (4) and the writer gate (7) to consume; gate 7 (this phase)
is what wires production push to publish an initial pack per nonempty
series so that gate 4's reader has a cover to discover.

#### Dual reader and pack verification (gate 4)

`steward`'s content-graph fetch (`fetch_object_graph`) now dispatches every
series object it encounters by magic header
(`sync_store::content::decode_fetched_series_object`): a `dp.series.1` object
follows the unchanged v1 path (`FetchedObject::Series`); a `watertown.series.v1`
object is fully discovered, fetched, and cryptographically verified into a
new `FetchedObject::SeriesV2(FetchedSeriesV2)` graph entry. Verification
checks, before that entry is ever inserted:

- the manifest's `payload_kind` agrees with the owning tree entry's declared
  type (`FilePhysicalSeries` vs `TablePhysicalSeries`);
- every advertised pack candidate decodes and is either accepted or the
  fetch fails outright -- a malformed or vanished candidate is never
  silently skipped;
- `select_exact_cover` chooses a deterministic exact cover of the whole
  series;
- every physical object the selected packs name is fetched through the same
  inline/external duality (`FetchedObject::Blob`/`FetchedObject::External`)
  the v1 path already uses, so a future materializer can reuse them exactly
  like ordinary version blobs;
- file packs are decoded and partitioned by streaming physical bytes through
  a leaf-by-leaf incremental hasher
  (`sync_store::content::IncrementalFileLeafHasher`), so a single logical
  leaf may cross a physical-object boundary without ever buffering an
  external object whole;
- table packs are decoded per physical object with
  `ParquetRecordBatchReaderBuilder` (off the async runtime, via
  `spawn_blocking`), checking each object's canonical schema fingerprint
  against `manifest.schema_fingerprint()`, and partitioned into leaves by
  row count (a leaf may span row-group batches or physical objects);
- `verify_pack_against_manifest` binds every recomputed leaf hash back to
  the manifest's own leaf Merkle root; the selected packs' logical counts
  and aggregate event-time bounds are cross-checked against the manifest's
  own aggregate fields.

#### Native v2 materialization (gate 4/7)

Native v2 materialization -- fetching and verifying a v2 series, then
applying it into a destination pond's tree via `rebuild_pond` /
`import_pond` -- **is implemented**. The planning stage now dispatches on
the fetched object kind (`content_pull.rs`'s series-planning match arm)
*before* ever consulting the v1-only `series_versions` helper: a
`FetchedObject::SeriesV2` is planned via `plan_series_v2_leaves` and
applied via `materialize_series_v2` and its per-payload-kind helpers
(`materialize_file_series_v2` / `materialize_table_series_v2`); only a
plain `FetchedObject::Series` ever reaches `series_versions`/
`plan_series_versions`, the unchanged v1 path (`series_versions` itself
still rejects a `SeriesV2` object, but only as an internal-dispatch-error
safety net -- reachable only if that upstream branch were ever bypassed,
which production planning never does). The materializer never trusts
unverified descriptor claims -- it only consumes the already-verified pack
payloads gate 4's fetch/verify step produced -- and reproduces, on the
destination pond, exactly the source series' logical leaf boundaries,
order, per-leaf logical hash/count, aggregate event-time bounds, canonical
attributes, table schema fingerprint, and series-level identity, such that
the destination's own fold recomputes the identical `SeriesManifest` and
root the source had:

- physical object boundaries are treated as independent of logical leaf
  boundaries in both directions: a file pack's bytes are split by
  descriptor `logical_count` (not by physical object boundary) while
  streaming through the source object sequence, and a table pack's decoded
  rows are split by descriptor `logical_count` across physical objects and
  Parquet row-group batches the same way gate 4's fetch already does;
- file leaves are re-materialized as an exact byte sub-stream per leaf;
  table leaves are re-encoded as a deterministic, self-contained Parquet
  append per leaf (pinned `WriterProperties`, one logical leaf per Oplog
  version) so row content, not physical Parquet framing, is what is
  preserved;
- every write is stamped through the same canonical hashing choke point
  (`tlogfs`'s `stamp_and_validate_series_entry`) real native appends use, and
  the materializer additionally re-derives each leaf's hash/count from what
  it is about to write and asserts it equals the verified descriptor's
  leaf hash/count *before* that Oplog append is committed -- a mismatch
  aborts before commit rather than silently diverging;
- external/large payloads are streamed through the existing bounded/
  external-blob write paths (no whole-payload buffering beyond what the
  existing writer already requires).

See `crates/steward/tests/content_pull_v2_test.rs` for the fixture-level
success tests (hand-encoded, dual-reader objects exercising the
materializer directly) covering file and table series, leaf/object boundary
crossings, external blobs, and multi-leaf/multi-object layouts; the old
"v2 materialization not implemented" assertions have been replaced with
rebuild/import success and destination-fold convergence assertions.
`crates/steward/tests/content_pull_test.rs` carries the equivalent
regression coverage against the real, native gate-7 writer end to end
(write via the native writer, push, then `rebuild_pond`/`import_pond`
against a fresh destination, asserting convergence) -- the tests that used
to assert an explicit v2-rejection error for these same collapse/repull
scenarios now assert successful round-trip convergence instead.

#### Zero-leaf series, exact logical attributes, and leafless-append rules

Three correctness gaps in the otherwise-complete gate-4/7 materialization
above were closed after the fact:

- **Zero-leaf series materialization.** A `watertown.series.v1` manifest with
  `leaf_count() == 0` -- a legitimately empty, never-appended-to series --
  has no packs to cover it (`select_exact_cover` special-cases this to an
  empty cover), so `materialize_file_series_v2` /
  `materialize_table_series_v2` previously never created a destination node
  for it at all: `create`/`adopt` silently did nothing. Both now dispatch a
  zero-leaf manifest to `materialize_empty_series`, which creates an empty
  node with the replicated mtime and nothing else -- exactly what a real
  writer produces for a zero-byte first version -- **for `FilePhysicalSeries`
  only**. A `TablePhysicalSeries` has no equivalent legitimate empty state:
  `SeriesManifest::new` unconditionally requires a schema fingerprint for
  `PayloadKind::Table` regardless of `leaf_count`, and the only way to ever
  obtain one is decoding real, nonempty Parquet bytes, so a materialized
  zero-content `TablePhysicalSeries` node could never be folded back into a
  valid manifest by this same destination's own next commit.
  `materialize_table_series_v2` therefore rejects a zero-leaf table
  manifest outright, with a clear `StewardError` naming the node, before any
  destination mutation -- never an opaque root-mismatch or precommit-fold
  failure. Symmetrically, `tlogfs`'s write choke point
  (`stamp_and_validate_series_entry`) rejects a genuinely empty
  (zero-byte) `TablePhysicalSeries` append at *any* version, including the
  first (`TLogFSError::SeriesTableRequiresSchemaBearingFirstVersion`), so
  no real writer can create this un-foldable state in the first place --
  matching what the destination now also refuses to materialize. A
  genuinely empty `FilePhysicalSeries` append remains legitimate only as a
  series' very first version; a later leafless append to an
  already-existing series (any payload kind) is rejected too
  (`TLogFSError::SeriesLeaflessAppendAfterFirstVersion`), since a trailing
  metadata-only version's attribute/mtime change is invisible to both
  `build_series_manifest`'s aggregation and the incremental v2 planner and
  so could never be reproduced by a destination fold. See
  `crates/steward/tests/content_pull_v2_test.rs`'s
  `rebuild_materializes_an_empty_v2_file_series` and
  `rebuild_rejects_an_empty_v2_table_series`, and
  `crates/tlogfs/src/persistence.rs`'s `stamping_choke_point_tests` module.

- **Exact logical attributes, not just `timestamp_column`.** V2
  materialization used to reconstruct only the well-known
  `watertown.timestamp_column` attribute (via
  `FileMetadataWriter::set_temporal_metadata`), silently dropping any other
  `ExtendedAttributes::set_raw` key the source series' canonical logical
  attributes carried. `FileMetadataWriter` gained an exact logical-attributes
  setter (`set_exact_logical_attributes`, a no-op default for backends that
  don't need it); `OpLogFileWriter` implements it by capturing the raw bytes
  and applying them, byte-for-byte, as the stamped entry's
  `extended_attributes` just before the write choke point re-stamps the
  logical leaf hash -- so the destination's re-derived hash is computed
  against the *exact* canonical attribute bytes the source leaf was hashed
  against, not a lossy reconstruction. `content_pull.rs`'s
  `apply_descriptor_exact_attributes` calls it for every v2 file/table leaf
  descriptor that carries logical attributes, validated for canonical-JSON
  well-formedness and non-ambiguous `timestamp_column` agreement before
  being applied. See
  `rebuild_round_trips_an_arbitrary_extra_logical_attribute_key` for a
  round-trip covering an extra, non-well-known attribute key.

- **`tlogfs::set_extended_attributes` stale-hash fix.** Mutating an
  already-stamped series row's `extended_attributes` directly (bypassing a
  fresh append) used to leave `logical_leaf_hash` stale -- the row's
  identity no longer matched its own attributes. `set_extended_attributes`
  now re-stamps (`stamp_logical_leaf`) immediately after mutating, so the
  invariant "a series row's logical leaf hash always matches its own
  content and attributes" holds even for direct attribute mutation.

- **No inventing bounds for a table descriptor with absent bounds.** A
  table leaf descriptor with neither `min_event_time` nor `max_event_time`
  set (as `build_table_pack`'s default fixture produces) used to make
  `feed_table_batch` call `infer_temporal_bounds()` on the already-decoded,
  already-verified leaf bytes -- inventing identity inputs the leaf hash was
  never actually computed against. It now rejects this case explicitly,
  before any destination write, with a clear diagnostic naming the missing
  bounds; current production table writers always require both bounds
  (`declared_temporal_file_series_without_bounds_is_rejected`-style
  enforcement already exists at the write choke point), so explicit
  rejection here costs nothing real writers rely on. See
  `rebuild_rejects_a_table_series_with_no_temporal_bounds`.

#### Pack builder/repacker prototype (gate 5)

`sync_store::content::series_pack_builder` (a new pure module, sibling to
`series_leaf`/`series_manifest`/`series_pack`) turns logical leaves already
in hand into physical objects plus a self-verified `PackIndex`, without
touching `pond maintain`, `Ship::collapse_versions`, tlogfs, or any real v2
pond write path. `FileLeafInput`/`TableLeafInput` recompute their own leaf
hash from real content at construction (never trusting a caller-supplied
hash or count); `build_file_pack`/`build_table_pack` require every supplied
leaf's recomputed hash to equal its entry in the caller-supplied whole-
series ordered leaf-hash list, require that list to fold to the supplied
`SeriesManifest`'s own `leaf_merkle_root`, generate the range proof against
that whole list, construct the `PackIndex`, and call
`verify_pack_against_manifest` on their own result before ever returning it
-- so a pack cannot be minted for the wrong manifest, range, schema, or
aggregate metadata. `FilePackLayout`/`TablePackLayout` cap physical objects
by logical size (max bytes, max rows) rather than leaf count or compressed-
byte guesswork; physical boundaries are independent of leaf boundaries (a
leaf may split across objects), no object is ever empty, and table physical
objects are self-contained Parquet files written with pinned, explicit
`WriterProperties` so repacking identical input under an identical layout
reproduces bit-identical bytes. `crates/sync-store/src/content/series_pack_builder.rs`'s
own tests cover both payload kinds across one-object, one-per-leaf, and
uneven/max-rows layouts, decode Parquet output to prove row order/content,
and check that every rejection path (wrong manifest/hash/root/schema/kind/
range/leaf-hash/aggregate-bounds/attrs/count, zero layout limits, empty
leaves) fails before any pack is returned.
`crates/steward/tests/gate5_pack_builder_test.rs` publishes two builder-
produced layouts of the same series to two independent clean remotes and
fetches both through the unmodified gate-4 dual reader, proving identical
ordered verified leaf hashes and an identical manifest while the fetched
physical object sets differ -- the prototype's success condition ("a repack
does not [change the logical root]") demonstrated end to end, without
claiming `FetchedSeriesV2` stores decoded rows (it stores hashes; the
sync-store unit tests are what actually decode content).

#### Pack-only local maintenance (implemented)

Gate 7 wired **initial** pack publication only: production push calls the
gate-5 pack builder once, per nonempty series, to publish a non-incremental
pack immediately after a commit's series are materialized
(`steward::content_tree::publish_initial_series_packs`), purely so gate 4's
reader has a cover to discover -- it is not a repacking or maintenance
path, and the builder still returns all completed physical-object bytes in
memory for that single publication call.

`pond maintain --collapse-versions N` and `Ship::collapse_versions` now run
real, production, pack-only local maintenance
(`steward::pack_maintenance::run_pack_maintenance`, see that module's own
docs), rather than refusing to touch a v2 series. On each run it: (1)
discovers native v2 series whose current physical fanout exceeds the
requested threshold and whose achievable bounded layout is actually
smaller (`discover_candidates`/`current_pack_fanout` -- a series already at
its bounded floor settles and is never re-flagged); (2) for each such
candidate, streams its live rows into one bounded, content-addressed set of
physical pack objects under the local pond's own `data/_packs` namespace
(`steward::pack_store`), recomputing and verifying every leaf hash against
the untouched, canonical manifest before trusting it, and self-verifying
the assembled `PackIndex` against that manifest before ever publishing it;
(3) sweeps any physical pack object no longer referenced by a live pack
index (`sweep_unreferenced_pack_objects`); and (4) unconditionally runs the
ordinary `reclaim` pass (superseded-row/blob cleanup), independent of
whether any series needed repacking. None of this ever rewrites or deletes
an Oplog append row, changes a `watertown.series.v1` manifest/tree/commit root,
Delta version, or txn sequence, or changes logical metadata -- a pack
advertisement is purely an additional, bounded way to *read* already-
committed content, so a crash at any point leaves only harmless orphaned
physical objects (an index is published strictly after every object it
names is durably written) and re-running is deterministic/idempotent.
`steward::content_source::LocalPondSource` (the `ContentSource` a `pond://`
reader/dual reader uses to pull from a local pond) now also consults this
same `_packs` namespace for object presence/listing/streaming, so a
maintained series' newly published pack objects -- not just the ordinary
external-blob objects captured at `open()` time -- are actually fetchable
by a remote or clean-reader reconstruction, closing the loop between
publishing a bounded pack index and a reader being able to use it. Packed
physical objects are also GC roots for the ordinary reclaim/large-file
sweep: `_packs/objects/` is a namespace distinct from `_large_files/`
specifically so the unrelated large-file GC pass can never treat a pack
object as orphaned; pack maintenance owns its own GC sweep scoped only to
its own namespace and its own pack-index advertisements.

`pond maintain`'s CLI surface (`crates/cmd/src/commands/maintain.rs`)
reports this work rather than a no-op: a dry run
(`Steward::survey_pack_maintenance`) lists each real v2 candidate and
whether it needs a repack or is already bounded, without publishing
anything, and a real run reports how many series were repacked plus the
usual `ReclaimStats` (superseded rows/blobs) from the reclaim pass that
always follows.

One concrete limitation remains, unchanged by this work: pack objects only
give a *remote/pond://* reader a bounded way to fetch a series' content.
They do not change how a local, in-process read against the pond's own
Oplog/Delta table works -- a local read still lists/scans every live
physical-version row for a series (`current_pack_fanout`'s own fanout
count reflects exactly this), so Oplog read fanout and local storage
overhead for a series with many live versions remain unbounded by this
maintenance; only the sidecar pack layout a remote reader selects is
bounded. Making local reads themselves pack-aware is out of scope for this
work and remains a real, open item.

#### Atomic pack publication and probe elimination (push path)

Two push-path correctness/performance fixes landed alongside gate 7's
initial-pack-publication wiring:

- **Publication ordering.** `steward::content_push::push_content_inner`
  previously wrote physical objects and advanced the remote ref in one
  atomic commit (`ContentRemote::push_commit`), then published pack
  advertisements afterward -- so a crash between those two steps left a
  new, already-visible v2 tip with no discoverable pack cover, making that
  tip unfetchable. Push now performs three explicit steps in order: (1)
  `ContentRemote::push_objects` durably writes every physical object with
  no ref change; (2) `publish_initial_series_packs` publishes the required
  pack indexes, now provably safe because every object they reference was
  just durably written in step 1; (3) only then does
  `ContentRemote::advance_ref` move the ref/tip. A crash before step 3
  leaves the old ref visible and durable, never a new tip a reader cannot
  fetch a pack cover for. `push_commit` itself is unchanged and remains in
  use by existing test fixtures that have no ordering hazard to avoid.
- **Bounded, non-probing pack publication.** `ContentRemote::publish_pack`
  previously re-verified every physical object hash a pack declares via
  `has_physical_object`, which falls back to a full history-scanning
  `Store::get` query (a DataFusion query over the whole registered Delta
  log, not a bounded exact-key lookup) whenever the object wasn't already
  in the in-memory blob cache -- so publishing a pack for a long-lived
  series cost work proportional to that series' entire commit history.
  Since the push path, by the time it publishes packs, already knows
  exactly which object hashes it just durably wrote (plus which external
  blobs it already confirmed present while streaming them), publication
  now takes a `known_present: &HashSet<ObjectHash>` proof set
  (`ContentRemote::publish_pack_with_known_present`) and skips the
  recheck for any hash the caller already proved durable, while still
  performing the exact check for any hash not covered by that proof.
  `publish_pack` itself is unchanged (an empty `known_present`, i.e. full
  exact validation) and remains the entry point for any caller without
  such proof.

See `crates/sync-store/tests/content_remote.rs`'s
`push_objects_writes_durably_without_advancing_any_ref` /
`advance_ref_moves_the_ref_after_objects_are_already_durable` for the
ordering/crash-simulation coverage, and
`crates/sync-store/tests/pack_advertisement.rs`'s
`known_present_hash_skips_the_exact_presence_recheck` /
`known_present_does_not_cover_hashes_it_does_not_name` for proof that the
probe is actually skipped for named hashes and still exact for unnamed
ones.

### Logical metadata

The current tree entry carries one `VersionMeta` per live physical version.
That is also physical identity: collapse changes the version list even if
`child_hash` becomes logical. A v2 series tree entry instead carries one
series-level logical metadata record derived from its manifest (logical mtime,
aggregate event-time bounds, and logical attributes). Pack timestamps and
per-pack bounds remain outside the tree. Appending logical content updates this
metadata; repacking does not.

## Wire compatibility

`watertown.series.v1` is the logical manifest object; `watertown.series-pack.v1` is a
separately tagged pack-index object. A tree entry remains structurally
unchanged: its `child_hash` names a series object, detected by object magic
after fetch. Native ponds no longer ever produce `dp.series.1`; the dual
reader may still decode a `dp.series.1` object it encounters (see "Dual
reader and pack verification (gate 4)" above), because a remote can still
hold pre-reset history from a pond that has not yet been reset, but no
production writer emits it.

Commit encoding has advanced from `dp.commit.3` to `watertown.commit.v1` and carries
an explicit content-model version (`ContentModelVersion`, a typed enum, not a
bare integer -- see `sync-store/src/content/commit.rs`). Per the reset
decision below, this is a hard cutover, not a dual-format bridge: production
encoding only ever writes `watertown.commit.v1` with the logical-series-v2 model, and
decoding only accepts `watertown.commit.v1`. There is no reader support for
`dp.commit.3` and none is planned -- a pond that still has `dp.commit.3` tips
must go through the destructive reset described below before it can be read
by v2-era code.

## Reset instead of migration

**Decision (superseding the migration plan originally sketched for this
design): there is no migration path, no rollback window, and no mixed v1/v2
writer support.** Adopting logical-series-v2 is a **destructive pond reset**:
a pond upgrades by being recreated from scratch as v2-only. Pre-reset
history is not translated forward and cannot be opened by v2-era code -- it
is out of scope by design, not an oversight. `watertown.commit.v1` is published as
the compatibility fence: any binary that only understands `dp.commit.3` is
structurally unable to read a post-reset pond's tip, and any post-reset pond
structurally cannot have a `dp.commit.3` tip, so the two populations never
need to interoperate.

This replaces the seven-step online migration procedure (freeze collapse,
translate v1 rows to v2 leaves in place, verify against a second scan,
commit one migration transaction, keep the old tip pinned through a
rollback window, then re-enable collapse) that this document described in
earlier drafts. That procedure remains a reasonable design for a future
project that needs a live, in-place, no-downtime upgrade of an existing
pond's history; it is not being built now because the destructive reset is
sufficient for current needs and is far simpler to implement and verify.
Anyone reviving it should note the original rationale still holds: v1 series
commit to physical parquet bytes and v2 commits to canonical logical rows,
so any translation, live or reset-based, necessarily produces one
intentional root change that all later physical repacks must preserve.

## Delivery gates

1. Freeze canonical row encoding with cross-process golden vectors. **Done.**
2. Implement the ordered BLAKE3 Merkle, v2 logical-manifest and pack codecs,
   range proofs, and hostile-input tests without changing writers. **Done**
   (`sync-store/src/content/{series_leaf,series_manifest,series_merkle}.rs`).
3. Implement the non-logical pack-discovery protocol for every remote kind and
   prove a clean replica can reconstruct from more than one valid pack layout.
   **Done** (`pack_keys.rs`, `content_remote.rs`'s `publish_pack` /
   `list_pack_hashes` / `get_pack_index_bytes`).
4. Add a dual reader and mixed v1/v2 fetch tests. **Done**
   (`steward/src/content_pull.rs`'s `fetch_series_v2`,
   `content_pull_v2_test.rs`).
5. Prototype pack verification and prove repeated collapse preserves the v2
   series root and logical metadata while changing physical blob layout.
   **Done** as a pure pack builder/repacker library
   (`series_pack_builder.rs`, `series_dispatch.rs`; see "Pack
   builder/repacker prototype (gate 5)" above).
6. ~~Add interrupted-migration, rollback, remote refetch, out-of-order pack,
   and corrupt/substituted-pack tests.~~ Superseded by the reset decision
   above: there is no migration or rollback to test. Remote-refetch,
   out-of-order-pack, and corrupt/substituted-pack coverage remains and lives
   in `content_pull_v2_test.rs` / `gate5_pack_builder_test.rs`.
7. **Native v2 writer and persistence -- done.** Every nonempty
   `FilePhysicalSeries` / `TablePhysicalSeries` append stamps its immutable
   canonical logical leaf hash, logical count, and (for tables) a schema
   fingerprint directly on the Oplog row at write time
   (`tlogfs/src/series_identity.rs`); the steward fold
   (`steward/src/content_tree.rs`) derives the ordered `watertown.series.v1`
   manifest, RFC-6962 Merkle root, and one series-level logical-metadata
   record (latest logical append mtime/attributes, aggregate
   min/max event-time bounds, checked-arithmetic logical count) from those
   persisted rows during both the full and incremental fold, never from
   `row_blob_hash`. Production commits are `watertown.commit.v1` only. The write
   choke point (`tlogfs`'s `stamp_and_validate_series_entry`, reached by
   every public append path including `State::add_oplog_entry`, with no
   bypass) additionally rejects a nonempty-Parquet-but-zero-row
   `TablePhysicalSeries` append outright rather than allowing it to commit
   and fail later at fold time, and external `FilePhysicalSeries` content is
   hashed via `IncrementalFileLeafHasher` over a streaming reader rather
   than buffered whole into memory. `pond maintain --collapse-versions`
   now performs real pack-only local maintenance
   (`Ship::collapse_versions` delegates to
   `steward::pack_maintenance::run_pack_maintenance`) instead of a gated
   no-op -- see "Pack-only local maintenance (implemented)" above for the
   full design and its one remaining open item (local Oplog read fanout is
   not itself bounded by this, only the pack layout a remote reader
   selects). Push additionally
   performs **initial pack publication**: after materializing a commit,
   every nonempty captured series' physical leaves are packed (one full,
   non-incremental pack per series -- see `series_pack_builder.rs`'s module
   doc for why this is explicitly the "initial", not the streaming,
   publication strategy) and published so that gate 4's dual-reader
   fetch/verify has a pack cover to find; without this a v2-native pond
   could never be pulled at all. Publication is atomically ordered behind
   ref advancement (objects durable, then packs published, then the ref
   moves -- see "Atomic pack publication and probe elimination" above) so a
   crash never exposes an unfetchable tip, and pack publication reuses the
   push path's own proof of already-durable objects instead of re-probing
   the remote (`ContentRemote::publish_pack_with_known_present`), so
   publishing a pack no longer costs work proportional to a series' whole
   commit history.

   **Materializing a *fetched and verified* v2 series into a destination
   pond (`rebuild_pond` / `import_pond`) is also done, this phase** (see
   "Native v2 materialization (gate 4/7)" above) -- both entry points now
   apply a verified v2 series to the destination tree instead of rejecting
   it, covered end to end by `content_pull_v2_test.rs`'s materialization
   success tests and `content_pull_test.rs`'s native-writer round-trip
   convergence tests. **Pack-only local maintenance (repack of
   over-threshold native v2 series into bounded, content-addressed
   packs, plus GC of unreferenced pack objects) is also done, this
   phase** -- see "Pack-only local maintenance (implemented)" above. Its
   one remaining, intentionally open limitation: this bounds only the pack
   layout a remote/`pond://` reader selects, not a local, in-process read
   against the pond's own Oplog/Delta table, which still scans every live
   physical-version row for a series.

The prototype succeeds only if an append changes the logical root, a repack
does not, and a clean replica reconstructs identical rows using a different
verified pack selection. All three properties are exercised by
`content_tree.rs`'s
`native_fold_emits_decodable_series_manifest_and_append_changes_root`
and by the gate 4/5/6 test suites.
