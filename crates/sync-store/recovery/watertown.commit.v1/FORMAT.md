# Native backup format: `watertown.commit.v1` (logical-series v2)

The downloaded source is a Delta table with columns:

```text
pond_id UTF-8
partition_key UTF-8
item_key UTF-8
txn_seq i64
deleted bool
value binary
value_blake3 binary[32]
ts_micros i64
```

For each `(pond_id, partition_key, item_key)`, the live row is the row with the
greatest `txn_seq`; a live row whose `deleted` value is true is absent.

The source pond UUID is the live row at:

```text
pond_id       = 00000000-0000-0000-0000-000000000000
partition_key = meta
item_key      = pond_id
```

For the source pond UUID:

- `partition_key=refs`, `item_key=<name>` stores a raw 32-byte commit hash.
- `partition_key=objects`, `item_key=<lowercase hash hex>` stores exact native
  object bytes.
- `_blobs/blob=<lowercase hash hex>` stores exact external payload bytes.

Every value must match `value_blake3`; every object or blob must match the hash
in its key.

Native object magic and framing:

```text
watertown.commit.v1\n   content model byte, commit and provenance
watertown.tree.v1\n     canonical directory entries
watertown.manifest.v1\n source node identities and metadata
dp.series.1\n              legacy ordered physical-hash list
watertown.series.v1\n   homogeneous logical-series manifest and Merkle root
watertown.series.v2\n   current per-leaf-schema logical-series manifest and Merkle root
watertown.series-pack.v1\n legacy pack index and range proof
watertown.series-pack.v2\n current pack index with per-leaf schema fingerprints
watertown.recipe.v1\n   dynamic factory type and raw configuration
dp.recipe.1\n          legacy dynamic factory type and raw configuration
```

Integers are little-endian. Strings and variable byte fields use an unsigned
32-bit little-endian length followed by exact bytes. Hashes are 32 raw BLAKE3
bytes. `native-fixtures.json` contains commit, tree, manifest, series, and recipe
objects whose shared framing bytes are checked by the independent decoder and
the Rust source codecs.

For a nonempty `watertown.series.v1` or `.v2` node, the extractor requires
the sibling pack advertisements at
`_packs/series=<series-hash>/pack=<pack-hash>`. Every advertised filename,
content hash, index framing, descriptor, schema fingerprint, physical object,
exact cover, and range proof is checked. Each selected pack's physical stream
is independently partitioned by its own descriptor range, with no bytes or
rows permitted to cross from another pack; its leaf hashes and range proof
are validated before verified ranges are concatenated in leaf order. Table
objects are standard Parquet. A `.v2` table's descriptor supplies the schema
fingerprint for each leaf, so schema evolution is preserved.

`dp.series.1`, `dp.manifest.2`, `dp.tree.2`, and `dp.recipe.1` remain
supported with their original ordered physical-object semantics. Pack-aware
series require their advertisements to be present locally; this offline kit
does not fetch missing advertisements or objects from another remote.
