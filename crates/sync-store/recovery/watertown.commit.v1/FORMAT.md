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
watertown.series.v1\n   logical-series summary and Merkle root
watertown.recipe.v1\n   dynamic factory type and raw configuration
```

Integers are little-endian. Strings and variable byte fields use an unsigned
32-bit little-endian length followed by exact bytes. Hashes are 32 raw BLAKE3
bytes. `native-fixtures.json` contains commit, tree, manifest, series, and recipe
objects whose shared framing bytes are checked by the independent decoder and
the Rust source codecs. The v2 physical-row and pack mapping remains
fail-closed in this first standalone kit; a `watertown.series.v1` object is diagnosed
as unsupported rather than misread as a v1 physical hash list.
