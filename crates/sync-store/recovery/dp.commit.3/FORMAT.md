# Native backup format: `dp.commit.3`

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
dp.commit.3\n   commit and provenance
dp.tree.2\n     canonical directory entries
dp.manifest.2\n source node identities and metadata
dp.series.1\n   ordered physical version hashes
dp.recipe.1\n   dynamic factory type and raw configuration
```

Integers are little-endian. Strings and variable byte fields use an unsigned
32-bit little-endian length followed by exact bytes. Hashes are 32 raw BLAKE3
bytes. `native-fixtures.json` contains commit, tree, manifest, series, and recipe
objects whose exact bytes are checked by both the independent decoder and the
Rust source codecs.
