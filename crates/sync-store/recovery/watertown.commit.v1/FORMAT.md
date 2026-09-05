# Native backup format: `watertown.commit.v1`

This recovery kit accepts only the current Watertown native content model.
Every structured object is strict: the exact magic below is required,
truncation and trailing bytes are errors, and no historical magic is
recognized.

## Content graph

The Delta table contains content-addressed rows and refs:

- `partition_key=objects`, `item_key=<blake3 hex>` stores inline object bytes.
- `partition_key=refs`, `item_key=<name>` stores a 32-byte commit hash.
- large physical payloads are stored beneath `_blobs/blob=<blake3 hex>`.
- pack indexes are advertised beneath `_packs/series=<series hash>/pack=<pack hash>`.

The table must use the current `pond_id, partition_key` partition layout.

## Retained native object magics

```text
watertown.commit.v1\n
watertown.tree.v1\n
watertown.manifest.v1\n
watertown.series.v2\n
watertown.series-pack.v2\n
watertown.series-range-proof.v1\n
watertown.recipe.v1\n
```

Logical table identity continues to use these stable hashing domains:

```text
watertown.series-schema.v1\n
watertown.series-rows.v1\n
watertown.series-leaf.v1\n
watertown.series-merkle.v1\n
```

A series manifest commits to payload kind, aggregate logical count, leaf
count, event-time bounds, canonical logical attributes, and the ordered leaf
Merkle root. Table schema identity is per leaf and is carried by each
`watertown.series-pack.v2` descriptor.

The extractor verifies object hashes, manifest/tree topology, exact pack
coverage, per-pack range proofs, physical payload hashes and sizes, canonical
logical rows or bytes, per-leaf schema fingerprints, aggregate bounds, and
the commit's node-manifest root before producing a capsule.

The only emitted portable format is `pondcapsule.4`.
