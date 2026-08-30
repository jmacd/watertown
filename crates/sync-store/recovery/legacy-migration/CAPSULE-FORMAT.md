# Opaque migration capsule: `pondcapsule.legacy.1`

The layout is:

```text
recovery/refs/latest
recovery/manifests/<root>.json
recovery/objects/blake3=<hash>
```

The root is BLAKE3 over `pondcapsule.legacy.root.1\n` followed by the exact
canonical JSON manifest bytes. This domain is intentionally distinct from
`pondcapsule.1` and `pondcapsule.2`.

Each physical entry records its original entry type, native child hash,
optional raw `dp.series.1` object, and ordered versions. Every version records
its zero-based source position, exact raw object hash/size, source timestamp,
event-time bounds, and extended attributes. For `dp.commit.3`, one raw object
maps to each version. Table objects are opaque Parquet bytes: neither the
extractor nor the capsule verifier computes schemas, rows, canonical table
hashes, or a series-global schema.

Verification requires canonical sorted paths, safe topology, strict fields and
types, the exact object closure, raw object hashes/sizes, and exact agreement
between every series version mapping and its authenticated `dp.series.1`
object.
