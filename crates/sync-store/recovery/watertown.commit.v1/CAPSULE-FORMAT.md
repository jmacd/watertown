# Portable recovery capsule: `pondcapsule.4`

`pondcapsule.4` is the only supported portable recovery format.

## Layout

```text
recovery/
  refs/latest
  manifests/<capsule-root>.json
  objects/blake3=<payload-hash>
CAPSULE-README.md
CAPSULE-FORMAT.md
capsule.py
parquet_schema.py
capsule-requirements.lock
recover.sh
```

`refs/latest` is exactly one lowercase 64-character BLAKE3 hash plus a
newline. The named manifest is canonical JSON. Its authenticated root is:

```text
BLAKE3("pondcapsule.root.4\n" || canonical_manifest_json)
```

Physical nodes contain an ordered object stream and ordered logical leaves.
Their logical root uses the retained current capsule series domain:

```text
BLAKE3("pondcapsule.series.3\n" || framed node and leaf descriptors)
```

Every table leaf carries its own `schema_fingerprint`. A homogeneous table
may also carry the same fingerprint at the node level; a schema-evolved table
omits the node-level field. An empty table must retain a node-level schema
fingerprint and a Parquet schema carrier.

Logical leaf hashes use the native stable domains
`watertown.series-schema.v1`, `watertown.series-rows.v1`, and
`watertown.series-leaf.v1`. Dynamic recipe payloads must use
`watertown.recipe.v1`; the verifier never executes them.

The verifier rejects unknown fields, noncanonical paths or JSON, duplicate
entries, inconsistent object sizes, undeclared objects, invalid schemas,
logical-root mismatches, payload hash mismatches, and every obsolete capsule
or native child encoding.
