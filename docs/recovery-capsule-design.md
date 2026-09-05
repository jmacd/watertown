# Recovery Capsule Design

Watertown recovery has one native format and one portable format:

- native content graph: `watertown.commit.v1`
- portable capsule: `pondcapsule.4`

Obsolete native, series, pack, recipe, and capsule encodings are not
recognized. Recovery fails closed instead of guessing, translating, or
falling back.

## Static recovery recipe

`pond capsule recipe publish <backup>` installs the reviewed bootstrap at:

```text
recovery/recipes/watertown.commit.v1/<recipe-hash>/README.sh
recovery/README.sh
```

The recipe hash is domain-separated with `pondcapsule.recipe.1`. Both writes
are create-only. The current hash-addressed recipe is always installed.
An existing discoverable bootstrap remains unchanged when its exact bytes
match the immutable object named by their own recipe hash. A missing or
mismatched immutable copy is rejected; no compiled historical recipe list or
backfill path is used.

The extracted kit contains the native extractor, standalone capsule verifier,
safe materializer, dependency locks, reviewed object-store download helpers,
and format documentation. It contains no legacy readers or fixture-derived
translation logic.

## Native extraction

The extractor reads a current content-addressed remote:

```text
watertown.commit.v1
  -> watertown.manifest.v1
  -> watertown.tree.v1
  -> watertown.series.v2
  -> watertown.series-pack.v2
  -> physical payload objects
```

Dynamic nodes reference `watertown.recipe.v1`.

Extraction verifies each object's BLAKE3 address, commit content-model byte,
manifest and tree topology, series aggregate identity, deterministic exact
pack cover, range proofs, physical object hashes and sizes, logical leaf
hashes, per-leaf table schema fingerprints, aggregate bounds, and canonical
logical attributes.

## `pondcapsule.4`

A capsule directory contains:

```text
recovery/refs/latest
recovery/manifests/<root>.json
recovery/objects/blake3=<hash>
CAPSULE-README.md
CAPSULE-FORMAT.md
capsule.py
parquet_schema.py
capsule-requirements.lock
recover.sh
```

The root is:

```text
BLAKE3("pondcapsule.root.4\n" || canonical_manifest_json)
```

Each live path records its entry type, source node identity, and one of:
directory, symlink target object, dynamic recipe object plus optional
timestamp metadata, or physical file/table content.

Physical content separates logical identity from physical packing:

- `objects` is the ordered physical byte stream.
- `leaves` is the ordered logical append history.
- file leaves carry byte counts and logical metadata.
- table leaves additionally carry their own schema fingerprint.
- `logical_root` authenticates the ordered leaf descriptors under the current
  `pondcapsule.series.3` domain.

The capsule logical leaf hashes use the same stable native domains as the
series model. Repacking or changing Parquet encoding does not change them.

## Verification

Both Rust and standalone Python verifiers require exactly
`pondcapsule.4`. They reject unknown fields, duplicate or noncanonical paths,
noncanonical JSON, inconsistent object descriptors, missing or extra payloads,
hash or size mismatches, invalid Parquet schemas, schema transitions inside a
physical object, leaf mismatches, logical-root mismatches, and non-current
dynamic recipe framing.

## Safe materialization

Standalone materialization writes to a nonexistent destination with
no-replace promotion. It produces ordinary files, Parquet versions, symlink
target descriptions, and inert recipe/config files. It never executes a
recipe, creates a live symlink, contacts a remote, or modifies the capsule.

## Staged pond import

`pond capsule import`:

1. requires a nonexistent target;
2. verifies the complete capsule before writing;
3. creates a private sibling staging pond with a fresh identity;
4. persistently suppresses post-commit dispatch and automatic pushes;
5. recreates entries in canonical parent-before-child order;
6. preserves leaf order, timestamps, bounds, attributes, and schemas;
7. rebuilds a fresh `pondcapsule.4` from the staging pond;
8. compares its logical projection with the source capsule;
9. syncs and atomically renames the staging pond onto the target.

Failed staging directories remain for inspection.

## Compatibility boundary

The only retained versioned identities are:

```text
watertown.commit.v1
watertown.tree.v1
watertown.manifest.v1
watertown.series.v2
watertown.series-pack.v2
watertown.recipe.v1
pondcapsule.4
```

The logical hashing domains ending in `.v1` remain intentionally stable; they
identify the canonical schema, row, leaf, and Merkle algorithms rather than
obsolete storage formats.
