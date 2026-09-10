# Recovery Capsule Design

Watertown recovery has one native format and one portable format:

- native content graph: `watertown.commit.v2`
- portable capsule: `pondcapsule.4`

Obsolete native, series, pack, recipe, and capsule encodings are not
recognized. Recovery fails closed instead of guessing, translating, or
falling back.

## Native-v2 capsule publication

`pond capsule publish <backup>` traverses the current persistent manifest map,
builds and verifies `pondcapsule.4`, then publishes immutable payloads and the
capsule manifest before advancing `recovery/refs/latest`. It is explicit and
separate from ordinary native backup publication.

## Legacy static recovery recipe

`pond capsule recipe publish <backup>` installs the reviewed bootstrap at:

```text
recovery/recipes/watertown.commit.v1/<recipe-hash>/README.sh
recovery/README.sh
```

This recipe extracts an explicitly retained `watertown.commit.v1` rollback
remote. It is not installed by native-v2 push and is not a native-v2 reader.
The recipe hash is domain-separated with `pondcapsule.recipe.1`. Both writes
are create-only. The current hash-addressed recipe is always installed.
An existing discoverable bootstrap remains unchanged when its exact bytes
match the immutable object named by their own recipe hash. A missing or
mismatched immutable copy is rejected; no compiled historical recipe list or
backfill path is used.

The legacy extracted kit contains the native-v1 extractor, standalone capsule verifier,
safe materializer, dependency locks, reviewed object-store download helpers,
and format documentation. It contains no legacy readers or fixture-derived
translation logic.

## Legacy native extraction

The extractor reads a retained legacy content-addressed remote:

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
3. rejects non-UTF-8 targets, then creates an exclusive regular ownership-intent
   file in the target's parent before creating a dedicated initialization
   container and its `pond/` child;
4. persistently suppresses post-commit dispatch and automatic pushes;
5. creates a capsule-root-addressed journal beside `data/` and `control/`;
6. syncs the initialized pond and first journal, then atomically publishes it
   under the deterministic resumable staging name with no replacement;
7. recreates entries in canonical parent-before-child order using bounded,
   deterministic transactions (at most 64 entry/leaf units and 64 MiB of
   logical bytes/rows by default, except one indivisible oversized leaf);
8. checkpoints the exact entry/leaf cursor only after each transaction
   commits, with an in-flight transaction sequence that resolves the
   commit-before-checkpoint crash window without duplicate series leaves;
9. locates and resumes the matching staging directory on retry, while a
   conflicting capsule, birthplace, target, or batch policy fails loudly;
10. preserves leaf order, timestamps, bounds, attributes, and schemas;
11. rebuilds a fresh `pondcapsule.4` from the staging pond;
12. compares its logical projection with the source capsule;
13. syncs and atomically renames the staging pond onto the target.

The journal and provenance are outside the tinyfs namespace. Journal progress
is encoded completely into a non-journal staging file in the same directory,
the file is synced, and a no-replace atomic rename publishes the monotonically
numbered checkpoint before the directory is synced. Resume ignores and removes
unpublished staging files but never skips a corrupt final checkpoint.

Initialization intent names include the exact UTF-8 target, capsule root, and a
random token; their regular-file contents bind those values and the birthplace.
The matching `.work` container is never adopted without that intent, and
symlinks or non-directories are rejected before opening. A retry may delete and
recreate only the `pond/` child of an authenticated importer-owned container
when pond creation stopped before an openable checkpoint. Once provenance and
the first journal are durable, the child is atomically renamed to the
deterministic staging name and the intent/container are removed. A matching
staging directory without a durable journal is foreign and is never adopted or
deleted.

File and table import each build one validated physical-object offset index per
invocation. Each logical leaf binary-searches its first overlapping object and
iterates only overlaps, so work is `O(objects + leaves + overlaps)` across
bounded batches. Table planning additionally opens each object once for row
count and schema metadata. Resume may rebuild either linear index once per
invocation but never rescans every preceding leaf or object for every leaf.
Failed published staging directories remain for inspection and are never
recursively deleted. Before staging publication, only a malformed/incomplete
`pond/` child covered by the exact importer ownership intent may be removed and
recreated. The downloaded capsule is opened read-only and is never modified.

The promoted pond remains persistently inert. `pond capsule activate` is the
only supported activation path: it reads `/sys/remotes/*` attachments and
`/system/run/*` dynamic configs without instantiating factories, validates
YAML, URLs, secret references, limiter/storage bindings, remote modes and
mount semantics, registered factory names, expanded typed factory configs,
and remote pond identities. It sets `post_commit_dispatch=enabled` only after
every check succeeds. Any invalid or unsafe entry leaves dispatch suppressed
and reports the path and repair action.

Capsule import is a format-independent reset. It preserves current logical
live content and ordered series leaves, but deliberately mints a fresh pond
identity and does not preserve the source pond's native commit history.

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
