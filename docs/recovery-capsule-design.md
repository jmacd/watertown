# Recovery Capsules and Format Upgrades

> **Status:** `pondcapsule.1` remains the frozen migration input format.
> `pondcapsule.2` is implemented as the new explicit output format for
> `watertown.*.v1` ponds; native v2 pack-aware extraction is still in progress.
> Phase one is implemented for `dp.commit.3`; production
> publication and recovery exercises remain.
>
> This document is the authoritative design for the current static-recipe
> mechanism. Superseded per-commit capsule publication is retained only in Git
> history, not described here.
>
> For the executable watershop and Azure migration procedure, including writer
> quiescence, exact-tip selection, staged import, cutover, and rollback, see
> [Capsule Recovery and Storage-Format Migration Runbook](capsule-recovery-runbook.md).

## Purpose

Watertown's native remote backup is an exact-replica protocol. Its Delta table,
commit objects, and content graph are efficient for incremental replication but
require a reader compatible with that native format.

A recovery capsule is the format-upgrade boundary. It lets an operator recover
the complete logical pond after the source binary and native format are
obsolete, using:

- ordinary Azure Blob or S3-compatible copy tools;
- standard Delta Lake and Parquet libraries;
- a reviewed recovery recipe for the source native format; and
- capsule-local, Pond-free verification and inert materialization tooling.

A current `pond` binary is optional compatibility coverage and is needed only
for a later native import workflow, not to prove or inspect recovered content.

## Terminology

`dp` is the legacy native wire-format namespace for **data pond** objects. The
portable capsule uses its own explicit `pondcapsule` namespace:

- `dp.commit.3` is native commit format 3;
- `watertown.tree.v1`, `watertown.manifest.v1`, `dp.series.1`, and `watertown.recipe.v1` are native
  object formats; and
- `pondcapsule.1` is the frozen portable recovery-capsule format 1.
- `pondcapsule.2` is portable recovery-capsule format 2 for newly-created
  Watertown ponds. It has a distinct manifest format identifier and root hash
  domain, while retaining the same verified logical payload model during the
  migration transition.

The mechanism has two distinct artifacts:

1. A **recovery recipe** is small, reviewed code published once per native
   format.
2. A **portable capsule** is a selected logical pond snapshot materialized on
   demand by that recipe.

The native backup remains the only routine cloud copy of pond data.

## Architecture

For `dp.commit.3`, ContentRemote stores:

- current and historical key/value rows in its Delta table;
- small content objects as `value` bytes in the `objects` partition;
- refs as raw commit hashes in the `refs` partition; and
- large content objects as raw bytes at `_blobs/blob=<blake3>`.

Standard Delta and Parquet readers can access all of this without running a
source-format `pond`.

The static recipe:

1. downloads the complete native backup;
2. resolves live rows by greatest transaction sequence;
3. selects a native ref or exact commit hash;
4. verifies the commit, node-Merkle root, tree/manifest agreement, and every
   content address;
5. decodes the selected native graph;
6. writes a `pondcapsule.2` manifest and plain payload objects; and
7. verifies the portable capsule.

Capsule bytes are created only during recovery. Ordinary native pushes perform
no capsule generation; before writing backup data, they idempotently ensure
that the small static recovery recipe is present and byte-identical.

There are no per-commit capsule manifests, duplicated cloud payloads, capsule
generation refs, publication leases, retention windows, or capsule-generation
garbage collection.

## Static recovery recipe

### Layout and identity

Recipes use immutable, versioned paths:

```text
recovery/
  README.sh
  recipes/
    dp.commit.3/
      <recipe-hash>/
        README.sh
```

`<recipe-hash>` is domain-separated BLAKE3 over the exact `README.sh` bytes.
The versioned object is created before the discoverable top-level object.
Retries accept only byte-identical existing objects; differing content is never
overwritten.

Every native backup push idempotently installs the current hash-addressed
recipe and verifies the discoverable recipe against its own immutable copy
before writing backup data or advancing a ref. An existing discoverable recipe
is never replaced, so an older binary cannot downgrade it and a newer binary
cannot strand an existing backup. Old versioned recipes remain immutable. The
explicit publish command remains a strict, idempotent provisioning, repair, and
inspection aid for the current kit.

### Bootstrap safety

`README.sh` is a deterministic POSIX self-extracting bootstrap. It:

- uses `set -eu` and a restrictive umask;
- accepts only a new, safe destination;
- extracts a fixed list of reviewed files;
- emits SHA-256 checksums; and
- prints the next review and execution steps.

It contains no credentials, performs no network access, executes no extracted
file, modifies no pond or backup, and deletes nothing.

The extracted `dp.commit.3` kit contains:

- safety and operator instructions;
- an independent native-format specification;
- pinned Python 3.13 dependencies;
- the independent Delta/Parquet extractor;
- the standalone capsule verifier/materializer and its capsule-local README;
- a portable-capsule format reference and one-command recovery wrapper;
- a separate exact direct-dependency file for the capsule tool;
- cross-language native wire fixtures;
- a native-backup-to-capsule integration test; and
- credential-free AzCopy and MinIO Client download helpers.

Cloud authentication happens separately through managed identity or an
already-authenticated client.

### Current commands

```text
pond capsule recipe publish <remote>
pond capsule recipe inspect <remote>
pond capsule inspect <capsule-directory>
pond capsule verify <capsule-directory>
```

Recipe commands use a named remote attachment and its storage profile.
Downloaded-capsule inspection and verification do not require a local pond.
Every produced capsule also carries these operator commands at its root:

```text
python capsule.py verify CAPSULE
python capsule.py materialize CAPSULE NEW_DESTINATION
```

The native recipe kit and produced capsule are deliberately distinct. The kit
contains `extract.py`, Delta dependencies, native fixtures, and download
helpers. The capsule contains only its payload and manifest/ref plus the exact root
files `CAPSULE-README.md`, `CAPSULE-FORMAT.md`, `capsule.py`,
`capsule-requirements.lock`, and `recover.sh`.

On-demand extraction currently runs from the reviewed kit:

```text
python extract.py BACKUP CAPSULE --ref REF --birthplace LABEL
python extract.py BACKUP CAPSULE --commit HASH --birthplace LABEL
```

The source backup is read-only and the capsule destination must not exist.

## Portable capsule v1 contract

The format identifier is:

```text
pondcapsule.1
```

The canonical manifest records:

- source pond identity, birthplace, selected native tip, and export time;
- the producer version;
- every live absolute path in canonical UTF-8 byte order;
- entry type and source node identity;
- directory topology;
- symlink target objects;
- dynamic factory recipe objects;
- ordered physical file and table objects;
- ordered logical series leaves;
- logical schema fingerprints;
- logical byte or row counts;
- independently optional event-time bounds;
- canonical logical attributes;
- logical leaf hashes; and
- logical series roots.

The capsule excludes:

- native Delta logs and physical Delta layout;
- native commits, trees, manifests, and refs;
- control-table transaction state;
- replication watermarks and import journals;
- caches; and
- superseded source transactions.

The complete live namespace is otherwise preserved, including `/sys` entries
and remote definitions.

### Payload representation

Payloads are plain immutable files named:

```text
recovery/objects/blake3=<payload-hash>
```

File payloads are exact byte streams. Table payloads are standard Parquet.
Every payload object records its exact BLAKE3 hash and size.

Logical identity is independent of physical Parquet encoding and object
boundaries. Verification decodes table rows, canonicalizes supported Arrow
schemas and scalar values, recomputes every logical leaf hash, and recomputes
each ordered series root.

### Downloaded capsule layout

```text
CAPSULE-README.md
CAPSULE-FORMAT.md
capsule.py
capsule-requirements.lock
recover.sh
recovery/
  refs/latest
  manifests/<capsule-root>.json
  objects/blake3=<payload-hash>
```

`refs/latest` names the canonical manifest root. The manifest declares the
complete required payload closure. The capsule-local files explain, verify,
and safely materialize that closure without Pond. Executable helper files are
not covered by the capsule root; an operator must compare all recovery aids
with copies from the independently hash-authenticated recovery kit before
execution. The trusted kit's `recover.sh` automates dependency setup,
verification, and inert materialization without Pond.

### Canonical encoding

- Paths are absolute, normalized, and free of traversal components.
- Entries are uniquely sorted by UTF-8 path bytes.
- Parent topology must be complete.
- Unknown fields and unsupported format versions fail.
- Hashes use lowercase hexadecimal in text.
- Integers have fixed widths and meanings.
- Absent and empty values remain distinct.
- Logical attributes use recursively key-sorted canonical JSON and only the
  supported integer number model.
- Golden vectors freeze native decoders and portable hash preimages.

## Recovery workflow

The following is the artifact-level workflow. The operator gates and commands
for an authoritative migration are defined in
[capsule-recovery-runbook.md](capsule-recovery-runbook.md).

1. Obtain `recovery/README.sh` and its hash through independent channels.
2. Review the bootstrap.
3. Extract the kit into a new directory.
4. Verify `SHA256SUMS` and review every extracted file.
5. Authenticate AzCopy or MinIO Client separately.
6. Download the complete native Delta backup, including `_delta_log/` and
   `_blobs/`.
7. Run the dependency-free native fixture verification.
8. Select a native ref or commit and run the extractor.
9. Run `python CAPSULE/capsule.py verify CAPSULE`.
10. Run `python CAPSULE/capsule.py materialize CAPSULE NEW_DESTINATION` to
    inspect exact file bytes, Parquet rows, symlink targets, and recipes as
    inert data.
11. Optionally run a current `pond capsule verify CAPSULE` as additional
    compatibility coverage.
12. Import only into a new staged pond if an import-capable binary is available.

The capsule root should be recorded outside the source storage. Content hashes
detect corruption; an out-of-band root also detects replacement of both data
and in-storage checksums.

A selected native commit is immutable and internally consistent, but it may
not be the final state of a live writer. For an authoritative format
migration, operators must stop every source writer, run `pond freeze enable`,
record the protected exact tip, perform and verify a final push of that tip,
and extract with `--commit HASH`. The durable freeze is enforced after the
cross-process write lock is acquired by ordinary writes, replay,
pull/import, compaction, reclamation, and control-history pruning. Forced
control rebuild refuses to remove an active marker. Locking or withholding a
remote ref is not a substitute for freezing the writer.

## Extraction invariants

The `dp.commit.3` extractor:

- resolves one live row per `(pond_id, partition_key, item_key)` by greatest
  `txn_seq`;
- rejects duplicate winners;
- treats a winning tombstone as absence;
- verifies each row's `value_blake3`;
- verifies object and blob bytes against their address;
- verifies commit provenance belongs to the selected source pond;
- recomputes the node-keyed manifest Merkle root;
- verifies the manifest root against the commit;
- verifies every physical directory tree against the manifest;
- rejects disconnected, cyclic, duplicate, or unsafe namespace paths;
- validates series/version metadata cardinality;
- validates dynamic recipes;
- validates Parquet schemas and logical hashes; and
- writes through a private sibling staging directory before atomic promotion.

Any mismatch is fatal. The extractor never repairs or guesses source state.

## Inspection and verification

The capsule-local `capsule.py verify` command and Pond's optional
`pond capsule verify` compatibility command:

- decode and validate the canonical manifest;
- verify the latest manifest root;
- verify topology and entry/content compatibility;
- verify the complete declared payload closure;
- check every physical hash and size;
- decode every Parquet schema;
- recompute logical leaf hashes and series roots; and
- report entry, object, byte, and logical counts.

Extra undeclared files do not satisfy a missing declared object.

`capsule.py materialize` first performs the same deep verification and refuses
an existing destination. It maps each full logical path to one bounded ASCII
directory name containing a basename hint and SHA-256 of the exact UTF-8 path,
avoiding collisions and excessive path growth across common filesystems. Separate
roots hold directories, files, tables, symlink targets, and dynamic recipes.
File and table leaves are exported into numbered versions. Symlink targets and
native dynamic recipes remain inert; recipes are additionally decoded into a
JSON factory name and exact configuration bytes for inspection. The tool never
creates an active symlink, imports a pond, or executes a recipe. A text README
and JSON inventory preserve mappings and logical metadata.

## Generic staged import

The initial importer implements staged reconstruction, exact logical
verification, and atomic promotion. Bounded transactions, resumable
checkpoints, and active-remote preflight remain before it is production-ready.

`pond capsule import` requires an explicit `--experimental` acknowledgement
until the remaining production-readiness work is complete, and accepts only a
nonexistent target:

1. Create a private sibling staging directory.
2. Initialize a fresh pond with a new pond identity.
3. Persist immutable provenance naming the source pond, source tip, capsule
   root, and importer version.
4. Persistently suppress post-commit factories and automatic pushes before
   restoring namespace content.
5. Recreate parents before children in canonical manifest order.
6. Stream files and decoded table rows in bounded transactions.
7. Recreate series leaves in original order with their logical metadata.
8. Record content-addressed resumable journal checkpoints.
9. Re-read the staged pond and recompute the complete capsule inventory.
10. Resolve and preflight every restored active remote.
11. Seal only on an exact logical match.
12. Sync the staged tree, atomically rename staging to the requested target,
    and sync the parent directory so successful promotion is crash-durable.

Node IDs, pond identity, transaction sequence, Parquet encoding, object
boundaries, and pack layout may change. Paths, entry types, exact file bytes,
decoded table rows, schema identity, logical attributes, leaf order, event
bounds, leaf hashes, and series roots may not.

A failed transaction commits no partial batch. A retry must validate every
journal checkpoint rather than trusting staged state.

`pondcapsule.1` cannot encode a zero-length logical leaf. A single empty
physical node remains representable only when its version metadata is empty.
Extraction fails closed for a metadata-bearing empty singleton or any empty
member of a multi-version series; silently dropping either would lose metadata
or change series order.

### Active remote safety

Remote definitions are namespace content and are restored. Automatic
post-commit dispatch remains disabled after promotion until the operator
explicitly enables it. Before sealing, the completed importer will refuse:

- a destination equal to the source backup namespace;
- an unresolved destination;
- a destination with an incompatible pond identity; or
- conflicting restored attachments.

The operator must redirect environment-based URLs to fresh namespaces before
unsealing.

## Destructive cleanup

Extraction, import, cutover, and verification never delete source data.

Source cleanup is a separate exact-target plan/verify/apply workflow. Planning
occurs while the source attachment exists. Before verify or apply, the operator
detaches it. Later phases:

- accept the exact planned URL and an explicitly named storage profile;
- reject any live attachment resolving to the target;
- revalidate replacement completeness and plan identity;
- reject roots, wildcards, unresolved variables, and inferred parent paths;
- use raw object-store access so retries remain possible after Delta metadata
  deletion; and
- require the reviewed plan hash and typed target confirmation.

## Delivery sequence

### Phase one: `dp.commit.3`

1. Freeze the portable capsule v1 contract.
2. Ship and test the independent static recipe.
3. Publish and inspect the recipe at the existing Azure backup.
4. Complete the generic staged importer and exact-target cleanup workflow.
5. Prove native backup to capsule to fresh pond end to end.
6. Merge and build while `dp.commit.3` remains readable.
7. Independently extract and preserve the production capsule root.

### Phase two: logical series v2

1. Return to `jmacd/incremental1`.
2. Merge updated `origin/main` without rebasing.
3. Preserve `pondcapsule.1` unchanged.
4. Implement the v2 import sink.
5. Import the preserved capsule into a fresh v2 pond.
6. Verify logical identity and publish to a new Azure namespace.
7. Publish and test the v2 native-format recipe before enabling v2 backups.
8. Retain the old namespace until separately reviewed cleanup is safe.

## Current implementation status

Completed:

- canonical capsule manifest, logical hashing, and offline verifier;
- deterministic `dp.commit.3` bootstrap and recipe identity;
- independent Delta/Parquet extractor;
- capsule-local standalone verifier, safe materializer, README, and dependency
  file requiring no Pond code;
- commit, node-Merkle, tree, manifest, series, and recipe verification;
- native wire fixtures checked by Python and Rust;
- credential-free AzCopy and MinIO download helpers;
- integration coverage for live-row history, tombstones, external blobs,
  Pond-free verification/materialization, exact files and Parquet rows, inert
  symlink targets and dynamic recipes, series, and destination refusal;
- immutable recipe publish and inspect commands;
- staged import with a fresh identity, immutable provenance, persistent
  post-commit suppression, exact logical verification, and atomic target
  promotion; and
- removal of the superseded per-generation operator commands.

Remaining:

- bounded import transactions and content-addressed resumable journaling;
- active-remote preflight;
- detached exact-target erase plan/verify/apply;
- failure-injection and interrupted-resume coverage; and
- production Azure publication and independent recovery exercise.

## Acceptance criteria

- Ordinary native pushes generate no capsules; they ensure the static recovery
  recipe is present and internally consistent.
- Each recipe version is immutable and published at most once per backup.
- Recovery requires no source-format `pond` binary.
- Every native and portable content address is verified before trust.
- A capsule preserves complete live logical namespace and series identity.
- Import writes only to a new staged target and remains inert until sealed.
- No recovery or cutover operation deletes source data.
- Cleanup requires a separate exact reviewed plan.
- The old native format may be retired only after a production recovery
  exercise succeeds.
