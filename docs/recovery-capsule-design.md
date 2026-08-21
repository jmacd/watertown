# Recovery Capsules and Format Upgrades

> **Status:** Phase one implementation in progress.
>
> **Originating work:** `jmacd/incremental1` at
> `58fd3cf39a6dc5032cd58dc064c51940f5faddf3`.
>
> This document was carried uncommitted from the logical series v2 branch to
> the old-format-compatible phase-one branch `jmacd/capsule1`. That branch
> implements and merges capsule publication while `dp.commit.3` is still
> readable. The human will then return to `jmacd/incremental1`, merge the
> capsule work from `origin/main` without rebasing, and add the v2 import sink.
> Record the phase-one commit and PR here before merging it.

## Architecture correction: 2026-08-20

The recovery artifact is split into two distinct things:

- a **recovery recipe**, published once per native storage format; and
- a **portable capsule**, materialized on demand from a selected native tip.

For `dp.commit.3`, the native ContentRemote backup remains the only cloud copy
of payload bytes. Small content objects are ordinary `value` bytes in the
Delta table's `objects` partition. Large objects are raw bytes at
`_blobs/blob=<blake3>`. Delta and Parquet readers can retrieve both without a
source-format `pond` binary.

The static recipe contains a reviewed `README.sh` bootstrap, pinned standard
tool requirements, format documentation, extraction code, Azure CLI and MinIO
download helpers, golden vectors, and verification code. `README.sh` only
extracts named files into a new directory and prints instructions. It never
uses credentials, performs network access, executes extracted code, modifies a
pond, or deletes storage.

An operator or agent:

1. downloads `recovery/README.sh` and verifies its out-of-band hash;
2. reviews and runs it to extract the recovery kit;
3. reviews the extracted files and authenticates ordinary cloud tools;
4. downloads the native Delta backup and `_blobs/`;
5. selects a native ref/tip;
6. decodes `dp.commit.3`, `dp.tree.2`, `dp.manifest.2`, `dp.series.1`, and
   `dp.recipe.1`;
7. materializes the portable manifest and `recovery/objects/blake3=<hash>`
   layout locally;
8. verifies every source object, logical leaf, series root, and capsule root;
   and
9. imports the verified capsule into a fresh current-format pond.

Versioned recipes live under
`recovery/recipes/dp.commit.3/<recipe-hash>/README.sh`. A discoverable
top-level `recovery/README.sh` changes only when the native format changes.
The recipe is published and round-trip tested before the first write in a new
native format.

This removes all capsule I/O from ordinary native pushes. There are no
per-commit capsule manifests, duplicated cloud payloads, mutable capsule refs,
generation history, publication leases, or capsule-generation GC. The native
backup ref remains the snapshot selector. Recipe integrity is checked
periodically and during recovery, not by rereading cloud objects after every
commit.

The implementation described in the following dated section predates this
correction. Its canonical portable manifest, logical hashing, offline verifier,
and planned importer remain useful. Its automatic per-commit publisher,
generation artifacts, retention GC, and payload replication are prototypes to
remove or repurpose.

## Static recipe implementation update: 2026-08-21

The `dp.commit.3` recovery kit now has a deterministic POSIX `README.sh`
bootstrap, safety and format documentation, pinned Python 3.13 dependencies,
an independent Delta/Parquet extractor, and cross-language native wire
fixtures. The bootstrap only creates a new kit directory, writes its reviewed
files, and prints instructions; it performs no network access and executes no
extracted code.

The extractor resolves live Delta rows by greatest transaction sequence,
rejects ambiguous winners and unsafe paths, verifies native values and content
addresses, decodes commit/manifest/series/recipe objects, and writes a portable
capsule through a private sibling staging directory. A local integration check
has generated a representative native Delta backup, extracted files, a table,
a series, a symlink, and a dynamic recipe without invoking `pond`, then passed
the resulting capsule through the Rust verifier.

The checked-in integration harness covers source tree/manifest agreement,
external `_blobs`, tombstones, historical rows, all physical payload kinds, a
symlink, and a dynamic recipe, then invokes the Rust capsule verifier only
after independent extraction. Credential-free Azure AzCopy and MinIO Client
helpers download into new directories and reject results without `_delta_log`.
`pond capsule recipe publish` creates the hash-addressed recipe first and the
discoverable `recovery/README.sh` second; retries accept only byte-identical
objects and never overwrite a difference. `pond capsule recipe inspect`
verifies both copies against the reviewed build. Actual production publication
remains blocked until this branch is reviewed and merged.

## Implementation update: 2026-08-19

Phase-one work on `jmacd/capsule1` currently spans `e8ad3617` through
`3673d356`. No phase-one PR has been opened yet.

Completed:

- frozen `dp.recovery-capsule.1` manifest, logical leaf, series-root, and
  inventory-root contracts with golden vectors;
- old-format live-namespace traversal and deterministic capsule construction;
- disk-backed bounded staging, streaming external files and uploads, and
  batch-bounded two-pass table hashing and verification;
- changed-tip reuse of unchanged prior leaves and payloads without a second
  native materialization pass;
- payload-first publication, deep staged-change verification, reference-last
  finalization, a portable atomic publication lease, and safe stale-lock
  refusal;
- explicit full repair of untrusted or corrupt same-name payload objects;
- generated credential-free Azure CLI and MinIO Client download scripts and
  runbook;
- `pond capsule publish`, `list`, `inspect`, and `verify`;
- three-generation retention; and
- canonical, reviewed `pond capsule gc plan|verify|apply` with retention
  snapshots, reader grace, exact-key deletion, drift detection, and idempotent
  retry.

In progress:

- exact-target erase planning after source attachment removal. Detached
  verify/apply will reopen the exact planned URL through an explicit storage
  profile, reject any live attachment resolving there, and use raw object-store
  access so partial deletion remains resumable.

Still required before the phase-one PR:

- complete exact-target erase plan/verify/apply;
- implement the generic staged old-format importer with hook suppression,
  resumable bounded transactions, provenance, remote preflight, exact
  post-write verification, and atomic promotion;
- add failure-injection, interrupted-retry, malformed-input, cleanup-race, and
  full export/download/import round-trip coverage; and
- update this section with the final phase-one commit and PR.

After phase one merges and builds, publish the current Azure capsule, preserve
its root out of band, download it independently, verify it offline, and perform
a throwaway recovery before returning to `jmacd/incremental1`.

## Problem

Watertown's native remote backup is an exact-replica protocol. Its Delta
tables, commit objects, and content graph require a compatible `pond` reader.
That is the right representation for efficient incremental replication, but it
is not a durable format-upgrade boundary.

An operator should be able to recover logical pond contents after the source
format and binary are obsolete. Recovery should require only:

- ordinary cloud copy tools such as Azure CLI or MinIO Client;
- a downloaded, self-describing artifact;
- a current `pond` binary; and
- credentials supplied by managed identity or the environment.

Cloud copy tools can retrieve native backup bytes, but cannot infer pond paths,
entry types, series boundaries, schemas, or metadata from those bytes. That
semantic description must be published while the source format is readable.

## Decision

Publish a versioned **recovery recipe** alongside each native backup format.
Use it to materialize a portable recovery capsule on demand.

A capsule is a portable snapshot of the complete live namespace plus ordered
logical series leaf history. It is not:

- a copy of the source Delta table;
- a replay of every source transaction;
- a source-format commit graph;
- a source of embedded credentials; or
- an executable script fetched and run without review.

The extracted capsule contains a stable manifest and plain content-addressed
payload objects. Files remain exact bytes. Tables use standard Parquet. The
manifest preserves enough logical information to reconstruct every series leaf
and verify its identity independently of physical Parquet, pack, or object
boundaries. Those payloads are extracted from the native backup rather than
duplicated in cloud storage during each push.

Import creates a new pond identity and transaction lineage. Immutable import
provenance links the new pond to the source pond, source tip, and capsule root.

## Two-branch delivery sequence

The independent recovery recipe must exist before the old reader is retired.

### Phase one: old-format-compatible branch

Starting from the old-format-compatible `main`:

1. Freeze the capsule v1 wire contract in shared, format-independent code.
2. Implement and freeze an independent `dp.commit.3` extractor using standard
   Delta/Parquet libraries and documented native object decoders.
3. Publish the static self-extracting recipe once.
4. Add capsule inspection, verification, cloud-tool instructions, safe cleanup
   planning, and a generic staged importer.
5. Prove a complete old-format export/download/import round trip.
6. Merge and build an image.
7. Install the recipe in the current Azure backup, independently materialize a
   capsule, and preserve its root out of band.

No legacy decoder is added to logical series v2.

### Phase two: logical series v2 branch

After phase one merges:

1. Return to `jmacd/incremental1`.
2. Merge updated `origin/main`; do not rebase.
3. Preserve the capsule wire contract unchanged.
4. Implement a v2 import sink using native logical leaf stamping.
5. Import the preserved capsule into a fresh v2 pond.
6. Verify logical contents and publish to a new Azure namespace.
7. Publish and test the v2 native-format recipe before enabling v2 backups.
8. Retain the old namespace until a separately reviewed deletion plan is safe
   to apply.

The agent does not create, switch, rename, or delete either branch.

## Capsule v1 contract

The initial format identifier is:

```text
dp.recovery-capsule.1
```

The canonical manifest commits to:

- capsule format and creation tool versions;
- source pond identity and birthplace;
- source content tip and export time;
- one canonical record for every live path and entry type;
- directory topology;
- symlink targets;
- dynamic recipe and configuration bytes;
- physical file and table descriptors;
- ordered file-series and table-series leaf descriptors;
- payload object hashes and sizes;
- logical schema fingerprints;
- logical row or byte counts;
- independently optional event-time bounds;
- canonical logical attributes;
- each logical leaf hash;
- each logical series root; and
- a capsule-wide inventory root over paths, types, metadata, and content.

The capsule excludes native implementation state that is not a live namespace
node:

- Delta transaction logs and Parquet layout;
- control-table transaction state;
- replication watermarks and local import journals;
- caches; and
- source transaction history.

The complete live namespace is otherwise preserved, including `/sys` nodes and
remote definitions.

### Payload representation

File payloads are exact byte streams. Table payloads are standard Parquet whose
decoded Arrow rows must reproduce the manifest's logical schema and hashes.

Physical object boundaries are independent of logical leaf boundaries. The
manifest therefore records ordered payload object references and ordered leaf
descriptors. An importer concatenates decoded logical content and partitions it
using leaf counts, as the logical series pack model does.

This permits:

- one logical leaf to span several payload objects;
- one payload object to contain several logical leaves;
- physical repacking without changing a capsule's logical inventory; and
- content-addressed reuse between capsule generations.

Every payload object is independently BLAKE3-addressed. The importer also
recomputes the project-defined logical leaf hash after decoding; verifying only
physical object bytes is insufficient.

### Object-store layout

A capsule uses ordinary independently downloadable objects:

```text
recovery/
  refs/latest
  manifests/<capsule-root>.json
  generations/<capsule-root>/objects.list
  generations/<capsule-root>/checksums
  generations/<capsule-root>/RUNBOOK.txt
  generations/<capsule-root>/download-az.sh
  generations/<capsule-root>/download-mc.sh
  objects/blake3=<payload-hash>
```

Native small content objects are stored as Delta rows and cannot be extracted
with `az` or `mc` alone. Capsule publication must materialize its own plain
payload namespace. A provider may use server-side copies for existing raw blobs,
but capsule correctness cannot depend on understanding native storage layout.

### Canonical encoding

The manifest encoding must be deterministic and validated strictly:

- UTF-8 path bytes use one canonical absolute representation;
- entries are sorted by path bytes;
- duplicate, parentless, relative, or traversal paths are rejected;
- integers have fixed ranges and meanings;
- arbitrary metadata uses the project's canonical JSON rules;
- absent and empty values remain distinct where logically distinct;
- unknown required fields and unsupported format versions fail loudly; and
- golden vectors freeze manifest bytes, leaf records, and inventory roots.

Future readers may add support for a new capsule version, but must never silently
reinterpret v1.

## Recipe publication and extraction

Recipe publication is a one-time prerequisite for writing a native format:

1. Build and test the self-extracting recovery kit.
2. Hash the exact `README.sh` bytes.
3. Create
   `recovery/recipes/<native-format>/<recipe-hash>/README.sh` immutably.
4. Install the discoverable top-level `recovery/README.sh`.
5. Record the recipe hash in release documentation and outside the backup.
6. Prove extraction from a representative native backup using no `pond`
   binary.

Ordinary native pushes do not read, rewrite, probe, or otherwise maintain the
recipe. A native-format upgrade publishes and verifies its new recipe before
the first backup using that format. Old recipes remain immutable.

Recovery selects an existing native ref or content tip. The extractor reads the
latest Delta snapshot that contains the requested content-addressed objects; it
does not require historical Delta files to survive indefinitely. It writes the
portable capsule to local or separately chosen destination storage, never into
the source backup namespace.

## Cost and retention

The recipe adds zero requests, bytes, or CPU to an ordinary native push. Its
one-time publication cost is the small recovery-kit payload plus its versioned
and discoverable object writes.

The native backup already retains the required data. Recovery extraction pays
the read and local-materialization cost only when an operator actually requests
a capsule. Recipe versions are tiny and retained indefinitely; there are no
capsule generations or capsule payload objects to garbage-collect.

## Operator command surface

Use a top-level `pond capsule` command family. The existing `pond recover`
command remains the local crash-transaction repair operation.

```text
pond capsule recipe publish <remote>
pond capsule recipe inspect <source>
pond capsule extract <source> --tip <ref-or-hash> --target <empty-path>
pond capsule inspect <source>
pond capsule verify <source>
pond capsule scripts <source> --provider az|mc
pond capsule import <source> --target <empty-path> --birthplace <label>
pond capsule erase plan|verify|apply ...
```

Commands that operate only on a remote or downloaded capsule must not require a
local pond.

### Generated recovery artifacts

Each generation includes:

- a human-readable runbook;
- a generated Azure CLI download script;
- a generated MinIO Client download script;
- pinned minimum tool requirements;
- the capsule root and complete object list; and
- checksum commands that verify the download before `pond` is invoked.

Scripts contain no access keys, tokens, connection strings, or resolved secret
values. They use managed identity, an already authenticated client, or
environment variables. They are downloaded, reviewed, and explicitly executed;
cloud-resident text is never automatically evaluated.

The capsule root should also be recorded outside the source storage, such as an
operator log or deployment record. Content hashes detect corruption; an
out-of-band root protects against replacement of both content and checksums by
an attacker controlling the storage account.

## Inspection and import

### Read-only preflight

`pond capsule inspect` writes no pond data. It:

- validates the capsule version and canonical manifest;
- verifies path safety and complete parent topology;
- verifies the complete declared payload closure;
- checks object BLAKE3 hashes and sizes;
- decodes every Parquet schema;
- rejects unsupported logical types;
- recomputes every leaf, series, node, and capsule inventory root;
- reports source identity and provenance;
- reports path, object, leaf, row, and byte counts;
- estimates staging disk requirements; and
- lists active remote definitions and their resolved destinations when
  credentials/configuration permit.

Extra undeclared objects do not affect verification, but missing or modified
declared objects are fatal.

### Staged rewrite

`pond capsule import` accepts only an empty target path:

1. Create a private sibling staging directory.
2. Initialize a fresh pond with a new `pond_id`.
3. Persist immutable provenance naming the source pond, source tip, capsule
   root, and import tool version.
4. Suppress all post-commit factories, automatic push, and remote pull while
   staging.
5. Recreate parents before children in deterministic manifest order.
6. Stream physical files and decoded table rows into bounded transactions.
7. Recreate every series leaf in original append order with its original
   logical metadata.
8. Record completed batches in a content-addressed import journal.
9. Re-read staged data and recompute the complete capsule inventory.
10. Preflight every restored active remote.
11. Seal the pond only on an exact match.
12. Atomically rename staging to the requested target.

A failed batch commits no rows. A retry validates completed journal entries and
resumes; it never trusts an unverified staging checkpoint. A mismatch leaves the
staging pond inspectable but unsealed and the requested target absent.

Logical content is preserved, but physical Parquet encoding, object boundaries,
pack layout, node IDs, pond identity, and transaction sequence are permitted to
change.

### Active remote safety

The live namespace, including remote definitions, is restored exactly. That does
not permit accidental writes to the source backup.

During staging, remote dispatch is disabled regardless of restored configuration.
Before sealing, the importer resolves every active attachment using the target
environment and refuses to proceed when:

- a destination equals the capsule's recorded source namespace;
- a destination is unresolved;
- a destination contains a store with an incompatible pond identity; or
- two restored attachments create an unsafe destination conflict.

The operator must point environment-referenced URLs at fresh namespaces before
unsealing. No source credentials or resolved secrets are stored in the capsule.

## Destructive cleanup

Export, import, cutover, and verification never delete native backup data.

Cleanup is a separate plan/verify/apply workflow. A deletion plan records:

- the exact provider, account, container, and normalized prefix;
- every object or provider-native prefix to remove;
- expected object count and bytes;
- the source capsule/native store identity;
- the verified replacement capsule and native backup roots;
- a retention-not-before condition; and
- the plan's own content hash.

`apply` requires:

- the exact reviewed plan hash;
- a typed confirmation token naming the target namespace;
- current proof that the replacement remains complete;
- current proof that no live attachment resolves to the target; and
- revalidation that the target still matches the plan.

Erase planning resolves the source through its named attachment. Before
`verify` or `apply`, the operator removes that attachment. Those later phases
take the exact planned URL plus an explicitly named storage profile, scan all
current attachments to prove none resolves to the target URL, and open the raw
object store rather than its Delta table. Raw access keeps a partially completed
erase resumable after native Delta metadata has already been removed. Inline
credentials are not copied into plans and are not accepted for detached erase.

Deletion is narrowly scoped to a fully resolved container/prefix. Unresolved
variables, account roots, broad wildcards, and inferred parent paths are rejected.

## Azure format-upgrade runbook

For the current deployment:

1. Merge and deploy phase-one capsule publication while the source pond remains
   old-format-compatible.
2. Publish a capsule to the existing Azure backup without modifying its native
   ref or deleting data.
3. Record the capsule root outside Azure.
4. Download the declared capsule closure using its reviewed `az` or `mc` script.
5. Run offline inspect and verify.
6. Perform a throwaway old-format import and compare inventory, bytes, rows,
   schemas, logical attributes, leaf hashes, and series roots.
7. Preserve that downloaded capsule independently.
8. Merge capsule support into logical series v2.
9. Import the preserved capsule into a fresh staged v2 pond, preferably on a
   temporary Azure VM to reduce egress and latency.
10. Point restored environment-based remotes at a fresh Azure container/prefix.
11. Seal, explicitly push, and verify the new native backup.
12. Publish, independently download, and verify a new v2 capsule.
13. Restore normal automation.
14. Retain the old namespace through the chosen retention period.
15. Generate and review a deletion plan; apply it only after no live attachment
    references the old namespace.

## Tests

### Protocol

- canonical manifest and inventory-root golden vectors;
- physical repacking produces the same logical inventory;
- malformed, duplicate, relative, traversal, and parentless paths fail;
- malformed hashes, missing objects, wrong sizes, schema mismatches, and unknown
  required fields fail;
- unsupported capsule versions fail explicitly.

### Publication

- interruption before payload, manifest, artifacts, verification, and latest-ref
  publication;
- retry after each interruption;
- unchanged publication writes only bounded generation metadata;
- incremental append reuses unchanged prefix payload objects;
- retained-generation GC preserves every reachable object;
- concurrent reader grace prevents deletion races;
- generated scripts contain no credentials and fetch exactly the declared
  closure.

### Import

- empty and large files;
- files and leaves spanning physical object boundaries;
- table versions and file/table series;
- dictionary and plain Arrow schemas;
- out-of-order and duplicate timestamps;
- independently optional event bounds;
- logical attributes;
- symlinks and dynamic recipes;
- complete `/sys` namespace and active remotes;
- failure before, during, and after bounded transactions;
- validated resume and rejection of modified staging state;
- hook suppression through final seal;
- source remote and incompatible target rejection;
- exact bytes, rows, schemas, leaf hashes, series roots, and inventory root;
- new pond identity and immutable source provenance.

### End to end

- old native remote to capsule to offline `az`/`mc`-shaped download to
  old-format import;
- preserved old-format capsule to v2 pond to new native remote;
- new v2 native remote publishes a capsule recoverable without source-format
  state;
- cleanup planning cannot target a live source or run without verified
  replacement evidence.

## Acceptance criteria

- A future machine with only `az` or `mc`, a trusted capsule root, and a current
  `pond` can recover the logical pond without the source-format binary.
- Failed publication never replaces the latest verified capsule.
- Failed import never exposes a partial target and resumes cleanly.
- Every logical series leaf retains its content, order, metadata, and hash across
  the rewrite.
- Physical maintenance and format rewrite may repack data without altering
  logical identity.
- Source storage is read-only throughout extraction and import.
- No destructive command can run without an exact reviewed plan and a verified
  replacement.
- Incremental publication cost is proportional to changed logical payload rather
  than total history.
