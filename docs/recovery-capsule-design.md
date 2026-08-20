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

Publish a versioned **recovery capsule** alongside each native backup.

A capsule is a portable snapshot of the complete live namespace plus ordered
logical series leaf history. It is not:

- a copy of the source Delta table;
- a replay of every source transaction;
- a source-format commit graph;
- a source of embedded credentials; or
- an executable script fetched and run without review.

The capsule contains a stable manifest and plain content-addressed payload
objects. Files remain exact bytes. Tables use standard Parquet. The manifest
preserves enough logical information to reconstruct every series leaf and
verify its identity independently of physical Parquet, pack, or object
boundaries.

Import creates a new pond identity and transaction lineage. Immutable import
provenance links the new pond to the source pond, source tip, and capsule root.

## Two-branch delivery sequence

Capsule publication must exist before the old reader is retired.

### Phase one: old-format-compatible branch

Starting from the old-format-compatible `main`:

1. Freeze the capsule v1 wire contract in shared, format-independent code.
2. Traverse the live `dp.commit.3` graph using the existing supported reader.
3. Publish portable capsule payloads and manifests.
4. Add inspection, verification, retention, cloud-tool instructions, safe
   cleanup planning, and a generic staged importer.
5. Prove a complete old-format export/download/import round trip.
6. Merge and build an image.
7. Publish and independently preserve a verified capsule of the current Azure
   pond.

No legacy decoder is added to logical series v2.

### Phase two: logical series v2 branch

After phase one merges:

1. Return to `jmacd/incremental1`.
2. Merge updated `origin/main`; do not rebase.
3. Preserve the capsule wire contract unchanged.
4. Implement a v2 import sink using native logical leaf stamping.
5. Import the preserved capsule into a fresh v2 pond.
6. Verify logical contents and publish to a new Azure namespace.
7. Publish and verify a new v2 recovery capsule.
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

## Publication protocol

Capsule publication is a trailing, idempotent operation after a successful
native backup push:

1. Resolve the source's current immutable content tip.
2. Build or incrementally update the live logical inventory.
3. Upload missing plain payload objects.
4. Verify every referenced payload is durable.
5. Upload the canonical manifest.
6. Upload the object list, checksums, runbook, and generated scripts.
7. Verify the complete generation.
8. Atomically acquire `recovery/locks/publish` and recheck the prior ref.
9. Advance `recovery/refs/latest` last, merge retained history, and release the
   lock.

A crash before the final reference update leaves the prior verified capsule
current. Retrying reuses content-addressed objects and completes the same
generation. A crashed finalizer can leave a stale publication lock; it is never
stolen automatically. Operators must inspect and explicitly remove a lock older
than fifteen minutes so two live publishers cannot both believe they own it.

`pond capsule publish <remote>` explicitly publishes or repairs a capsule when
there has been no new logical pond commit. Normal backup push also attempts
capsule publication after its native push succeeds. Capsule failure is reported
as a backup-health failure but must not corrupt or roll back the already durable
native backup.

## Incremental cost and retention

The first capsule may require approximately one additional live logical-data
copy. Native inline objects cannot be exposed to simple copy tools without
materialization. Compression and existing raw-blob reuse affect the exact
ratio.

Later capsule generations reuse immutable content-addressed payload objects.
Normal CPU, requests, and upload bytes must be proportional to changed logical
payload and metadata, not accumulated history. Each generation adds a small
manifest and only previously absent payload.

Retention keeps the latest verified capsule plus a configurable history,
defaulting to three generations. This does not normally mean three full copies:
storage is the union of payload objects reachable from retained manifests. High
churn can retain close to three generations of changed data.

Capsule garbage collection:

1. snapshots retained generation references;
2. computes their complete reachable payload set;
3. writes an immutable deletion plan;
4. rechecks references before applying it; and
5. never deletes an object reachable from any retained generation or active
   reader grace period.

## Operator command surface

Use a top-level `pond capsule` command family. The existing `pond recover`
command remains the local crash-transaction repair operation.

```text
pond capsule publish <remote>
pond capsule list <source>
pond capsule inspect <source>
pond capsule verify <source>
pond capsule scripts <source> --provider az|mc
pond capsule import <source> --target <empty-path> --birthplace <label>
pond capsule gc plan|verify|apply ...
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
