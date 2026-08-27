# Capsule Recovery and Storage-Format Migration Runbook

This runbook is the operator procedure for recovering a `dp.commit.3` native
backup into a portable `pondcapsule.1`, inspecting it without Pond, and
importing it into a fresh pond created by the current binary.

Use this procedure for a storage-format migration only after rehearsing the
complete sequence against a disposable staging namespace. Recovery and cutover
never require deleting or overwriting the source pond or its native backup.

The format and trust model are specified in
[recovery-capsule-design.md](recovery-capsule-design.md). The frozen standalone
kit has additional command details in
[`crates/sync-store/recovery/dp.commit.3/README.md`](../crates/sync-store/recovery/dp.commit.3/README.md).

## Safety model

A native commit is an immutable, internally consistent snapshot. A capsule
created from an exact commit is therefore valid even if the source pond was
live. It is not necessarily the final authoritative state: later writes may
exist.

An authoritative migration requires writer quiescence. Freezing a remote ref
is insufficient because it does not stop the local pond from accepting writes.
`pond freeze enable` creates a durable local marker while holding the same
cross-process lock used by every data-write path. Ordinary writes, replay,
pull/import, compaction, reclamation, and control-history pruning then fail
closed; forced control rebuild refuses to remove the marker. Reads, backup
push, remote verification, and capsule inspection remain available.

The command fails if a writer currently holds the lock; it does not terminate
or wait for that writer. Operators must first stop every process capable of
writing the source pond and keep it stopped until cutover or rollback is
chosen. This also prevents supervisors from repeatedly attempting forbidden
writes and makes the final-tip declaration operationally meaningful.

This includes:

- ingestion and API writers;
- scheduled `pond run`, `pond apply`, copy, maintenance, and import jobs;
- factories or services that can start write transactions;
- interactive operator sessions; and
- old service instances that would not understand the target format.

Stopping only automatic remote push is not a write freeze. Do not run two
writable ponds as successors to the same authoritative source.

## Required records

Create an incident or migration record outside both pond namespaces. Record:

- source environment, pond path, pond identity, and birthplace;
- source native format and source binary version;
- backup attachment name and exact object-store URL;
- exact final native commit hash;
- recovery recipe hash;
- capsule root from `CAPSULE/recovery/refs/latest`;
- target environment, path, namespace, pond identity, and binary version;
- every command and its outcome;
- application smoke-test results;
- new-format backup and recovery-test results; and
- the rollback retention deadline.

Hashes prove integrity, not freshness. The independently recorded final commit
and capsule root establish which snapshot was selected.

## Migration gates

Do not proceed to the next gate unless the current gate succeeds:

1. The recovery recipe is present and verifies on the source backup.
2. All source writers are stopped and a persistent write freeze succeeds.
3. Freeze status records the exact source content tip.
4. A final push completes and `pond verify` reports identical local and remote
   tips.
5. The exact final tip is recorded outside the backup.
6. The downloaded native backup produces a deeply verified capsule for that
   exact commit.
7. Pond-free materialization is inspected.
8. Experimental staged import succeeds into a nonexistent target.
9. The target passes logical, application, and remote-configuration checks
   while post-commit dispatch remains suppressed.
10. A backup written in the target native format is recovered in a separate
   drill.
11. Exactly one pond is enabled as the authoritative writer.

Any mismatch is a stop condition. Do not repair, skip, or silently accept a
failed verification.

## 1. Prepare and rehearse

Build or select the exact source-compatible and target binaries. Confirm the
target binary imports the frozen `pondcapsule.1` compatibility fixture in its
test suite.

The target native format must have its own reviewed standalone recovery recipe
before production relies exclusively on backups written in that format.
Implementing capsule import alone is not sufficient: import proves old-to-new
migration, while the new recipe proves future disaster recovery.

Use fresh destinations throughout. Example shell variables:

```sh
SOURCE_POND=/srv/watertown/source
SOURCE_REMOTE=backup
BOOTSTRAP=/srv/recovery/README.sh
BACKUP=/srv/recovery/native-backup
KIT=/srv/recovery/dp.commit.3-kit
CAPSULE=/srv/recovery/capsule
MATERIALIZED=/srv/recovery/materialized
TARGET_POND=/srv/watertown/recovered
TARGET_BIRTHPLACE=watershop-capsule-rehearsal
```

The `BOOTSTRAP`, `BACKUP`, `KIT`, `CAPSULE`, `MATERIALIZED`, and `TARGET_POND`
paths must not exist when their creating command starts.

First execute the entire runbook against watershop and a disposable MinIO
target. Do not reuse the production target namespace during rehearsal.

## 2. Verify the source recovery recipe

While the source pond is still available:

```sh
POND="$SOURCE_POND" pond capsule recipe publish "$SOURCE_REMOTE"
POND="$SOURCE_POND" pond capsule recipe inspect "$SOURCE_REMOTE"
```

Record the reported recipe hash independently. Publication is strict and
idempotent: it does not overwrite differing recipe content.

## 3. Quiesce the source writer

Stop every writer listed in the safety model. Prevent supervisors, timers, and
orchestrators from restarting them. Keep read-only inspection available if it
does not launch factories or write maintenance state.

Persist the write freeze and record its exact source tip:

```sh
POND="$SOURCE_POND" pond freeze enable \
  --reason "storage-format migration"
POND="$SOURCE_POND" pond freeze status
```

The enable command acquires the pond's write lock before creating
`control/write.freeze`. If another writer is active it fails with
`PondLocked`; stop or allow that known writer to finish, then rerun the
command. A successful marker survives process restarts and is checked afresh
after every future write-lock acquisition, including by already-open
processes.

Run the final push from a controlled operator session:

```sh
POND="$SOURCE_POND" pond push "$SOURCE_REMOTE"
POND="$SOURCE_POND" pond verify --exact "$SOURCE_REMOTE"
```

`pond verify --exact` exits nonzero unless the local and remote tips are
identical. Record the complete commit hash printed by freeze status and the
final push as `SOURCE_TIP`; all three values from freeze, local verification,
and remote verification must agree. Do not recover an authoritative migration
from a moving `latest` ref.

```sh
SOURCE_TIP=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

Any attempted source write after this point must fail with
`PondWriteFrozen`. An unexpected successful write means the writer bypassed
the supported steward path or is using an older binary; stop the migration,
repeat freeze and final verification with compatible binaries, and record the
replacement exact tip.

## 4. Authenticate and test the frozen kit

Authenticate separately. Credentials do not belong in commands, capsule
metadata, or the recovery kit.

Fetch only the passive bootstrap from the source backup. For MinIO:

```sh
mc alias set ALIAS ENDPOINT
mc cp ALIAS/BUCKET/PREFIX/recovery/README.sh "$BOOTSTRAP"
```

For Azure Blob Storage:

```sh
azcopy login
azcopy copy \
  https://ACCOUNT.blob.core.windows.net/CONTAINER/PREFIX/recovery/README.sh \
  "$BOOTSTRAP"
```

Authenticate `BOOTSTRAP` using the independently recorded recipe hash as
described by the kit README. Review it, extract it into `KIT`, verify
`SHA256SUMS`, and review every extracted file. The bootstrap performs no
network access and executes none of the extracted files.

```sh
sh "$BOOTSTRAP" "$KIT"
(cd "$KIT" && shasum -a 256 -c SHA256SUMS)
```

Create the pinned Python 3.13 environment and test both native and portable
decoders before processing the production snapshot:

```sh
python3.13 -m venv recovery-venv
. recovery-venv/bin/activate
python -m pip install -r "$KIT/requirements.lock"
python "$KIT/extract.py" --verify-fixtures "$KIT/native-fixtures.json"
python "$KIT/capsule_test.py"
```

For an offline recovery, use an independently authenticated wheelhouse and
`pip --no-index --find-links`. Do not silently substitute dependency versions.

## 5. Download the complete native backup

Use the authenticated kit's constrained helper. For MinIO:

```sh
sh "$KIT/download-mc.sh" ALIAS/BUCKET/PREFIX "$BACKUP"
```

For Azure Blob Storage:

```sh
sh "$KIT/download-azcopy.sh" \
  https://ACCOUNT.blob.core.windows.net/CONTAINER/PREFIX "$BACKUP"
```

The download must contain `_delta_log/`, `_blobs/` when external objects are
present, and `recovery/README.sh`. Compare the downloaded
`recovery/README.sh` with `BOOTSTRAP`. Keep the downloaded backup read-only.
Never run cleanup against the source namespace as part of recovery.

```sh
cmp "$BACKUP/recovery/README.sh" "$BOOTSTRAP"
```

## 6. Extract the exact source commit

Use the recorded commit hash, not a ref:

```sh
python "$KIT/extract.py" "$BACKUP" "$CAPSULE" \
  --commit "$SOURCE_TIP" \
  --birthplace production-before-format-migration
```

Extraction verifies the native Delta rows, commit, node-Merkle root,
tree/manifest agreement, content addresses, Parquet data, logical leaves, and
series roots before promoting the capsule directory. Any error is fatal.

Record and independently preserve the capsule root:

```sh
cat "$CAPSULE/recovery/refs/latest"
```

## 7. Verify and inspect without Pond

Run the trusted kit copies, not unauthenticated scripts copied beside capsule
data:

```sh
python "$KIT/capsule.py" verify "$CAPSULE"
python "$KIT/capsule.py" materialize "$CAPSULE" "$MATERIALIZED"
```

Inspect `MATERIALIZED/README.txt` and `MATERIALIZED/inventory.json`. Check exact
file bytes, table schemas and rows, version order and metadata, symlink target
bytes, and inert dynamic recipes. Materialization never activates symlinks or
executes recipes.

The current Pond binary provides additional compatibility coverage:

```sh
pond capsule verify "$CAPSULE"
```

## 8. Import into a fresh target pond

The target path must not exist. Capsule import is experimental because bounded
resume and active-remote preflight are not yet implemented:

```sh
POND="$TARGET_POND" pond capsule import "$CAPSULE" \
  --birthplace "$TARGET_BIRTHPLACE" \
  --experimental
```

The importer creates a private sibling staging pond with a fresh identity,
suppresses post-commit factories and automatic pushes, reconstructs logical
content using the target binary's native format, rebuilds the logical capsule
projection, and atomically promotes only an exact match. On failure, it leaves
the staging path for inspection; do not treat a failed import as usable.

## 9. Validate the inert target

Keep `post_commit_dispatch` suppressed. Before enabling any writer:

- record the new pond identity and confirm immutable import provenance;
- run representative `pond list`, `pond cat`, and query operations;
- compare critical files, tables, schemas, rows, version order, and metadata
  with the materialized inventory;
- inspect every restored remote definition with `pond remote list` and
  `pond backup list`;
- redirect restored remotes to fresh target namespaces;
- confirm no target resolves to the source backup namespace;
- run application-specific read-only smoke tests; and
- confirm the source writer remains stopped.

The current importer does not perform active-remote preflight. This is an
operator responsibility and a release-readiness limitation, not an optional
check.

## 10. Prove target-format recovery

Before production cutover, configure a fresh backup namespace for the recovered
pond and perform a push with the target binary. Verify the remote tip, then
recover that new-format backup into another disposable pond using the target
format's standalone recipe.

This drill must not reuse either the source backup or intended production
target. It must prove that future recovery works after the old decoder and old
pond are unavailable.

## 11. Cut over exactly one writer

After every prior gate succeeds, choose the recovered pond as authoritative.
Enable post-commit dispatch only after remote definitions and destinations
have been reviewed:

```sh
POND="$TARGET_POND" pond config set post_commit_dispatch enabled
```

Start target writers and confirm one controlled write, backup push, remote
verification, and application read. Do not restart source writers.

Repeat the already-proven watershop procedure for Azure. Rehearsal success does
not waive any production gate; record new exact source-tip, capsule-root, and
target-backup values for production.

## Rollback and retention

Before the first target write, rollback means keeping the target inert,
explicitly removing the source freeze, and restarting the unchanged source
writer:

```sh
POND="$SOURCE_POND" pond freeze disable
```

Status and disable access the stable marker directly, so they remain available
if the source control Delta table is damaged and requires `pond
rebuild-control`.

After the target accepts writes, a simple rollback would discard those writes
or create split-brain. Stop and design an explicit reconciliation; do not
enable both ponds.

Retain all of the following through an agreed rollback window and at least one
successful scheduled recovery drill:

- the old source pond;
- the complete old native backup namespace;
- the independently authenticated old recovery kit and dependency wheelhouse;
- the exact source commit and capsule root records;
- the verified capsule; and
- the first verified target-format backup.

Deletion is a separate reviewed operation. Recovery, migration, and cutover
commands never delete old pond data.

## Abort conditions

Stop the migration if:

- a writer cannot be conclusively stopped;
- the persistent write freeze cannot be created or read back;
- the source tip changes after the final push;
- local and remote tips differ;
- recipe, fixture, native graph, capsule, or import verification fails;
- the capsule source tip differs from the recorded final tip;
- restored remotes are unresolved or overlap source storage;
- target-format backup recovery has not been demonstrated; or
- rollback ownership and retention are unclear.

Preserve logs and staging directories for diagnosis. Do not bypass a failed
gate to meet a cutover schedule.
