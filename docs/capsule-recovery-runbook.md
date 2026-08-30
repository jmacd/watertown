# Legacy Capsule Recovery and Storage-Format Migration Runbook

This runbook is the operator procedure for migrating an authoritative
`dp.commit.3` pond through the opaque `pondcapsule.legacy.2` envelope into a
fresh pond created by the current Watertown binary.

This is not the normal target-format recovery path. The legacy recipe has its
own explicit CLI operation and its own single object-store namespace:

```text
recovery/legacy-migration-v2/README.sh
recovery/legacy-migration-v2/recipes/<recipe-hash>/README.sh
```

The earlier `pondcapsule.legacy.1` recipe remains immutable but does not
preserve synthetic dynamic-node metadata. Do not use it for a migration. The
`legacy` name identifies the `dp.commit.3` source format, never the target
pond format.

The existing generic recipe remains the `watertown.commit.v1` to
`pondcapsule.2` target-format recipe under `recovery/README.sh` and
`recovery/recipes/watertown.commit.v1/<recipe-hash>/README.sh`. Do not
overwrite, reinterpret, or use that generic bootstrap for this migration.

Use this procedure only after rehearsing the complete sequence against a
disposable staging namespace. Recovery and cutover never require deleting or
overwriting the source pond, native backup, or either recipe namespace.

The formats and trust model are specified in
[recovery-capsule-design.md](recovery-capsule-design.md). The reviewed opaque
extractor is embedded from
[`crates/sync-store/recovery/legacy-migration-v2/`](../crates/sync-store/recovery/legacy-migration-v2/).

## Safety model

An exact native commit is an immutable snapshot, but it is authoritative only
if all writers are quiescent and no later write exists.

**Old `dp.commit.3` writers do not honor `control/write.freeze`.** Stop and
disable every old writer at the service, timer, supervisor, factory, and
operator-session level before invoking the current binary's legacy-safe
`pond freeze enable`. Keep them stopped. The marker is durable evidence and
protects write paths using the current binary; it is not a kill switch for an
old process.

Stop all of the following:

- ingestion and API writers;
- scheduled `pond run`, `pond apply`, copy, maintenance, and import jobs;
- factories or services that can start write transactions;
- interactive operator sessions;
- timers and supervisors that could restart any writer; and
- every old service instance that does not understand the freeze marker.

After those processes are stopped, the current binary acquires the stable
cross-process write lock, re-reads the source tip, and creates
`control/write.freeze`. If a remaining writer holds the lock, the command
fails. Do not retry until that process is identified and stopped.

Recipe publication is create-only. It can create the two exact
`recovery/legacy-migration-v2` objects above, accept byte-identical retries, or
fail when a discoverable or immutable object already has different bytes. It
does not provide remote delete, repair-by-overwrite, or namespace cleanup.

## Required records

Create an incident or migration record outside both pond namespaces. Record:

- source environment, pond path, pond identity, and birthplace;
- source native format (`dp.commit.3`) and old and current binary versions;
- every stopped service, timer, supervisor, and operator write path;
- backup attachment name and exact object-store URL;
- exact final native commit hash;
- legacy-migration-v2 recipe hash and both exact remote object keys;
- capsule format (`pondcapsule.legacy.2`) and root from
  `CAPSULE/recovery/refs/latest`;
- target environment, path, namespace, identity, birthplace, and binary;
- every command and its outcome;
- deterministic target replay report and application smoke-test results;
- separate target-format backup/recovery drill results; and
- rollback ownership and retention deadline.

Hashes prove integrity, not freshness. The independently recorded final commit
and capsule root establish which snapshot was selected.

## Migration gates

Do not proceed to the next gate unless the current gate succeeds:

1. The explicit legacy-migration-v2 recipe publishes and inspects successfully.
2. Every old writer and restart mechanism is stopped outside Pond.
3. The current binary's write freeze succeeds and records the exact source tip.
4. A final push completes and `pond verify --exact` reports equal tips.
5. The complete native backup and both legacy recipe objects are downloaded.
6. The authenticated extractor creates and verifies
   `pondcapsule.legacy.2` for the exact recorded commit.
7. Pond-free opaque materialization is inspected without claiming logical or
   Parquet validation.
8. Experimental staged import succeeds into a nonexistent target.
9. The target passes replay-identity, metadata, application, and remote checks
   while post-commit dispatch remains suppressed.
10. A separate `watertown.commit.v1` target-format backup is recovered using
    the generic `pondcapsule.2` recipe.
11. Exactly one pond is enabled as the authoritative writer.

Any mismatch is a stop condition. Do not repair, skip, overwrite, or silently
accept a failed verification.

## 1. Prepare and rehearse

Build or select the current binary that contains the legacy-compatible reader,
legacy-safe freeze, `pondcapsule.legacy.2` importer, and explicit recipe
commands. Keep the old source binary available for rollback, but do not use it
after writer shutdown except under an approved rollback.

Use fresh destinations throughout:

```sh
SOURCE_POND=/srv/watertown/source
SOURCE_REMOTE=backup
BOOTSTRAP=/srv/recovery/legacy-migration-v2-README.sh
BACKUP=/srv/recovery/native-backup
KIT=/srv/recovery/legacy-migration-v2-kit
CAPSULE=/srv/recovery/legacy-capsule
MATERIALIZED=/srv/recovery/legacy-materialized
TARGET_POND=/srv/watertown/recovered
TARGET_BIRTHPLACE=watershop-legacy-migration-rehearsal
TARGET_REMOTE=target-backup
RECIPE_HASH=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

`BOOTSTRAP`, `BACKUP`, `KIT`, `CAPSULE`, `MATERIALIZED`, and `TARGET_POND`
must not exist when their creating command starts. Substitute the independently
recorded recipe hash after publication.

First execute the entire runbook against watershop and a disposable MinIO
target. Do not reuse the production target namespace during rehearsal.

## 2. Publish and inspect the legacy-migration-v2 recipe

While the source pond and its backup attachment are available, run the
deliberate migration operation:

```sh
POND="$SOURCE_POND" pond capsule recipe legacy-migration-v2 publish "$SOURCE_REMOTE"
POND="$SOURCE_POND" pond capsule recipe legacy-migration-v2 inspect "$SOURCE_REMOTE"
```

The success log must identify all of:

```text
flavor=legacy-migration-v2
native_format=dp.commit.3
capsule_format=pondcapsule.legacy.2
```

Record the reported hash as `RECIPE_HASH`. Confirm with the object-store
inventory that exactly these recipe keys exist:

```text
recovery/legacy-migration-v2/README.sh
recovery/legacy-migration-v2/recipes/<RECIPE_HASH>/README.sh
```

Both objects must contain identical bytes. A retry is successful only when the
existing bytes are identical. Differing pre-existing bytes require
investigation and a new reviewed plan; the command will not overwrite them.

Ordinary backup push remains associated with the separate target-format
recipe. It does not automatically publish this legacy-migration-v2 recipe and
does not make this explicit gate optional.

## 3. Stop old writers, then freeze with the current binary

Stop every writer and restart mechanism listed in the safety model. Verify at
the service-manager and scheduler level that none can restart. This step must
precede the freeze because old binaries ignore `control/write.freeze`.

Using only the current binary, persist the marker and record its source tip:

```sh
POND="$SOURCE_POND" pond freeze enable \
  --reason "dp.commit.3 to pondcapsule.legacy.2 migration"
POND="$SOURCE_POND" pond freeze status
```

The enable command acquires `control/write.lock` before creating
`control/write.freeze`. `PondLocked` means a process may still be writing;
identify and stop it before retrying. A successful marker survives restarts,
but an old binary still will not enforce it, so keep all old services stopped.

Run the final push from a controlled current-binary operator session:

```sh
POND="$SOURCE_POND" pond push "$SOURCE_REMOTE"
POND="$SOURCE_POND" pond verify --exact "$SOURCE_REMOTE"
```

Record the complete hash printed by freeze status and the final push as
`SOURCE_TIP`; freeze, local tip, and remote tip must agree:

```sh
SOURCE_TIP=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

Do not recover an authoritative migration from a moving `latest` ref. An
unexpected successful source write means the operational shutdown or binary
selection failed; abort, stop writers, repeat freeze and final verification,
and record the replacement exact tip.

The final push may exercise the generic target-format recipe compatibility
check under `recovery/README.sh`. That independent behavior must neither
replace nor be mistaken for the explicit legacy-migration-v2 namespace verified
in step 2.

## 4. Authenticate and test the legacy-migration-v2 kit

Authenticate separately. Credentials do not belong in commands, capsule
metadata, or the recovery kit.

Fetch only the passive discoverable bootstrap. For MinIO:

```sh
mc alias set ALIAS ENDPOINT
mc cp \
  ALIAS/BUCKET/PREFIX/recovery/legacy-migration-v2/README.sh \
  "$BOOTSTRAP"
```

For Azure Blob Storage:

```sh
azcopy login
azcopy copy \
  https://ACCOUNT.blob.core.windows.net/CONTAINER/PREFIX/recovery/legacy-migration-v2/README.sh \
  "$BOOTSTRAP"
```

Authenticate `BOOTSTRAP` with the independently recorded `RECIPE_HASH` using
the `pondcapsule.recipe.1` domain procedure documented by the kit. Review the
script, extract only into the nonexistent `KIT`, verify `SHA256SUMS`, and
review every extracted file:

```sh
sh "$BOOTSTRAP" "$KIT"
(cd "$KIT" && shasum -a 256 -c SHA256SUMS)
```

The bootstrap performs no network access, executes no extracted file, and
does not modify the source pond or backup.

Create the pinned environment and test the native fixtures and opaque capsule
tool:

```sh
python3 -m venv recovery-venv
. recovery-venv/bin/activate
python -m pip install -r "$KIT/requirements.lock"
python "$KIT/extract.py" --verify-fixtures "$KIT/native-fixtures.json"
python "$KIT/capsule_test.py"
```

The source extractor intentionally does not depend on or import PyArrow. For
offline recovery, use an independently authenticated wheelhouse and
`pip --no-index --find-links`; do not silently substitute dependency versions.

## 5. Download the complete native backup

Use a provider-approved recursive copy into the nonexistent `BACKUP`.
For MinIO:

```sh
test ! -e "$BACKUP"
mc mirror ALIAS/BUCKET/PREFIX "$BACKUP"
```

For Azure Blob Storage:

```sh
test ! -e "$BACKUP"
azcopy copy \
  https://ACCOUNT.blob.core.windows.net/CONTAINER/PREFIX \
  "$BACKUP" --recursive=true
```

Do not use mirror removal, synchronization, overwrite, or cleanup options
against the source namespace. These commands only copy; remote retirement is a
separate reviewed operation outside this runbook.

The download must contain `_delta_log/`, `_blobs/` when external objects are
present, and both legacy-migration-v2 recipe objects. Compare both downloaded
copies with the independently authenticated bootstrap:

```sh
cmp "$BACKUP/recovery/legacy-migration-v2/README.sh" "$BOOTSTRAP"
cmp \
  "$BACKUP/recovery/legacy-migration-v2/recipes/$RECIPE_HASH/README.sh" \
  "$BOOTSTRAP"
```

Keep the downloaded backup read-only.

## 6. Extract the exact source commit

Use the recorded commit hash, not a ref:

```sh
python "$KIT/extract.py" "$BACKUP" "$CAPSULE" \
  --commit "$SOURCE_TIP" \
  --birthplace production-before-legacy-migration
```

The extractor verifies the live legacy Delta key/value rows,
`value_blake3`, the `dp.commit.3` commit and node-manifest root,
`dp.manifest.2`, `dp.tree.2`, `dp.series.1`, `dp.recipe.1`, graph mappings,
and every raw object BLAKE3 before promoting the capsule directory.

It treats physical table payloads as opaque bytes. It does **not** import
PyArrow, decode table Parquet, validate table schemas or rows, calculate
logical leaves, or require one schema across a series. Those are target
importer responsibilities.

Record and independently preserve the opaque capsule root:

```sh
cat "$CAPSULE/recovery/refs/latest"
```

## 7. Verify and inspect the opaque capsule without Pond

Run the authenticated kit copy, not an unauthenticated script copied beside
capsule data:

```sh
python "$KIT/capsule.py" verify "$CAPSULE"
python "$KIT/capsule.py" materialize "$CAPSULE" "$MATERIALIZED"
```

This is the legacy kit's opaque materializer, not generic logical capsule
materialization. Inspect its inventory, exact object bytes, paths, native
version ordering, metadata, symlink target bytes, and inert dynamic recipes.
It does not create symlinks, execute recipes, analyze Parquet, or establish
target logical leaf identities.

Do not substitute `pond capsule inspect` or `pond capsule verify` here: those
commands describe logical `pondcapsule.1`/`pondcapsule.2` verification, not
this opaque source envelope.

## 8. Import into a fresh target pond

The target path must not exist:

```sh
POND="$TARGET_POND" pond capsule import "$CAPSULE" \
  --birthplace "$TARGET_BIRTHPLACE" \
  --experimental
```

The importer detects `pondcapsule.legacy.2`, verifies its raw closure and
native per-version mapping, and prepares each physical version separately.
For tables, the target importer opens each version's Parquet payload, derives
that version's supported target schema, replays its rows into the current
native format, and computes the target schema fingerprint and logical leaf
identity. It records a deterministic replay report, preserves source ordering
and metadata, suppresses post-commit dispatch and automatic pushes, and
re-reads the staged target to validate entry identities, version identities,
schema fingerprints, blob identities, logical counts, and metadata before
atomic promotion.

The importer is experimental because bounded resume and active-remote
preflight are not yet implemented. On failure it leaves the staging path for
inspection; do not treat a failed import as usable.

## 9. Validate the inert target

Keep `post_commit_dispatch` suppressed. Before enabling any writer:

- preserve and review `CAPSULE_IMPORT_PROVENANCE.json`;
- preserve and review `LEGACY_CAPSULE_REPLAY.json`;
- record the new pond identity and source commit/capsule identities;
- run representative `pond list`, `pond cat`, and query operations;
- compare critical file bytes, per-version table schemas and rows, ordering,
  event-time bounds, timestamps, and logical attributes;
- inspect every restored remote definition with `pond remote list` and
  `pond backup list`;
- redirect restored remotes to fresh target namespaces;
- confirm no target resolves to the source backup namespace;
- run application-specific read-only smoke tests; and
- confirm the source writer and every old restart mechanism remain stopped.

The current importer does not perform active-remote preflight. This is an
operator responsibility and a release-readiness limitation, not an optional
check.

## 10. Prove future target-format recovery separately

This gate is intentionally separate from legacy extraction and import.
Configure a fresh backup namespace for the recovered target. Explicitly
publish and inspect the existing generic target-format recipe:

```sh
POND="$TARGET_POND" pond capsule recipe publish "$TARGET_REMOTE"
POND="$TARGET_POND" pond capsule recipe inspect "$TARGET_REMOTE"
POND="$TARGET_POND" pond push "$TARGET_REMOTE"
POND="$TARGET_POND" pond verify --exact "$TARGET_REMOTE"
```

Those generic commands must report:

```text
flavor=target-format
native_format=watertown.commit.v1
capsule_format=pondcapsule.2
```

Recover that new backup into another disposable pond using its independently
authenticated `recovery/README.sh` target-format kit. This drill must not use
the legacy-migration-v2 bootstrap, source backup, or intended production target.
It proves future disaster recovery after the old decoder and old pond are
unavailable.

## 11. Cut over exactly one writer

After every prior gate succeeds, choose the recovered pond as authoritative.
Enable post-commit dispatch only after all restored remote definitions and
destinations have been reviewed:

```sh
POND="$TARGET_POND" pond config set post_commit_dispatch enabled
```

Start target writers and confirm one controlled write, target backup push,
remote verification, and application read. Do not restart source writers.

Repeat the rehearsed watershop procedure for production Azure. Rehearsal
success does not waive any production gate; record new exact source-tip,
recipe-hash, capsule-root, and target-backup values.

## Rollback and retention

Before the first target write, rollback means keeping the target inert,
removing the marker with the current binary, and only then re-enabling the old
source services under the approved rollback plan:

```sh
POND="$SOURCE_POND" pond freeze disable
```

Because old writers ignore the marker, service-manager state remains the
actual exclusion mechanism throughout migration.

After the target accepts writes, a simple rollback would discard those writes
or create split-brain. Stop and design an explicit reconciliation; never
enable both ponds.

Retain through the rollback window and at least one scheduled recovery drill:

- the old source pond;
- the complete old native backup namespace;
- both immutable and discoverable legacy-migration-v2 recipe objects;
- the authenticated legacy-migration-v2 kit and dependency wheelhouse;
- exact source commit, recipe hash, and capsule root records;
- the verified opaque capsule and deterministic replay report; and
- the first verified target-format backup and its recovery record.

Deletion or remote namespace retirement is a separate reviewed provider
operation. No recipe, recovery, migration, import, or cutover command in this
runbook automates it.

## Abort conditions

Stop the migration if:

- any old writer or restart mechanism cannot be conclusively stopped;
- the write freeze cannot be created or read back;
- the source tip changes after the final push;
- local and remote tips differ;
- either legacy recipe object is absent, differs, or has an unexpected path;
- fixture, native graph, raw object, capsule, replay, or import verification
  fails;
- the capsule source tip differs from the recorded final tip;
- target replay identities, schemas, counts, ordering, or metadata differ;
- restored remotes are unresolved or overlap source storage;
- separate target-format backup recovery has not been demonstrated; or
- rollback ownership and retention are unclear.

Preserve logs and staging directories for diagnosis. Do not bypass a failed
gate to meet a cutover schedule.
