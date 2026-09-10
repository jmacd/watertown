# Watertown Capsule Recovery Runbook

This runbook covers recovery of the current native
`watertown.commit.v2` format through the current portable
`pondcapsule.4` format. There is no historical-format migration path.

## Safety model

- Recover from an authenticated, read-only copy of the native backup.
- Record the exact source commit and capsule root before cutover.
- Never overwrite the source backup or an existing destination.
- Keep restored dynamic recipes inert until the target is reviewed.
- Treat every unsupported object or capsule magic as an abort condition.

## 1. Quiesce the source

Stop every service, timer, supervisor, CLI session, and other writer. Then
freeze the pond:

```bash
POND="$SOURCE_POND" pond freeze enable --reason "approved recovery"
POND="$SOURCE_POND" pond freeze status
```

Record the source pond identity and exact content tip.

## 2. Publish and authenticate the capsule

```bash
POND="$SOURCE_POND" pond push backup
POND="$SOURCE_POND" pond verify --exact backup
POND="$SOURCE_POND" pond capsule publish backup
```

Record the printed capsule root and retrieve the complete immutable tree:

```text
recovery/refs/latest
recovery/manifests/<capsule-root>.json
recovery/objects/blake3=<payload-hash>
CAPSULE-README.md
CAPSULE-FORMAT.md
capsule.py
parquet_schema.py
capsule-requirements.lock
recover.sh
```

`pond capsule recipe publish/inspect` is only for an explicitly retained
legacy `watertown.commit.v1` rollback remote. It is not part of native-v2
publication or cutover.

## 3. Download the complete portable capsule

Use the reviewed storage-backend download helper and preserve the `recovery/`
tree's names and bytes exactly. Do not continue from a partial listing.

## 4. Verify the capsule without Pond

```bash
python ./capsule/capsule.py verify ./capsule
```

Confirm the report identifies `pondcapsule.4`, the recorded source commit,
the expected entry count, and plausible logical and physical totals.

The verifier checks canonical JSON, the `pondcapsule.root.4` root, the exact
object closure, payload BLAKE3 and sizes, Parquet schemas, logical leaves,
series roots, dynamic metadata, and `watertown.recipe.v1` framing.

## 5. Rehearse safe materialization

```bash
python ./capsule/capsule.py materialize ./capsule ./materialized
```

Review `inventory.json`, regular file versions, Parquet table versions,
symlink target descriptions, and inert dynamic recipe files. No recipe is
executed and no live symlink is created.

## 6. Import into a fresh pond

The target path must not exist:

```bash
POND="$TARGET_POND" pond capsule import ./capsule \
  --birthplace recovered-production
```

The importer first creates the pond in a same-parent, capsule-addressed unique
initialization directory. It writes persistent dispatch suppression,
provenance, and the first durable journal, syncs them, and atomically publishes
that directory under the deterministic resumable staging name. It then commits
bounded entry/leaf batches, rebuilds a `pondcapsule.4` from the staged pond,
compares the logical projection, syncs it, and atomically renames it onto the
target.

A retry can finish a pristine initialization interrupted before suppression,
provenance, the first journal, or staging-name publication. It validates the
target/capsule-addressed directory, birthplace, provenance when present, and
initial transaction sequence. Modified, conflicting, or corrupt state fails
loudly and remains in place for inspection.

Each numbered checkpoint is published by a synced same-directory staging file
and atomic no-replace rename. A crash may leave an unpublished staging file;
resume removes it and continues from the highest final checkpoint. A corrupt
final checkpoint is an error and must be investigated, not skipped.

File and table series imports build linear offset indexes once per invocation;
each leaf reads only overlapping physical objects. Resuming may rebuild those
indexes, but does not rescan all preceding leaves or objects.

## 7. Validate and cut over

While the target remains inert:

```bash
POND="$TARGET_POND" pond status
POND="$TARGET_POND" pond verify
POND="$TARGET_POND" pond capsule recipe inspect backup
```

Review remotes and every restored dynamic recipe. Enable exactly one writer
only after the target has passed application-specific checks. Repair unsafe
attachments/configs with the ordinary remote and mknod/apply commands, then:

```bash
POND="$TARGET_POND" pond capsule activate
```

Activation reads and validates every `/sys/remotes/*` attachment and
`/system/run/*` config without executing factories. Any failure leaves the
pond inert.

## Rollback and retention

Rollback means stopping target writers and returning to the still-frozen
source or another independently verified current-format recovery. Keep the
native backup, authenticated recipe, `pondcapsule.4`, verification logs, and
cutover record until the retention policy explicitly permits deletion.

## Abort conditions

Abort on any unexpected writer activity, changing source tip, unsupported
format, missing object, hash mismatch, noncanonical manifest, pack coverage or
proof failure, schema mismatch, existing destination, or staged logical
comparison failure.
