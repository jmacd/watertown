# Watertown Capsule Recovery Runbook

This runbook covers recovery of the current native
`watertown.commit.v1` format through the current portable
`pondcapsule.4` format. There is no historical-format migration path.

## Safety model

- Recover from an authenticated, read-only copy of the native backup.
- Record the exact source commit and recovery-recipe hash before extraction.
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

## 2. Publish and authenticate the recovery recipe

```bash
POND="$SOURCE_POND" pond capsule recipe publish backup
POND="$SOURCE_POND" pond capsule recipe inspect backup
POND="$SOURCE_POND" pond push backup
POND="$SOURCE_POND" pond verify --exact backup
```

Record the printed recipe hash. Retrieve both:

```text
recovery/recipes/watertown.commit.v1/<recipe-hash>/README.sh
recovery/README.sh
```

The current hash-addressed object must match the reviewed current recipe.
`recovery/README.sh` may be an older recipe, but its exact bytes must already
exist at
`recovery/recipes/watertown.commit.v1/<hash-of-discoverable-bytes>/README.sh`.
Missing or mismatched immutable copies are rejected and never backfilled.

## 3. Download a complete native backup

Use the reviewed download helper for the storage backend. Preserve object
names and bytes exactly, including:

```text
Delta table data and _delta_log/
_blobs/
_packs/
recovery/
```

Do not extract from a partial listing.

## 4. Extract the recorded commit

Run the authenticated kit into a nonexistent destination:

```bash
python extract.py ./native-backup ./capsule \
  --commit "$SOURCE_COMMIT" \
  --birthplace recovery-rehearsal
```

The extractor accepts only `watertown.commit.v1`, `watertown.tree.v1`,
`watertown.manifest.v1`, `watertown.series.v2`,
`watertown.series-pack.v2`, and `watertown.recipe.v1`. Any other native
object is a hard failure.

## 5. Verify the capsule without Pond

```bash
python ./capsule/capsule.py verify ./capsule
```

Confirm the report identifies `pondcapsule.4`, the recorded source commit,
the expected entry count, and plausible logical and physical totals.

The verifier checks canonical JSON, the `pondcapsule.root.4` root, the exact
object closure, payload BLAKE3 and sizes, Parquet schemas, logical leaves,
series roots, dynamic metadata, and `watertown.recipe.v1` framing.

## 6. Rehearse safe materialization

```bash
python ./capsule/capsule.py materialize ./capsule ./materialized
```

Review `inventory.json`, regular file versions, Parquet table versions,
symlink target descriptions, and inert dynamic recipe files. No recipe is
executed and no live symlink is created.

## 7. Import into a fresh pond

The target path must not exist:

```bash
POND="$TARGET_POND" pond capsule import ./capsule \
  --birthplace recovered-production \
  --experimental
```

The importer creates a private sibling staging pond, suppresses post-commit
dispatch and automatic pushes, rebuilds a `pondcapsule.4` from the staged
pond, compares the logical projection, syncs it, and atomically renames it
onto the target.

## 8. Validate and cut over

While the target remains inert:

```bash
POND="$TARGET_POND" pond status
POND="$TARGET_POND" pond verify
POND="$TARGET_POND" pond capsule recipe inspect backup
```

Review remotes and every restored dynamic recipe. Enable exactly one writer
only after the target has passed application-specific checks.

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
