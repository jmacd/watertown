# Watertown Pond-free Disaster Recovery

**You do not need a Pond binary or the Watertown source tree to recover this
capsule.** Keep the capsule read-only and work on a copy. Never delete or
overwrite the source backup during recovery.

You need Python 3.13, enough free space for the capsule plus recovered data,
and the Python packages listed in `capsule-requirements.lock`. Read
`CAPSULE-FORMAT.md` for the portable layout and output mapping.

## 1. Confirm that this is the intended snapshot

`recovery/refs/latest` contains the capsule root. Compare it with a value kept
through an independent channel:

```sh
cat recovery/refs/latest
```

Hashes prove integrity, not freshness. If no independently recorded capsule
root or native commit exists, record that limitation in the incident log
before proceeding.

## 2. Authenticate the recovery aids

The capsule root authenticates the manifest and payload data, but not the
scripts beside this README. Obtain the `watertown.commit.v1` recovery kit and its
`README.sh` hash through the independent process described in the kit README.
After authenticating and extracting that kit, compare all capsule aids:

```sh
for name in CAPSULE-README.md CAPSULE-FORMAT.md capsule.py parquet_schema.py \
  capsule-requirements.lock recover.sh; do
  cmp "/trusted/recovery-kit/$name" "./$name" || exit 1
done
```

Use the trusted kit copies directly. A checksum stored only beside an untrusted
capsule does not establish authenticity.

## 3. Recover

The trusted wrapper creates a virtual environment, installs exact direct
dependency versions, deeply verifies the capsule, and writes a new
materialized directory:

```sh
sh /trusted/recovery-kit/recover.sh \
  /path/to/CAPSULE /path/to/NEW-RECOVERED-DIRECTORY
```

This online form contacts the configured Python package index. For an offline
or controlled recovery, prepare platform-compatible wheels for `blake3`,
`pyarrow`, and their transitive dependencies in advance, authenticate them
through an independent channel, and pass the wheelhouse as the third argument:

```sh
sh /trusted/recovery-kit/recover.sh \
  /path/to/CAPSULE /path/to/NEW-RECOVERED-DIRECTORY \
  /trusted/wheelhouse
```

Set `PYTHON` if Python 3.13 has a different command name. Set `RECOVERY_VENV`
to choose where the reusable virtual environment is created.

## 4. Understand the result

Start with `NEW-RECOVERED-DIRECTORY/README.txt`, then consult
`inventory.json`, which maps every original logical path to its recovered
files and metadata.

- `files/` contains numbered exact byte-stream versions.
- `tables/` contains numbered standard Parquet versions.
- `symlinks/` contains inert target bytes; no symlink is created.
- `dynamic-recipes/` contains inert `recipe.bin`, readable `factory.json`, and
  exact `config.bin`; no recipe is executed.
- `directories/` records directory entries that have no payload.

Each output path directory includes a readable basename hint and a digest of
the complete logical path. This deliberately avoids collisions and excessive
path growth on common filesystems; use `inventory.json`, not filename guessing,
as the authoritative mapping.

Schema-evolved table series materialize one Parquet file per logical leaf; the
leaf's schema is recorded in `inventory.json` metadata and verified before
writing. Native pack advertisements retain no per-leaf write timestamp, so
their materialized leaf metadata records the authenticated aggregate native
series timestamp while preserving each leaf's count, bounds, attributes, and
schema exactly.

The destination must not already exist. Verification completes before any
result is promoted into place.

## Manual commands

If the wrapper cannot be used, create a Python 3.13 virtual environment,
install `capsule-requirements.lock`, then run:

```sh
python /trusted/recovery-kit/capsule.py verify /path/to/CAPSULE
python /trusted/recovery-kit/capsule.py materialize \
  /path/to/CAPSULE /path/to/NEW-RECOVERED-DIRECTORY
```

`pondcapsule.4` cannot encode an empty member of a multi-version series.
Extraction fails rather than silently dropping one. A single empty physical
file or table is representable only when its native version metadata is empty;
otherwise extraction fails rather than silently discarding that metadata.
