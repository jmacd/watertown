# Watertown Pond-free Recovery Capsule

This directory is a produced `pondcapsule.1`, not the native-backup recovery
recipe kit that created it. Keep the whole directory together and record the
capsule root from `recovery/refs/latest` through an independent channel.

`capsule.py` never imports or invokes Pond. It uses Python's standard library
plus the exact direct dependencies in `capsule-requirements.lock`.

## Authenticate the recovery tool

The capsule root authenticates the manifest and payload data, not this
executable helper. Before running a capsule copy downloaded from object
storage, obtain the `dp.commit.3` recovery kit and its `README.sh` hash through
the independent process in the kit README. Then compare the reviewed,
hash-authenticated kit files byte for byte:

```sh
cmp /trusted/recovery-kit/capsule.py ./capsule.py
cmp /trusted/recovery-kit/capsule-requirements.lock \
  ./capsule-requirements.lock
```

Run the trusted kit copy directly if either comparison fails. A checksum
downloaded from the same untrusted storage is not an independent authority.

## Environment and verification

Use Python 3.13 on a compatible machine:

```sh
python3.13 -m venv capsule-venv
. capsule-venv/bin/activate
python -m pip install -r capsule-requirements.lock
python capsule.py verify .
```

Package installation normally needs network access. For offline recovery,
obtain reviewed, platform-compatible wheels for `blake3`, `pyarrow`, and any
installer-reported transitive dependencies in advance, transfer them with
their independently recorded hashes, then use:

```sh
python -m pip install --no-index --find-links /trusted/wheelhouse \
  -r capsule-requirements.lock
python capsule.py verify .
```

## Safe materialization

The destination must not exist:

```sh
python capsule.py materialize . ../recovered-capsule
```

The tool verifies the complete capsule before writing. It separates
directories, files, tables, symlink targets, and dynamic recipes into distinct
type roots. File and table leaves become numbered versions. Symlink targets
and recipes are copied as inert bytes: no symlink is created and no recipe is
executed. Materialization is staged beside the destination and becomes visible
only after it is complete. `README.txt` and `inventory.json` explain every
mapping and preserve the metadata present in the capsule.

`pondcapsule.1` cannot encode an empty member of a multi-version series.
Extraction therefore fails closed instead of dropping such a member. A single
empty physical file or table node remains representable as an empty stream or
a Parquet schema carrier. Capsule v1 does not retain leaf metadata for those
empty singleton nodes.
