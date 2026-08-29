# Watertown `watertown.commit.v1` Native-backup Recovery Kit

This reviewed kit validates a downloaded `watertown.commit.v1` native ContentRemote
backup and converts supported content into a
portable `pondcapsule.2` without importing anything and without running Pond.
It is not itself a capsule. Every produced capsule contains
`CAPSULE-README.md`, `CAPSULE-FORMAT.md`, `capsule.py`,
`capsule-requirements.lock`, and `recover.sh` at its root for Pond-free
verification and materialization.

## Bootstrap identity and safety

Obtain both `README.sh` and its expected 64-character recipe hash through
independent channels. The identity is BLAKE3 over the ASCII domain
`pondcapsule.recipe.1\n` followed immediately by the exact `README.sh` bytes.
With a separately trusted `b3sum`:

```sh
EXPECTED=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
ACTUAL=$({ printf 'pondcapsule.recipe.1\n'; cat README.sh; } | b3sum | awk '{print $1}')
test "$ACTUAL" = "$EXPECTED"
```

Substitute the independently obtained expected hash. Do not use a hash stored
beside an untrusted bootstrap as its only authority. `b3sum` is not bundled;
install or transfer a reviewed implementation before an offline emergency.

Then review `README.sh` and extract it only into a new path:

```sh
sh README.sh watertown-recovery-watertown.commit.v1
cd watertown-recovery-watertown.commit.v1
sha256sum -c SHA256SUMS
# macOS:
shasum -a 256 -c SHA256SUMS
```

Review every extracted file. `README.sh` creates files only: it performs no
network access, executes no extracted file, modifies no pond or backup, and
deletes nothing. Keep the source backup read-only.

## Exact environment

The extractor requires Python 3.13 and the exact direct dependency versions in
`requirements.lock`:

```sh
python3.13 -m venv recovery-venv
. recovery-venv/bin/activate
python -m pip install -r requirements.lock
python extract.py --verify-fixtures native-fixtures.json
python capsule_test.py
```

Normal package installation requires network access. For offline recovery,
pre-stage reviewed, platform-compatible wheels for the pinned packages and any
installer-reported transitive dependencies. Record their hashes independently,
transfer them with the kit, and install without an index:

```sh
python -m pip install --no-index --find-links /trusted/wheelhouse \
  -r requirements.lock
python extract.py --verify-fixtures native-fixtures.json
python capsule_test.py
```

## Download the complete native backup

Authenticate separately; no credential belongs in this kit:

```sh
azcopy login
sh download-azcopy.sh \
  https://ACCOUNT.blob.core.windows.net/CONTAINER/PREFIX BACKUP

mc alias set ALIAS ENDPOINT
sh download-mc.sh ALIAS/BUCKET/PREFIX BACKUP
```

The helpers accept no credential options, refuse an existing destination, and
require `_delta_log/`. They never delete a partial download.

## Extract, verify, and materialize

Select a native ref or exact commit hash. `BACKUP`, `CAPSULE`, and
`MATERIALIZED` below are local paths; both output paths must not exist:

```sh
python extract.py BACKUP CAPSULE --ref REF --birthplace LABEL
# Or:
python extract.py BACKUP CAPSULE --commit HASH --birthplace LABEL

python CAPSULE/capsule.py verify CAPSULE
python CAPSULE/capsule.py materialize CAPSULE MATERIALIZED
```

Or use the authenticated kit's wrapper, which performs both operations and
creates its own virtual environment:

```sh
sh recover.sh CAPSULE MATERIALIZED
# Strictly offline:
sh recover.sh CAPSULE MATERIALIZED /trusted/wheelhouse
```

The extractor reads the native Delta table and `_blobs/`, resolves live rows,
verifies the selected native graph, and writes a portable capsule. It does not
import into a pond. The capsule-local tool then independently verifies the
latest ref, canonical manifest/root, topology, object closure, Parquet schemas,
logical leaves, and series roots.

For a capsule downloaded separately from object storage, compare its
`CAPSULE-README.md`, `CAPSULE-FORMAT.md`, `capsule.py`,
`capsule-requirements.lock`, and `recover.sh` byte for byte with this
hash-authenticated kit before execution. The capsule root authenticates the
manifest and payload data, not executable helper files. Prefer running this
kit's trusted `recover.sh` rather than the capsule copy.

Materialization creates type-separated roots for directories, files, tables,
symlink targets, and dynamic recipes. It writes numbered file/Parquet versions
plus inventories. Symlinks and recipes remain inert data; they are never
activated or executed. Dynamic recipes are also decoded into a factory name
and exact configuration bytes for inspection.

The v2 logical-series physical-row and pack mapping is deliberately fail-closed
in this first kit: a `watertown.series.v1` series is diagnosed as unsupported rather
than being mistaken for a v1 physical hash list. This probe must pass before
the v2 pack-aware extractor is used for a migration.

`pondcapsule.2` cannot encode an empty member of a multi-version series.
Extraction fails closed when one is encountered rather than silently changing
series order or metadata. A single empty physical file or table node remains
representable only when its native version metadata is empty. Extraction fails
closed for a metadata-bearing empty singleton rather than discarding metadata.

Maintainers can run the full independent integration test without Pond:

```sh
python integration_test.py
```

An optional current binary adds compatibility coverage only:

```sh
python integration_test.py --pond /path/to/pond
```
