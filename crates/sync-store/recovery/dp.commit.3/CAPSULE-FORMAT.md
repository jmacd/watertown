# Portable capsule format: `pondcapsule.1`

This document describes the files needed to inspect a capsule without Pond or
the Watertown source tree. Treat the capsule as read-only during recovery.

## Layout

```text
CAPSULE-README.md             human recovery runbook
CAPSULE-FORMAT.md             this format reference
capsule.py                    independent verifier and materializer
capsule-requirements.lock     exact direct Python dependencies
recover.sh                    one-command recovery wrapper
recovery/
  refs/latest                 one lowercase capsule-root hash plus newline
  manifests/<root>.json       canonical UTF-8 JSON manifest
  objects/blake3=<hash>       immutable payload bytes
```

The capsule root is BLAKE3 over the ASCII domain `pondcapsule.root.1\n`
followed by the exact manifest bytes. The manifest names the source pond,
source commit, every logical path, and the complete payload closure. Each
payload descriptor records the BLAKE3 hash and exact byte size.

The root authenticates the manifest and payload data. Recovery-aid files at the
capsule root are authenticated by comparison with the independently
hash-authenticated `dp.commit.3` recovery kit.

## Payloads

- File payloads are exact byte streams.
- Table payloads are standard Parquet.
- Symlink targets are raw bytes and are never activated.
- Dynamic recipes use the source `dp.recipe.1` framing. Materialization emits
  the raw recipe plus `factory.json` and exact `config.bin`; it never
  executes the recipe.

Physical objects may not align one-for-one with logical versions. The manifest
therefore records ordered logical leaves and roots independently of object
boundaries. `capsule.py verify` checks both physical bytes and logical content.
Empty physical versions with source metadata are not representable in
`pondcapsule.1`; extraction fails rather than silently dropping that metadata.

## Materialized layout

Materialization writes separate `directories/`, `files/`, `tables/`,
`symlinks/`, and `dynamic-recipes/` roots. Every logical path becomes one
bounded ASCII directory name containing a basename hint and a SHA-256 digest of
the exact full UTF-8 path. This prevents collisions and excessive path growth
on common filesystems. `inventory.json` is the authoritative mapping from
original logical paths to output files.

File versions end in `.bin`; table versions are ordinary `.parquet` files.
Symlink targets and dynamic configurations remain inert files. The destination
must not already exist and is promoted only after complete verification and
materialization.

## Trust and snapshot selection

Content hashes establish integrity, not freshness. Preserve the expected
capsule root or native commit hash through an independent channel such as an
incident record, offline inventory, signed message, or separate account.
During recovery, compare that trusted value with `recovery/refs/latest` before
accepting the snapshot.
