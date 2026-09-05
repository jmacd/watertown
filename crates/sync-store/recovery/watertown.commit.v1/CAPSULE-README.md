# Watertown portable recovery capsule

This directory contains a verified `pondcapsule.4` snapshot. It is independent
of the Watertown/Pond binary and can be checked with ordinary Python tooling.

## Verify

Use Python 3.13, create an isolated environment, install the locked
dependencies, then run:

```sh
python capsule.py verify .
```

Verification authenticates the manifest root, requires the exact
`pondcapsule.4` schema, checks the declared object closure, hashes every
payload, verifies logical file/table leaves and per-leaf Arrow schemas, and
validates every dynamic recipe as `watertown.recipe.v1`.

## Materialize safely

Choose a new destination that does not exist:

```sh
python capsule.py materialize . ./recovered
```

or:

```sh
sh recover.sh . ./recovered
```

Materialization writes regular files, Parquet table versions, symlink target
descriptions, and inert dynamic recipe files. It does not create live
symlinks, execute recipes, contact remotes, or overwrite an existing
destination.

Review `CAPSULE-FORMAT.md`, `capsule.py`, `parquet_schema.py`, and
`recover.sh` before use.
