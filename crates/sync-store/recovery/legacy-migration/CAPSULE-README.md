# Inspecting `pondcapsule.legacy.1`

Keep the capsule read-only. Verify it without Pond:

```sh
python3 -m venv capsule-venv
. capsule-venv/bin/activate
python -m pip install -r capsule-requirements.lock
python capsule.py verify .
```

Materialization copies raw objects into inert type-separated directories. It
does not import a pond, execute dynamic recipes, create symlinks, or analyze
Parquet:

```sh
python capsule.py materialize . MATERIALIZED
```

Use a current Watertown binary's capsule importer to perform semantic Parquet
replay into a fresh pond. The target importer verifies the raw inventory and
native leaf mapping before writing, analyzes each table version separately,
records a deterministic replay report, suppresses dispatch, validates staged
target identities/metadata, and promotes atomically.
