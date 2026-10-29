---
title: SQL and table transforms
doc_id: reference:sql
---

# SQL and table transforms

These are read-time views/transforms, not `pond run` executables. They require
queryable inputs in the effective pond/import root. Installing a node validates
configuration shape; opening/querying it resolves inputs and plans SQL, which
can still fail. No source bytes are changed by a SELECT.

## SQL-derived table and series

Install `sql-derived-table` for single-file table inputs or
`sql-derived-series` for multi-file/version series inputs:

```yaml
patterns:
  readings: series:///sensors/*
query: SELECT * FROM readings
locality:
  type: global
```

Install with `pond mknod sql-derived-series /derived/readings --config-path
view.yaml`; inspect with `pond describe /derived/readings` and query with
`pond cat /derived/readings --format=table --sql 'SELECT * FROM source'`.
Tables reject multi-file matches; series combine matches by column name.
Missing columns become null where schemas can be harmonized; incompatible
types can fail planning. Output schema is the query schema, not a fixed schema.

`global` is the conservative default: unrestricted SQL is not automatically
incremental. `timestamp-local` requires `event_time` and a single-source,
immutable projection/filter preserving event time. It is an explicit locality
contract, not an optimization hint for arbitrary joins/aggregates.
Transforms run in configured order before SQL. Skipped internal fields
(`pattern_transforms`, `scope_prefixes`, `provider_wrapper`) are not YAML keys.

<!-- docs:reference factory:sql-derived-table -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | yes |
| initialize | no |
| execute | no |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `not registered`.

### SqlDerivedConfig

Configuration for SQL-derived file generation

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `locality` | SqlDerivedLocality | no | {"type": "global"} | Declared query locality. Defaults to conservative global execution. |
| `patterns` | map of WatertownUrl | no | {} | Named URL patterns for matching files. Each pattern name becomes a table in the SQL query. Each pattern can match multiple files which are automatically harmonized with UNION ALL BY NAME.  **URL Format**: scheme:///path/pattern - series:///pattern - FileSeries (for sql-derived-series mode) - table:///pattern - FileTable (for sql-derived-table mode)  Example: {"vulink": "series:///data/vulink*.series", "at500": "series:///data/at500*.series"} |
| `query` | string &#124; null | no | not specified | SQL query to execute on the source data. Defaults to "SELECT * FROM source" if not specified |
| `transforms` | array &#124; null | no | not specified | Optional list of table transform factory paths to apply to each pattern's TableProvider. Transforms are applied in order before scope prefixes and SQL execution. Each transform is a path like "/transforms/column-rename" |

Unknown fields: **rejected**.

### SqlDerivedLocality

Explicit locality contract for user-authored SQL.

### SqlDerivedLocality variant 1

Unrestricted SQL. The query is visible as global and non-incremental.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `type` | "global" | yes | not specified |  |

Unknown fields: not rejected by this shape; see runtime notes.

### SqlDerivedLocality variant 2

A single-source immutable projection/filter that preserves event time.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `event_time` | string | yes | not specified | Event-time column retained by the output. |
| `type` | "timestamp-local" | yes | not specified |  |

Unknown fields: not rejected by this shape; see runtime notes.

### WatertownUrl

Accepted value: string.
<!-- docs:end-reference -->

<!-- docs:reference factory:sql-derived-series -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | yes |
| initialize | no |
| execute | no |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `not registered`.

### SqlDerivedConfig

Configuration for SQL-derived file generation

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `locality` | SqlDerivedLocality | no | {"type": "global"} | Declared query locality. Defaults to conservative global execution. |
| `patterns` | map of WatertownUrl | no | {} | Named URL patterns for matching files. Each pattern name becomes a table in the SQL query. Each pattern can match multiple files which are automatically harmonized with UNION ALL BY NAME.  **URL Format**: scheme:///path/pattern - series:///pattern - FileSeries (for sql-derived-series mode) - table:///pattern - FileTable (for sql-derived-table mode)  Example: {"vulink": "series:///data/vulink*.series", "at500": "series:///data/at500*.series"} |
| `query` | string &#124; null | no | not specified | SQL query to execute on the source data. Defaults to "SELECT * FROM source" if not specified |
| `transforms` | array &#124; null | no | not specified | Optional list of table transform factory paths to apply to each pattern's TableProvider. Transforms are applied in order before scope prefixes and SQL execution. Each transform is a path like "/transforms/column-rename" |

Unknown fields: **rejected**.

### SqlDerivedLocality

Explicit locality contract for user-authored SQL.

### SqlDerivedLocality variant 1

Unrestricted SQL. The query is visible as global and non-incremental.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `type` | "global" | yes | not specified |  |

Unknown fields: not rejected by this shape; see runtime notes.

### SqlDerivedLocality variant 2

A single-source immutable projection/filter that preserves event time.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `event_time` | string | yes | not specified | Event-time column retained by the output. |
| `type` | "timestamp-local" | yes | not specified |  |

Unknown fields: not rejected by this shape; see runtime notes.

### WatertownUrl

Accepted value: string.
<!-- docs:end-reference -->

## Column rename

`column-rename` registers a table transform, not a standalone queryable table.
Install it with `mknod`, then reference its pond path in a consuming factory's
`transforms` list:

```yaml
rules:
  - type: direct
    from: temperature
    to: temperature_c
  - type: pattern
    pattern: "^sensor_(.*)$"
    replacement: "$1"
```

Rules apply first-match-wins to source column names. A direct rule can request
a `cast`; invalid types/regexes or conflicting output schemas fail. Transforms
change the view's names/types, not stored source data. Inspect the consuming
node's schema to see the result.

<!-- docs:reference factory:column-rename -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | no |
| initialize | no |
| execute | no |
| table transform | yes |
| raw validation | no |

Automatic post-commit access: `not registered`.

### ColumnRenameConfig

Configuration for column rename transformation

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `rules` | array of RenameRule | yes | not specified | List of rename rules applied in order |

Unknown fields: **rejected**.

### RenameRule

A single column rename rule

### RenameRule variant 1

Direct column name mapping  Example: yaml - type: direct   from: "DateTime"   to: "timestamp"   cast: "timestamp"

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `cast` | string &#124; null | no | not specified |  |
| `from` | string | yes | not specified |  |
| `to` | string | yes | not specified |  |
| `type` | "direct" | yes | not specified |  |

Unknown fields: **rejected**.

### RenameRule variant 2

Regex pattern-based renaming  Example: yaml - type: pattern   pattern: "^(.+) \\((.+)\\)$"   replacement: "$1.$2"  This transforms "Temperature (C)" -&gt; "Temperature.C"

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `pattern` | string | yes | not specified |  |
| `replacement` | string | yes | not specified |  |
| `type` | "pattern" | yes | not specified |  |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Dynamic directory

`dynamic-dir` produces a `DirectoryDynamic` whose children use independently
validated factory configurations. For example:

```yaml
entries:
  - name: readings
    factory: sql-derived-series
    config:
      patterns:
        source: series:///sensors/*
```

List the installed directory, then describe/query a child. Child identities
derive from parent identity, entry name, factory, and config; changing a child
config can change its identity. Nested `config` is intentionally open JSON:
the selected child's validator, not the directory schema, defines its fields.
Do not treat directory enumeration as executing every child factory.

<!-- docs:reference factory:dynamic-dir -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | yes |
| file | no |
| initialize | no |
| execute | no |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `not registered`.

### DynamicDirConfig

Configuration for the dynamic directory

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `entries` | array of DynamicDirEntry | yes | not specified | List of directory entries to create |

Unknown fields: **rejected**.

### DynamicDirEntry

Configuration for a single directory entry

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `config` | any JSON value | yes | not specified | Configuration for the specific factory (as raw Value for flexibility) |
| `factory` | string | yes | not specified | Factory type to use for creating this entry |
| `name` | string | yes | not specified | Name of the entry in the directory |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Updates and evidence

Repeat the original `mknod ... --config-path ... --overwrite` to update a node;
re-query to observe the new recipe. These views do not have an ingestion cursor.
Pond installation is the documented path; host availability is not inferred
from registry membership. Typed schema contracts are checked during reference
generation; the examples here are configuration fragments, not claimed offline
end-to-end workflows. Start with the asserted [timeseries guide](../../guides/timeseries.md).
