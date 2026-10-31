---
title: SQL-derived series
doc_id: factory:sql-derived-series
---

# SQL-derived series

## Overview

`sql-derived-series` creates a read-time SQL view over matching series inputs. Matching schemas are harmonized by column name before SQL execution.

## Example configuration

```yaml
patterns:
  readings: series:///sensors/*
query: SELECT * FROM readings
locality:
  type: global
```

## Use and behavior

Install with `pond mknod sql-derived-series /derived/readings --config-path view.yaml`. Inspect it with `pond describe /derived/readings` and query it with `pond cat /derived/readings --format=table --sql 'SELECT * FROM source'`.

Missing columns become null where schemas can be harmonized; incompatible types can fail planning. Transforms run before SQL. `timestamp-local` is an explicit restricted locality contract, not an optimization hint for arbitrary joins or aggregates.

<!-- docs:reference factory:sql-derived-series -->
## Full configuration

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

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
