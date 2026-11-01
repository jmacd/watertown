---
title: SQL-derived series
doc_id: factory:sql-derived-series
---

# SQL-derived series

## Overview

`sql-derived-series` creates a read-time SQL view over matching series inputs. Matching schemas are harmonized by column name before SQL execution.

## Example configuration

{{ source path="/docs/examples/sql-derived-series/default.yaml" language="yaml" /}}

The example supplies `source` because the default query needs it; an empty
`patterns` map is only the deserialization default, not a working data source.
For a timestamp-local projection/filter, replace `locality` with
`{type: timestamp-local, event_time: timestamp}`. That variant requires an
event-time column and must satisfy the restrictions below.

## Use and behavior

Install with `pond mknod sql-derived-series /derived/readings --config-path view.yaml`. Inspect it with `pond describe /derived/readings` and query it with `pond cat /derived/readings --format=table --sql 'SELECT * FROM source'`.

Missing columns become null where schemas can be harmonized; incompatible types can fail planning. Transforms run before SQL. `timestamp-local` is an explicit restricted locality contract, not an optimization hint for arbitrary joins or aggregates.

<!-- docs:reference factory:sql-derived-series -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `locality` | Sql Derived Locality | no | {"type": "global"} | Declared query locality. Defaults to conservative global execution. |
| `patterns` | map of Watertown Url | no | {} | Named URL patterns for matching files. Each pattern name becomes a table in the SQL query. Each pattern can match multiple files which are automatically harmonized with UNION ALL BY NAME.  **URL Format**: scheme:///path/pattern - series:///pattern - FileSeries (for sql-derived-series mode) - table:///pattern - FileTable (for sql-derived-table mode)  Example: {"vulink": "series:///data/vulink*.series", "at500": "series:///data/at500*.series"} |
| `query` | string &#124; null | no | not specified | SQL query to execute on the source data. Defaults to "SELECT * FROM source" if not specified |
| `transforms` | array &#124; null | no | not specified | Optional list of table transform factory paths to apply to each pattern's TableProvider. Transforms are applied in order before scope prefixes and SQL execution. Each transform is a path like "/transforms/column-rename" |

### Sql Derived Locality

Explicit locality contract for user-authored SQL.

Choose one of the following:

#### Unrestricted SQL

Unrestricted SQL. The query is visible as global and non-incremental.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `type` | "global" | yes | not specified |  |

#### A single-source immutable projection/filter that preserves event time

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `event_time` | string | yes | not specified | Event-time column retained by the output. |
| `type` | "timestamp-local" | yes | not specified |  |

### Watertown Url

Accepted value: string.
<!-- docs:end-reference -->
