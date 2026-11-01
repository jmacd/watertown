---
title: SQL-derived table
doc_id: factory:sql-derived-table
---

# SQL-derived table

## Overview

`sql-derived-table` creates a read-time SQL view over single-file table inputs. It changes no source bytes and is queried rather than executed with `pond run`.

## Example configuration

{{ source path="/docs/examples/sql-derived-table/default.yaml" language="yaml" /}}

All fields have omission behavior, but an empty `patterns` map does not make
the default query useful: it needs an input named `source`. Set `query` to your
own SQL to select, filter, or calculate columns. For the restricted local
variant, replace `locality` with `{type: timestamp-local, event_time: timestamp}`;
the event-time field is required for that variant.

## Use and behavior

Install with `pond mknod sql-derived-table /derived/readings --config-path view.yaml`. Inspect it with `pond describe /derived/readings` and query it with `pond cat /derived/readings --format=table --sql 'SELECT * FROM source'`.

Table inputs reject multi-file matches. Output schema is the SQL query schema. `global` is the conservative locality default; `timestamp-local` is limited to a single-source immutable projection/filter that preserves its configured event-time column.

<!-- docs:reference factory:sql-derived-table -->
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
