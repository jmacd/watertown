---
title: SQL-derived series
doc_id: factory:sql-derived-series
---

# SQL-derived series

## Overview

`sql-derived-series` creates a read-time SQL view over matching series inputs. Matching schemas are harmonized by column name before SQL execution.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

The example supplies `source` because the default query needs it; an empty
`patterns` map is only the deserialization default, not a working data source.
For a timestamp-local projection/filter, replace `locality` with
`{type: timestamp-local, event_time: timestamp}`. That variant requires an
event-time column and must satisfy the restrictions below.

## Use and behavior

Install with `pond mknod sql-derived-series /derived/readings --config-path view.yaml`. Inspect it with `pond describe /derived/readings` and query it with `pond cat /derived/readings --format=table --sql 'SELECT * FROM source'`.

Missing columns become null where schemas can be harmonized; incompatible types can fail planning. Transforms run before SQL. `timestamp-local` is an explicit restricted locality contract, not an optimization hint for arbitrary joins or aggregates.

<!-- docs:reference factory:sql-derived-series -->
<!-- docs:end-reference -->
