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

Timestamp-local queries may use immutable projections, filters, and `ORDER BY`
without a row limit. Sorting preserves the selected rows and their values;
`LIMIT`, `OFFSET`, and top-k sorts can change which rows belong to the result and
remain unsupported. Aggregates, windows, subquery expressions, and non-immutable
expressions also remain unsupported in this mode.

For aggregation within complete groups of one exact timestamp, opt in separately:

```yaml
locality:
  type: timestamp-grouped
  event_time: timestamp
```

This mode requires exactly one `GROUP BY` key: the configured event-time column.
Its name and value must survive every projection. The grouping key must already
be a timestamp or an `Int64` epoch-microsecond value; string timestamps may be
parsed into nanosecond timestamps before grouping. Time bucketing, precision
reduction, renaming another value as event time, and parsing time only after
grouping are rejected.

Aggregates and their arguments must be immutable. Filters and non-limiting
sorts are permitted, but limits, offsets, windows, grouping sets, joins, and
subquery expressions remain unsupported. Ordinary `timestamp-local` mode still
rejects aggregation.

Bounded execution must retain every input row contributing to an included
timestamp group, even across source versions. This contract describes query
dependencies, not group finality: it does not change `materialize-series`'
append-only handling of late data.

<!-- docs:reference factory:sql-derived-series -->
<!-- docs:end-reference -->
