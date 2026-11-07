---
title: Temporal reduction directory
doc_id: factory:temporal-reduce
---

# Temporal reduction directory

## Overview

`temporal-reduce` creates a dynamic directory containing one reduced series per configured resolution and output pattern.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

Use `columns: [temperature]` to restrict the aggregation and, for example,
`output_aliases: {temperature.avg: temperature}` to rename its output.
Omitted or null cache settings resolve to the explicit defaults shown above.

## Use and behavior

Install with `pond mknod temporal-reduce /derived/reduced --config-path reduce.yaml`, then list and query its children. Buckets are aligned time windows; a sample on the next boundary belongs to that next bucket. Reduction is aggregation, not interpolation.

Incremental caches use source versions and event-time metadata. Missing bounds can force full-history invalidation. Late backfill unseals and recomputes affected segments rather than silently dropping data.

### Grouping dimensions

Set `group_by: [site_id, page_uri]` to produce one row per time bucket and
distinct combination of those source columns. Keys retain their names and
types, including null values; null keys group together as in SQL. With an
empty or omitted `group_by`, existing time-only reduction is unchanged.

Grouping columns are exact names, not patterns or SQL expressions. They are
carried through shared cached partials, coarser resolutions, and late-data
recomputation. Changing the grouping selects a separate cache identity.
Default aggregation-column discovery excludes the grouping keys.

The input table schema must contain the keys. Missing keys are errors, not an
implicit null group; an unmatched source cannot supply their types. Keys must
be unique and cannot conflict with the timestamp or aggregate outputs.
`time_bucket` and names beginning `__p_` are reserved internal names.

For daily event counts, combine `group_by` with `resolutions: [1d]`,
`aggregations: [{type: count, columns: ["*"]}]`, and
`output_aliases: {timestamp.count: views}`. Group cardinality determines the
number of aggregate rows; grouping does not impose a cardinality limit.

<!-- docs:reference factory:temporal-reduce -->
<!-- docs:end-reference -->
