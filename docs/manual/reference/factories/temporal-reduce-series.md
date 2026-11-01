---
title: Direct temporal-reduction series
doc_id: factory:temporal-reduce-series
---

# Direct temporal-reduction series

## Overview

`temporal-reduce-series` exposes one temporal resolution directly as a queryable series instead of creating a directory.

## Example configuration

{{ source path="/docs/examples/temporal-reduce-series/default.yaml" language="yaml" /}}

Set `columns: [temperature]` for a selected-column rollup. Null or omitted
cache settings resolve to the explicit defaults above.

## Use and behavior

Install with `pond mknod temporal-reduce-series /derived/hourly --config-path reduce.yaml`, then inspect and query `/derived/hourly`.

`resolution` must be a positive duration. The same cache, lateness, sealing, schema, and backfill semantics as [temporal reduction](temporal-reduce.md) apply.

<!-- docs:reference factory:temporal-reduce-series -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `aggregations` | array of Aggregation | yes | not specified |  |
| `allowed_lateness` | string &#124; null | no | not specified |  |
| `in_pattern` | Watertown Url | yes | not specified |  |
| `max_live_segments` | integer &#124; null | no | not specified | Constraints: format="uint"; minimum=0. |
| `output_aliases` | object &#124; null | no | not specified |  |
| `resolution` | string | yes | not specified |  |
| `seal_target_bytes` | integer &#124; null | no | not specified | Constraints: format="uint64"; minimum=0. |
| `time_column` | string | yes | not specified |  |
| `transforms` | array &#124; null | no | not specified |  |

### Aggregation

Configuration for a single aggregation operation

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `columns` | array &#124; null | no | null | Columns to apply the aggregation to If not specified, applies to all numeric columns Use ["*"] for count operations |
| `type` | Aggregation Type | yes | not specified | Type of aggregation (avg, min, max, count, sum) |

### Aggregation Type

Aggregation types supported by the temporal reduce factory

Accepted value: "avg" &#124; "min" &#124; "max" &#124; "count" &#124; "sum".

### Watertown Url

Accepted value: string.
<!-- docs:end-reference -->
