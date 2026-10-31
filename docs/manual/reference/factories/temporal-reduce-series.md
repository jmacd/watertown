---
title: Direct temporal-reduction series
doc_id: factory:temporal-reduce-series
---

# Direct temporal-reduction series

## Overview

`temporal-reduce-series` exposes one temporal resolution directly as a queryable series instead of creating a directory.

## Example configuration

```yaml
in_pattern: series:///sensors/*
time_column: timestamp
resolution: 1h
aggregations:
  - type: avg
    columns: [temperature]
```

## Use and behavior

Install with `pond mknod temporal-reduce-series /derived/hourly --config-path reduce.yaml`, then inspect and query `/derived/hourly`.

`resolution` must be a positive duration. The same cache, lateness, sealing, schema, and backfill semantics as [temporal reduction](temporal-reduce.md) apply.

<!-- docs:reference factory:temporal-reduce-series -->
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

### TemporalReduceSeriesConfig

Configuration for a direct single-resolution temporal-reduce series.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `aggregations` | array of AggregationConfig | yes | not specified |  |
| `allowed_lateness` | string &#124; null | no | not specified |  |
| `in_pattern` | WatertownUrl | yes | not specified |  |
| `max_live_segments` | integer &#124; null | no | not specified | Constraints: format="uint"; minimum=0. |
| `output_aliases` | object &#124; null | no | not specified |  |
| `resolution` | string | yes | not specified |  |
| `seal_target_bytes` | integer &#124; null | no | not specified | Constraints: format="uint64"; minimum=0. |
| `time_column` | string | yes | not specified |  |
| `transforms` | array &#124; null | no | not specified |  |

Unknown fields: **rejected**.

### AggregationConfig

Configuration for a single aggregation operation

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `columns` | array &#124; null | no | null | Columns to apply the aggregation to If not specified, applies to all numeric columns Use ["*"] for count operations |
| `type` | AggregationType | yes | not specified | Type of aggregation (avg, min, max, count, sum) |

Unknown fields: **rejected**.

### AggregationType

Aggregation types supported by the temporal reduce factory

Accepted value: "avg" &#124; "min" &#124; "max" &#124; "count" &#124; "sum".

### WatertownUrl

Accepted value: string.
<!-- docs:end-reference -->

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
