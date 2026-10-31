---
title: Temporal reduction directory
doc_id: factory:temporal-reduce
---

# Temporal reduction directory

## Overview

`temporal-reduce` creates a dynamic directory containing one reduced series per configured resolution and output pattern.

## Example configuration

```yaml
in_pattern: series:///sensors/*
out_pattern: "$0"
time_column: timestamp
resolutions: [1h, 6h]
aggregations:
  - type: avg
    columns: [temperature]
```

## Use and behavior

Install with `pond mknod temporal-reduce /derived/reduced --config-path reduce.yaml`, then list and query its children. Buckets are aligned time windows; a sample on the next boundary belongs to that next bucket. Reduction is aggregation, not interpolation.

Incremental caches use source versions and event-time metadata. Missing bounds can force full-history invalidation. Late backfill unseals and recomputes affected segments rather than silently dropping data.

<!-- docs:reference factory:temporal-reduce -->
## Full configuration

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

### TemporalReduceConfig

Configuration for the temporal-reduce factory

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `aggregations` | array of AggregationConfig | yes | not specified | Aggregation operations to perform |
| `allowed_lateness` | string &#124; null | no | not specified | Maximum allowed lateness for the segment partial-aggregate cache: how far behind the newest bucket a late/backfilled sample may still land before its output bucket is treated as sealed. Buckets older than newest_bucket - allowed_lateness are frozen into immutable segment files and never recomputed, which bounds the rollup's working set to this window instead of all history. Parsed with humantime::parse_duration; defaults to one day. Data arriving older than the sealed watermark is not an error: the segments covering it are unsealed and recomputed from source. Changing this value invalidates the merged (segment) cache, which is rebuilt from the cached source versions. |
| `in_pattern` | WatertownUrl | yes | not specified | Input URL pattern to match source files (supports glob patterns) |
| `max_live_segments` | integer &#124; null | no | not specified | Maximum number of segment files to leave live per resolution before compaction merges them regardless of size class. Defaults to [crate::partial_aggregate_cache::MAX_LIVE_SEGMENTS]. Like seal_target_bytes, changing it invalidates nothing. Constraints: format="uint"; minimum=0. |
| `out_pattern` | string | yes | not specified | Output basename pattern using captured groups (e.g., "$0", "$1") - no URL scheme |
| `output_aliases` | object &#124; null | no | not specified | Optional final output aliases keyed by the default &lt;column&gt;.&lt;aggregate&gt; name. |
| `resolutions` | array of string | yes | not specified | List of temporal resolutions (e.g., "1h", "6h", "1d") Parsed using humantime::parse_duration |
| `seal_target_bytes` | integer &#124; null | no | not specified | Minimum accumulated size before frozen buckets are sealed into an immutable segment file. Accepts a byte count; defaults to [crate::partial_aggregate_cache::SEAL_TARGET_BYTES].  Unlike allowed_lateness, changing this does NOT invalidate the cache: a segment already sealed is correct at any target, so the new value simply governs the next seal. Exposed mainly so the tradeoff can be measured against real data -- larger means fewer files but a heavier hot-window recompute each build. Constraints: format="uint64"; minimum=0. |
| `time_column` | string | yes | not specified | Name of the timestamp column |
| `transforms` | array &#124; null | no | not specified | Optional list of table transform factory paths to apply to input TableProvider. Transforms are applied in order before SQL execution. Each transform is a path like "/etc/hydro_rename" |

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
