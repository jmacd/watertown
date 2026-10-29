---
title: Timeseries transformations
doc_id: reference:timeseries-factories
---

# Timeseries transformations

Use the [synthetic factory](synthetic-timeseries.md) for a deterministic source.
Except for materialization, the factories here expose dynamic queryable views:
install using `pond mknod FACTORY /derived/NAME --config-path FILE`, inspect
using `pond describe /derived/NAME`, and query using `pond cat /derived/NAME
--format=table --sql 'SELECT * FROM source'`. They are not interchangeable
with physical append-only series or runnable collectors.

## Join

`timeseries-join` needs at least two inputs. It FULL OUTER JOINs on the time
column and COALESCEs timestamps, preserving timestamps present on only one
side. Input scopes prefix value columns to avoid collisions. Range endpoints
are RFC 3339 strings; source schema/type compatibility is checked when queried.

```yaml
inputs:
  - pattern: series:///sensors/temperature
    scope: air
  - pattern: series:///sensors/humidity
    scope: weather
```

<!-- docs:reference factory:timeseries-join -->
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

### TimeseriesJoinConfig

Configuration for the timeseries-join factory

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `inputs` | array of TimeseriesInput | yes | not specified | List of input sources with patterns and optional time ranges |
| `time_column` | string | no | "timestamp" | Name of the timestamp column (defaults to "timestamp") |

Unknown fields: **rejected**.

### TimeRange

Time range bounds for filtering

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `begin` | string &#124; null | no | not specified | Optional start time (ISO 8601 format) |
| `end` | string &#124; null | no | not specified | Optional end time (ISO 8601 format) |

Unknown fields: **rejected**.

### TimeseriesInput

Input source with URL pattern and optional time range

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `pattern` | WatertownUrl | yes | not specified | URL pattern for matching time series files  Supported URL schemes: - series:///pattern - Builtin TinyFS FileSeries (Parquet) - series+gzip:///pattern - Compressed FileSeries - csv:///pattern - CSV files (requires format conversion) - csv+gzip:///pattern?delimiter=; - CSV with decompression and options - excelhtml:///pattern - HydroVu HTML exports  Example: series:///data/sensors/*.series |
| `range` | TimeRange &#124; null | no | not specified | Optional time range filter for this specific input |
| `scope` | string &#124; null | no | not specified | Optional scope prefix to add to all column names (e.g., "temperature" -&gt; "BDock.temperature") |
| `transforms` | array &#124; null | no | not specified | Optional list of table transform factory paths to apply to this input's TableProvider. Transforms are applied in order before scope prefixes and SQL execution. Each transform is a path like "/etc/hydro_rename" |

Unknown fields: **rejected**.

### WatertownUrl

Accepted value: string.
<!-- docs:end-reference -->

## Pivot

`timeseries-pivot` selects named columns across matching inputs. The first
wildcard capture becomes each source alias; without a capture, the basename
is used. No matches or duplicate aliases fail. Missing selected columns are
null-padded; output names reflect source aliases.

```yaml
pattern: series:///sensors/*
columns: [temperature]
```

<!-- docs:reference factory:timeseries-pivot -->
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

### TimeseriesPivotConfig

Configuration for timeseries-pivot factory

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `columns` | array of string | yes | not specified | List of column names to pivot across all matched inputs |
| `pattern` | WatertownUrl | yes | not specified | Pattern to match input files (e.g., "series:///combined/*") |
| `time_column` | string | no | "timestamp" | Time column name (default: "timestamp") |
| `transforms` | array &#124; null | no | not specified | Optional list of table transform factory paths to apply to input TableProvider. Transforms are applied in order before SQL execution. Each transform is a path like "/etc/hydro_rename" |

Unknown fields: **rejected**.

### WatertownUrl

Accepted value: string.
<!-- docs:end-reference -->

## Temporal reduction

`temporal-reduce` produces a dynamic directory of resolutions;
`temporal-reduce-series` exposes one resolution directly as a queryable series.
Input timestamps must be usable by the reduction recipe. Aggregation `columns`
defaults to numeric columns; `count` can use `["*"]`. Default result names use
`column.aggregate`; `output_aliases` can rename them.

```yaml
in_pattern: series:///sensors/*
out_pattern: "$0"
time_column: timestamp
resolutions: [1h, 6h]
aggregations:
  - type: avg
    columns: [temperature]
```

For the direct-series variant omit `out_pattern` and replace `resolutions`
with `resolution: 1h`. Resolutions are positive duration strings. Buckets are
aligned time windows; a sample on the next bucket boundary belongs to that
next bucket. Reduction is not interpolation.

Incremental caches track source versions and event-time metadata. Missing
bounds can force full-history invalidation. `allowed_lateness` resolves to
one day when absent; older backfill unseals and recomputes affected segments,
not silently drops it. Changing lateness invalidates merged cache state.
`seal_target_bytes` resolves to 1 MiB and `max_live_segments` to 50; these are
operational defaults, distinct from optional/null deserialization defaults.
Changing them governs subsequent sealing/compaction rather than invalidating
correct aggregates. Schema discovery uses the lexicographically last match;
do not use that shortcut for incompatible evolving schemas.

<!-- docs:reference factory:temporal-reduce -->
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

<!-- docs:reference factory:temporal-reduce-series -->
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

## Pump state

`pump-state-series` classifies well depth into pumping, recovering, and static
episodes using a trailing ceiling. It requires one exact source URL (no
wildcards), timestamp/depth columns, finite ordered depth limits, and positive
whole-minute lookback. Depth/drop values use the source's depth units.

```yaml
source: series:///sensors/well
time_column: timestamp
depth_column: depth
lookback: 60m
disturbance_drop: 1.0
valid_depth_min: 0.0
valid_depth_max: 100.0
```

The recipe consumes ordered event-time samples, maintains boundary/cache
state, and can recompute affected tails. It is an analytical classification,
not pump control or a safety alarm. Inspect the actual output schema before
building downstream queries.

<!-- docs:reference factory:pump-state-series -->
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

### PumpStateConfig

Configuration for a typed adaptive pump-state series.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `depth_column` | string | yes | not specified | Source well-depth column. |
| `disturbance_drop` | number | yes | not specified | Minimum depth drop below the trailing ceiling that starts disturbance. Constraints: format="double". |
| `lookback` | string | yes | not specified | Width of the trailing ceiling. |
| `source` | WatertownUrl | yes | not specified | Exact logical source URL. |
| `time_column` | string | yes | not specified | Source event-time column. |
| `valid_depth_max` | number | yes | not specified | Inclusive valid depth ceiling. Constraints: format="double". |
| `valid_depth_min` | number | yes | not specified | Inclusive valid depth floor. Constraints: format="double". |

Unknown fields: **rejected**.

### WatertownUrl

Accepted value: string.
<!-- docs:end-reference -->

## Materialization

`materialize-series` is instead a pond-writing executable. It creates a
`TablePhysicalSeries` at `target` on first append. Install it under `/etc`,
then run `pond run /etc/materialize -- push` (also the default operation).
`pull` is accepted for scheduling symmetry but does not reverse data flow.

```yaml
source: series:///derived/temperature
target: /stored/temperature
time_column: timestamp
```

Each run selects rows beyond the target's event-time watermark and appends
one new version when needed. This is not snapshot replacement, deduplication
by arbitrary keys, or a promise to repair old records arriving behind the
watermark. Metadata accelerates watermark discovery; legacy/missing bounds
may require a scan. Keep original sources recoverable.

<!-- docs:reference factory:materialize-series -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | no |
| initialize | yes |
| execute | yes |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `ReadWrite`.

### MaterializeSeriesConfig

Configuration for the materialize-series factory.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `source` | WatertownUrl | yes | not specified | URL of the signal to materialize, e.g. series:///derived/p-water-prod.  Anything the provider can turn into a table works, so a derived node, a physical series, or a raw log read through a format scheme are all valid sources. |
| `target` | string | yes | not specified | Pond path of the TablePhysicalSeries to append to.  Created on the first run. |
| `time_column` | string | yes | not specified | Event-time column, present in the source and used both as the watermark and as the series' temporal index. |

Unknown fields: **rejected**.

### WatertownUrl

Accepted value: string.
<!-- docs:end-reference -->

## Updates and evidence

Update configs with the original `mknod` command and `--overwrite`. Dynamic
views resolve current source state on reads; materialization changes stored
state only when run. Changing a materialization recipe does not erase the old
target: use a distinct target for a changed historical interpretation.
These configurations require a pond/effective root; no host-only execution
claim is made. Existing tests 101/102/103/730 are implementation evidence,
not substituted for a complete asserted documentation package. The
[site guide](../../guides/timeseries-site.md) asserts export artifacts and values.
