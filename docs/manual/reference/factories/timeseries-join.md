---
title: Timeseries join
doc_id: factory:timeseries-join
---

# Timeseries join

## Overview

`timeseries-join` FULL OUTER JOINs at least two series inputs on their time column and coalesces timestamps, retaining timestamps found on only one side.

## Example configuration

```yaml
inputs:
  - pattern: series:///sensors/temperature
    scope: air
  - pattern: series:///sensors/humidity
    scope: weather
```

## Use and behavior

Install with `pond mknod timeseries-join /derived/weather --config-path join.yaml`, then inspect and query `/derived/weather`. Input scopes prefix value columns to avoid collisions. Optional range endpoints are RFC 3339 strings.

The source schemas and timestamp types are checked when the view is queried. This is a dynamic view, not a physical append-only series.

<!-- docs:reference factory:timeseries-join -->
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

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
