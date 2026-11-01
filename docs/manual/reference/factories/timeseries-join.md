---
title: Timeseries join
doc_id: factory:timeseries-join
---

# Timeseries join

## Overview

`timeseries-join` FULL OUTER JOINs at least two series inputs on their time column and coalesces timestamps, retaining timestamps found on only one side.

## Example configuration

{{ source path="/docs/examples/timeseries-join/default.yaml" language="yaml" /}}

When selecting a range, its complete shape is
`{begin: "2024-01-01T00:00:00Z", end: "2024-01-02T00:00:00Z"}`.
Each bound defaults to null (unbounded); those dates are examples, not defaults.

## Use and behavior

Install with `pond mknod timeseries-join /derived/weather --config-path join.yaml`, then inspect and query `/derived/weather`. Input scopes prefix value columns to avoid collisions. Optional range endpoints are RFC 3339 strings.

The source schemas and timestamp types are checked when the view is queried. This is a dynamic view, not a physical append-only series.

<!-- docs:reference factory:timeseries-join -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `inputs` | array of Timeseries Input | yes | not specified | List of input sources with patterns and optional time ranges |
| `time_column` | string | no | "timestamp" | Name of the timestamp column (defaults to "timestamp") |

### Time Range

Time range bounds for filtering

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `begin` | string &#124; null | no | not specified | Optional start time (ISO 8601 format) |
| `end` | string &#124; null | no | not specified | Optional end time (ISO 8601 format) |

### Timeseries Input

Input source with URL pattern and optional time range

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `pattern` | Watertown Url | yes | not specified | URL pattern for matching time series files  Supported URL schemes: - series:///pattern - Builtin TinyFS FileSeries (Parquet) - series+gzip:///pattern - Compressed FileSeries - csv:///pattern - CSV files (requires format conversion) - csv+gzip:///pattern?delimiter=; - CSV with decompression and options - excelhtml:///pattern - HydroVu HTML exports  Example: series:///data/sensors/*.series |
| `range` | Time Range &#124; null | no | not specified | Optional time range filter for this specific input |
| `scope` | string &#124; null | no | not specified | Optional scope prefix to add to all column names (e.g., "temperature" -&gt; "BDock.temperature") |
| `transforms` | array &#124; null | no | not specified | Optional list of table transform factory paths to apply to this input's TableProvider. Transforms are applied in order before scope prefixes and SQL execution. Each transform is a path like "/etc/hydro_rename" |

### Watertown Url

Accepted value: string.
<!-- docs:end-reference -->
