---
title: Timeseries pivot
doc_id: factory:timeseries-pivot
---

# Timeseries pivot

## Overview

`timeseries-pivot` selects named columns across matching series inputs and emits one scoped output column per source.

## Example configuration

{{ source path="/docs/examples/timeseries-pivot/default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod timeseries-pivot /derived/temperatures --config-path pivot.yaml`, then inspect and query the resulting view.

The first wildcard capture is the source alias; without a capture, the basename is used. No matches and duplicate aliases fail. Missing selected columns are null-padded.

<!-- docs:reference factory:timeseries-pivot -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `columns` | array of string | yes | not specified | List of column names to pivot across all matched inputs |
| `pattern` | Watertown Url | yes | not specified | Pattern to match input files (e.g., "series:///combined/*") |
| `time_column` | string | no | "timestamp" | Time column name (default: "timestamp") |
| `transforms` | array &#124; null | no | not specified | Optional list of table transform factory paths to apply to input TableProvider. Transforms are applied in order before SQL execution. Each transform is a path like "/etc/hydro_rename" |

### Watertown Url

Accepted value: string.
<!-- docs:end-reference -->
