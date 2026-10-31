---
title: Timeseries pivot
doc_id: factory:timeseries-pivot
---

# Timeseries pivot

## Overview

`timeseries-pivot` selects named columns across matching series inputs and emits one scoped output column per source.

## Example configuration

```yaml
pattern: series:///sensors/*
columns: [temperature]
```

## Use and behavior

Install with `pond mknod timeseries-pivot /derived/temperatures --config-path pivot.yaml`, then inspect and query the resulting view.

The first wildcard capture is the source alias; without a capture, the basename is used. No matches and duplicate aliases fail. Missing selected columns are null-padded.

<!-- docs:reference factory:timeseries-pivot -->
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

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
