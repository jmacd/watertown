---
title: Pump-state series
doc_id: factory:pump-state-series
---

# Pump-state series

## Overview

`pump-state-series` classifies well depth into pumping, recovering, and static episodes using a trailing ceiling.

## Example configuration

{{ source path="/docs/examples/pump-state-series/default.yaml" language="yaml" /}}

There are no default thresholds. These numbers illustrate the shape; choose
depth bounds and disturbance criteria for your actual data.

## Use and behavior

Install with `pond mknod pump-state-series /derived/pump-state --config-path pump.yaml`, then inspect and query the resulting view.

The source URL must be exact, without wildcards. Depth limits must be finite and ordered, and lookback must be positive whole minutes. This is analytical classification, not pump control or a safety alarm.

<!-- docs:reference factory:pump-state-series -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `depth_column` | string | yes | not specified | Source well-depth column. |
| `disturbance_drop` | number | yes | not specified | Minimum depth drop below the trailing ceiling that starts disturbance. Constraints: format="double". |
| `lookback` | string | yes | not specified | Width of the trailing ceiling. |
| `source` | Watertown Url | yes | not specified | Exact logical source URL. |
| `time_column` | string | yes | not specified | Source event-time column. |
| `valid_depth_max` | number | yes | not specified | Inclusive valid depth ceiling. Constraints: format="double". |
| `valid_depth_min` | number | yes | not specified | Inclusive valid depth floor. Constraints: format="double". |

### Watertown Url

Accepted value: string.
<!-- docs:end-reference -->
