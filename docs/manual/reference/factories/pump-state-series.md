---
title: Pump-state series
doc_id: factory:pump-state-series
---

# Pump-state series

## Overview

`pump-state-series` classifies well depth into pumping, recovering, and static episodes using a trailing ceiling.

## Example configuration

```yaml
source: series:///sensors/well
time_column: timestamp
depth_column: depth
lookback: 60m
disturbance_drop: 1.0
valid_depth_min: 0.0
valid_depth_max: 100.0
```

## Use and behavior

Install with `pond mknod pump-state-series /derived/pump-state --config-path pump.yaml`, then inspect and query the resulting view.

The source URL must be exact, without wildcards. Depth limits must be finite and ordered, and lookback must be positive whole minutes. This is analytical classification, not pump control or a safety alarm.

<!-- docs:reference factory:pump-state-series -->
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

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
