---
title: Rate-limit budget
doc_id: factory:rate-limit
---

# Rate-limit budget

## Overview

`rate-limit` declares a consumption budget that other operations can explicitly consume.

## Example configuration

```yaml
unit: MiB/day
limit: 10
burst: 1
```

## Use and behavior

Install with `pond mknod rate-limit /etc/upload-budget --config-path budget.yaml`. Inspect the node before attaching a consumer.

The unit carries scale and period; `limit` is the numeric amount per period. `burst` is capacity in the same scale. Declaring a node does not globally throttle arbitrary I/O: consumers must explicitly use the budget.

<!-- docs:reference factory:rate-limit -->
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

### RateLimitConfig

On-disk config for a rate-limit node.  The unit is one string; the magnitudes are numbers (Decision L4a).

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `burst` | number &#124; null | no | not specified | Optional instantaneous allowance, in the **scale** component of unit only -- the period does not apply to a burst.  unit: MiB/day with burst: 1 permits 1 MiB to be spent faster than the smoothed rate.  Defaults to one period's worth of limit, i.e. a pure sliding window with no extra allowance. Constraints: format="double". |
| `limit` | number | yes | not specified | How many units per period are permitted.  unit: MiB/day with limit: 10 means 10 MiB per day. Constraints: format="double". |
| `unit` | string | yes | not specified | &lt;scale&gt;/&lt;period&gt;, e.g. MiB/day, iops/second, B/hour. Carries dimension, scale, and period together. |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
