---
title: Rate-limit budget
doc_id: factory:rate-limit
---

# Rate-limit budget

## Overview

`rate-limit` declares a consumption budget that other operations can explicitly consume.

## Example configuration

{{ source path="/docs/examples/rate-limit/default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod rate-limit /etc/upload-budget --config-path budget.yaml`. Inspect the node before attaching a consumer.

The unit carries scale and period; `limit` is the numeric amount per period. `burst` is capacity in the same scale. Declaring a node does not globally throttle arbitrary I/O: consumers must explicitly use the budget.

<!-- docs:reference factory:rate-limit -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `burst` | number &#124; null | no | not specified | Optional instantaneous allowance, in the **scale** component of unit only -- the period does not apply to a burst.  unit: MiB/day with burst: 1 permits 1 MiB to be spent faster than the smoothed rate.  Defaults to one period's worth of limit, i.e. a pure sliding window with no extra allowance. Constraints: format="double". |
| `limit` | number | yes | not specified | How many units per period are permitted.  unit: MiB/day with limit: 10 means 10 MiB per day. Constraints: format="double". |
| `unit` | string | yes | not specified | &lt;scale&gt;/&lt;period&gt;, e.g. MiB/day, iops/second, B/hour. Carries dimension, scale, and period together. |
<!-- docs:end-reference -->
