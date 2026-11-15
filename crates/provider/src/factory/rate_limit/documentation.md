---
title: Rate-limit budget
doc_id: factory:rate-limit
---

# Rate-limit budget

## Overview

`rate-limit` declares a consumption budget that other operations can explicitly consume.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod rate-limit /etc/upload-budget --config-path budget.yaml`. Inspect the node before attaching a consumer.

The unit carries scale and period; `limit` is the numeric amount per period. `burst` is capacity in the same scale. Declaring a node does not globally throttle arbitrary I/O: consumers must explicitly use the budget.

<!-- docs:reference factory:rate-limit -->
<!-- docs:end-reference -->
