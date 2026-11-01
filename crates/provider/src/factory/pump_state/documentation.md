---
title: Pump-state series
doc_id: factory:pump-state-series
---

# Pump-state series

## Overview

`pump-state-series` classifies well depth into pumping, recovering, and static episodes using a trailing ceiling.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

There are no default thresholds. These numbers illustrate the shape; choose
depth bounds and disturbance criteria for your actual data.

## Use and behavior

Install with `pond mknod pump-state-series /derived/pump-state --config-path pump.yaml`, then inspect and query the resulting view.

The source URL must be exact, without wildcards. Depth limits must be finite and ordered, and lookback must be positive whole minutes. This is analytical classification, not pump control or a safety alarm.

<!-- docs:reference factory:pump-state-series -->
<!-- docs:end-reference -->
