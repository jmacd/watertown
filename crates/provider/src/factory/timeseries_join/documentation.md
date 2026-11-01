---
title: Timeseries join
doc_id: factory:timeseries-join
---

# Timeseries join

## Overview

`timeseries-join` FULL OUTER JOINs at least two series inputs on their time column and coalesces timestamps, retaining timestamps found on only one side.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

When selecting a range, its complete shape is
`{begin: "2024-01-01T00:00:00Z", end: "2024-01-02T00:00:00Z"}`.
Each bound defaults to null (unbounded); those dates are examples, not defaults.

## Use and behavior

Install with `pond mknod timeseries-join /derived/weather --config-path join.yaml`, then inspect and query `/derived/weather`. Input scopes prefix value columns to avoid collisions. Optional range endpoints are RFC 3339 strings.

The source schemas and timestamp types are checked when the view is queried. This is a dynamic view, not a physical append-only series.

<!-- docs:reference factory:timeseries-join -->
<!-- docs:end-reference -->
