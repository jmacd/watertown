---
title: Timeseries pivot
doc_id: factory:timeseries-pivot
---

# Timeseries pivot

## Overview

`timeseries-pivot` selects named columns across matching series inputs and emits one scoped output column per source.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod timeseries-pivot /derived/temperatures --config-path pivot.yaml`, then inspect and query the resulting view.

The first wildcard capture is the source alias; without a capture, the basename is used. No matches and duplicate aliases fail. Missing selected columns are null-padded.

<!-- docs:reference factory:timeseries-pivot -->
<!-- docs:end-reference -->
