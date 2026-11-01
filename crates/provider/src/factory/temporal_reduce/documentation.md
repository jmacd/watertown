---
title: Temporal reduction directory
doc_id: factory:temporal-reduce
---

# Temporal reduction directory

## Overview

`temporal-reduce` creates a dynamic directory containing one reduced series per configured resolution and output pattern.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

Use `columns: [temperature]` to restrict the aggregation and, for example,
`output_aliases: {temperature.avg: temperature}` to rename its output.
Omitted or null cache settings resolve to the explicit defaults shown above.

## Use and behavior

Install with `pond mknod temporal-reduce /derived/reduced --config-path reduce.yaml`, then list and query its children. Buckets are aligned time windows; a sample on the next boundary belongs to that next bucket. Reduction is aggregation, not interpolation.

Incremental caches use source versions and event-time metadata. Missing bounds can force full-history invalidation. Late backfill unseals and recomputes affected segments rather than silently dropping data.

<!-- docs:reference factory:temporal-reduce -->
<!-- docs:end-reference -->
