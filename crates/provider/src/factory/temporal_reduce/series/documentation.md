---
title: Direct temporal-reduction series
doc_id: factory:temporal-reduce-series
---

# Direct temporal-reduction series

## Overview

`temporal-reduce-series` exposes one temporal resolution directly as a queryable series instead of creating a directory.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

Set `columns: [temperature]` for a selected-column rollup. Null or omitted
cache settings resolve to the explicit defaults above.

## Use and behavior

Install with `pond mknod temporal-reduce-series /derived/hourly --config-path reduce.yaml`, then inspect and query `/derived/hourly`.

`resolution` must be a positive duration. The same cache, lateness, sealing, schema, and backfill semantics as [temporal reduction](temporal-reduce.md) apply.

`group_by` also works here. For example, `group_by: [site_id, page_uri]`,
`resolution: 1d`, `aggregations: [{type: count, columns: ["*"]}]`, and
`output_aliases: {timestamp.count: views}` expose daily per-site/page counts
through the same shared reduction cache.

<!-- docs:reference factory:temporal-reduce-series -->
<!-- docs:end-reference -->
