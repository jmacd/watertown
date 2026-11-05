---
title: Site analytics table
doc_id: factory:analytics-table
---

# Site analytics table

`analytics-table` exposes the committed partitions from
[analytics-rollup](analytics-rollup.md) as one read-only table of hourly
**reported page views**.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

Install with:

```sh
pond mknod analytics-table /analytics/views --config-path analytics-table.yaml
```

Use `series:///analytics/views` in queries and Sitegen exports. Its schema is
`timestamp` (UTC microseconds), `page_uri` (UTF-8), and `page_views` (UInt64).

The table reads only currently selected daily objects, not a glob of retained
historical objects. An empty published manifest is a typed empty table; an absent
manifest is an error. Providers obey the pond's stale/closed transaction checks.
Use typed queries or exports, not raw byte reads.

Each site/environment needs a separate rollup target. This factory neither
collects events nor establishes collector uptime.

The URI field carries the [rollup's encoding policy](analytics-rollup.md).
Older rollup snapshots must be rebuilt before use with the new schema; deleting
a query cache is not a migration.

<!-- docs:reference factory:analytics-table -->
<!-- docs:end-reference -->
