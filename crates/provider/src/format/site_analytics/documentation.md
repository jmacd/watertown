---
title: Strict site analytics records
doc_id: format:site-analytics
---

# Strict site analytics records

`site-analytics:///analytics/raw/views-*.jsonl` reads version-one collector
records as typed Arrow batches. Use it over a **single site's** raw destination
populated by `logfile-ingest` with `analytics_site_id`, not over arbitrary host
globs. The ingestion inventory establishes completeness and exact membership;
the format alone cannot detect an entirely missing file.

The fixed, non-null schema is `schema_version` (UInt32), `timestamp` (UTC
microseconds, from `received_at_us`), `site_id`, `catalog_id`, `page_uri`,
`segment_id` (UTF-8 strings), and `record_seq` (UInt64).

Parsing fails on malformed JSON, unknown fields, invalid identities/paths,
missing newlines, oversized records, or nonconsecutive sequences within a chunk.
No NUL stripping, warning-and-skip behavior, deduplication, or timestamp watermark
is used. A physical append chunk may start after sequence one; ingestion verifies
the whole segment. The reader buffers at most 1,024 records of at most 4 KiB
each. A later error fails the query, even if earlier batches were decoded.
`jsonlogs://` retains its existing permissive behavior.

For a nonempty validated snapshot, the full-history reference query is:

```sql
SELECT date_trunc('hour', timestamp) AS timestamp,
       page_uri,
       COUNT(*) AS page_views
FROM 'site-analytics:///analytics/raw/views-*.jsonl'
GROUP BY date_trunc('hour', timestamp), page_uri
ORDER BY timestamp, page_uri;
```

This is a reference query, not an incremental materializer. A verified inventory
with zero records has no raw series yet; do not infer empty coverage from a
failed file match. Site/environment identity stays in the inventory and the
separate destination, never inferred from a page's mount path.

<!-- docs:reference format:site-analytics -->
<!-- docs:end-reference -->
