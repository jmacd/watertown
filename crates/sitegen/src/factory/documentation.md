---
title: Site generation
doc_id: factory:sitegen
---

# Site generation

## Overview

`sitegen` builds a static directory from Markdown, routes, exports, reports, and local assets. It is executable and does not create a queryable data node.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

This includes every top-level field and every field of the selected static
route. Empty collections and null values mean those optional features are
disabled; they are not placeholders for required values. The home route is
an intentional override of the empty default. Create `/site/index.md` in the
selected input root before building. The full reference below describes the
nested configuration when enabling exports, reports, feeds, or other features.

## Use and behavior

Install with `pond mknod sitegen /etc/site --config-path site.yaml`, then run `pond run /etc/site -- build OUTPUT --strict`. The same YAML can run without a pond using `pond run -d SOURCE host+sitegen:///site.yaml -- build OUTPUT --strict`.

Paths are rooted in the selected input filesystem, not the YAML file's directory. See the [configuration topics](../sitegen/configuration.md), [blog guide](../../guides/blog.md), and [timeseries-site guide](../../guides/timeseries-site.md).

### Optional reported page views

Instrumentation is disabled by default. To prepare an instrumented build:

```yaml
analytics:
  enabled: true
  site_id: caspar-water
  endpoint: /_analytics/view
  default_section: Static content
  sections:
    /blog: Blog
    /data: Monitoring
    /analysis: Monitoring
    /septic: Monitoring
    /explore: Monitoring
  exclude:
    - /status
```

Use the existing web server to record beacons in a dedicated, privacy-filtered
log, separate from ordinary access logs. The script sends a bodyless POST to
the configured endpoint with URL-encoded `page_uri` and `catalog_id` query
parameters. These are publisher metadata, not the current browser URL.
The endpoint should return HTTP 204 with `Cache-Control: no-store`; a successful
response is not a durable-journal acknowledgement or proof of a human visit.
Do not publish an enabled build until log routing, privacy filtering, and
periodic ingestion into the owning pond are configured. The script makes
one best-effort request per top-level document, at first visibility after
prerender activation. Tab returns and already-reported back/forward-cache
restores do not report again; reloads create new documents. Failures warn without
blocking the page, and there are no retries, cookies, visitor identifiers, client
timestamps, or referrers in the request.

Each enabled site emits `_analytics/catalog.json`, `_analytics/pages.parquet`, and one shared
`_analytics/view-<sha256>.js` asset in its own output directory. The catalog
contains version 2, stable `site_id`, sorted entries (`page_uri`, title, aliases, section),
and a SHA-256 identity derived from version, site identity, and entries serialized
as compact JSON in that field order. Neither build timestamps nor exported data
change its identity. Public catalog labels describe only eligible pages.
The web server does not need an installed catalog registry. Beacon page and
catalog identities remain untrusted claims, not server-validated membership.

Sections are publisher-owned labels. `sections` maps site-relative path prefixes
to labels, matching whole path segments; the longest matching prefix wins.
Prefixes start with `/` and, except for `/`, have no trailing slash.
`default_section` applies to eligible pages without a match and defaults to
`Other / unclassified`. A prefix `/blog` matches `/blog/entry`, not `/blogger`.
Mount prefixes are stripped before matching. Subsites do not inherit the parent's
section rules; Noyo can use `default_section: Noyo` and an empty `sections` map.
Changing an emitted section changes the catalog identity.

The Parquet inventory contains one row per eligible canonical page, with only
`site_id`, `page_uri`, and `section`, all non-null UTF-8 strings. After publishing,
copy it into the owning analytics pond with ordinary `pond copy host:///...`,
as a current versioned data file, not `host+table` or `host+series`. Query it
through `file+table:///analytics/pages.parquet` to read only the current file.
It is a publisher inventory, not observations or a host-only report database.
Unknown paths can then be grouped as `Other / unclassified` without exposing
their names. An empty inventory is a valid zero-row Parquet file with the same
schema: it replaces previous classifications, so all retained views become
Other. Missing or unreadable inventory is an error, not a known-empty inventory.

Canonical paths include `site.base_url` exactly once. With the supported Caddy
`try_files {path} {path}.html {path}/` configuration, `water.html` identifies
`/water`, while `section/index.html` identifies `/section/`. Explicit aliases
include those emitted paths and the directory's slashless request path.
Ambiguous serving paths fail enabled builds rather than merging page counts.
The `_analytics/` output namespace is reserved for generated analytics artifacts.

Subsites must opt in independently and use distinct `site_id` values and beacon
endpoints, each routed to its own Caddy log. Parent
enablement is never inherited, and no subsite entries enter the parent's catalog.
A subsite's configured `base_url` must match its actual publishing mount.
Noyo therefore remains its own site while mounted at `/noyo-harbor/`; a later
domain move leaves its stable site identity unchanged but changes its catalog
paths. The endpoint is always an origin-relative path, never mount-prefixed.

Use frontmatter `analytics: false` or exact canonical paths in `exclude` for
private, diagnostic, or synthetic pages. Navigation `hidden` is unrelated.
The site's `/analytics` dashboard (including its directory form) is excluded by
default. Raw HTML inside Markdown is instrumented by the surrounding layout.
Copied standalone HTML assets must be explicitly excluded or converted to
layout-rendered pages; they are never silently counted as instrumented.

Documentation configurations remain off unless explicitly enabled. `build
--no-analytics` suppresses instrumentation for the whole tree even when sites
opt in; `--quick` always suppresses it.
Use this for previews. These controls
do not disable unrelated chart or page scripts.

### Analytics publication boundary

The browser instrumentation does not publish an analytics report. Ingest the
dedicated Caddy logs through ordinary `logfile-ingest` and query them through
`jsonlogs://` and SQL-derived nodes in the owning pond. Sitegen's ordinary
reporting/export facilities are the integration boundary; public analytics
report integration is pending.

The disclosure contract remains **daily per-page counts and daily site totals
over 30 UTC calendar days, including today**. Reports must not expose raw events,
hourly detail, or catalog identities. Missing observations are not zero, and
maximum event time does not establish collection uptime or publication
freshness. Coverage stays unknown unless independently established.

The former standalone collector, custom rollup/table factories, and host-state
`analytics_report` setting and refresh command have been removed. There is no
replacement host report directory or application-owned cache.

<!-- docs:reference factory:sitegen -->
<!-- docs:end-reference -->
