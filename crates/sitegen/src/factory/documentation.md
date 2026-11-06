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
  exclude:
    - /status
```

The standalone collector is available but not deployed; see
`crates/site-analytics-collector/README.md` in the source tree. Do not publish an
enabled build until a same-origin collector has installed and accepted its
catalog and passed the deployment privacy checks. The script makes
one best-effort request per top-level document, at first visibility after
prerender activation. Tab returns and already-reported back/forward-cache
restores do not report again; reloads create new documents. Failures warn without
blocking the page, and there are no retries, cookies, visitor identifiers, client
timestamps, or referrers in the request.

Each enabled site emits `_analytics/catalog.json` and one shared
`_analytics/view-<sha256>.js` asset in its own output directory. The catalog
contains version, stable `site_id`, sorted entries (`page_uri`, title, aliases),
and a SHA-256 identity derived from version, site identity, and entries serialized
as compact JSON in that field order. Neither build timestamps nor exported data
change its identity. Public catalog labels describe only eligible pages; the
collector's installed registry and historical catalogs must remain private.

Canonical paths include `site.base_url` exactly once. With the supported Caddy
`try_files {path} {path}.html {path}/` configuration, `water.html` identifies
`/water`, while `section/index.html` identifies `/section/`. Explicit aliases
include those emitted paths and the directory's slashless request path.
Ambiguous serving paths fail enabled builds rather than merging page counts.
The `_analytics/` output namespace is reserved for generated analytics artifacts.

Subsites must opt in independently and use distinct `site_id` values. Parent
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
opt in; `--quick` always suppresses it. Both also suppress analytics reports.
Use this for previews. These controls
do not disable unrelated chart or page scripts.

### Optional daily analytics report

The approved public contract is **daily per-page counts and daily site totals
over 30 UTC calendar days, including today**. Hourly detail remains private.
Reporting is disabled when `analytics_report` is absent and can be configured
independently of instrumentation:

```yaml
analytics_report:
  site_id: noyo
  target: /analytics/hourly
  catalog_dir: /private/noyo/catalogs
  current_catalog: /private/noyo/current-catalog.json
  state_path: /private/noyo/report-state.json
```

`target` is a committed `analytics-rollup` directory. Run refresh in the
**owning site's pond**, not through another pond's imported mount:

```sh
pond run /etc/site -- analytics-report
```

Schedule this as a separate monitored job, after ingestion/rollup. It queries
only selected aggregate objects, never collector logs. Catalogs must be trusted
publisher catalogs for that site; hashes and identities are checked. Current
titles take precedence; ambiguous historical titles fall back to the URI.
Keep the state file and retained catalogs private, outside every web root.

The refresh command atomically saves data and original aggregate-publication
time together. On query/catalog failure it records failure, preserves any valid
last-good bundle, and **still returns an error**. Never mask this exit status
with `|| true`, or make the core site build depend on refresh success.
If failure-state storage itself is unavailable, the job still fails and any
older saved bundle retains its original timestamp.

An ordinary site build only reads this saved file. It generates `analytics.html`
and `_analytics/report.json`, without querying the pond's analytics tables.
Missing or corrupt state logs an error and renders **analytics unavailable**;
failed refreshes show last-good data with an explicit failure notice.
Successful core publication does not mean the separate analytics job succeeded.
Filesystem write failures and unsafe report configuration still fail the build.

The dashboard includes daily totals with a static bar chart, ranked pages,
expandable daily histories, UTC/metric definitions, unknown coverage, and the
original aggregate timestamp. Missing counts are never zero-filled. Partial
snapshot days remain marked partial even when served later. Historical pages
not in the current catalog or current build remain visible without live links.
Titles are escaped. Add an explicit sidebar link to `/analytics` (or its mounted
equivalent) when enabling the feature; do not create a competing route.

The public JSON contains only daily counts, public URI/title labels, partial-day
flags, site identity, the original window and original publication timestamp,
and unknown coverage. Even fallback rendering removes rows older than the
current 30-day window without refreshing their publication timestamp. No raw
events, hourly rows, catalog/segment IDs, source inventories, private paths or
error details are exported. Do not separately expose the private hourly table
through ordinary `exports` or the SQL explorer.

Each site/environment needs its own state file and job. Subsites never inherit
this setting. Mount the same private state into the report job and renderer,
read-only for the renderer, with explicit service ownership/read permissions.
Only the report job needs access to private catalogs. The outer repository
provides an opt-in staging runner, narrow report mounts, and inactive
`pond-analytics-report@` user-unit templates; see its operations guide.
Provisioning, timer installation and alerting remain explicit deployment work.
The core `run.sh` does not invoke report refresh.

Limits fail visibly rather than truncating counts: 100,000 daily URI/day rows,
8 MiB projected JSON, 1,024-byte titles, and at most 4,096 catalog-directory
entries with 2 MiB per catalog and 64 MiB retained catalog bytes.
Coverage intervals, live monitoring integration and staging failure exercises
remain deployment gates; this configuration does not enable them automatically.

<!-- docs:reference factory:sitegen -->
<!-- docs:end-reference -->
