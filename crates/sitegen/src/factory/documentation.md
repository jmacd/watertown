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
opt in; `--quick` always suppresses it. Use this for previews. These controls
do not disable unrelated chart or page scripts.

<!-- docs:reference factory:sitegen -->
<!-- docs:end-reference -->
