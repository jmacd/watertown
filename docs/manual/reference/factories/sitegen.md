---
title: Site generation
doc_id: factory:sitegen
---

# Site generation

## Overview

`sitegen` builds a static directory from Markdown, routes, exports, reports, and local assets. It is executable and does not create a queryable data node.

## Example configuration

{{ source path="/docs/examples/sitegen/default.yaml" language="yaml" /}}

This includes every top-level field and every field of the selected static
route. Empty collections and null values mean those optional features are
disabled; they are not placeholders for required values. The home route is
an intentional override of the empty default. Create `/site/index.md` in the
selected input root before building. The full reference below describes the
nested configuration when enabling exports, reports, feeds, or other features.

## Use and behavior

Install with `pond mknod sitegen /etc/site --config-path site.yaml`, then run `pond run /etc/site -- build OUTPUT --strict`. The same YAML can run without a pond using `pond run -d SOURCE host+sitegen:///site.yaml -- build OUTPUT --strict`.

Paths are rooted in the selected input filesystem, not the YAML file's directory. See the [configuration topics](../sitegen/configuration.md), [blog guide](../../guides/blog.md), and [timeseries-site guide](../../guides/timeseries-site.md).

<!-- docs:reference factory:sitegen -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `content` | array of Content Stage | no | [] |  |
| `documentation` | Documentation &#124; null | no | null | Opt in to source-relative manual paths, literal code, and lean assets. |
| `explore` | Explore &#124; null | no | null | Browser data explorer configuration. When present, the &#123;&#123; explore }} shortcode (on an layout: explore page) exposes the named export stages as queryable datasets in a Datasette-style SQL playground. Opt-in per site: absent means the explorer falls back to the single export bound to the page's route. |
| `exports` | array of Export Stage | no | [] |  |
| `feed` | Feed &#124; null | no | null | RSS feed configuration. If present, an RSS feed is generated. Requires site.site_url to be set for absolute link generation. |
| `footer` | string &#124; null | no | null | Custom text shown in the page footer alongside the build identifier. Example: footer: "Made in Mendocino" renders as "Made in Mendocino &#124; Watertown v0.39.0". |
| `header` | string &#124; null | no | null | Image path for a full-width centered banner at the top of every page. Path is relative to base_url (e.g., "img/logo.svg"). If absent, no banner is rendered. |
| `labels` | map of string | no | {} | Optional display-name overrides for template-route capture values. Keyed by the raw capture string (the $0/$1 value taken from the export path, e.g. DO), with the friendly label as the value (e.g. Dissolved Oxygen).  Applied when a capture is rendered as a page title or heading via &#123;&#123; $0 }}; URL slugs keep the raw value so links are unaffected.  Missing keys fall back to the raw capture. |
| `metric_captions` | map of string | no | {} | Optional human-readable caption per chart, keyed by &lt;param&gt;.&lt;unit&gt; (the same key used in metric_registry).  When present, the dashboard renders the caption directly beneath the corresponding chart.  Missing entries render no caption. |
| `metric_registry` | map of string | no | {} | Metric instrument-kind registry, keyed by metric name (the &lt;param&gt;.&lt;unit&gt; portion of a wide column name).  Values are OpenTelemetry instrument kinds: counter, updowncounter, or gauge.  Default kind is gauge; only non-gauge entries need to appear here.  chart.js reads this map and applies a per-kind transform:   counter         -&gt; first-difference per scope, plot rate   updowncounter   -&gt; plot value as-is (today; future: sum                       across scopes for spatial aggregation)   rate            -&gt; plot value as-is, but with a zero-anchored,                       always-rendered y-axis (for flow/rate gauges                       such as usage.gpm where 0 is meaningful)   gauge / missing -&gt; plot value as-is (latest/avg)  The canonical source of truth for each project should be a Weaver semconv registry (e.g. caspar.water's config/semconv/watertown-pond.yaml); mirror its non-gauge entries here. |
| `partials` | map of string | no | {} |  |
| `reports` | map of Report | no | {} | Named reports rendered from the same exported datasets as the site. |
| `routes` | array of Route | no | [] |  |
| `sidebar` | array of Sidebar Entry | no | [] | Ordered list of sidebar entries. Each entry is either a plain string (matched by page title, preferring exact match) or a map with label and children for hierarchical navigation. When present, content_nav renders pages in this exact order. Pages not listed are excluded from the sidebar. |
| `site` | Site Meta | yes | not specified |  |
| `static` | array of Static Asset | no | [] |  |
| `status_grid` | Status Grid &#124; null | no | null | Optional pond status grid configuration.  When present, sitegen runs DataFusion queries against the configured journal data at render time and exposes per-unit status to the &#123;&#123; pond_status_grid /}} shortcode as static HTML cards. See StatusGridConfig for fields. |
| `strict` | boolean | no | false | Reject incomplete routes, navigation, includes, and local links. |
| `subsites` | array of Subsite | no | [] | Sub-sites to generate recursively from imported ponds. Each sub-site references a sitegen config inside an imported mount and generates into a subdirectory of the output. |
| `theme` | map of string | no | {} | Theme overrides for CSS custom properties. Any key here overrides the corresponding CSS variable in :root. Example: accent: "#1a365d" sets --accent: #1a365d. |

### Content Stage

A content stage: globs markdown data files in the pond and reads their frontmatter for metadata (title, weight, slug).

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | yes | not specified | Name referenced by content routes (e.g., "pages") |
| `pattern` | string | yes | not specified | Pond glob pattern matching data files (e.g., "/pages/*.md") |

### Documentation

Bounded documentation capabilities; paths are relative to the input root.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `include_roots` | array of string | no | [] | Approved directories for literal source inclusions. |
| `source_root` | string | yes | not specified | Markdown root used to derive nested slugs (excluding _generated). |
| `version` | integer | yes | not specified | Required renderer contract. Unsupported revisions fail explicitly. Constraints: format="uint32"; minimum=0. |

### Explore

Browser data explorer configuration.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `datasets` | array of Explore Dataset | yes | not specified | Datasets exposed in the explorer, in display order. |
| `url` | string &#124; null | no | null | URL of the explorer page, joined relative to base_url (e.g. explore/; a leading slash is tolerated). When set, chart pages render an "Explore this data" button that opens this page pre-loaded with the chart's current files and time window. Absent means no cross-link. |

### Explore Dataset

One explorer dataset: an export stage surfaced as a named DuckDB view.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `export` | string | yes | not specified | Name of the exports: stage providing this dataset's parquet files. |
| `label` | string | yes | not specified | Human-readable label shown in the dataset picker. |
| `table` | string | yes | not specified | DuckDB view/table name the explorer registers (SQL identifier). |

### Export Stage

An export stage: runs pond export internally to produce data files that become template context.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | yes | not specified | Name referenced by routes (e.g., "params", "sites") |
| `pattern` | string | yes | not specified | Pond glob pattern or format-provider URL pattern.  Bare paths (e.g., /reduced/single_param/*/*.series) are resolved directly as queryable pond files.  URL-scheme patterns (e.g., jsonlogs:///logs/watershop/*.jsonl) are resolved through the format provider registry, allowing raw data files to be queried via DataFusion. |
| `target_points` | integer | no | 1500 | Target data points per screen width for partition sizing. Typical value: 1000-2000. Default: 1500. Controls how parquet files are temporally partitioned per resolution. Constraints: format="uint64"; minimum=0. |
| `timestamp_column` | string | no | "timestamp" | Name of the timestamp column for temporal partitioning. Default: "timestamp". Override for format-provider patterns where the timestamp column has a different name (e.g., "__REALTIME_TIMESTAMP" for journald logs). |

### Feed

RSS feed configuration (optional section in site.yaml).

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `content` | string &#124; null | no | null | Which content stage provides pages (default: first content stage). |
| `description` | string &#124; null | no | null | Channel description (default: site title). |
| `section` | string | no | "Blog" | Content section to include in the feed (default: "Blog"). |

### Report

A named, transport-independent report.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `include_pond_size` | boolean | no | false | Include the pond's allocated storage size. |
| `message` | string &#124; null | no | null | Optional reminder or explanatory text below the heading. |
| `sections` | array of Report Section | yes | not specified | Ordered data sections. |
| `title` | string | yes | not specified | Heading shown in rendered report artifacts. |
| `window` | string | yes | not specified | Trailing time window, such as 7d or 168h. |

### Report Section

A numeric series included in a report.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `captures` | array of string | no | [] | Exact wildcard captures selecting one file from the export pattern. |
| `chart` | boolean | no | false | Render a static PNG line chart. |
| `export` | string | yes | not specified | Name of an entry in exports. |
| `href` | string | yes | not specified | Relative or absolute link to the corresponding site page. |
| `summary` | Report Summary | no | "range" | Summary calculation. |
| `title` | string &#124; null | no | null | Optional heading override. Defaults to the first capture's configured site label, then to a title-cased capture value. |
| `unit` | string | yes | not specified | Display unit appended to numeric values. |
| `value` | string | yes | not specified | Numeric column to summarize and chart. |

### Report Summary

Numeric aggregation shown for a report section.

Accepted value: "range" &#124; "sum".

### Route

A route in the hierarchical route tree.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `annotations` | string &#124; null | no | null | Optional secondary export whose files are handed to the page as annotation data (e.g. leak/no-leak interval periods drawn as a shaded chart background). Resolved pond-wide (all files of the named export), so every page under the route shares the same annotations. |
| `content` | string &#124; null | no | null | For content routes: which content stage provides pages |
| `export` | string &#124; null | no | null | For template routes: which export stage provides data |
| `name` | string | yes | not specified | Route name (for logging/debugging) |
| `page` | string &#124; null | no | null | Path to markdown page (e.g., "/site/index.md") |
| `routes` | array of Route | no | [] | Nested child routes |
| `slug` | string | yes | not specified | URL slug -- may contain $0, $1 for template routes |
| `type` | Route Type | yes | not specified | "static" or "template" |

### Route Type

Route type.

Accepted value: "static" &#124; "template" &#124; "content".

### Sidebar Child

A child item in a sidebar entry with sub-navigation.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `children` | array of Sidebar Child | no | [] | Nested sub-navigation items. Empty for leaf entries. |
| `href` | string &#124; null | no | null | Direct URL path (e.g., "/data/well-depth.html"). Optional: parent-only entries (those with children) may omit href. |
| `label` | string | yes | not specified | Display label |

### Sidebar Entry

A sidebar entry: either a simple label or a label with sub-navigation children.

Choose one of the following:

#### Label with nested sub-navigation items and optional parent href

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `children` | array of Sidebar Child | yes | not specified |  |
| `href` | string &#124; null | no | null |  |
| `label` | string | yes | not specified |  |

#### Direct link with label and href (no children, no page matching)

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `href` | string | yes | not specified |  |
| `label` | string | yes | not specified |  |

#### Simple label matched by page title (exact &gt; case-insensitive &gt; substring)

Accepted value: string.

### Site Meta

Site-wide metadata.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `base_url` | string | no | "/" |  |
| `default_range` | string &#124; null | no | null | Default time-range button selected when a chart first loads, given as one of the chart.js duration labels such as "1M", "3M" or "24h". When absent, chart.js falls back to its adaptive default based on the data span. The value is clamped to the ranges that fit the available data. |
| `github_url` | string &#124; null | no | null | GitHub repository URL shown as an icon in the top bar. If absent, the GitHub icon is omitted. |
| `site_url` | string &#124; null | no | null | Canonical site URL for RSS feed links (e.g., "https://casparwater.org"). Required for RSS feed generation; if absent, feed is skipped. |
| `title` | string | yes | not specified |  |

### Static Asset

Static asset to copy verbatim.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `pattern` | string | yes | not specified |  |

### Status Grid

Config for the per-pond systemd status card grid.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `hot_window` | string &#124; null | no | null | Optional lookback window (humantime, e.g. "1d", "12h") bounding how far back the per-unit status/tail scan reads the append-only journal series. Journal versions whose newest event predates now - hot_window are pruned before the reader materializes them, so per-render memory stays bounded regardless of retained history. The bound also applies to perf_pattern enrichment. All history remains on disk and queryable; only this default status render is windowed. Unset defaults to [DEFAULT_STATUS_HOT_WINDOW] (1 day). |
| `journal_pattern` | string | yes | not specified | URL pattern resolving to the journal .jsonl files written by the journal-ingest factory, e.g. "jsonlogs:///logs/journal/*.jsonl". |
| `perf_pattern` | string &#124; null | no | null | Optional URL pattern (with {pond} placeholder) resolving to the per-pond perf series produced by measure-pond.sh + sql-derived-series. When set, sitegen reads the latest (timer.active, last_run.seconds_ago, timer.interval_s, run.wall_s) row per unit within hot_window and uses it for health classification. When the lookup fails for an individual unit (missing series, unparsable data, etc.) the card stays at Health::Unknown rather than failing the build.  {pond} is the unit's bare pond name -- the basename with user-pond@ / user-pond-selfmon@ prefix and .service suffix stripped.  Example value: "series:///derived/p-{pond}". |
| `tail_lines` | integer | no | 10 | Number of MESSAGE lines to render in the tail block per card. Constraints: format="uint"; minimum=0. |
| `unit_globs` | array of string | no | [] | Shell-style globs (matched against the journal file basename without the .jsonl suffix) selecting which units to render. Examples: ["user-pond@*.service", "user-pond-selfmon@*.service"].  Empty list disables filtering (every journal file becomes a card -- usually not what you want). |

### Subsite

A sub-site to generate recursively from an imported pond.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `base_url` | string &#124; null | no | null | Override the sub-site's base_url for the combined site. If omitted, the sub-site's own base_url is used. |
| `config` | string | yes | not specified | Path to the sitegen config within the foreign pond (absolute, resolved relative to the mount point via effective_root). |
| `name` | string | yes | not specified | Display name (used for logging and output directory fallback). |
| `path` | string | yes | not specified | Pond path to the import mount point (e.g., "/sources/noyo"). |
<!-- docs:end-reference -->
