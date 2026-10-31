---
title: Site generation
doc_id: factory:sitegen
---

# Site generation

## Overview

`sitegen` builds a static directory from Markdown, routes, exports, reports, and local assets. It is executable and does not create a queryable data node.

## Example configuration

```yaml
site:
  title: My site
  base_url: /
routes:
  - name: home
    type: static
    slug: ""
    page: /site/index.md
```

## Use and behavior

Install with `pond mknod sitegen /etc/site --config-path site.yaml`, then run `pond run /etc/site -- build OUTPUT --strict`. The same YAML can run without a pond using `pond run -d SOURCE host+sitegen:///site.yaml -- build OUTPUT --strict`.

Paths are rooted in the selected input filesystem, not the YAML file's directory. See the [configuration topics](../sitegen/configuration.md), [blog guide](../../guides/blog.md), and [timeseries-site guide](../../guides/timeseries-site.md).

<!-- docs:reference factory:sitegen -->
## Full configuration

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | no |
| initialize | yes |
| execute | yes |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `ReadWrite`.

### SiteConfig

Top-level site configuration.  yaml site:   title: "Noyo Harbor Blue Economy"   base_url: "/"  exports:   - name: "params"     pattern: "/reduced/single_param/*/*.series"  routes:   - name: "home"     type: static     slug: ""     page: "/site/index.md"     routes: [...]  partials:   sidebar: "/site/sidebar.md"  static:   - pattern: "/static/*"

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `content` | array of ContentStage | no | [] |  |
| `documentation` | DocumentationConfig &#124; null | no | null | Opt in to source-relative manual paths, literal code, and lean assets. |
| `explore` | ExploreConfig &#124; null | no | null | Browser data explorer configuration. When present, the &#123;&#123; explore }} shortcode (on an layout: explore page) exposes the named export stages as queryable datasets in a Datasette-style SQL playground. Opt-in per site: absent means the explorer falls back to the single export bound to the page's route. |
| `exports` | array of ExportStage | no | [] |  |
| `feed` | FeedConfig &#124; null | no | null | RSS feed configuration. If present, an RSS feed is generated. Requires site.site_url to be set for absolute link generation. |
| `footer` | string &#124; null | no | null | Custom text shown in the page footer alongside the build identifier. Example: footer: "Made in Mendocino" renders as "Made in Mendocino &#124; Watertown v0.39.0". |
| `header` | string &#124; null | no | null | Image path for a full-width centered banner at the top of every page. Path is relative to base_url (e.g., "img/logo.svg"). If absent, no banner is rendered. |
| `labels` | map of string | no | {} | Optional display-name overrides for template-route capture values. Keyed by the raw capture string (the $0/$1 value taken from the export path, e.g. DO), with the friendly label as the value (e.g. Dissolved Oxygen).  Applied when a capture is rendered as a page title or heading via &#123;&#123; $0 }}; URL slugs keep the raw value so links are unaffected.  Missing keys fall back to the raw capture. |
| `metric_captions` | map of string | no | {} | Optional human-readable caption per chart, keyed by &lt;param&gt;.&lt;unit&gt; (the same key used in metric_registry).  When present, the dashboard renders the caption directly beneath the corresponding chart.  Missing entries render no caption. |
| `metric_registry` | map of string | no | {} | Metric instrument-kind registry, keyed by metric name (the &lt;param&gt;.&lt;unit&gt; portion of a wide column name).  Values are OpenTelemetry instrument kinds: counter, updowncounter, or gauge.  Default kind is gauge; only non-gauge entries need to appear here.  chart.js reads this map and applies a per-kind transform:   counter         -&gt; first-difference per scope, plot rate   updowncounter   -&gt; plot value as-is (today; future: sum                       across scopes for spatial aggregation)   rate            -&gt; plot value as-is, but with a zero-anchored,                       always-rendered y-axis (for flow/rate gauges                       such as usage.gpm where 0 is meaningful)   gauge / missing -&gt; plot value as-is (latest/avg)  The canonical source of truth for each project should be a Weaver semconv registry (e.g. caspar.water's config/semconv/watertown-pond.yaml); mirror its non-gauge entries here. |
| `partials` | map of string | no | {} |  |
| `reports` | map of ReportConfig | no | {} | Named reports rendered from the same exported datasets as the site. |
| `routes` | array of RouteConfig | no | [] |  |
| `sidebar` | array of SidebarEntry | no | [] | Ordered list of sidebar entries. Each entry is either a plain string (matched by page title, preferring exact match) or a map with label and children for hierarchical navigation. When present, content_nav renders pages in this exact order. Pages not listed are excluded from the sidebar. |
| `site` | SiteMeta | yes | not specified |  |
| `static` | array of StaticAsset | no | [] |  |
| `status_grid` | StatusGridConfig &#124; null | no | null | Optional pond status grid configuration.  When present, sitegen runs DataFusion queries against the configured journal data at render time and exposes per-unit status to the &#123;&#123; pond_status_grid /}} shortcode as static HTML cards. See StatusGridConfig for fields. |
| `strict` | boolean | no | false | Reject incomplete routes, navigation, includes, and local links. |
| `subsites` | array of SubsiteConfig | no | [] | Sub-sites to generate recursively from imported ponds. Each sub-site references a sitegen config inside an imported mount and generates into a subdirectory of the output. |
| `theme` | map of string | no | {} | Theme overrides for CSS custom properties. Any key here overrides the corresponding CSS variable in :root. Example: accent: "#1a365d" sets --accent: #1a365d. |

Unknown fields: **rejected**.

### ContentStage

A content stage: globs markdown data files in the pond and reads their frontmatter for metadata (title, weight, slug).  Content stages are resolved before route expansion. A content route references a stage by name, producing one page per matched file.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | yes | not specified | Name referenced by content routes (e.g., "pages") |
| `pattern` | string | yes | not specified | Pond glob pattern matching data files (e.g., "/pages/*.md") |

Unknown fields: **rejected**.

### DocumentationConfig

Bounded documentation capabilities; paths are relative to the input root.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `include_roots` | array of string | no | [] | Approved directories for literal source inclusions. |
| `source_root` | string | yes | not specified | Markdown root used to derive nested slugs (excluding _generated). |
| `version` | integer | yes | not specified | Required renderer contract. Unsupported revisions fail explicitly. Constraints: format="uint32"; minimum=0. |

Unknown fields: **rejected**.

### ExploreConfig

Browser data explorer configuration.  Lists which export stages to surface as queryable datasets in the explorer.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `datasets` | array of ExploreDataset | yes | not specified | Datasets exposed in the explorer, in display order. |
| `url` | string &#124; null | no | null | URL of the explorer page, joined relative to base_url (e.g. explore/; a leading slash is tolerated). When set, chart pages render an "Explore this data" button that opens this page pre-loaded with the chart's current files and time window. Absent means no cross-link. |

Unknown fields: **rejected**.

### ExploreDataset

One explorer dataset: an export stage surfaced as a named DuckDB view.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `export` | string | yes | not specified | Name of the exports: stage providing this dataset's parquet files. |
| `label` | string | yes | not specified | Human-readable label shown in the dataset picker. |
| `table` | string | yes | not specified | DuckDB view/table name the explorer registers (SQL identifier). |

Unknown fields: **rejected**.

### ExportStage

An export stage: runs pond export internally to produce data files that become template context.  # Automatic Temporal Partitioning  Parquet files are split into Hive-style temporal partitions automatically. The system discovers resolutions from the matched files (e.g., res=1h, res=24h) and computes the right partition level for each resolution so that any viewport loads at most 2 files.  target_points controls the trade-off: higher values mean finer partitions (more data per file, fewer files). Default is 1500, matching a typical screen width in pixels.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | yes | not specified | Name referenced by routes (e.g., "params", "sites") |
| `pattern` | string | yes | not specified | Pond glob pattern or format-provider URL pattern.  Bare paths (e.g., /reduced/single_param/*/*.series) are resolved directly as queryable pond files.  URL-scheme patterns (e.g., jsonlogs:///logs/watershop/*.jsonl) are resolved through the format provider registry, allowing raw data files to be queried via DataFusion. |
| `target_points` | integer | no | 1500 | Target data points per screen width for partition sizing. Typical value: 1000-2000. Default: 1500. Controls how parquet files are temporally partitioned per resolution. Constraints: format="uint64"; minimum=0. |
| `timestamp_column` | string | no | "timestamp" | Name of the timestamp column for temporal partitioning. Default: "timestamp". Override for format-provider patterns where the timestamp column has a different name (e.g., "__REALTIME_TIMESTAMP" for journald logs). |

Unknown fields: **rejected**.

### FeedConfig

RSS feed configuration (optional section in site.yaml).

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `content` | string &#124; null | no | null | Which content stage provides pages (default: first content stage). |
| `description` | string &#124; null | no | null | Channel description (default: site title). |
| `section` | string | no | "Blog" | Content section to include in the feed (default: "Blog"). |

Unknown fields: **rejected**.

### ReportConfig

A named, transport-independent report.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `include_pond_size` | boolean | no | false | Include the pond's allocated storage size. |
| `message` | string &#124; null | no | null | Optional reminder or explanatory text below the heading. |
| `sections` | array of ReportSectionConfig | yes | not specified | Ordered data sections. |
| `title` | string | yes | not specified | Heading shown in rendered report artifacts. |
| `window` | string | yes | not specified | Trailing time window, such as 7d or 168h. |

Unknown fields: **rejected**.

### ReportSectionConfig

A numeric series included in a report.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `captures` | array of string | no | [] | Exact wildcard captures selecting one file from the export pattern. |
| `chart` | boolean | no | false | Render a static PNG line chart. |
| `export` | string | yes | not specified | Name of an entry in exports. |
| `href` | string | yes | not specified | Relative or absolute link to the corresponding site page. |
| `summary` | ReportSummary | no | "range" | Summary calculation. |
| `title` | string &#124; null | no | null | Optional heading override. Defaults to the first capture's configured site label, then to a title-cased capture value. |
| `unit` | string | yes | not specified | Display unit appended to numeric values. |
| `value` | string | yes | not specified | Numeric column to summarize and chart. |

Unknown fields: **rejected**.

### ReportSummary

Numeric aggregation shown for a report section.

Accepted value: "range" &#124; "sum".

### RouteConfig

A route in the hierarchical route tree.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `annotations` | string &#124; null | no | null | Optional secondary export whose files are handed to the page as annotation data (e.g. leak/no-leak interval periods drawn as a shaded chart background). Resolved pond-wide (all files of the named export), so every page under the route shares the same annotations. |
| `content` | string &#124; null | no | null | For content routes: which content stage provides pages |
| `export` | string &#124; null | no | null | For template routes: which export stage provides data |
| `name` | string | yes | not specified | Route name (for logging/debugging) |
| `page` | string &#124; null | no | null | Path to markdown page (e.g., "/site/index.md") |
| `routes` | array of RouteConfig | no | [] | Nested child routes |
| `slug` | string | yes | not specified | URL slug -- may contain $0, $1 for template routes |
| `type` | RouteType | yes | not specified | "static" or "template" |

Unknown fields: **rejected**.

### RouteType

Route type.

Accepted value: "static" &#124; "template" &#124; "content".

### SidebarChild

A child item in a sidebar entry with sub-navigation.  Supports recursive nesting: a child may have its own children, enabling arbitrary-depth sidebar trees. href is optional; a child without an href but with children renders as a section heading that links to its first descendant leaf.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `children` | array of SidebarChild | no | [] | Nested sub-navigation items. Empty for leaf entries. |
| `href` | string &#124; null | no | null | Direct URL path (e.g., "/data/well-depth.html"). Optional: parent-only entries (those with children) may omit href. |
| `label` | string | yes | not specified | Display label |

Unknown fields: **rejected**.

### SidebarEntry

A sidebar entry: either a simple label or a label with sub-navigation children.  Simple entries are plain strings matched by page title. Match priority: exact title &gt; case-insensitive exact &gt; substring. Entries with children render a parent link plus nested sub-items that expand when the parent or any child is the current page.  yaml sidebar:   - "Political"                    # simple   - label: "Monitoring"            # with children     children:       - label: "Well Depth"         href: "/data/well-depth.html"

### SidebarEntry variant 1

Label with nested sub-navigation items and optional parent href.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `children` | array of SidebarChild | yes | not specified |  |
| `href` | string &#124; null | no | null |  |
| `label` | string | yes | not specified |  |

Unknown fields: not rejected by this shape; see runtime notes.

### SidebarEntry variant 2

Direct link with label and href (no children, no page matching).

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `href` | string | yes | not specified |  |
| `label` | string | yes | not specified |  |

Unknown fields: not rejected by this shape; see runtime notes.

### SidebarEntry variant 3

Simple label matched by page title (exact &gt; case-insensitive &gt; substring).

Accepted value: string.

### SiteMeta

Site-wide metadata.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `base_url` | string | no | "/" |  |
| `default_range` | string &#124; null | no | null | Default time-range button selected when a chart first loads, given as one of the chart.js duration labels such as "1M", "3M" or "24h". When absent, chart.js falls back to its adaptive default based on the data span. The value is clamped to the ranges that fit the available data. |
| `github_url` | string &#124; null | no | null | GitHub repository URL shown as an icon in the top bar. If absent, the GitHub icon is omitted. |
| `site_url` | string &#124; null | no | null | Canonical site URL for RSS feed links (e.g., "https://casparwater.org"). Required for RSS feed generation; if absent, feed is skipped. |
| `title` | string | yes | not specified |  |

Unknown fields: **rejected**.

### StaticAsset

Static asset to copy verbatim.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `pattern` | string | yes | not specified |  |

Unknown fields: **rejected**.

### StatusGridConfig

Config for the per-pond systemd status card grid.  Sitegen lists journal .jsonl files matching journal_pattern, filters the basenames against unit_globs, and for each surviving unit runs two SQL queries: a status query (latest event, latest "Started" / exit / "Peak memory usage" lines) and a tail query (most recent tail_lines MESSAGE values).  Results are rendered as static HTML cards by the pond_status_grid shortcode; refresh requires re-running sitegen.  When perf_pattern is set, sitegen ALSO queries each unit's perf series within hot_window for the latest (timer.active, last_run.seconds_ago, timer.interval_s, run.wall_s) tuple. Those values feed the per-card health classification (green/yellow/red) and Timer/Period/Last-run metadata lines in the rendered HTML. All four values come from one latest-row provider scan per card.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `hot_window` | string &#124; null | no | null | Optional lookback window (humantime, e.g. "1d", "12h") bounding how far back the per-unit status/tail scan reads the append-only journal series. Journal versions whose newest event predates now - hot_window are pruned before the reader materializes them, so per-render memory stays bounded regardless of retained history. The bound also applies to perf_pattern enrichment. All history remains on disk and queryable; only this default status render is windowed. Unset defaults to [DEFAULT_STATUS_HOT_WINDOW] (1 day). |
| `journal_pattern` | string | yes | not specified | URL pattern resolving to the journal .jsonl files written by the journal-ingest factory, e.g. "jsonlogs:///logs/journal/*.jsonl". |
| `perf_pattern` | string &#124; null | no | null | Optional URL pattern (with {pond} placeholder) resolving to the per-pond perf series produced by measure-pond.sh + sql-derived-series. When set, sitegen reads the latest (timer.active, last_run.seconds_ago, timer.interval_s, run.wall_s) row per unit within hot_window and uses it for health classification. When the lookup fails for an individual unit (missing series, unparsable data, etc.) the card stays at Health::Unknown rather than failing the build.  {pond} is the unit's bare pond name -- the basename with user-pond@ / user-pond-selfmon@ prefix and .service suffix stripped.  Example value: "series:///derived/p-{pond}". |
| `tail_lines` | integer | no | 10 | Number of MESSAGE lines to render in the tail block per card. Constraints: format="uint"; minimum=0. |
| `unit_globs` | array of string | no | [] | Shell-style globs (matched against the journal file basename without the .jsonl suffix) selecting which units to render. Examples: ["user-pond@*.service", "user-pond-selfmon@*.service"].  Empty list disables filtering (every journal file becomes a card -- usually not what you want). |

Unknown fields: **rejected**.

### SubsiteConfig

A sub-site to generate recursively from an imported pond.  yaml subsites:   - name: "noyo"     path: "/sources/noyo"     config: "/system/etc/90-sitegen"     base_url: "/noyo/"

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `base_url` | string &#124; null | no | null | Override the sub-site's base_url for the combined site. If omitted, the sub-site's own base_url is used. |
| `config` | string | yes | not specified | Path to the sitegen config within the foreign pond (absolute, resolved relative to the mount point via effective_root). |
| `name` | string | yes | not specified | Display name (used for logging and output directory fallback). |
| `path` | string | yes | not specified | Pond path to the import mount point (e.g., "/sources/noyo"). |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
