// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Site configuration -- parsed from `site.yaml` (pond or host filesystem).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Top-level site configuration.
///
/// ```yaml
/// site:
///   title: "Noyo Harbor Blue Economy"
///   base_url: "/"
///
/// exports:
///   - name: "params"
///     pattern: "/reduced/single_param/*/*.series"
///
/// routes:
///   - name: "home"
///     type: static
///     slug: ""
///     page: "/site/index.md"
///     routes: [...]
///
/// partials:
///   sidebar: "/site/sidebar.md"
///
/// static:
///   - pattern: "/static/*"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SiteConfig {
    pub site: SiteMeta,
    #[serde(default)]
    pub content: Vec<ContentStage>,
    #[serde(default)]
    pub exports: Vec<ExportStage>,
    /// Named reports rendered from the same exported datasets as the site.
    #[serde(default)]
    pub reports: BTreeMap<String, ReportConfig>,
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    #[serde(default)]
    pub partials: std::collections::BTreeMap<String, String>,
    #[serde(rename = "static", default)]
    pub static_assets: Vec<StaticAsset>,
    /// Ordered list of sidebar entries.
    /// Each entry is either a plain string (matched by page title, preferring exact match)
    /// or a map with `label` and `children` for hierarchical navigation.
    /// When present, `content_nav` renders pages in this exact order.
    /// Pages not listed are excluded from the sidebar.
    #[serde(default)]
    pub sidebar: Vec<SidebarEntry>,
    /// RSS feed configuration. If present, an RSS feed is generated.
    /// Requires `site.site_url` to be set for absolute link generation.
    #[serde(default)]
    pub feed: Option<FeedConfig>,
    /// Theme overrides for CSS custom properties.
    /// Any key here overrides the corresponding CSS variable in `:root`.
    /// Example: `accent: "#1a365d"` sets `--accent: #1a365d`.
    #[serde(default)]
    pub theme: std::collections::BTreeMap<String, String>,
    /// Sub-sites to generate recursively from imported ponds.
    /// Each sub-site references a sitegen config inside an imported mount
    /// and generates into a subdirectory of the output.
    #[serde(default)]
    pub subsites: Vec<SubsiteConfig>,
    /// Custom text shown in the page footer alongside the build identifier.
    /// Example: `footer: "Made in Mendocino"` renders as
    /// "Made in Mendocino | Watertown v0.39.0".
    #[serde(default)]
    pub footer: Option<String>,
    /// Image path for a full-width centered banner at the top of every page.
    /// Path is relative to `base_url` (e.g., `"img/logo.svg"`).
    /// If absent, no banner is rendered.
    #[serde(default)]
    pub header: Option<String>,
    /// Browser data explorer configuration. When present, the `{{ explore }}`
    /// shortcode (on an `layout: explore` page) exposes the named export stages
    /// as queryable datasets in a Datasette-style SQL playground. Opt-in per
    /// site: absent means the explorer falls back to the single export bound to
    /// the page's route.
    #[serde(default)]
    pub explore: Option<ExploreConfig>,
    /// Metric instrument-kind registry, keyed by metric name (the
    /// `<param>.<unit>` portion of a wide column name).  Values are
    /// OpenTelemetry instrument kinds: `counter`, `updowncounter`,
    /// or `gauge`.  Default kind is `gauge`; only non-gauge entries
    /// need to appear here.
    ///
    /// chart.js reads this map and applies a per-kind transform:
    ///   counter         -> first-difference per scope, plot rate
    ///   updowncounter   -> plot value as-is (today; future: sum
    ///                       across scopes for spatial aggregation)
    ///   rate            -> plot value as-is, but with a zero-anchored,
    ///                       always-rendered y-axis (for flow/rate gauges
    ///                       such as `usage.gpm` where 0 is meaningful)
    ///   gauge / missing -> plot value as-is (latest/avg)
    ///
    /// The canonical source of truth for each project should be a
    /// Weaver semconv registry (e.g. caspar.water's
    /// `config/semconv/watertown-pond.yaml`); mirror its
    /// non-gauge entries here.
    #[serde(default)]
    pub metric_registry: std::collections::BTreeMap<String, String>,

    /// Optional human-readable caption per chart, keyed by `<param>.<unit>`
    /// (the same key used in `metric_registry`).  When present, the
    /// dashboard renders the caption directly beneath the corresponding
    /// chart.  Missing entries render no caption.
    #[serde(default)]
    pub metric_captions: std::collections::BTreeMap<String, String>,

    /// Optional display-name overrides for template-route capture values.
    /// Keyed by the raw capture string (the `$0`/`$1` value taken from the
    /// export path, e.g. `DO`), with the friendly label as the value
    /// (e.g. `Dissolved Oxygen`).  Applied when a capture is rendered as a
    /// page title or heading via `{{ $0 }}`; URL slugs keep the raw value so
    /// links are unaffected.  Missing keys fall back to the raw capture.
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,

    /// Optional pond status grid configuration.  When present, sitegen
    /// runs DataFusion queries against the configured journal data at
    /// render time and exposes per-unit status to the
    /// `{{ pond_status_grid /}}` shortcode as static HTML cards.
    /// See `StatusGridConfig` for fields.
    #[serde(default)]
    pub status_grid: Option<StatusGridConfig>,
}

/// Config for the per-pond systemd status card grid.
///
/// Sitegen lists journal `.jsonl` files matching `journal_pattern`, filters
/// the basenames against `unit_globs`, and for each surviving unit runs
/// two SQL queries: a status query (latest event, latest "Started" /
/// exit / "Peak memory usage" lines) and a tail query (most recent
/// `tail_lines` MESSAGE values).  Results are rendered as static HTML
/// cards by the `pond_status_grid` shortcode; refresh requires
/// re-running sitegen.
///
/// When `perf_pattern` is set, sitegen ALSO queries each unit's perf
/// series for the latest `(timer.active, last_run.seconds_ago,
/// timer.interval_s)` tuple.  Those values feed the per-card health
/// classification (green/yellow/red) and Timer/Period/Last-run
/// metadata lines in the rendered HTML.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusGridConfig {
    /// URL pattern resolving to the journal `.jsonl` files written by
    /// the `journal-ingest` factory, e.g.
    /// `"jsonlogs:///logs/journal/*.jsonl"`.
    pub journal_pattern: String,

    /// Shell-style globs (matched against the journal file basename
    /// without the `.jsonl` suffix) selecting which units to render.
    /// Examples: `["user-pond@*.service",
    /// "user-pond-selfmon@*.service"]`.  Empty list disables filtering
    /// (every journal file becomes a card -- usually not what you want).
    #[serde(default)]
    pub unit_globs: Vec<String>,

    /// Number of MESSAGE lines to render in the tail block per card.
    #[serde(default = "default_tail_lines")]
    pub tail_lines: usize,

    /// Optional lookback window (humantime, e.g. `"1d"`, `"12h"`) bounding how
    /// far back the per-unit status/tail scan reads the append-only journal
    /// series. Journal versions whose newest event predates
    /// `now - hot_window` are pruned before the reader materializes them, so
    /// per-render memory stays bounded regardless of retained history. All
    /// history remains on disk and queryable; only this default status render
    /// is windowed. Unset defaults to [`DEFAULT_STATUS_HOT_WINDOW`] (1 day).
    #[serde(default)]
    pub hot_window: Option<String>,

    /// Optional URL pattern (with `{pond}` placeholder) resolving to
    /// the per-pond perf series produced by `measure-pond.sh` +
    /// `sql-derived-series`.  When set, sitegen reads the latest
    /// `(timer.active, last_run.seconds_ago, timer.interval_s)` row
    /// per unit and uses it for health classification.  When the
    /// lookup fails for an individual unit (missing series, unparsable
    /// data, etc.) the card stays at `Health::Unknown` rather than
    /// failing the build.
    ///
    /// `{pond}` is the unit's bare pond name -- the basename with
    /// `user-pond@` / `user-pond-selfmon@` prefix and `.service`
    /// suffix stripped.  Example value:
    /// `"series:///derived/p-{pond}"`.
    #[serde(default)]
    pub perf_pattern: Option<String>,
}

fn default_tail_lines() -> usize {
    10
}

/// Default status-grid lookback window when `hot_window` is unset: one day.
/// Generous relative to the ~1-min selfmon tick, so ok/err events land in the
/// window on healthy ponds.
pub const DEFAULT_STATUS_HOT_WINDOW: std::time::Duration =
    std::time::Duration::from_secs(24 * 60 * 60);

impl StatusGridConfig {
    /// Resolve the configured `hot_window` as whole seconds, defaulting to
    /// [`DEFAULT_STATUS_HOT_WINDOW`]. A malformed duration is a hard error
    /// (surfaced to the caller) rather than a silent default.
    pub fn hot_window_secs(&self) -> Result<i64, tinyfs::Error> {
        let dur = match &self.hot_window {
            Some(s) => humantime::parse_duration(s).map_err(|e| {
                tinyfs::Error::Other(format!("status_grid: invalid hot_window '{}': {}", s, e))
            })?,
            None => DEFAULT_STATUS_HOT_WINDOW,
        };
        Ok(dur.as_secs() as i64)
    }
}

impl SiteConfig {
    /// Rewrite sidebar hrefs when a subsite is mounted at a different base_url.
    ///
    /// Any href starting with `old_base` is rewritten to start with `new_base`.
    pub fn rewrite_sidebar_urls(&mut self, old_base: &str, new_base: &str) {
        fn rewrite(href: &mut String, old: &str, new: &str) {
            if let Some(rest) = href.strip_prefix(old) {
                *href = format!("{}{}", new, rest);
            }
        }

        fn rewrite_children(children: &mut [SidebarChild], old: &str, new: &str) {
            for child in children {
                if let Some(h) = &mut child.href {
                    rewrite(h, old, new);
                }
                rewrite_children(&mut child.children, old, new);
            }
        }

        for entry in &mut self.sidebar {
            match entry {
                SidebarEntry::WithChildren { href, children, .. } => {
                    if let Some(h) = href {
                        rewrite(h, old_base, new_base);
                    }
                    rewrite_children(children, old_base, new_base);
                }
                SidebarEntry::DirectLink { href, .. } => {
                    rewrite(href, old_base, new_base);
                }
                SidebarEntry::Simple(_) => {}
            }
        }
    }
}

/// Site-wide metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SiteMeta {
    pub title: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    /// Canonical site URL for RSS feed links (e.g., "https://casparwater.org").
    /// Required for RSS feed generation; if absent, feed is skipped.
    #[serde(default)]
    pub site_url: Option<String>,
    /// GitHub repository URL shown as an icon in the top bar.
    /// If absent, the GitHub icon is omitted.
    #[serde(default)]
    pub github_url: Option<String>,
    /// Default time-range button selected when a chart first loads, given as
    /// one of the chart.js duration labels such as "1M", "3M" or "24h". When
    /// absent, chart.js falls back to its adaptive default based on the data
    /// span. The value is clamped to the ranges that fit the available data.
    #[serde(default)]
    pub default_range: Option<String>,
}

/// RSS feed configuration (optional section in site.yaml).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedConfig {
    /// Content section to include in the feed (default: "Blog").
    #[serde(default = "default_feed_section")]
    pub section: String,
    /// Which content stage provides pages (default: first content stage).
    #[serde(default)]
    pub content: Option<String>,
    /// Channel description (default: site title).
    #[serde(default)]
    pub description: Option<String>,
}

fn default_feed_section() -> String {
    "Blog".to_string()
}

fn default_base_url() -> String {
    "/".to_string()
}

/// A content stage: globs markdown data files in the pond and reads
/// their frontmatter for metadata (title, weight, slug).
///
/// Content stages are resolved before route expansion. A `content` route
/// references a stage by name, producing one page per matched file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentStage {
    /// Name referenced by content routes (e.g., "pages")
    pub name: String,
    /// Pond glob pattern matching data files (e.g., "/pages/*.md")
    pub pattern: String,
}

/// An export stage: runs `pond export` internally to produce data files
/// that become template context.
///
/// # Automatic Temporal Partitioning
///
/// Parquet files are split into Hive-style temporal partitions automatically.
/// The system discovers resolutions from the matched files (e.g., `res=1h`,
/// `res=24h`) and computes the right partition level for each resolution
/// so that any viewport loads at most 2 files.
///
/// `target_points` controls the trade-off: higher values mean finer partitions
/// (more data per file, fewer files). Default is 1500, matching a typical
/// screen width in pixels.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportStage {
    /// Name referenced by routes (e.g., "params", "sites")
    pub name: String,
    /// Pond glob pattern or format-provider URL pattern.
    ///
    /// Bare paths (e.g., `/reduced/single_param/*/*.series`) are resolved
    /// directly as queryable pond files.
    ///
    /// URL-scheme patterns (e.g., `jsonlogs:///logs/watershop/*.jsonl`)
    /// are resolved through the format provider registry, allowing raw
    /// data files to be queried via DataFusion.
    pub pattern: String,
    /// Target data points per screen width for partition sizing.
    /// Typical value: 1000-2000. Default: 1500.
    /// Controls how parquet files are temporally partitioned per resolution.
    #[serde(default = "default_target_points")]
    pub target_points: u64,
    /// Name of the timestamp column for temporal partitioning.
    /// Default: "timestamp". Override for format-provider patterns where
    /// the timestamp column has a different name (e.g., "__REALTIME_TIMESTAMP"
    /// for journald logs).
    #[serde(default = "default_timestamp_column")]
    pub timestamp_column: String,
}

/// A named, transport-independent report.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportConfig {
    /// Heading shown in rendered report artifacts.
    pub title: String,
    /// Optional reminder or explanatory text below the heading.
    #[serde(default)]
    pub message: Option<String>,
    /// Trailing time window, such as `7d` or `168h`.
    pub window: String,
    /// Include the pond's allocated storage size.
    #[serde(default)]
    pub include_pond_size: bool,
    /// Ordered data sections.
    pub sections: Vec<ReportSectionConfig>,
}

/// A numeric series included in a report.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportSectionConfig {
    /// Optional heading override. Defaults to the first capture's configured
    /// site label, then to a title-cased capture value.
    #[serde(default)]
    pub title: Option<String>,
    /// Name of an entry in `exports`.
    pub export: String,
    /// Exact wildcard captures selecting one file from the export pattern.
    #[serde(default)]
    pub captures: Vec<String>,
    /// Numeric column to summarize and chart.
    pub value: String,
    /// Display unit appended to numeric values.
    pub unit: String,
    /// Summary calculation.
    #[serde(default)]
    pub summary: ReportSummary,
    /// Render a static PNG line chart.
    #[serde(default)]
    pub chart: bool,
    /// Relative or absolute link to the corresponding site page.
    pub href: String,
}

/// Numeric aggregation shown for a report section.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReportSummary {
    #[default]
    Range,
    Sum,
}

fn default_target_points() -> u64 {
    1500
}

fn default_timestamp_column() -> String {
    "timestamp".to_string()
}

/// Browser data explorer configuration.
///
/// Lists which export stages to surface as queryable datasets in the explorer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExploreConfig {
    /// Datasets exposed in the explorer, in display order.
    pub datasets: Vec<ExploreDataset>,
    /// URL of the explorer page, joined relative to `base_url` (e.g.
    /// `explore/`; a leading slash is tolerated). When set, chart pages render
    /// an "Explore this data" button that opens this page pre-loaded with the
    /// chart's current files and time window. Absent means no cross-link.
    #[serde(default)]
    pub url: Option<String>,
}

/// One explorer dataset: an export stage surfaced as a named DuckDB view.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExploreDataset {
    /// Name of the `exports:` stage providing this dataset's parquet files.
    pub export: String,
    /// DuckDB view/table name the explorer registers (SQL identifier).
    pub table: String,
    /// Human-readable label shown in the dataset picker.
    pub label: String,
}

/// A route in the hierarchical route tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    /// Route name (for logging/debugging)
    pub name: String,
    /// "static" or "template"
    #[serde(rename = "type")]
    pub route_type: RouteType,
    /// URL slug -- may contain `$0`, `$1` for template routes
    pub slug: String,
    /// Path to markdown page (e.g., "/site/index.md")
    #[serde(default)]
    pub page: Option<String>,
    /// For template routes: which export stage provides data
    #[serde(default)]
    pub export: Option<String>,
    /// Optional secondary export whose files are handed to the page as
    /// annotation data (e.g. leak/no-leak interval periods drawn as a shaded
    /// chart background). Resolved pond-wide (all files of the named export),
    /// so every page under the route shares the same annotations.
    #[serde(default)]
    pub annotations: Option<String>,
    /// For content routes: which content stage provides pages
    #[serde(default)]
    pub content: Option<String>,
    /// Nested child routes
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
}

/// Route type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RouteType {
    Static,
    Template,
    Content,
}

/// Static asset to copy verbatim.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticAsset {
    pub pattern: String,
}

/// A sidebar entry: either a simple label or a label with sub-navigation children.
///
/// Simple entries are plain strings matched by page title.
/// Match priority: exact title > case-insensitive exact > substring.
/// Entries with children render a parent link plus nested sub-items
/// that expand when the parent or any child is the current page.
///
/// ```yaml
/// sidebar:
///   - "Political"                    # simple
///   - label: "Monitoring"            # with children
///     children:
///       - label: "Well Depth"
///         href: "/data/well-depth.html"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SidebarEntry {
    /// Label with nested sub-navigation items and optional parent href.
    WithChildren {
        label: String,
        #[serde(default)]
        href: Option<String>,
        children: Vec<SidebarChild>,
    },
    /// Direct link with label and href (no children, no page matching).
    DirectLink { label: String, href: String },
    /// Simple label matched by page title (exact > case-insensitive > substring).
    Simple(String),
}

impl SidebarEntry {
    /// The display label for this entry.
    pub fn label(&self) -> &str {
        match self {
            SidebarEntry::Simple(s) => s,
            SidebarEntry::WithChildren { label, .. } => label,
            SidebarEntry::DirectLink { label, .. } => label,
        }
    }

    /// Children, if any.
    pub fn children(&self) -> &[SidebarChild] {
        match self {
            SidebarEntry::Simple(_) => &[],
            SidebarEntry::WithChildren { children, .. } => children,
            SidebarEntry::DirectLink { .. } => &[],
        }
    }

    /// Direct href, if specified.
    pub fn href(&self) -> Option<&str> {
        match self {
            SidebarEntry::Simple(_) => None,
            SidebarEntry::WithChildren { href, .. } => href.as_deref(),
            SidebarEntry::DirectLink { href, .. } => Some(href),
        }
    }
}

/// A child item in a sidebar entry with sub-navigation.
///
/// Supports recursive nesting: a child may have its own children, enabling
/// arbitrary-depth sidebar trees. `href` is optional; a child without an
/// href but with children renders as a section heading that links to its
/// first descendant leaf.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SidebarChild {
    /// Display label
    pub label: String,
    /// Direct URL path (e.g., "/data/well-depth.html").
    /// Optional: parent-only entries (those with children) may omit href.
    #[serde(default)]
    pub href: Option<String>,
    /// Nested sub-navigation items. Empty for leaf entries.
    #[serde(default)]
    pub children: Vec<SidebarChild>,
}

/// A sub-site to generate recursively from an imported pond.
///
/// ```yaml
/// subsites:
///   - name: "noyo"
///     path: "/sources/noyo"
///     config: "/system/etc/90-sitegen"
///     base_url: "/noyo/"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubsiteConfig {
    /// Display name (used for logging and output directory fallback).
    pub name: String,
    /// Pond path to the import mount point (e.g., "/sources/noyo").
    pub path: String,
    /// Path to the sitegen config within the foreign pond (absolute,
    /// resolved relative to the mount point via effective_root).
    pub config: String,
    /// Override the sub-site's base_url for the combined site.
    /// If omitted, the sub-site's own base_url is used.
    #[serde(default)]
    pub base_url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let yaml = r#"
site:
  title: "Test Site"

exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
    routes:
      - name: "param"
        type: template
        slug: "$0"
        export: "params"
        page: "/site/param.md"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.site.title, "Test Site");
        assert_eq!(config.site.base_url, "/");
        assert_eq!(config.exports.len(), 1);
        assert_eq!(config.exports[0].name, "params");
        assert_eq!(config.exports[0].target_points, 1500); // default
        assert_eq!(config.routes.len(), 1);
        assert_eq!(config.routes[0].routes.len(), 1);
        assert_eq!(config.routes[0].routes[0].route_type, RouteType::Template);
    }

    #[test]
    fn parse_content_config() {
        let yaml = r#"
site:
  title: "Caspar Water"

content:
  - name: "pages"
    pattern: "/pages/*.md"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
    routes:
      - name: "pages"
        type: content
        slug: ""
        content: "pages"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.content.len(), 1);
        assert_eq!(config.content[0].name, "pages");
        assert_eq!(config.content[0].pattern, "/pages/*.md");
        assert_eq!(config.routes[0].routes[0].route_type, RouteType::Content);
        assert_eq!(config.routes[0].routes[0].content.as_deref(), Some("pages"));
    }

    #[test]
    fn parse_config_with_target_points() {
        let yaml = r#"
site:
  title: "Custom Points"

exports:
  - name: "metrics"
    pattern: "/reduced/*/*/*.series"
    target_points: 2000
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.exports[0].target_points, 2000);
    }

    #[test]
    fn parse_minimal_config_no_site_url() {
        let yaml = r#"
site:
  title: "Test"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert!(config.site.site_url.is_none());
        assert!(config.feed.is_none());
    }

    #[test]
    fn parse_config_with_site_url_and_feed() {
        let yaml = r#"
site:
  title: "Test Blog"
  site_url: "https://example.com"

feed:
  section: "Updates"
  description: "Latest updates"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.site.site_url.as_deref(), Some("https://example.com"));
        let feed = config.feed.expect("feed config");
        assert_eq!(feed.section, "Updates");
        assert_eq!(feed.description.as_deref(), Some("Latest updates"));
        assert!(feed.content.is_none());
    }

    #[test]
    fn parse_feed_defaults() {
        let yaml = r#"
site:
  title: "Test"
  site_url: "https://example.com"

feed: {}
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        let feed = config.feed.expect("feed config");
        assert_eq!(feed.section, "Blog");
        assert!(feed.content.is_none());
        assert!(feed.description.is_none());
    }

    #[test]
    fn parse_sidebar_mixed_entries() {
        let yaml = r#"
site:
  title: "Test"

sidebar:
  - "Political"
  - "Blog"
  - label: "Monitoring"
    children:
      - label: "Well Depth"
        href: "/data/well-depth.html"
      - label: "Tank Level"
        href: "/data/tank-level.html"
  - "Thanks"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.sidebar.len(), 4);
        assert_eq!(config.sidebar[0].label(), "Political");
        assert!(config.sidebar[0].children().is_empty());
        assert_eq!(config.sidebar[2].label(), "Monitoring");
        assert_eq!(config.sidebar[2].children().len(), 2);
        assert_eq!(config.sidebar[2].children()[0].label, "Well Depth");
        assert_eq!(
            config.sidebar[2].children()[0].href.as_deref(),
            Some("/data/well-depth.html")
        );
        assert_eq!(config.sidebar[3].label(), "Thanks");
    }

    #[test]
    fn parse_config_with_subsites() {
        let yaml = r#"
site:
  title: "Caspar Infrastructure"
  base_url: "/"

subsites:
  - name: "noyo"
    path: "/sources/noyo"
    config: "/system/etc/90-sitegen"
    base_url: "/noyo/"
  - name: "water"
    path: "/sources/water"
    config: "/etc/site.yaml"
  - name: "septic"
    path: "/sources/septic"
    config: "/etc/site.yaml"
    base_url: "/septic/"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/system/site/index.md"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.subsites.len(), 3);
        assert_eq!(config.subsites[0].name, "noyo");
        assert_eq!(config.subsites[0].path, "/sources/noyo");
        assert_eq!(config.subsites[0].config, "/system/etc/90-sitegen");
        assert_eq!(config.subsites[0].base_url.as_deref(), Some("/noyo/"));
        assert_eq!(config.subsites[1].name, "water");
        assert!(config.subsites[1].base_url.is_none());
        assert_eq!(config.subsites[2].base_url.as_deref(), Some("/septic/"));
        // Existing fields still work
        assert_eq!(config.routes.len(), 1);
        assert!(config.exports.is_empty());
    }

    #[test]
    fn parse_config_no_subsites_default() {
        let yaml = r#"
site:
  title: "Standalone Site"
"#;
        let config: SiteConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert!(config.subsites.is_empty());
    }
}
