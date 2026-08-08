// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Built-in shortcodes for sitegen markdown templates.
//!
//! Shortcodes are `{{ name key="value" }}` directives in markdown that expand
//! to HTML at build time. They are the serialization boundary between typed
//! Rust data (ExportedFile structs) and client-side HTML/JS.
//!
//! Uses our own shortcode parser (in `crate::markdown`) with closures that
//! capture an `Arc<ShortcodeContext>` with Watertown data.

use crate::config::{SidebarChild, SidebarEntry};
use crate::markdown::{ShortcodeArgs, Shortcodes};
use crate::routes::ContentPage;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Build the canonical href for a content page.
///
/// Uses the page's `url_path` (recorded by route expansion) when
/// available and falls back to `/{slug}` for pages that aren't bound
/// to a route. The result is `<base_url><path>.html` with `base_url`
/// trimmed of any trailing slash.
fn content_page_href(base_url: &str, page: &ContentPage) -> String {
    let base = base_url.trim_end_matches('/');
    let path = if page.url_path.is_empty() {
        format!("/{}", page.slug)
    } else {
        page.url_path.clone()
    };
    if base.is_empty() {
        format!("{}.html", path)
    } else {
        format!("{}{}.html", base, path)
    }
}

/// One exported data file with capture groups and time range.
///
/// This is the typed struct that shortcodes receive directly -- no JSON
/// serialization boundary between export and rendering.
#[derive(Debug, Clone)]
pub struct ExportedFile {
    /// Pond path to the data file
    pub path: String,
    /// Relative URL to exported .parquet file (e.g., "data/Temperature/res=1h.parquet")
    pub file: String,
    /// Capture groups from pattern matching ($0, $1, ...)
    pub captures: Vec<String>,
    /// Temporal partition values (e.g., {"year": "2025", "month": "01"})
    pub temporal: BTreeMap<String, String>,
    /// UTC timestamp for start of data (seconds since epoch)
    pub start_time: i64,
    /// UTC timestamp for end of data (seconds since epoch)
    pub end_time: i64,
}

/// All exported files for one export stage, grouped by $0 value.
#[derive(Debug, Clone)]
pub struct ExportContext {
    /// Files grouped by the $0 capture value
    pub by_key: BTreeMap<String, Vec<ExportedFile>>,
    /// Union of column names seen across this stage's exported parquet files.
    /// Used by the data explorer to present a dataset's schema without a
    /// round-trip query. Empty if no schema was captured.
    pub columns: Vec<String>,
}

/// One pond's "systemctl status"-style summary, computed at sitegen
/// render time by `factory::run_status_grid_queries` and rendered as
/// an HTML card by the `pond_status_grid` shortcode.
///
/// All time fields are microseconds since epoch (the units used by
/// systemd's `__REALTIME_TIMESTAMP`).  `peak_rss_bytes` is parsed from
/// the pond CLI's `Peak memory usage: NN.NN MB` exit line.  Any field
/// may be `None` if the corresponding event hasn't been seen yet.
#[derive(Debug, Clone)]
pub struct PondStatus {
    /// Systemd unit name (e.g. `pond@water-prod.service`,
    /// `pond-selfmon@watershop-selfmon.service`).
    pub unit: String,
    /// Most-recent journal entry seen for this unit (microseconds).
    pub last_seen_us: Option<i64>,
    /// Most-recent successful run (microseconds + the pond
    /// `Run summary ... outcome=ok` MESSAGE that anchored it).  These
    /// are emitted by pond on every run, so they're authoritative for
    /// per-unit journals (unlike systemd's "Started" lines, which live
    /// under `_SYSTEMD_UNIT=init.scope`).
    pub last_ok_us: Option<i64>,
    pub last_ok_msg: Option<String>,
    /// Most-recent failed run (microseconds + the pond
    /// `Run summary ... outcome=err` MESSAGE).
    pub last_err_us: Option<i64>,
    pub last_err_msg: Option<String>,
    /// Peak resident-set size of the most recent run, in bytes,
    /// parsed from "Peak memory usage: NN.NN MB" lines.  Reported in
    /// bytes for consistency with semconv's `By` unit.
    pub peak_rss_bytes: Option<u64>,
    /// Tail of the most-recent N MESSAGE lines (oldest -> newest)
    /// regardless of run boundary.
    pub tail_messages: Vec<String>,
    /// Latest value of `timer.active` from the per-pond perf series
    /// (1/0).  `Some(false)` flips the card to red because no future
    /// runs are scheduled.  `None` when the perf-series lookup
    /// failed or `StatusGridConfig.perf_pattern` is unset.
    pub timer_active: Option<bool>,
    /// Latest value of `last_run.seconds_ago` from the perf series.
    /// `-1` means "no parseable exit timestamp" (never run, currently
    /// running, or systemctl parse failure) -- not a reliable
    /// "currently running" signal on its own; the renderer surfaces
    /// it as a "Last run" row but the health classifier ignores it.
    pub last_run_seconds_ago: Option<i64>,
    /// Latest value of `timer.interval_s` from the perf series
    /// (`OnUnitActiveSec` in seconds).  Combined with `run_wall_s` it
    /// drives the staleness threshold for the yellow/green boundary.
    /// `None` or `Some(0)` keeps the card at `Health::Unknown` for
    /// staleness.
    pub timer_interval_s: Option<i64>,
    /// Latest value of `run.wall_s` from the perf series -- the full
    /// wall-clock duration of the unit's most recent service run
    /// (`ExecMainExitTimestamp - ExecMainStartTimestamp`), in seconds.
    /// The staleness threshold is `2 * (timer_interval_s + run_wall_s)`
    /// so a unit whose run takes longer than its timer interval is not
    /// misreported as overdue.  `None` or `Some(0)` falls back to a
    /// threshold of `2 * timer_interval_s`.
    pub run_wall_s: Option<i64>,
    /// Computed health classification (green/yellow/red/unknown).
    /// Assigned by `factory::run_status_grid_queries` from `classify`.
    pub health: Health,
}

/// Three-color health classification for a pond status card, computed
/// at sitegen render time from the journal- and perf-series-derived
/// fields on `PondStatus`.
///
/// Renders as a `health-{green,yellow,red,unknown}` CSS class on the
/// card section so the page stylesheet can colour the left border /
/// background per state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    /// Last successful run is recent (within the staleness threshold,
    /// `2 * (timer_interval_s + run_wall_s)`) and there is no fresher
    /// failure.
    Green,
    /// Last successful run is older than the staleness threshold,
    /// `2 * (timer_interval_s + run_wall_s)` -- the unit is overdue.
    Yellow,
    /// Most recent terminal event was a failure
    /// (`last_err_us > last_ok_us` or no `last_ok_us` at all), or the
    /// timer is inactive.
    Red,
    /// Insufficient data to classify -- typically means
    /// `perf_pattern` is unset, the perf-series lookup failed, or
    /// the unit has no `timer.interval_s` value yet.
    Unknown,
}

impl Health {
    /// CSS class suffix for HTML rendering -- `health-green` etc.
    pub fn css_class(self) -> &'static str {
        match self {
            Health::Green => "health-green",
            Health::Yellow => "health-yellow",
            Health::Red => "health-red",
            Health::Unknown => "health-unknown",
        }
    }
}

/// Compute the health classification for a single pond status card.
///
/// Pure function over the inputs so it is easy to unit-test.  Inputs
/// mirror the corresponding fields on `PondStatus`; `now_us` is the
/// page-build wall clock in microseconds since epoch (used for the
/// staleness comparison).
///
/// Rules, in evaluation order:
///   1. `Red` when `timer_active == Some(false)` (the timer unit is
///      stopped/disabled, so no further runs are scheduled).
///   2. `Red` when `last_err_us` is Some and either `last_ok_us` is
///      None or `last_err_us > last_ok_us` (most recent terminal
///      event was a failure).
///   3. `Yellow` when we know the interval and `last_ok_us` is older
///      than the staleness threshold `2 * (timer_interval_s +
///      run_wall_s)` ("overdue").  Including the run's own wall-clock
///      duration keeps a unit whose run takes longer than its timer
///      interval from being misreported as overdue, since with
///      `OnUnitActiveSec` the true period between successful runs is
///      `interval + run duration`.
///   4. `Green` when we have a `last_ok_us` and a positive
///      `timer_interval_s` and the staleness check above passed.
///   5. `Unknown` otherwise (e.g. perf data missing, no successful
///      run yet, interval not configured).
pub fn classify(
    now_us: i64,
    last_ok_us: Option<i64>,
    last_err_us: Option<i64>,
    timer_active: Option<bool>,
    timer_interval_s: Option<i64>,
    run_wall_s: Option<i64>,
) -> Health {
    if matches!(timer_active, Some(false)) {
        return Health::Red;
    }
    if let Some(err_us) = last_err_us
        && last_ok_us.is_none_or(|ok_us| err_us > ok_us)
    {
        return Health::Red;
    }
    let Some(interval_s) = timer_interval_s.filter(|&s| s > 0) else {
        return Health::Unknown;
    };
    let Some(ok_us) = last_ok_us else {
        return Health::Unknown;
    };
    // True period between successful runs under `OnUnitActiveSec` is
    // `interval + run duration`; allow two periods before overdue.
    let run_wall_s = run_wall_s.filter(|&s| s > 0).unwrap_or(0);
    let stale_threshold_us = interval_s
        .saturating_add(run_wall_s)
        .saturating_mul(2)
        .saturating_mul(1_000_000);
    if now_us.saturating_sub(ok_us) > stale_threshold_us {
        Health::Yellow
    } else {
        Health::Green
    }
}

/// Context passed to shortcodes during rendering.
///
/// Each template page gets its own `ShortcodeContext`, wrapped in `Arc` and
/// captured by the shortcode closures. This is the bridge between Watertown's
/// typed data and the shortcode expansion engine.
#[derive(Debug, Clone)]
pub struct ShortcodeContext {
    /// Capture group values for this page ($0, $1, ...)
    pub captures: Vec<String>,
    /// All exported data files for this page (filtered to this $0 value)
    pub datafiles: Vec<ExportedFile>,
    /// Named collections for nav-list (e.g., "params" -> ["Temperature", "DO", ...])
    pub collections: BTreeMap<String, Vec<String>>,
    /// Content page lists for content_nav (e.g., "pages" -> [ContentPage, ...])
    pub content_pages: BTreeMap<String, Vec<ContentPage>>,
    /// Site title from config
    pub site_title: String,
    /// Current URL path
    pub current_path: String,
    /// Breadcrumb segments: (label, href)
    pub breadcrumbs: Vec<(String, String)>,
    /// Base URL prefix for all generated links (e.g., "/noyo-harbor/")
    pub base_url: String,
    /// Ordered sidebar entries from site.yaml.
    /// When non-empty, `content_nav` renders pills in this order,
    /// with optional sub-navigation for entries that have children.
    pub sidebar_sections: Vec<SidebarEntry>,
    /// Metric instrument-kind registry passed through from
    /// SiteConfig.metric_registry.  Keyed by `<param>.<unit>` (the
    /// chart.js chartKey for wide-column data).  Values: counter |
    /// updowncounter | gauge.  Empty map = all metrics treated as
    /// gauges (no transforms).
    pub metric_registry: BTreeMap<String, String>,

    /// Per-chart caption strings keyed by `<param>.<unit>` (matches the
    /// chart.js chartKey).  Plumbed through from
    /// `SiteConfig.metric_captions`; empty map = no captions rendered.
    pub metric_captions: BTreeMap<String, String>,

    /// Per-pond status records produced by
    /// `factory::run_status_grid_queries` when `SiteConfig.status_grid`
    /// is set.  The `pond_status_grid` shortcode renders these as an
    /// HTML card grid; an empty list renders an empty-state message
    /// and `None` (the `status_grid` config absent) renders nothing.
    pub pond_statuses: Option<Vec<PondStatus>>,

    /// UTC timestamp at which the page was rendered, used by the
    /// `pond_status_grid` shortcode to bake a "last refreshed" line
    /// into the static HTML output.  Format: ISO 8601, second
    /// resolution (e.g. `2026-04-27T21:00:00Z`).
    pub generated_at: String,

    /// Optional default chart time-range label (e.g. "1M") plumbed through
    /// from `SiteConfig.site.default_range`.  Emitted as `data-default-range`
    /// on the chart container so chart.js can honor it; `None` leaves chart.js
    /// to choose its adaptive default.
    pub default_range: Option<String>,

    /// Display-name overrides for capture values, plumbed through from
    /// `SiteConfig.labels`.  The `cap{N}` shortcodes (`{{ $0 }}` etc.) map
    /// their raw capture through this table so headings show a friendly name
    /// (e.g. `DO` -> `Dissolved Oxygen`); missing keys emit the raw value.
    pub labels: BTreeMap<String, String>,

    /// Resolved data-explorer datasets for this page, built from
    /// `SiteConfig.explore` against the export stages.  When non-empty the
    /// `explore` shortcode emits one manifest entry per dataset (multi-dataset
    /// picker); when empty the shortcode falls back to a single dataset built
    /// from the page's `datafiles`.
    pub explore_datasets: Vec<ResolvedExploreDataset>,

    /// Site-relative URL of the explorer page (already `base_url`-prefixed),
    /// from `SiteConfig.explore.url`. When `Some`, the `chart` shortcode emits
    /// it as `data-explore-url` so chart.js can offer an "Explore this data"
    /// cross-link that hands the current files + window to the explorer.
    pub explore_url: Option<String>,

    /// Optional annotation data files (a secondary export resolved pond-wide)
    /// drawn as a shaded chart background, e.g. leak/no-leak interval periods.
    /// Plumbed through from `PageJob.annotations`; empty when the route
    /// declares no `annotations` export. Renderers that support annotations
    /// (see `VizRenderer::include_annotations`) emit these as an inline
    /// `chart-annotations` manifest.
    pub annotations: Vec<ExportedFile>,
}

/// A data-explorer dataset resolved from `SiteConfig.explore` against an
/// export stage's `ExportContext`.  Carries the flattened file list and the
/// stage's unioned column names so the explorer can register the dataset and
/// show its schema without a round-trip query.
#[derive(Debug, Clone)]
pub struct ResolvedExploreDataset {
    /// SQL-safe table/view name the dataset registers as (e.g. "reduced").
    pub table: String,
    /// Human-friendly label shown in the dataset picker.
    pub label: String,
    /// All parquet files comprising this dataset, across all $0 keys.
    pub files: Vec<ExportedFile>,
    /// Union of column names across the dataset's files.
    pub columns: Vec<String>,
}

/// Build a `Shortcodes` instance with all built-in shortcodes registered.
///
/// Each closure captures `Arc<ShortcodeContext>` -- no thread-locals, no RefCell.
/// This function is called once per page, creating a fresh shortcodes set
/// with the page's specific context.
pub fn register_shortcodes(ctx: Arc<ShortcodeContext>) -> Shortcodes {
    let mut shortcodes = Shortcodes::new();

    // {{ cap0 }}, {{ cap1 }}, ... -- capture group values
    //
    // Design doc uses `{{ $0 }}` syntax, but shortcode name validation
    // requires ^[A-Za-z_][0-9A-Za-z_]+$. So templates use `{{ cap0 }}` and
    // `preprocess_variables()` rewrites `{{ $0 }}` -> `{{ cap0 }}` before rendering.
    for i in 0..10usize {
        let c = ctx.clone();
        let name = format!("cap{}", i);
        shortcodes.register(&name, move |_args: &ShortcodeArgs| {
            let raw = c.captures.get(i).cloned().unwrap_or_default();
            c.labels.get(&raw).cloned().unwrap_or(raw)
        });
    }

    // {{ chart }} -- emit chart container with inline datafile manifest as JSON.
    // Client-side chart.js reads the JSON to load parquet via DuckDB-WASM.
    {
        let c = ctx.clone();
        shortcodes.register("chart", move |args: &ShortcodeArgs| {
            render_chart(
                &c.datafiles,
                &c.metric_registry,
                &c.metric_captions,
                args.get_str("range").or(c.default_range.as_deref()),
                c.explore_url.as_deref(),
                &c.annotations,
            )
        });
    }

    // {{ overlay_chart }} -- emit overlay chart container for pump cycle analysis.
    // Client-side overlay.js renders drawdown/recovery overlays and Horner plots.
    {
        let c = ctx.clone();
        shortcodes.register("overlay_chart", move |_args: &ShortcodeArgs| {
            render_overlay_chart(&c.datafiles, c.explore_url.as_deref())
        });
    }

    // {{ log_viewer }} -- emit log viewer container with inline datafile manifest.
    // Client-side log-viewer.js reads the JSON to query parquet via DuckDB-WASM.
    {
        let c = ctx.clone();
        shortcodes.register("log_viewer", move |_args: &ShortcodeArgs| {
            render_log_viewer(&c.datafiles)
        });
    }

    // {{ viz renderer="chart|overlay|logs" /}} -- unified visualization
    // container. Emits the container div + inline parquet manifest that the
    // named client-side module binds to. This is the general, extensible seam
    // for telemetry visualizations; the {{ chart }}, {{ overlay_chart }} and
    // {{ log_viewer }} shortcodes above are thin aliases over the same path.
    {
        let c = ctx.clone();
        shortcodes.register("viz", move |args: &ShortcodeArgs| {
            let name = args.get_str("renderer").unwrap_or("chart");
            match viz_renderer(name) {
                Some(r) => render_viz(
                    r,
                    &c.datafiles,
                    &c.metric_registry,
                    &c.metric_captions,
                    args.get_str("range").or(c.default_range.as_deref()),
                    c.explore_url.as_deref(),
                    &c.annotations,
                ),
                None => format!(
                    "<div class=\"chart-container\"><p>Unknown viz renderer: {}</p></div>",
                    html_escape(name)
                ),
            }
        });
    }

    // {{ explore table="reduced" label="Reduced tiers" /}} -- emit the data
    // explorer container with a `datasets` manifest built from this page's
    // exported files. Client-side explore.js registers the parquet as a DuckDB
    // view and offers a SQL playground over it.
    {
        let c = ctx.clone();
        shortcodes.register("explore", move |args: &ShortcodeArgs| {
            if !c.explore_datasets.is_empty() {
                render_explore_multi(&c.explore_datasets)
            } else {
                let table = args.get_str("table").unwrap_or("data");
                let label = args.get_str("label").unwrap_or(table);
                render_explore(&c.datafiles, table, label)
            }
        });
    }

    // {{ nav_list collection="params" base="/params" /}} -- link list for a collection
    {
        let c = ctx.clone();
        shortcodes.register("nav_list", move |args: &ShortcodeArgs| {
            let collection = args.get_str("collection").unwrap_or("");
            let base = args
                .get_str("base")
                .or_else(|| args.get_str("path"))
                .unwrap_or("");
            let prefixed_base = prefix_with_base_url(&c.base_url, base);
            render_nav_list(&c, collection, &prefixed_base)
        });
    }

    // {{ breadcrumb }} -- breadcrumb trail
    {
        let c = ctx.clone();
        shortcodes.register("breadcrumb", move |_args: &ShortcodeArgs| {
            render_breadcrumb(&c.breadcrumbs)
        });
    }

    // {{ site_title }} -- site title from config
    {
        let c = ctx.clone();
        shortcodes.register("site_title", move |_args: &ShortcodeArgs| {
            c.site_title.clone()
        });
    }

    // {{ base_url }} -- base URL for use in markdown links: [Home]({{ base_url /}})
    {
        let c = ctx.clone();
        shortcodes.register("base_url", move |_args: &ShortcodeArgs| c.base_url.clone());
    }

    // {{ content_nav content="pages" /}} -- navigation list for content pages
    // Renders titles sorted by weight with active-page highlighting.
    {
        let c = ctx.clone();
        shortcodes.register("content_nav", move |args: &ShortcodeArgs| {
            let content_name = args.get_str("content").unwrap_or("");
            render_content_nav(&c, content_name)
        });
    }

    // {{ figure src="/img/photo.jpg" caption="..." float="right" /}}
    // Renders a <figure> with optional float positioning.
    {
        let c = ctx.clone();
        shortcodes.register("figure", move |args: &ShortcodeArgs| {
            render_figure(&c, args)
        });
    }

    // {{ blog_grid content="pages" section="Blog" /}}
    // Renders a responsive card grid of blog posts filtered by section.
    {
        let c = ctx.clone();
        shortcodes.register("blog_grid", move |args: &ShortcodeArgs| {
            let content_name = args.get_str("content").unwrap_or("");
            let section = args.get_str("section").unwrap_or("");
            render_blog_grid(&c, content_name, section)
        });
    }

    // {{ pond_status_grid /}} -- render the per-pond systemctl-style
    // status cards baked at sitegen time from journal data.  See
    // `render_pond_status_grid` for HTML structure; the data source is
    // `factory::run_status_grid_queries` driven by
    // `SiteConfig.status_grid`.
    {
        let c = ctx.clone();
        shortcodes.register("pond_status_grid", move |_args: &ShortcodeArgs| {
            render_pond_status_grid(c.pond_statuses.as_deref(), &c.generated_at)
        });
    }

    shortcodes
}

/// Pre-process markdown, rewriting design-doc syntax into valid shortcode names.
///
/// Maudit requires shortcode names matching `^[A-Za-z_][0-9A-Za-z_]+$`, so:
/// - `{{ $0 }}` -> `{{ cap0 }}`
/// - `{{ $1 }}` -> `{{ cap1 }}`
/// - `{{ nav-list ... }}` -> `{{ nav_list ... }}`
/// - `{{ site-title }}` -> `{{ site_title }}`
///
/// Also rewrites the YAML frontmatter variable references.
pub fn preprocess_variables(content: &str) -> String {
    // Only replace shortcode names within {{ }} delimiters to avoid
    // corrupting prose that happens to contain these strings.
    let mut result = String::with_capacity(content.len());
    let mut rest = content;

    while let Some(start) = rest.find("{{") {
        result.push_str(&rest[..start]);
        if let Some(end) = rest[start..].find("}}") {
            let block = &rest[start..start + end + 2];
            let replaced = block
                .replace("{{ $0 }}", "{{ cap0 /}}")
                .replace("{{ $1 }}", "{{ cap1 /}}")
                .replace("{{ $2 }}", "{{ cap2 /}}")
                .replace("{{ $3 }}", "{{ cap3 /}}")
                .replace("nav-list", "nav_list")
                .replace("site-title", "site_title")
                .replace("content-nav", "content_nav")
                .replace("base-url", "base_url")
                .replace("blog-grid", "blog_grid")
                .replace("overlay-chart", "overlay_chart");
            result.push_str(&replaced);
            rest = &rest[start + end + 2..];
        } else {
            result.push_str(&rest[start..]);
            rest = "";
        }
    }
    result.push_str(rest);
    result
}

// ---------------------------------------------------------------------------
// Shortcode renderers
// ---------------------------------------------------------------------------

/// Prepend base_url to an absolute path.
///
/// If `base_url` is `/` (default), returns `path` unchanged.
/// If `base_url` is `/noyo-harbor/`, returns `/noyo-harbor/params` for `/params`.
fn prefix_with_base_url(base_url: &str, path: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.is_empty() || base == "/" {
        return path.to_string();
    }
    if path.starts_with('/') {
        format!("{}{}", base, path)
    } else {
        format!("{}/{}", base, path)
    }
}

/// Serialize a page's exported parquet manifest to the JSON array that every
/// client-side visualization module consumes (chart.js, overlay.js,
/// log-viewer.js, and future telemetry renderers). One shape, one place.
fn datafiles_manifest_json(datafiles: &[ExportedFile]) -> String {
    let files_json: Vec<serde_json::Value> = datafiles
        .iter()
        .map(|f| {
            serde_json::json!({
                "path": f.path,
                "file": f.file,
                "captures": f.captures,
                "temporal": f.temporal,
                "start_time": f.start_time,
                "end_time": f.end_time,
            })
        })
        .collect();
    serde_json::to_string(&files_json).unwrap_or_else(|_| "[]".to_string())
}

/// Static description of a client-side visualization renderer: which DOM
/// container the matching JS module binds to, which optional inline data blocks
/// and attributes it consumes, and the message shown when the page exported no
/// data.
///
/// This descriptor is the single seam that makes telemetry visualizations
/// extensible through the `{{ viz }}` shortcode. Adding a new plot is:
///   1. a `VizRenderer` entry + a `viz_renderer()` arm here,
///   2. a JS module that binds to `id` / `data_class`,
///   3. a `<script type="module">` tag in the page layout, and
///   4. `{{ viz renderer="<id>" /}}` in the template.
///
/// No bespoke Rust rendering code is written per plot -- `render_viz` emits the
/// container for all of them.
struct VizRenderer {
    /// Container element `id`, and the `renderer=` value that selects it.
    id: &'static str,
    /// Class on the container `<div>` (empty string omits the attribute).
    container_class: &'static str,
    /// Class on the inline JSON manifest `<script>` the module reads.
    data_class: &'static str,
    /// Emit the metric registry + captions blocks (chart.js consumes these).
    include_registry: bool,
    /// Emit the `data-default-range` attribute (chart.js initial window).
    include_default_range: bool,
    /// Emit the `data-explore-url` attribute ("Explore this data" cross-link).
    include_explore: bool,
    /// Emit an inline `chart-annotations` manifest (a secondary interval export
    /// drawn as a shaded background) when the page carries annotation files.
    include_annotations: bool,
    /// Message rendered inside the container when there are no datafiles.
    empty_msg: &'static str,
}

/// Generic time-series charts (chart.js): metric registry/captions, a default
/// window, and a per-chart explore cross-link.
const VIZ_CHART: VizRenderer = VizRenderer {
    id: "chart",
    container_class: "chart-container",
    data_class: "chart-data",
    include_registry: true,
    include_default_range: true,
    include_explore: true,
    include_annotations: true,
    empty_msg: "No data files available.",
};

/// Bespoke multi-plot analysis (overlay.js): its own DOM id, an explore
/// cross-link, no metric registry.
const VIZ_OVERLAY: VizRenderer = VizRenderer {
    id: "overlay-chart",
    container_class: "chart-container",
    data_class: "overlay-data",
    include_registry: false,
    include_default_range: false,
    include_explore: true,
    include_annotations: false,
    empty_msg: "No data files available.",
};

/// Raw log/table viewer (log-viewer.js): manifest only.
const VIZ_LOGS: VizRenderer = VizRenderer {
    id: "log-viewer",
    container_class: "",
    data_class: "log-data",
    include_registry: false,
    include_default_range: false,
    include_explore: false,
    include_annotations: false,
    empty_msg: "No log files available.",
};

/// Resolve a `renderer=` name (from the `{{ viz }}` shortcode) to its
/// descriptor. Unknown names yield `None` so the shortcode can surface an error.
fn viz_renderer(name: &str) -> Option<&'static VizRenderer> {
    match name {
        "chart" => Some(&VIZ_CHART),
        "overlay" => Some(&VIZ_OVERLAY),
        "logs" | "log" | "log-viewer" => Some(&VIZ_LOGS),
        _ => None,
    }
}

/// Emit the container `<div>` + inline parquet manifest (and any optional data
/// blocks/attributes) for a visualization renderer. The matching client-side
/// module self-selects by `id`/`data_class` and no-ops when its container is
/// absent, so a layout can load several modules and each page activates only
/// the one whose container the template emitted.
#[allow(clippy::too_many_arguments)]
fn render_viz(
    r: &VizRenderer,
    datafiles: &[ExportedFile],
    registry: &BTreeMap<String, String>,
    captions: &BTreeMap<String, String>,
    default_range: Option<&str>,
    explore_url: Option<&str>,
    annotations: &[ExportedFile],
) -> String {
    let class_attr = if r.container_class.is_empty() {
        String::new()
    } else {
        format!(" class=\"{}\"", r.container_class)
    };

    if datafiles.is_empty() {
        return format!(
            "<div{} id=\"{}\"><p>{}</p></div>",
            class_attr, r.id, r.empty_msg
        );
    }

    let json = datafiles_manifest_json(datafiles);

    let default_range_attr = if r.include_default_range {
        match default_range {
            Some(v) if !v.is_empty() => format!(" data-default-range=\"{}\"", html_escape(v)),
            _ => String::new(),
        }
    } else {
        String::new()
    };

    let explore_url_attr = if r.include_explore {
        match explore_url {
            Some(u) if !u.is_empty() => format!(" data-explore-url=\"{}\"", html_escape(u)),
            _ => String::new(),
        }
    } else {
        String::new()
    };

    let registry_block = if r.include_registry && !registry.is_empty() {
        let registry_json = serde_json::to_string(registry).unwrap_or_else(|_| "{}".to_string());
        format!(
            "<script type=\"application/json\" class=\"chart-registry\">{}</script>",
            registry_json
        )
    } else {
        String::new()
    };

    let captions_block = if r.include_registry && !captions.is_empty() {
        let captions_json = serde_json::to_string(captions).unwrap_or_else(|_| "{}".to_string());
        format!(
            "<script type=\"application/json\" class=\"chart-captions\">{}</script>",
            captions_json
        )
    } else {
        String::new()
    };

    let annotations_block = if r.include_annotations && !annotations.is_empty() {
        format!(
            "<script type=\"application/json\" class=\"chart-annotations\">{}</script>",
            datafiles_manifest_json(annotations)
        )
    } else {
        String::new()
    };

    format!(
        "<div{} id=\"{}\"{}{}>\
         <script type=\"application/json\" class=\"{}\">{}</script>\
         {}{}{}\
         </div>",
        class_attr,
        r.id,
        default_range_attr,
        explore_url_attr,
        r.data_class,
        json,
        registry_block,
        captions_block,
        annotations_block
    )
}

/// Render a chart container with inline datafile manifest.
///
/// Thin alias over [`render_viz`] with the [`VIZ_CHART`] renderer; kept for the
/// legacy `{{ chart }}` shortcode and existing call sites/tests.
fn render_chart(
    datafiles: &[ExportedFile],
    registry: &BTreeMap<String, String>,
    captions: &BTreeMap<String, String>,
    default_range: Option<&str>,
    explore_url: Option<&str>,
    annotations: &[ExportedFile],
) -> String {
    render_viz(
        &VIZ_CHART,
        datafiles,
        registry,
        captions,
        default_range,
        explore_url,
        annotations,
    )
}

/// Render an overlay (bespoke multi-plot analysis) container.
///
/// Thin alias over [`render_viz`] with the [`VIZ_OVERLAY`] renderer; overlay.js
/// binds to `id="overlay-chart"` and drives the pump-cycle plots.
fn render_overlay_chart(datafiles: &[ExportedFile], explore_url: Option<&str>) -> String {
    render_viz(
        &VIZ_OVERLAY,
        datafiles,
        &BTreeMap::new(),
        &BTreeMap::new(),
        None,
        explore_url,
        &[],
    )
}

/// Render a log/table viewer container.
///
/// Thin alias over [`render_viz`] with the [`VIZ_LOGS`] renderer; log-viewer.js
/// binds to `id="log-viewer"` and offers a SQL grid over the parquet manifest.
fn render_log_viewer(datafiles: &[ExportedFile]) -> String {
    render_viz(
        &VIZ_LOGS,
        datafiles,
        &BTreeMap::new(),
        &BTreeMap::new(),
        None,
        None,
        &[],
    )
}

/// Render the data explorer container with a `datasets` manifest.
///
/// Emits a `<div id="explore">` carrying a `<script class="datasets">` JSON
/// block. The manifest is a single dataset (Stage 0) listing this page's
/// exported parquet files. Client-side explore.js registers them as a DuckDB
/// view named `table` and offers a SQL playground. The `url` field carries the
/// relative parquet URL (the `file` of each ExportedFile), matching the key
/// explore.js reads.
fn render_explore(datafiles: &[ExportedFile], table: &str, label: &str) -> String {
    let files_json: Vec<serde_json::Value> = datafiles
        .iter()
        .map(|f| {
            serde_json::json!({
                "url": f.file,
                "captures": f.captures,
                "start_time": f.start_time,
                "end_time": f.end_time,
            })
        })
        .collect();

    let datasets = serde_json::json!([{
        "table": table,
        "label": label,
        "files": files_json,
    }]);
    wrap_explore(datasets)
}

/// Render the data explorer container with a multi-dataset `datasets` manifest
/// resolved from `SiteConfig.explore`. Each dataset carries its file list and
/// the export stage's unioned column names so the explorer can show the schema
/// up front without a DESCRIBE round-trip.
fn render_explore_multi(datasets: &[ResolvedExploreDataset]) -> String {
    let arr: Vec<serde_json::Value> = datasets
        .iter()
        .map(|d| {
            let files_json: Vec<serde_json::Value> = d
                .files
                .iter()
                .map(|f| {
                    serde_json::json!({
                        "url": f.file,
                        "captures": f.captures,
                        "start_time": f.start_time,
                        "end_time": f.end_time,
                    })
                })
                .collect();
            serde_json::json!({
                "table": d.table,
                "label": d.label,
                "files": files_json,
                "columns": d.columns,
            })
        })
        .collect();
    wrap_explore(serde_json::Value::Array(arr))
}

/// Wrap a `datasets` JSON value in the explorer container markup.
fn wrap_explore(datasets: serde_json::Value) -> String {
    let json = serde_json::to_string(&datasets).unwrap_or_else(|_| "[]".to_string());
    format!(
        "<div id=\"explore\">\
         <script type=\"application/json\" class=\"datasets\">{}</script>\
         </div>",
        json
    )
}

/// Render microseconds-since-epoch as a `<time>` element carrying both
/// a UTC ISO-8601 fallback string and the raw microseconds in a
/// `data-utc-us` attribute.  The companion `relative-time.js` rewrites
/// these elements client-side to the visitor's local time and a
/// "X minutes ago" relative label that ticks every 30 s without a
/// page reload.  When `us` is `None` we render an em-dash rather than
/// an empty `<time>` so the script has nothing to convert.
fn fmt_time_marker(us: Option<i64>) -> String {
    match us {
        Some(us) => {
            let secs = us / 1_000_000;
            let nanos = ((us % 1_000_000) * 1_000) as u32;
            let iso = chrono::DateTime::from_timestamp(secs, nanos)
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .unwrap_or_default();
            let display = chrono::DateTime::from_timestamp(secs, nanos)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "—".to_string());
            format!(
                "<time datetime=\"{}\" data-utc-us=\"{}\">{}</time>",
                html_escape(&iso),
                us,
                html_escape(&display),
            )
        }
        None => "—".to_string(),
    }
}

/// Format an integer second count as a coarse human-readable duration
/// for the static "Period" row on each card (the `relative-time.js`
/// script doesn't touch this -- the value isn't a wall-clock instant).
fn fmt_seconds_coarse(s: i64) -> String {
    if s <= 0 {
        return "—".to_string();
    }
    if s < 90 {
        return format!("{} s", s);
    }
    let mins = (s + 30) / 60;
    if mins < 90 {
        return format!("{} min", mins);
    }
    let hours = (s + 1800) / 3600;
    if hours < 48 {
        return format!("{} h", hours);
    }
    let days = (s + 43200) / 86400;
    format!("{} d", days)
}

/// Humanize a byte count with IEC units (KiB / MiB / GiB ...) for
/// display in pond status cards.  Mirrors `formatBytes` in
/// `assets/chart.js` so server-rendered status and client-rendered
/// charts agree on units.
fn fmt_bytes_iec(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut n = bytes as f64;
    let mut i = 0;
    while n >= 1024.0 && i < UNITS.len() - 1 {
        n /= 1024.0;
        i += 1;
    }
    if i == 0 || n >= 100.0 {
        format!("{:.0} {}", n, UNITS[i])
    } else {
        format!("{:.1} {}", n, UNITS[i])
    }
}

/// Render the pond status grid as static HTML.
///
/// `statuses == None` means `SiteConfig.status_grid` was not configured
/// for this site; we render nothing rather than a confusing empty grid.
/// `statuses == Some(empty)` means the grid is configured but no
/// journal data matched -- we render an empty-state message so the
/// page is still useful on first boot.
fn render_pond_status_grid(statuses: Option<&[PondStatus]>, generated_at: &str) -> String {
    let Some(statuses) = statuses else {
        return String::new();
    };

    let mut out = String::new();
    out.push_str("<div class=\"pond-status-grid-wrap\">");
    out.push_str(&format!(
        "<p class=\"pond-status-generated-at\">Generated at {} (refreshes when sitegen runs).</p>",
        html_escape(generated_at),
    ));

    if statuses.is_empty() {
        out.push_str(
            "<p class=\"pond-status-empty\">\
             No pond status data available yet.  \
             Once the journal-ingest factory has captured logs for the \
             configured units, status cards will appear here.\
             </p>",
        );
        out.push_str("</div>");
        return out;
    }

    out.push_str("<div class=\"pond-status-grid\">");
    for s in statuses {
        out.push_str(&format!(
            "<section class=\"pond-status-card {}\">",
            s.health.css_class(),
        ));
        out.push_str(&format!(
            "<h3 class=\"pond-status-unit\">{}</h3>",
            html_escape(&s.unit),
        ));

        out.push_str("<dl class=\"pond-status-meta\">");
        out.push_str(&format!(
            "<dt>Last ok</dt><dd>{}{}</dd>",
            fmt_time_marker(s.last_ok_us),
            if let Some(msg) = &s.last_ok_msg {
                format!(
                    "<br><span class=\"pond-status-msg\">{}</span>",
                    html_escape(&sanitize_inline(msg))
                )
            } else {
                String::new()
            },
        ));
        out.push_str(&format!(
            "<dt>Last err</dt><dd>{}{}</dd>",
            fmt_time_marker(s.last_err_us),
            if let Some(msg) = &s.last_err_msg {
                format!(
                    "<br><span class=\"pond-status-msg\">{}</span>",
                    html_escape(&sanitize_inline(msg))
                )
            } else {
                String::new()
            },
        ));
        out.push_str(&format!(
            "<dt>Timer</dt><dd>{}</dd>",
            match s.timer_active {
                Some(true) => "active",
                Some(false) => "inactive",
                None => "—",
            },
        ));
        out.push_str(&format!(
            "<dt>Period</dt><dd>{}</dd>",
            html_escape(&match s.timer_interval_s {
                Some(s) => fmt_seconds_coarse(s),
                None => "—".to_string(),
            }),
        ));
        out.push_str(&format!(
            "<dt>Last seen</dt><dd>{}</dd>",
            fmt_time_marker(s.last_seen_us),
        ));
        out.push_str(&format!(
            "<dt>Peak RSS</dt><dd>{}</dd>",
            match s.peak_rss_bytes {
                Some(b) => html_escape(&fmt_bytes_iec(b)),
                None => "—".to_string(),
            },
        ));
        out.push_str("</dl>");

        if s.tail_messages.is_empty() {
            out.push_str("<p class=\"pond-status-tail-empty\">No recent log lines.</p>");
        } else {
            out.push_str("<pre class=\"pond-status-tail\">");
            for msg in &s.tail_messages {
                // Collapse each multi-line MESSAGE to a single line so
                // the surrounding markdown renderer doesn't see blank
                // lines inside the <pre> block and end the HTML block
                // there (which would wrap subsequent log text in
                // unwanted <p> tags).
                out.push_str(&html_escape(&sanitize_inline(msg)));
                out.push('\n');
            }
            out.push_str("</pre>");
        }

        out.push_str("</section>");
    }
    out.push_str("</div>");
    out.push_str("</div>");
    out
}

/// Render a navigation list for a named collection.
fn render_nav_list(ctx: &ShortcodeContext, collection: &str, path: &str) -> String {
    let items = match ctx.collections.get(collection) {
        Some(items) => items,
        None => return format!("<!-- nav_list: unknown collection '{}' -->", collection),
    };

    if items.is_empty() {
        return format!("<!-- nav_list: collection '{}' is empty -->", collection);
    }

    let mut html = String::from("<ul class=\"nav-list\">\n");
    for item in items {
        let href = format!("{}/{}.html", path, item);
        let is_active = ctx.current_path == href;
        let li_class = if is_active { " class=\"active\"" } else { "" };
        let aria = if is_active {
            " aria-current=\"page\""
        } else {
            ""
        };
        html.push_str(&format!(
            "  <li{}><a href=\"{}\"{}>{}</a></li>\n",
            li_class, href, aria, item
        ));
    }
    html.push_str("</ul>");
    html
}

/// Render a breadcrumb trail as an HTML nav element.
fn render_breadcrumb(breadcrumbs: &[(String, String)]) -> String {
    if breadcrumbs.is_empty() {
        return String::new();
    }

    let mut html = String::from("<nav class=\"breadcrumb\" aria-label=\"breadcrumb\"><ol>\n");
    let last = breadcrumbs.len() - 1;
    for (i, (label, href)) in breadcrumbs.iter().enumerate() {
        if i == last {
            html.push_str(&format!(
                "  <li class=\"active\" aria-current=\"page\">{}</li>\n",
                label
            ));
        } else {
            html.push_str(&format!("  <li><a href=\"{}\">{}</a></li>\n", href, label));
        }
    }
    html.push_str("</ol></nav>");
    html
}

/// Return the first usable href found when traversing children depth-first.
///
/// Used when a section heading has no direct href: clicking the heading
/// navigates to the first descendant leaf.
fn first_leaf_href(children: &[SidebarChild]) -> Option<&str> {
    for child in children {
        if let Some(h) = child.href.as_deref() {
            return Some(h);
        }
        if let Some(h) = first_leaf_href(&child.children) {
            return Some(h);
        }
    }
    None
}

/// Whether `child` or any of its descendants matches `current_path`.
fn any_descendant_active(child: &SidebarChild, current_path: &str) -> bool {
    if let Some(h) = child.href.as_deref()
        && h == current_path
    {
        return true;
    }
    child
        .children
        .iter()
        .any(|c| any_descendant_active(c, current_path))
}

/// Recursively render a sidebar child and its nested sub-navigation.
///
/// `indent` controls whitespace prefixing so the emitted HTML is readable.
fn render_child(html: &mut String, child: &SidebarChild, current_path: &str, indent: usize) {
    let pad = "  ".repeat(indent);
    let c_has_children = !child.children.is_empty();
    let c_own_active = child
        .href
        .as_deref()
        .map(|h| h == current_path)
        .unwrap_or(false);
    let c_descendant_active = child
        .children
        .iter()
        .any(|c| any_descendant_active(c, current_path));
    let c_active = c_own_active || c_descendant_active;
    let c_class = if c_active { " class=\"active\"" } else { "" };
    let c_aria = if c_own_active {
        " aria-current=\"page\""
    } else {
        ""
    };

    // Resolve the href: own href first, else first descendant leaf.
    let href_owned;
    let href: &str = if let Some(h) = child.href.as_deref() {
        h
    } else if let Some(h) = first_leaf_href(&child.children) {
        href_owned = h.to_string();
        &href_owned
    } else {
        ""
    };

    if c_has_children {
        let heading_cls = if child.href.is_none() {
            " class=\"nav-heading\""
        } else {
            ""
        };
        html.push_str(&format!(
            "{}<li{}><a href=\"{}\"{}{}>{}</a>\n",
            pad, c_class, href, c_aria, heading_cls, child.label
        ));
        let sub_class = if c_active {
            "subnav expanded"
        } else {
            "subnav"
        };
        html.push_str(&format!("{}  <ul class=\"{}\">\n", pad, sub_class));
        for grand in &child.children {
            render_child(html, grand, current_path, indent + 2);
        }
        html.push_str(&format!("{}  </ul>\n{}</li>\n", pad, pad));
    } else {
        html.push_str(&format!(
            "{}<li{}><a href=\"{}\"{}>{}</a></li>\n",
            pad, c_class, href, c_aria, child.label
        ));
    }
}

/// Render a navigation list for content pages (ordered by weight).
///
/// When `sidebar_sections` is defined in site.yaml, renders pills in the
/// configured order. Entries with `children` render sub-navigation items
/// that expand when the parent page or any child is the current page.
///
/// When `sidebar_sections` is empty, falls back to the grouped/collapsible
/// rendering with section headings.
fn render_content_nav(ctx: &ShortcodeContext, content_name: &str) -> String {
    // When sidebar sections are configured, render them directly.
    // Entries with explicit hrefs don't need content page matching.
    // Entries without hrefs fall back to title-matching against content pages.
    if !ctx.sidebar_sections.is_empty() {
        let pages = ctx.content_pages.get(content_name).map(|v| v.as_slice());

        let mut html = String::from("<ul>\n");
        for entry in &ctx.sidebar_sections {
            let label = entry.label();
            let children = entry.children();
            let direct_href = entry.href();

            // Resolve parent href: explicit href first, then page title match.
            // Match priority: exact title > case-insensitive exact > substring.
            let resolved_href = if let Some(href) = direct_href {
                Some(href.to_string())
            } else {
                let all_pages: Vec<_> = pages.iter().flat_map(|pp| pp.iter()).collect();
                let label_lower = label.to_lowercase();
                all_pages
                    .iter()
                    .find(|p| !p.hidden && p.title == label)
                    .or_else(|| {
                        all_pages
                            .iter()
                            .find(|p| !p.hidden && p.title.to_lowercase() == label_lower)
                    })
                    .or_else(|| {
                        all_pages
                            .iter()
                            .find(|p| !p.hidden && p.title.contains(label))
                    })
                    .map(|page| content_page_href(&ctx.base_url, page))
            };

            if let Some(parent_href) = resolved_href {
                let parent_active = ctx.current_path == parent_href;
                let child_active = children
                    .iter()
                    .any(|c| any_descendant_active(c, &ctx.current_path));
                let is_active = parent_active || child_active;
                let li_class = if is_active { " class=\"active\"" } else { "" };
                let aria = if parent_active {
                    " aria-current=\"page\""
                } else {
                    ""
                };

                if children.is_empty() {
                    html.push_str(&format!(
                        "  <li{}><a href=\"{}\"{}>{}</a></li>\n",
                        li_class, parent_href, aria, label
                    ));
                } else {
                    html.push_str(&format!(
                        "  <li{}><a href=\"{}\"{}>{}</a>\n",
                        li_class, parent_href, aria, label
                    ));
                    let sub_class = if is_active {
                        "subnav expanded"
                    } else {
                        "subnav"
                    };
                    html.push_str(&format!("    <ul class=\"{}\">\n", sub_class));
                    for child in children {
                        render_child(&mut html, child, &ctx.current_path, 3);
                    }
                    html.push_str("    </ul>\n  </li>\n");
                }
            } else if !children.is_empty() {
                // Section heading with children but no resolved href --
                // link to the first child so clicking expands the section.
                let first_href = first_leaf_href(children).unwrap_or("").to_string();
                let child_active = children
                    .iter()
                    .any(|c| any_descendant_active(c, &ctx.current_path));
                let li_class = if child_active {
                    " class=\"active\""
                } else {
                    ""
                };
                html.push_str(&format!(
                    "  <li{}><a href=\"{}\" class=\"nav-heading\">{}</a>\n",
                    li_class, first_href, label
                ));
                let sub_class = if child_active {
                    "subnav expanded"
                } else {
                    "subnav"
                };
                html.push_str(&format!("    <ul class=\"{}\">\n", sub_class));
                for child in children {
                    render_child(&mut html, child, &ctx.current_path, 3);
                }
                html.push_str("    </ul>\n  </li>\n");
            }
        }
        html.push_str("</ul>");
        return html;
    }

    // -- Fallback: no sidebar sections configured --
    // Render from content pages using grouped/collapsible mode.
    let pages = match ctx.content_pages.get(content_name) {
        Some(pages) => pages,
        None => return format!("<!-- content_nav: unknown content '{}' -->", content_name),
    };

    if pages.is_empty() {
        return format!("<!-- content_nav: content '{}' is empty -->", content_name);
    }

    // Render a single <li> for a page, with an optional display label override.
    let render_li = |page: &ContentPage, label: &str, indent: &str| -> String {
        let href = content_page_href(&ctx.base_url, page);
        let is_active = ctx.current_path == href;
        let li_class = if is_active { " class=\"active\"" } else { "" };
        let aria = if is_active {
            " aria-current=\"page\""
        } else {
            ""
        };
        format!(
            "{}<li{}><a href=\"{}\"{}>{}</a></li>\n",
            indent, li_class, href, aria, label
        )
    };

    // -- Grouped/collapsible mode (legacy fallback) --

    // Group pages by section, preserving weight order.
    let mut sections: Vec<(Option<String>, Vec<&ContentPage>)> = Vec::new();
    for page in pages {
        if page.hidden {
            continue;
        }
        let key = page.section.clone();
        if let Some(group) = sections.iter_mut().find(|(k, _)| *k == key) {
            group.1.push(page);
        } else {
            sections.push((key, vec![page]));
        }
    }

    let active_section: Option<Option<String>> =
        sections.iter().find_map(|(key, section_pages)| {
            if section_pages
                .iter()
                .any(|p| ctx.current_path == content_page_href(&ctx.base_url, p))
            {
                Some(key.clone())
            } else {
                None
            }
        });
    let expanded_section = active_section.or_else(|| sections.first().map(|(k, _)| k.clone()));

    let mut html = String::from("<nav class=\"nav-list\">\n");
    for (section, section_pages) in &sections {
        match section {
            None => {
                html.push_str("<ul>\n");
                for page in section_pages {
                    html.push_str(&render_li(page, &page.title, "  "));
                }
                html.push_str("</ul>\n");
            }
            Some(section_name) => {
                let expanded = expanded_section.as_ref() == Some(section);
                let class = if expanded {
                    "nav-section expanded"
                } else {
                    "nav-section"
                };
                html.push_str(&format!(
                    "<div class=\"{}\">\n  <h3 class=\"nav-section-title\">{}</h3>\n  <ul>\n",
                    class, section_name
                ));
                for page in section_pages {
                    html.push_str(&render_li(page, &page.title, "    "));
                }
                html.push_str("  </ul>\n</div>\n");
            }
        }
    }
    html.push_str("</nav>");
    html
}

/// Render a `<figure>` element with optional float positioning.
///
/// Shortcode: `{{ figure src="/img/photo.jpg" caption="..." float="right" /}}`
///
/// `float` can be "right", "left", or "full" (default: no float).
/// Caption text is also used as the `alt` attribute.
fn render_figure(_ctx: &ShortcodeContext, args: &ShortcodeArgs) -> String {
    let src = args.get_str("src").unwrap_or("");
    let caption = args.get_str("caption").unwrap_or("");
    let float = args.get_str("float").unwrap_or("");
    let alt = args.get_str("alt").unwrap_or(caption);

    let class = match float {
        "right" => " class=\"figure-right\"",
        "left" => " class=\"figure-left\"",
        "full" => " class=\"figure-full\"",
        _ => "",
    };

    let mut html = format!("<figure{}>\n", class);
    if !src.is_empty() {
        html.push_str(&format!(
            "  <a href=\"{}\" target=\"_blank\"><img src=\"{}\" alt=\"{}\"></a>\n",
            src,
            src,
            html_escape(alt),
        ));
    }
    if !caption.is_empty() {
        html.push_str(&format!(
            "  <figcaption>{}</figcaption>\n",
            html_escape(caption)
        ));
    }
    html.push_str("</figure>");
    html
}

/// Minimal HTML escaping for attribute values.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Collapse a possibly multi-line log MESSAGE to a single trimmed line.
///
/// Pond's stderr error messages can span multiple lines (e.g.
/// `Error: Transaction aborted:\nExecution failed for factory ...`).
/// When such a string is emitted into a `<pre>` block inside a
/// markdown-rendered document, the embedded blank line(s) cause
/// CommonMark's HTML-block parser to terminate the block early and
/// wrap subsequent text in `<p>` tags, producing visibly broken
/// markup.  Replacing every run of whitespace (including embedded
/// newlines) with a single space keeps each tail entry on one line.
fn sanitize_inline(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Format an ISO 8601 date string (e.g. "2024-06-15") for display.
///
/// Returns a human-readable date like "June 15, 2024".
/// Falls back to the raw string if parsing fails.
fn format_date(iso: &str) -> String {
    let parts: Vec<&str> = iso.split('-').collect();
    if parts.len() != 3 {
        return iso.to_string();
    }
    let month = match parts[1] {
        "01" => "January",
        "02" => "February",
        "03" => "March",
        "04" => "April",
        "05" => "May",
        "06" => "June",
        "07" => "July",
        "08" => "August",
        "09" => "September",
        "10" => "October",
        "11" => "November",
        "12" => "December",
        _ => return iso.to_string(),
    };
    let day = parts[2].trim_start_matches('0');
    format!("{} {}, {}", month, day, parts[0])
}

/// Render a blog card grid for content pages matching a section.
///
/// Shortcode: `{{ blog_grid content="pages" section="Blog" /}}`
///
/// Filters content pages by section name, sorts by date descending
/// (undated posts come last, sorted by weight), and renders a
/// responsive card grid with optional hero image, date, and summary.
fn render_blog_grid(ctx: &ShortcodeContext, content_name: &str, section: &str) -> String {
    let pages = match ctx.content_pages.get(content_name) {
        Some(pages) => pages,
        None => return format!("<!-- blog_grid: unknown content '{}' -->", content_name),
    };

    // Filter to matching section, exclude hidden
    let mut posts: Vec<&ContentPage> = pages
        .iter()
        .filter(|p| !p.hidden && p.section.as_deref() == Some(section))
        .collect();

    if posts.is_empty() {
        return format!("<!-- blog_grid: no posts in section '{}' -->", section);
    }

    // Sort by date descending (newest first); undated posts go last, ordered by weight
    posts.sort_by(|a, b| match (&b.date, &a.date) {
        (Some(bd), Some(ad)) => bd.cmp(ad),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.weight.cmp(&b.weight),
    });

    let mut html = String::from("<div class=\"blog-grid\">\n");

    for post in &posts {
        let href = content_page_href(&ctx.base_url, post);
        html.push_str("  <a class=\"blog-card\" href=\"");
        html.push_str(&href);
        html.push_str("\">\n");

        // Hero image
        if let Some(ref img) = post.image {
            html.push_str("    <div class=\"blog-card-image\">");
            html.push_str(&format!(
                "<img src=\"{}\" alt=\"{}\">",
                html_escape(img),
                html_escape(&post.title)
            ));
            html.push_str("</div>\n");
        }

        html.push_str("    <div class=\"blog-card-body\">\n");

        // Date
        if let Some(ref date) = post.date {
            html.push_str("      <time class=\"blog-card-date\">");
            html.push_str(&format_date(date));
            html.push_str("</time>\n");
        }

        // Title
        html.push_str("      <h3 class=\"blog-card-title\">");
        html.push_str(&html_escape(&post.title));
        html.push_str("</h3>\n");

        // Summary
        if let Some(ref summary) = post.summary {
            html.push_str("      <p class=\"blog-card-summary\">");
            html.push_str(&html_escape(summary));
            html.push_str("</p>\n");
        }

        html.push_str("    </div>\n");
        html.push_str("  </a>\n");
    }

    html.push_str("</div>");
    html
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preprocess_variables() {
        let input = "# {{ $0 }}\n\n{{ breadcrumb /}}\n\n{{ chart /}}";
        let output = preprocess_variables(input);
        assert!(output.contains("{{ cap0 /}}"));
        assert!(output.contains("{{ chart /}}"));
        assert!(output.contains("{{ breadcrumb /}}"));
    }

    #[test]
    fn test_register_shortcodes_creates_all() {
        let ctx = Arc::new(ShortcodeContext {
            captures: vec!["Temperature".to_string()],
            datafiles: vec![ExportedFile {
                path: "params/Temperature/data.parquet".to_string(),
                file: "data/Temperature/data.parquet".to_string(),
                captures: vec!["Temperature".to_string()],
                temporal: BTreeMap::from([
                    ("year".to_string(), "2025".to_string()),
                    ("month".to_string(), "01".to_string()),
                ]),
                start_time: 1704067200,
                end_time: 1706745599,
            }],
            collections: BTreeMap::from([(
                "params".to_string(),
                vec!["Temperature".to_string(), "DO".to_string()],
            )]),
            content_pages: BTreeMap::new(),
            site_title: "Test Site".to_string(),
            current_path: "/params/Temperature".to_string(),
            breadcrumbs: vec![
                ("Home".to_string(), "/".to_string()),
                ("Params".to_string(), "/params".to_string()),
                ("Temperature".to_string(), "/params/Temperature".to_string()),
            ],
            base_url: "/".to_string(),
            sidebar_sections: vec![],
            metric_registry: BTreeMap::new(),
            metric_captions: BTreeMap::new(),
            pond_statuses: None,
            generated_at: String::new(),
            default_range: None,
            labels: BTreeMap::new(),
            explore_datasets: vec![],
            explore_url: None,
            annotations: vec![],
        });

        let shortcodes = register_shortcodes(ctx);
        // Verify shortcodes work by running a known shortcode through preprocess
        use crate::markdown::preprocess_shortcodes;
        let result = preprocess_shortcodes("{{ cap0 /}}", &shortcodes, None);
        assert_eq!(result.unwrap(), "Temperature");
    }

    #[test]
    fn test_render_chart_empty() {
        let html = render_chart(&[], &BTreeMap::new(), &BTreeMap::new(), None, None, &[]);
        assert!(html.contains("No data files"));
    }

    #[test]
    fn test_render_explore_emits_datasets_manifest() {
        let files = vec![ExportedFile {
            path: "/reduced/Temp.series".to_string(),
            file: "/noyo/data/Temp/res=1h.parquet".to_string(),
            captures: vec!["Temp".to_string(), "res=1h".to_string()],
            temporal: BTreeMap::new(),
            start_time: 100,
            end_time: 200,
        }];
        let html = render_explore(&files, "reduced", "Reduced tiers");
        assert!(html.contains("id=\"explore\""), "explore container: {html}");
        assert!(
            html.contains("class=\"datasets\""),
            "datasets manifest script: {html}"
        );
        // The url field carries the ExportedFile.file value (what explore.js reads).
        assert!(html.contains("/noyo/data/Temp/res=1h.parquet"));
        assert!(html.contains("\"table\":\"reduced\""));
        assert!(html.contains("\"label\":\"Reduced tiers\""));
        // start/end times are carried through for lazy fetch.
        assert!(html.contains("\"start_time\":100"));
        assert!(html.contains("\"end_time\":200"));
    }

    #[test]
    fn test_render_explore_empty_files() {
        let html = render_explore(&[], "data", "Data");
        // Still emits a valid (empty) manifest so explore.js shows its own state.
        assert!(html.contains("class=\"datasets\""));
        assert!(html.contains("\"files\":[]"));
    }

    #[test]
    fn test_render_explore_multi_emits_all_datasets_with_columns() {
        let mk = |file: &str| ExportedFile {
            path: "p".to_string(),
            file: file.to_string(),
            captures: vec![],
            temporal: BTreeMap::new(),
            start_time: 0,
            end_time: 0,
        };
        let datasets = vec![
            ResolvedExploreDataset {
                table: "reduced".to_string(),
                label: "Reduced".to_string(),
                files: vec![mk("/d/reduced.parquet")],
                columns: vec!["timestamp".to_string(), "value".to_string()],
            },
            ResolvedExploreDataset {
                table: "combined".to_string(),
                label: "Combined".to_string(),
                files: vec![mk("/d/combined.parquet")],
                columns: vec!["timestamp".to_string()],
            },
        ];
        let html = render_explore_multi(&datasets);
        assert!(html.contains("id=\"explore\""), "container: {html}");
        // Both datasets present.
        assert!(html.contains("\"table\":\"reduced\""));
        assert!(html.contains("\"table\":\"combined\""));
        assert!(html.contains("/d/reduced.parquet"));
        assert!(html.contains("/d/combined.parquet"));
        // Columns carried in the manifest for the up-front schema display.
        assert!(html.contains("\"columns\":[\"timestamp\",\"value\"]"));
    }

    #[test]
    fn test_render_chart_with_files() {
        let files = vec![ExportedFile {
            path: "data.parquet".to_string(),
            file: "data/data.parquet".to_string(),
            captures: vec!["Temp".to_string()],
            temporal: BTreeMap::from([("year".to_string(), "2025".to_string())]),
            start_time: 100,
            end_time: 200,
        }];
        let html = render_chart(&files, &BTreeMap::new(), &BTreeMap::new(), None, None, &[]);
        assert!(html.contains("chart-container"));
        assert!(html.contains("data.parquet"));
        // Empty registry -> no chart-registry script element.
        assert!(!html.contains("chart-registry"));
        // No default range configured -> no data-default-range attribute.
        assert!(!html.contains("data-default-range"));
    }

    #[test]
    fn test_render_chart_with_default_range() {
        let files = vec![ExportedFile {
            path: "data.parquet".to_string(),
            file: "data/data.parquet".to_string(),
            captures: vec!["Temp".to_string()],
            temporal: BTreeMap::new(),
            start_time: 100,
            end_time: 200,
        }];
        let html = render_chart(
            &files,
            &BTreeMap::new(),
            &BTreeMap::new(),
            Some("1M"),
            None,
            &[],
        );
        assert!(html.contains("data-default-range=\"1M\""));
        // No explore url configured -> no cross-link attribute.
        assert!(!html.contains("data-explore-url"));
    }

    #[test]
    fn test_render_chart_with_explore_url() {
        let files = vec![ExportedFile {
            path: "data.parquet".to_string(),
            file: "data/data.parquet".to_string(),
            captures: vec!["Temp".to_string()],
            temporal: BTreeMap::new(),
            start_time: 100,
            end_time: 200,
        }];
        let html = render_chart(
            &files,
            &BTreeMap::new(),
            &BTreeMap::new(),
            None,
            Some("/explore/"),
            &[],
        );
        assert!(html.contains("data-explore-url=\"/explore/\""));
    }

    #[test]
    fn test_render_overlay_chart_with_explore_url() {
        let files = vec![ExportedFile {
            path: "pump-cycles.parquet".to_string(),
            file: "analysis/pump-cycles.parquet".to_string(),
            captures: vec!["pump-cycles".to_string()],
            temporal: BTreeMap::new(),
            start_time: 100,
            end_time: 200,
        }];
        let html = render_overlay_chart(&files, Some("/explore/"));
        assert!(html.contains("id=\"overlay-chart\""));
        assert!(html.contains("data-explore-url=\"/explore/\""));
        // No explorer URL configured: no cross-link attribute is emitted.
        let plain = render_overlay_chart(&files, None);
        assert!(!plain.contains("data-explore-url"));
    }

    #[test]
    fn test_render_chart_with_registry() {
        let files = vec![ExportedFile {
            path: "data.parquet".to_string(),
            file: "data/data.parquet".to_string(),
            captures: vec!["Temp".to_string()],
            temporal: BTreeMap::new(),
            start_time: 100,
            end_time: 200,
        }];
        let registry = BTreeMap::from([
            ("committed.txn_ids".to_string(), "counter".to_string()),
            ("size.bytes".to_string(), "updowncounter".to_string()),
        ]);
        let html = render_chart(&files, &registry, &BTreeMap::new(), None, None, &[]);
        assert!(html.contains("chart-registry"));
        assert!(html.contains("\"committed.txn_ids\":\"counter\""));
        assert!(html.contains("\"size.bytes\":\"updowncounter\""));
    }

    #[test]
    fn test_render_viz_matches_legacy_aliases() {
        let files = vec![ExportedFile {
            path: "data.parquet".to_string(),
            file: "data/data.parquet".to_string(),
            captures: vec!["Temp".to_string()],
            temporal: BTreeMap::new(),
            start_time: 100,
            end_time: 200,
        }];
        let registry = BTreeMap::from([("a.b".to_string(), "counter".to_string())]);
        let captions = BTreeMap::from([("a.b".to_string(), "A over B".to_string())]);

        // The unified renderer must produce byte-for-byte the same markup as the
        // legacy alias functions for every built-in renderer.
        assert_eq!(
            render_viz(
                &VIZ_CHART,
                &files,
                &registry,
                &captions,
                Some("1M"),
                Some("/explore/"),
                &[],
            ),
            render_chart(
                &files,
                &registry,
                &captions,
                Some("1M"),
                Some("/explore/"),
                &[]
            ),
        );
        assert_eq!(
            render_viz(
                &VIZ_OVERLAY,
                &files,
                &BTreeMap::new(),
                &BTreeMap::new(),
                None,
                Some("/explore/"),
                &[],
            ),
            render_overlay_chart(&files, Some("/explore/")),
        );
        assert_eq!(
            render_viz(
                &VIZ_LOGS,
                &files,
                &BTreeMap::new(),
                &BTreeMap::new(),
                None,
                None,
                &[],
            ),
            render_log_viewer(&files),
        );
    }

    #[test]
    fn test_render_viz_respects_renderer_capabilities() {
        let files = vec![ExportedFile {
            path: "data.parquet".to_string(),
            file: "data/data.parquet".to_string(),
            captures: vec![],
            temporal: BTreeMap::new(),
            start_time: 0,
            end_time: 0,
        }];
        let registry = BTreeMap::from([("a.b".to_string(), "counter".to_string())]);

        // Overlay ignores the registry and default-range, but keeps explore.
        let overlay = render_viz(
            &VIZ_OVERLAY,
            &files,
            &registry,
            &BTreeMap::new(),
            Some("1M"),
            Some("/explore/"),
            &[],
        );
        assert!(overlay.contains("id=\"overlay-chart\""));
        assert!(!overlay.contains("chart-registry"));
        assert!(!overlay.contains("data-default-range"));
        assert!(overlay.contains("data-explore-url=\"/explore/\""));

        // Logs ignore registry, default-range and explore entirely.
        let logs = render_viz(
            &VIZ_LOGS,
            &files,
            &registry,
            &BTreeMap::new(),
            Some("1M"),
            Some("/explore/"),
            &[],
        );
        assert!(logs.contains("id=\"log-viewer\""));
        assert!(!logs.contains("chart-registry"));
        assert!(!logs.contains("data-default-range"));
        assert!(!logs.contains("data-explore-url"));
    }

    #[test]
    fn test_viz_renderer_lookup() {
        assert!(viz_renderer("chart").is_some());
        assert!(viz_renderer("overlay").is_some());
        assert!(viz_renderer("logs").is_some());
        assert!(viz_renderer("log-viewer").is_some());
        assert!(viz_renderer("nope").is_none());
    }

    #[test]
    fn test_render_viz_emits_annotations_only_for_capable_renderers() {
        let files = vec![ExportedFile {
            path: "data.parquet".to_string(),
            file: "data/data.parquet".to_string(),
            captures: vec!["Temp".to_string()],
            temporal: BTreeMap::new(),
            start_time: 100,
            end_time: 200,
        }];
        let annotations = vec![ExportedFile {
            path: "leak-periods.parquet".to_string(),
            file: "analysis/leak-periods.parquet".to_string(),
            captures: vec!["leak-periods".to_string()],
            temporal: BTreeMap::new(),
            start_time: 100,
            end_time: 200,
        }];

        // chart supports annotations -> emits the manifest referencing the file.
        let chart = render_viz(
            &VIZ_CHART,
            &files,
            &BTreeMap::new(),
            &BTreeMap::new(),
            None,
            None,
            &annotations,
        );
        assert!(chart.contains("class=\"chart-annotations\""));
        assert!(chart.contains("analysis/leak-periods.parquet"));

        // No annotations -> no manifest, preserving legacy markup.
        let plain = render_viz(
            &VIZ_CHART,
            &files,
            &BTreeMap::new(),
            &BTreeMap::new(),
            None,
            None,
            &[],
        );
        assert!(!plain.contains("chart-annotations"));

        // overlay/logs never emit annotations even when supplied.
        let overlay = render_viz(
            &VIZ_OVERLAY,
            &files,
            &BTreeMap::new(),
            &BTreeMap::new(),
            None,
            None,
            &annotations,
        );
        assert!(!overlay.contains("chart-annotations"));
    }

    #[test]
    fn test_render_breadcrumb() {
        let crumbs = vec![
            ("Home".to_string(), "/".to_string()),
            ("Here".to_string(), "/here".to_string()),
        ];
        let html = render_breadcrumb(&crumbs);
        assert!(html.contains("<nav"));
        assert!(html.contains("Home"));
        assert!(html.contains("aria-current"));
    }

    #[test]
    fn test_render_nav_list() {
        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::from([(
                "params".to_string(),
                vec!["Temperature".to_string(), "DO".to_string()],
            )]),
            content_pages: BTreeMap::new(),
            site_title: String::new(),
            current_path: "/params/Temperature.html".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
            sidebar_sections: vec![],
            metric_registry: BTreeMap::new(),
            metric_captions: BTreeMap::new(),
            pond_statuses: None,
            generated_at: String::new(),
            default_range: None,
            labels: BTreeMap::new(),
            explore_datasets: vec![],
            explore_url: None,
            annotations: vec![],
        };
        let html = render_nav_list(&ctx, "params", "/params");
        assert!(html.contains("Temperature"));
        assert!(html.contains("/params/DO.html"));
        // Active item gets class and aria attribute
        assert!(
            html.contains(r#"<li class="active"><a href="/params/Temperature.html" aria-current="page">Temperature</a></li>"#),
            "Expected active class on Temperature, got: {}", html
        );
        // Non-active item has no extra attributes
        assert!(
            html.contains("<li><a href=\"/params/DO.html\">DO</a></li>"),
            "Expected plain li for DO, got: {}",
            html
        );
    }

    #[test]
    fn test_render_content_nav() {
        let pages = vec![
            ContentPage {
                title: "Water System".to_string(),
                slug: "water".to_string(),
                weight: 10,
                hidden: false,
                section: None,
                source_path: "/content/water.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
            ContentPage {
                title: "History".to_string(),
                slug: "history".to_string(),
                weight: 20,
                hidden: false,
                section: None,
                source_path: "/content/history.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
            ContentPage {
                title: "Hidden Page".to_string(),
                slug: "hidden".to_string(),
                weight: 5,
                hidden: true,
                section: None,
                source_path: "/content/hidden.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
        ];
        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::new(),
            content_pages: BTreeMap::from([("pages".to_string(), pages)]),
            site_title: String::new(),
            current_path: "/water.html".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
            sidebar_sections: vec![],
            metric_registry: BTreeMap::new(),
            metric_captions: BTreeMap::new(),
            pond_statuses: None,
            generated_at: String::new(),
            default_range: None,
            labels: BTreeMap::new(),
            explore_datasets: vec![],
            explore_url: None,
            annotations: vec![],
        };
        let html = render_content_nav(&ctx, "pages");
        // Hidden page excluded
        assert!(
            !html.contains("Hidden Page"),
            "Hidden page should be excluded: {}",
            html
        );
        // Wrapped in nav
        assert!(
            html.contains("<nav class=\"nav-list\">"),
            "Expected nav wrapper: {}",
            html
        );
        // No sections -> flat <ul>
        assert!(
            html.contains("<ul>"),
            "Expected ul for unsectioned pages: {}",
            html
        );
        // Active page gets class
        assert!(
            html.contains(r#"<li class="active"><a href="/water.html" aria-current="page">Water System</a></li>"#),
            "Expected active class on Water System, got: {}", html
        );
        // Non-active page
        assert!(
            html.contains("<li><a href=\"/history.html\">History</a></li>"),
            "Expected plain li for History, got: {}",
            html
        );
    }

    #[test]
    fn test_render_content_nav_sections() {
        let pages = vec![
            ContentPage {
                title: "Water".to_string(),
                slug: "water".to_string(),
                weight: 10,
                hidden: false,
                section: Some("About".to_string()),
                source_path: "/content/water.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
            ContentPage {
                title: "History".to_string(),
                slug: "history".to_string(),
                weight: 20,
                hidden: false,
                section: Some("About".to_string()),
                source_path: "/content/history.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
            ContentPage {
                title: "Blog".to_string(),
                slug: "blog".to_string(),
                weight: 80,
                hidden: false,
                section: Some("Blog".to_string()),
                source_path: "/content/blog.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
        ];
        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::new(),
            content_pages: BTreeMap::from([("pages".to_string(), pages)]),
            site_title: String::new(),
            current_path: "/water.html".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
            sidebar_sections: vec![],
            metric_registry: BTreeMap::new(),
            metric_captions: BTreeMap::new(),
            pond_statuses: None,
            generated_at: String::new(),
            default_range: None,
            labels: BTreeMap::new(),
            explore_datasets: vec![],
            explore_url: None,
            annotations: vec![],
        };
        let html = render_content_nav(&ctx, "pages");
        // Active section is expanded
        assert!(
            html.contains("nav-section expanded"),
            "Expected expanded class on active section, got: {}",
            html
        );
        // Section title rendered
        assert!(
            html.contains("<h3 class=\"nav-section-title\">About</h3>"),
            "Expected About section title, got: {}",
            html
        );
        // Inactive section is not expanded
        assert!(
            html.contains("\"nav-section\""),
            "Expected collapsed Blog section, got: {}",
            html
        );
        // No <details> or <summary>
        assert!(
            !html.contains("<details"),
            "Should not use <details>: {}",
            html
        );
        assert!(
            !html.contains("<summary"),
            "Should not use <summary>: {}",
            html
        );

        // Exactly one section is expanded
        assert_eq!(
            html.matches("nav-section expanded").count(),
            1,
            "Exactly one section should be expanded, got: {}",
            html
        );

        // When current_path is the Blog page, Blog section is expanded, About is not.
        let ctx_blog = ShortcodeContext {
            current_path: "/blog.html".to_string(),
            ..ctx.clone()
        };
        let html_blog = render_content_nav(&ctx_blog, "pages");
        assert!(
            html_blog.contains(
                "<div class=\"nav-section expanded\">\n  <h3 class=\"nav-section-title\">Blog</h3>"
            ),
            "Blog section should be expanded on blog page, got: {}",
            html_blog
        );
        assert_eq!(
            html_blog.matches("nav-section expanded").count(),
            1,
            "Only Blog section should be expanded, got: {}",
            html_blog
        );

        // When no page is active (home page), the first section is expanded.
        let ctx_home = ShortcodeContext {
            current_path: "/index.html".to_string(),
            ..ctx.clone()
        };
        let html_home = render_content_nav(&ctx_home, "pages");
        assert!(
            html_home.contains(
                "<div class=\"nav-section expanded\">\n  <h3 class=\"nav-section-title\">About</h3>"
            ),
            "First section (About) should be expanded on home page, got: {}",
            html_home
        );
        assert_eq!(
            html_home.matches("nav-section expanded").count(),
            1,
            "Only one section expanded on home page, got: {}",
            html_home
        );
    }

    #[test]
    fn test_render_content_nav_explicit_order() {
        let pages = vec![
            ContentPage {
                title: "Water".to_string(),
                slug: "water".to_string(),
                weight: 10,
                hidden: false,
                section: Some("Main".to_string()),
                source_path: "/content/water.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
            ContentPage {
                title: "Blog".to_string(),
                slug: "blog".to_string(),
                weight: 20,
                hidden: false,
                section: Some("Main".to_string()),
                source_path: "/content/blog.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
            ContentPage {
                // Page title is longer than sidebar label
                title: "Our History".to_string(),
                slug: "history".to_string(),
                weight: 30,
                hidden: false,
                section: Some("Main".to_string()),
                source_path: "/content/history.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
        ];
        // Sidebar labels: short names that match within page titles.
        // "History" matches "Our History", displayed as "History".
        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::new(),
            content_pages: BTreeMap::from([("pages".to_string(), pages)]),
            site_title: String::new(),
            current_path: "/blog.html".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
            sidebar_sections: vec![
                SidebarEntry::Simple("Blog".to_string()),
                SidebarEntry::Simple("History".to_string()),
                SidebarEntry::Simple("Water".to_string()),
            ],
            metric_registry: BTreeMap::new(),
            metric_captions: BTreeMap::new(),
            pond_statuses: None,
            generated_at: String::new(),
            default_range: None,
            labels: BTreeMap::new(),
            explore_datasets: vec![],
            explore_url: None,
            annotations: vec![],
        };
        let html = render_content_nav(&ctx, "pages");
        // All three present with sidebar label text (not page title)
        assert!(html.contains(">Blog<"), "Blog present: {}", html);
        assert!(html.contains(">History<"), "History label: {}", html);
        assert!(html.contains(">Water<"), "Water present: {}", html);
        // "Our History" should NOT appear -- sidebar label overrides
        assert!(!html.contains("Our History"), "Label override: {}", html);
        // Blog appears before History, History before Water (config order, not weight)
        let blog_pos = html.find("Blog").unwrap();
        let history_pos = html.find("History").unwrap();
        let water_pos = html.find("Water").unwrap();
        assert!(blog_pos < history_pos, "Blog before History");
        assert!(history_pos < water_pos, "History before Water");
    }

    #[test]
    fn test_sidebar_prefers_exact_title_match() {
        // "Water" sidebar label should match page titled "Water",
        // not "CSD for Water" which merely contains "Water".
        let pages = vec![
            ContentPage {
                title: "CSD for Water".to_string(),
                slug: "csd_for_water".to_string(),
                weight: 10,
                hidden: false,
                section: Some("Blog".to_string()),
                source_path: "/content/csd_for_water.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
            ContentPage {
                title: "Water".to_string(),
                slug: "water".to_string(),
                weight: 20,
                hidden: false,
                section: Some("Main".to_string()),
                source_path: "/content/water.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
        ];
        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::new(),
            content_pages: BTreeMap::from([("pages".to_string(), pages)]),
            site_title: String::new(),
            current_path: "/index.html".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
            sidebar_sections: vec![SidebarEntry::Simple("Water".to_string())],
            metric_registry: BTreeMap::new(),
            metric_captions: BTreeMap::new(),
            pond_statuses: None,
            generated_at: String::new(),
            default_range: None,
            labels: BTreeMap::new(),
            explore_datasets: vec![],
            explore_url: None,
            annotations: vec![],
        };
        let html = render_content_nav(&ctx, "pages");
        assert!(
            html.contains("href=\"/water.html\""),
            "Should link to water.html, not csd_for_water.html: {}",
            html
        );
        assert!(
            !html.contains("csd_for_water"),
            "Should not link to csd_for_water: {}",
            html
        );
    }

    #[test]
    fn test_render_content_nav_with_children() {
        use crate::config::SidebarChild;

        let pages = vec![
            ContentPage {
                title: "Blog".to_string(),
                slug: "blog".to_string(),
                weight: 10,
                hidden: false,
                section: Some("Main".to_string()),
                source_path: "/content/blog.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
            ContentPage {
                title: "Monitoring".to_string(),
                slug: "monitoring".to_string(),
                weight: 20,
                hidden: false,
                section: Some("Main".to_string()),
                source_path: "/content/monitoring.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
        ];

        // When on the monitoring page, children should be expanded
        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::new(),
            content_pages: BTreeMap::from([("pages".to_string(), pages.clone())]),
            site_title: String::new(),
            current_path: "/monitoring.html".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
            sidebar_sections: vec![
                SidebarEntry::Simple("Blog".to_string()),
                SidebarEntry::WithChildren {
                    label: "Monitoring".to_string(),
                    href: None,
                    children: vec![
                        SidebarChild {
                            label: "Well Depth".to_string(),
                            href: Some("/data/well-depth.html".to_string()),
                            children: vec![],
                        },
                        SidebarChild {
                            label: "Tank Level".to_string(),
                            href: Some("/data/tank-level.html".to_string()),
                            children: vec![],
                        },
                    ],
                },
            ],
            metric_registry: BTreeMap::new(),
            metric_captions: BTreeMap::new(),
            pond_statuses: None,
            generated_at: String::new(),
            default_range: None,
            labels: BTreeMap::new(),
            explore_datasets: vec![],
            explore_url: None,
            annotations: vec![],
        };
        let html = render_content_nav(&ctx, "pages");
        assert!(html.contains(">Monitoring<"), "Parent present: {}", html);
        assert!(html.contains(">Well Depth<"), "Child present: {}", html);
        assert!(html.contains(">Tank Level<"), "Child present: {}", html);
        assert!(
            html.contains("subnav expanded"),
            "Expanded when parent active: {}",
            html
        );
        // Blog should not have subnav
        assert!(
            html.contains("<li class=\"active\"><a href=\"/monitoring.html\""),
            "Parent is active: {}",
            html
        );

        // When on a child page, parent + children should be expanded
        let ctx_child = ShortcodeContext {
            current_path: "/data/well-depth.html".to_string(),
            ..ctx.clone()
        };
        let html2 = render_content_nav(&ctx_child, "pages");
        assert!(
            html2.contains("subnav expanded"),
            "Expanded when child active: {}",
            html2
        );
        assert!(
            html2.contains("<li class=\"active\"><a href=\"/data/well-depth.html\""),
            "Child is active: {}",
            html2
        );

        // When on blog page, subnav should be collapsed
        let ctx_blog = ShortcodeContext {
            current_path: "/blog.html".to_string(),
            ..ctx.clone()
        };
        let html3 = render_content_nav(&ctx_blog, "pages");
        assert!(
            html3.contains("class=\"subnav\""),
            "Collapsed when neither active: {}",
            html3
        );
        assert!(
            !html3.contains("subnav expanded"),
            "Not expanded: {}",
            html3
        );
    }

    #[test]
    fn test_render_figure() {
        use crate::markdown::ShortcodeArgs;
        use std::collections::HashMap;

        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::new(),
            content_pages: BTreeMap::new(),
            site_title: String::new(),
            current_path: "/".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
            sidebar_sections: vec![],
            metric_registry: BTreeMap::new(),
            metric_captions: BTreeMap::new(),
            pond_statuses: None,
            generated_at: String::new(),
            default_range: None,
            labels: BTreeMap::new(),
            explore_datasets: vec![],
            explore_url: None,
            annotations: vec![],
        };

        let args = ShortcodeArgs::from_map(HashMap::from([
            ("src".to_string(), "/img/photo.jpg".to_string()),
            ("caption".to_string(), "A nice photo".to_string()),
            ("float".to_string(), "right".to_string()),
        ]));

        let html = render_figure(&ctx, &args);
        assert!(
            html.contains("figure-right"),
            "Expected float right class: {}",
            html
        );
        assert!(
            html.contains("/img/photo.jpg"),
            "Expected src in img: {}",
            html
        );
        assert!(
            html.contains("<figcaption>A nice photo</figcaption>"),
            "Expected caption: {}",
            html
        );
    }

    #[test]
    fn test_render_blog_grid() {
        let pages = vec![
            ContentPage {
                title: "Old Post".to_string(),
                slug: "old-post".to_string(),
                weight: 10,
                hidden: false,
                section: Some("Blog".to_string()),
                source_path: "/content/old-post.md".to_string(),
                date: Some("2024-01-15".to_string()),
                summary: Some("An older post".to_string()),
                image: None,
                url_path: String::new(),
            },
            ContentPage {
                title: "New Post".to_string(),
                slug: "new-post".to_string(),
                weight: 20,
                hidden: false,
                section: Some("Blog".to_string()),
                source_path: "/content/new-post.md".to_string(),
                date: Some("2025-03-10".to_string()),
                summary: Some("A newer post".to_string()),
                image: Some("/img/hero.jpg".to_string()),
                url_path: "/blog/new-post".to_string(),
            },
            ContentPage {
                title: "Not a Blog".to_string(),
                slug: "about".to_string(),
                weight: 5,
                hidden: false,
                section: Some("Main".to_string()),
                source_path: "/content/about.md".to_string(),
                date: None,
                summary: None,
                image: None,
                url_path: String::new(),
            },
            ContentPage {
                title: "Hidden Blog".to_string(),
                slug: "hidden-blog".to_string(),
                weight: 30,
                hidden: true,
                section: Some("Blog".to_string()),
                source_path: "/content/hidden-blog.md".to_string(),
                date: Some("2025-02-01".to_string()),
                summary: None,
                image: None,
                url_path: String::new(),
            },
        ];
        let ctx = ShortcodeContext {
            captures: vec![],
            datafiles: vec![],
            collections: BTreeMap::new(),
            content_pages: BTreeMap::from([("pages".to_string(), pages)]),
            site_title: String::new(),
            current_path: "/blog.html".to_string(),
            breadcrumbs: vec![],
            base_url: "/".to_string(),
            sidebar_sections: vec![],
            metric_registry: BTreeMap::new(),
            metric_captions: BTreeMap::new(),
            pond_statuses: None,
            generated_at: String::new(),
            default_range: None,
            labels: BTreeMap::new(),
            explore_datasets: vec![],
            explore_url: None,
            annotations: vec![],
        };

        let html = render_blog_grid(&ctx, "pages", "Blog");

        // Grid wrapper
        assert!(
            html.contains("class=\"blog-grid\""),
            "Expected blog-grid class: {}",
            html
        );
        // New post appears (newest first)
        assert!(html.contains("New Post"), "Expected New Post: {}", html);
        // Old post appears
        assert!(html.contains("Old Post"), "Expected Old Post: {}", html);
        // New post should appear before Old post (date descending)
        let new_pos = html.find("New Post").unwrap();
        let old_pos = html.find("Old Post").unwrap();
        assert!(
            new_pos < old_pos,
            "New post should appear before old post (date desc)"
        );
        // Non-blog page excluded
        assert!(
            !html.contains("Not a Blog"),
            "Non-blog page should be excluded: {}",
            html
        );
        // Hidden page excluded
        assert!(
            !html.contains("Hidden Blog"),
            "Hidden page should be excluded: {}",
            html
        );
        // Hero image present for new post
        assert!(
            html.contains("/img/hero.jpg"),
            "Expected hero image: {}",
            html
        );
        // Date formatted
        assert!(
            html.contains("March 10, 2025"),
            "Expected formatted date: {}",
            html
        );
        // Summary present
        assert!(html.contains("A newer post"), "Expected summary: {}", html);
        // Links use url_path when set (nested route mount)
        assert!(
            html.contains("href=\"/blog/new-post.html\""),
            "Expected url_path-based href: {}",
            html
        );
        // Falls back to /{slug} when url_path is empty
        assert!(
            html.contains("href=\"/old-post.html\""),
            "Expected fallback href for old-post: {}",
            html
        );
    }

    #[test]
    fn test_format_date() {
        assert_eq!(format_date("2025-03-10"), "March 10, 2025");
        assert_eq!(format_date("1995-06-15"), "June 15, 1995");
        assert_eq!(format_date("2024-01-01"), "January 1, 2024");
        assert_eq!(format_date("bad-date"), "bad-date");
    }

    // ── Health classification ──────────────────────────────────────

    /// 5 minutes worth of microseconds, used as a reference "now".
    const NOW_US: i64 = 1_700_000_000_000_000;
    const ONE_MIN_US: i64 = 60 * 1_000_000;

    #[test]
    fn classify_green_when_recent_ok_and_no_err() {
        let h = classify(
            NOW_US,
            Some(NOW_US - 5 * ONE_MIN_US), // last_ok 5min ago
            None,
            Some(true),
            Some(600), // 10min interval -> 20min stale threshold
            None,
        );
        assert_eq!(h, Health::Green);
    }

    #[test]
    fn classify_yellow_when_ok_older_than_two_intervals() {
        let h = classify(
            NOW_US,
            Some(NOW_US - 25 * ONE_MIN_US), // last_ok 25min ago
            None,
            Some(true),
            Some(600), // 20min threshold
            None,
        );
        assert_eq!(h, Health::Yellow);
    }

    #[test]
    fn classify_green_when_run_wall_extends_threshold() {
        // interval 60s alone -> 2min threshold would flag this 3min-old
        // ok as overdue, but a 120s run wall extends the period to
        // interval+run=180s and the threshold to 2*180s=6min -> Green.
        let h = classify(
            NOW_US,
            Some(NOW_US - 3 * ONE_MIN_US), // last_ok 3min ago
            None,
            Some(true),
            Some(60),  // 1min interval
            Some(120), // 2min run wall
        );
        assert_eq!(h, Health::Green);
    }

    #[test]
    fn classify_yellow_ignores_zero_run_wall() {
        // With run_wall 0, threshold is the plain 2*interval; a 3min-old
        // ok against a 1min interval (2min threshold) is overdue.
        let h = classify(
            NOW_US,
            Some(NOW_US - 3 * ONE_MIN_US),
            None,
            Some(true),
            Some(60),
            Some(0),
        );
        assert_eq!(h, Health::Yellow);
    }

    #[test]
    fn classify_red_when_err_more_recent_than_ok() {
        let h = classify(
            NOW_US,
            Some(NOW_US - 5 * ONE_MIN_US),
            Some(NOW_US - ONE_MIN_US),
            Some(true),
            Some(600),
            None,
        );
        assert_eq!(h, Health::Red);
    }

    #[test]
    fn classify_red_when_err_only() {
        let h = classify(
            NOW_US,
            None,
            Some(NOW_US - ONE_MIN_US),
            Some(true),
            Some(600),
            None,
        );
        assert_eq!(h, Health::Red);
    }

    #[test]
    fn classify_red_when_timer_inactive() {
        let h = classify(
            NOW_US,
            Some(NOW_US - ONE_MIN_US),
            None,
            Some(false),
            Some(600),
            None,
        );
        assert_eq!(h, Health::Red);
    }

    #[test]
    fn classify_unknown_without_interval() {
        let h = classify(
            NOW_US,
            Some(NOW_US - ONE_MIN_US),
            None,
            Some(true),
            None,
            None,
        );
        assert_eq!(h, Health::Unknown);
    }

    #[test]
    fn classify_unknown_with_zero_interval() {
        let h = classify(
            NOW_US,
            Some(NOW_US - ONE_MIN_US),
            None,
            Some(true),
            Some(0),
            None,
        );
        assert_eq!(h, Health::Unknown);
    }

    #[test]
    fn classify_unknown_without_last_ok() {
        let h = classify(NOW_US, None, None, Some(true), Some(600), None);
        assert_eq!(h, Health::Unknown);
    }

    #[test]
    fn classify_red_takes_priority_over_yellow_for_inactive_timer() {
        // Even a stale ok with no err should still be Red when the
        // timer is stopped -- inactive trumps overdue.
        let h = classify(
            NOW_US,
            Some(NOW_US - 60 * ONE_MIN_US),
            None,
            Some(false),
            Some(600),
            None,
        );
        assert_eq!(h, Health::Red);
    }

    // ── render_pond_status_grid output ─────────────────────────────

    fn status_for(unit: &str, health: Health) -> PondStatus {
        PondStatus {
            unit: unit.to_string(),
            last_seen_us: Some(NOW_US),
            last_ok_us: Some(NOW_US - 30 * 1_000_000),
            last_ok_msg: Some("Run summary outcome=ok".to_string()),
            last_err_us: None,
            last_err_msg: None,
            peak_rss_bytes: Some(123 * 1024 * 1024),
            tail_messages: vec!["hello".to_string()],
            timer_active: Some(true),
            last_run_seconds_ago: Some(30),
            timer_interval_s: Some(60),
            run_wall_s: Some(30),
            health,
        }
    }

    #[test]
    fn render_emits_health_class_per_card() {
        let statuses = vec![
            status_for("a.service", Health::Green),
            status_for("b.service", Health::Yellow),
            status_for("c.service", Health::Red),
            status_for("d.service", Health::Unknown),
        ];
        let html = render_pond_status_grid(Some(&statuses), "2026-05-04 22:00:00 UTC");
        assert!(html.contains("pond-status-card health-green"), "{}", html);
        assert!(html.contains("pond-status-card health-yellow"), "{}", html);
        assert!(html.contains("pond-status-card health-red"), "{}", html);
        assert!(html.contains("pond-status-card health-unknown"), "{}", html);
    }

    #[test]
    fn render_emits_time_markers_with_data_utc_us() {
        let statuses = vec![status_for("a.service", Health::Green)];
        let html = render_pond_status_grid(Some(&statuses), "2026-05-04 22:00:00 UTC");
        assert!(
            html.contains("data-utc-us=\""),
            "expected <time data-utc-us> markers, got: {}",
            html
        );
        // The em-dash render path is used for None timestamps; ensure
        // we did not emit empty <time> elements.
        assert!(!html.contains("<time></time>"), "{}", html);
    }

    #[test]
    fn render_emits_period_and_timer_rows() {
        let statuses = vec![status_for("a.service", Health::Green)];
        let html = render_pond_status_grid(Some(&statuses), "2026-05-04 22:00:00 UTC");
        assert!(html.contains("<dt>Timer</dt>"), "{}", html);
        assert!(html.contains("<dt>Period</dt>"), "{}", html);
    }

    #[test]
    fn render_returns_empty_when_statuses_none() {
        let html = render_pond_status_grid(None, "2026-05-04 22:00:00 UTC");
        assert!(html.is_empty());
    }
}
