// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Route expansion -- turns a `SiteConfig` route tree + export results into
//! a flat list of `PageJob`s, one per output HTML file.
//!
//! Static routes produce one page. Template routes produce one page per unique
//! `$0` value from the linked export stage.

use crate::config::{RouteConfig, RouteType, SiteConfig};
use crate::shortcodes::{ExportContext, ExportedFile};
use std::collections::BTreeMap;

/// Metadata extracted from a content page's frontmatter.
#[derive(Debug, Clone)]
pub struct ContentPage {
    /// Page title (from frontmatter)
    pub title: String,
    /// URL slug (from frontmatter or derived from filename)
    pub slug: String,
    /// Sort weight (lower = higher in nav). Default: 100
    pub weight: i32,
    /// If true, page renders but does not appear in nav
    pub hidden: bool,
    /// Navigation section (for grouping in sidebar). Default: none
    pub section: Option<String>,
    /// Pond path to the source markdown file
    pub source_path: String,
    /// Publication date (ISO 8601, e.g. "2024-06-15")
    pub date: Option<String>,
    /// Short summary for blog card display
    pub summary: Option<String>,
    /// Image URL for blog card hero image
    pub image: Option<String>,
    /// Absolute path component of the page's emitted URL, with leading
    /// `/`, no `.html` extension, and no `base_url` prefix
    /// (e.g. `/blog/herewego` or `/water`). Populated by route
    /// expansion; empty when the page is not bound to a route.
    pub url_path: String,
}

/// All discovered content pages for one content stage, sorted by (weight, title).
#[derive(Debug, Clone)]
pub struct ContentContext {
    pub pages: Vec<ContentPage>,
}

/// One page to generate -- the flattened output of route expansion.
#[derive(Debug, Clone)]
pub struct PageJob {
    /// Output path relative to dist/ (e.g., "params/Temperature.html")
    pub output_path: String,
    /// Path to the markdown template (e.g., "/site/param.md")
    pub page_source: String,
    /// Capture group values ($0, $1, ...) -- empty for static routes
    pub captures: Vec<String>,
    /// Exported data files for this page (empty for static routes)
    pub datafiles: Vec<ExportedFile>,
    /// Optional annotation data files (a secondary export, resolved pond-wide)
    /// drawn as a shaded chart background; empty when the route declares none.
    pub annotations: Vec<ExportedFile>,
    /// Breadcrumb trail: (label, url)
    pub breadcrumbs: Vec<(String, String)>,
}

/// Index of canonical URL paths for content-stage pages discovered
/// during route expansion. Keyed first by content-stage name, then
/// by the page's `source_path`. Values are absolute URL path
/// components (with leading `/`, no `.html`, no `base_url` prefix).
pub type ContentUrlIndex = BTreeMap<String, BTreeMap<String, String>>;

/// Result of route expansion: the flat job list plus an index of
/// canonical URLs for content-stage pages so other components
/// (shortcodes, RSS feed) can build correct links regardless of
/// where a content stage is mounted in the route tree.
#[derive(Debug, Clone, Default)]
pub struct RouteExpansion {
    pub jobs: Vec<PageJob>,
    pub content_urls: ContentUrlIndex,
}

/// Expand the config route tree into a flat list of page jobs.
///
/// Template routes are expanded using export results: one page per unique
/// `$0` value in the matching export stage.
/// Content routes are expanded using content results: one page per matched file.
pub fn expand_routes(
    config: &SiteConfig,
    exports: &BTreeMap<String, ExportContext>,
    content: &BTreeMap<String, ContentContext>,
) -> RouteExpansion {
    let mut jobs = Vec::new();
    let mut content_urls: ContentUrlIndex = BTreeMap::new();
    let base = config.site.base_url.trim_end_matches('/').to_string();
    let home_url = if base.is_empty() || base == "/" {
        "/".to_string()
    } else {
        format!("{}/", base)
    };
    let breadcrumbs = vec![("Home".to_string(), home_url)];

    for route in &config.routes {
        expand_route(
            route,
            "",
            &breadcrumbs,
            &base,
            exports,
            content,
            &mut jobs,
            &mut content_urls,
        );
    }

    RouteExpansion { jobs, content_urls }
}

/// Resolve a route's optional `annotations` export into a flat file list.
///
/// Annotations are pond-wide interval data (all `$0` groups of the named export
/// flattened), so every page under the route shares the same set. Returns an
/// empty vec when the route declares no annotations (or the export is missing).
fn resolve_annotations(
    route: &RouteConfig,
    exports: &BTreeMap<String, ExportContext>,
) -> Vec<ExportedFile> {
    match route.annotations.as_deref() {
        Some(name) => match exports.get(name) {
            Some(ctx) => ctx.by_key.values().flatten().cloned().collect(),
            None => {
                log::warn!(
                    "Route '{}' references annotations export '{}' which has no results",
                    route.name,
                    name
                );
                vec![]
            }
        },
        None => vec![],
    }
}

/// Recursively expand a single route node and its children.
#[allow(clippy::too_many_arguments)]
fn expand_route(
    route: &RouteConfig,
    parent_path: &str,
    parent_breadcrumbs: &[(String, String)],
    base_url: &str,
    exports: &BTreeMap<String, ExportContext>,
    content: &BTreeMap<String, ContentContext>,
    jobs: &mut Vec<PageJob>,
    content_urls: &mut ContentUrlIndex,
) {
    match route.route_type {
        RouteType::Static => {
            let url_path = if route.slug.is_empty() {
                parent_path.to_string()
            } else {
                format!("{}/{}", parent_path, route.slug)
            };

            let output_path = if url_path.is_empty() {
                "index.html".to_string()
            } else {
                format!("{}/index.html", url_path.trim_start_matches('/'))
            };

            let mut breadcrumbs = parent_breadcrumbs.to_vec();
            if !route.slug.is_empty() {
                let raw_url = if url_path.starts_with('/') {
                    url_path.clone()
                } else {
                    format!("/{}", url_path)
                };
                let url = prefix_base(base_url, &raw_url);
                breadcrumbs.push((route.name.clone(), url));
            }

            if let Some(ref page) = route.page {
                // A static route may optionally bind an export stage: its files
                // (all $0 groups flattened) are attached to the single page so a
                // site-wide page (e.g. the data explorer) can list every file.
                let datafiles = match route.export.as_deref() {
                    Some(export_name) => match exports.get(export_name) {
                        Some(export_ctx) => export_ctx.by_key.values().flatten().cloned().collect(),
                        None => {
                            log::warn!(
                                "Static route '{}' references export '{}' which has no results",
                                route.name,
                                export_name
                            );
                            vec![]
                        }
                    },
                    None => vec![],
                };
                jobs.push(PageJob {
                    output_path,
                    page_source: page.clone(),
                    captures: vec![],
                    datafiles,
                    annotations: resolve_annotations(route, exports),
                    breadcrumbs: breadcrumbs.clone(),
                });
            }

            // Recurse into children
            for child in &route.routes {
                expand_route(
                    child,
                    &url_path,
                    &breadcrumbs,
                    base_url,
                    exports,
                    content,
                    jobs,
                    content_urls,
                );
            }
        }

        RouteType::Template => {
            let export_name = route.export.as_deref().unwrap_or("");
            let export_ctx = match exports.get(export_name) {
                Some(ctx) => ctx,
                None => {
                    log::warn!(
                        "Route '{}' references export '{}' which has no results",
                        route.name,
                        export_name
                    );
                    return;
                }
            };

            // One page per unique $0 value
            for (key, files) in &export_ctx.by_key {
                // Replace $0 in slug with actual value
                let slug = route.slug.replace("$0", key);
                let url_path = format!("{}/{}", parent_path, slug);
                let output_path = format!("{}.html", url_path.trim_start_matches('/'));

                let mut breadcrumbs = parent_breadcrumbs.to_vec();
                let raw_url = if url_path.starts_with('/') {
                    url_path.clone()
                } else {
                    format!("/{}", url_path)
                };
                let url = prefix_base(base_url, &raw_url);
                breadcrumbs.push((key.clone(), url));

                let captures = vec![key.clone()];

                if let Some(ref page) = route.page {
                    jobs.push(PageJob {
                        output_path,
                        page_source: page.clone(),
                        captures,
                        datafiles: files.clone(),
                        annotations: resolve_annotations(route, exports),
                        breadcrumbs: breadcrumbs.clone(),
                    });
                }

                // Recurse into children (rare for template routes, but supported)
                for child in &route.routes {
                    expand_route(
                        child,
                        &url_path,
                        &breadcrumbs,
                        base_url,
                        exports,
                        content,
                        jobs,
                        content_urls,
                    );
                }
            }
        }

        RouteType::Content => {
            let content_name = route.content.as_deref().unwrap_or("");
            let content_ctx = match content.get(content_name) {
                Some(ctx) => ctx,
                None => {
                    log::warn!(
                        "Route '{}' references content '{}' which has no results",
                        route.name,
                        content_name
                    );
                    return;
                }
            };

            // One page per content file, sorted by (weight, title)
            let urls = content_urls.entry(content_name.to_string()).or_default();
            for page in &content_ctx.pages {
                let url_path = if parent_path.is_empty() {
                    format!("/{}", page.slug)
                } else {
                    format!("{}/{}", parent_path, page.slug)
                };
                let output_path = format!("{}.html", url_path.trim_start_matches('/'));

                // Record canonical URL for this page. If the same source
                // file is mounted by multiple content routes, keep the
                // first URL and warn -- ambiguous link targets are a
                // config bug worth surfacing.
                if let Some(existing) = urls.get(&page.source_path) {
                    if existing != &url_path {
                        log::warn!(
                            "Content '{}' source '{}' is mounted at both '{}' and '{}'; using '{}' for links",
                            content_name,
                            page.source_path,
                            existing,
                            url_path,
                            existing
                        );
                    }
                } else {
                    urls.insert(page.source_path.clone(), url_path.clone());
                }

                let mut breadcrumbs = parent_breadcrumbs.to_vec();
                let url = prefix_base(base_url, &url_path);
                breadcrumbs.push((page.title.clone(), url));

                jobs.push(PageJob {
                    output_path,
                    page_source: page.source_path.clone(),
                    captures: vec![],
                    datafiles: vec![],
                    annotations: vec![],
                    breadcrumbs,
                });
            }

            // Content routes do not recurse -- each page is a leaf
        }
    }
}

/// Prepend the base URL prefix to a path.
///
/// `base_url` is pre-trimmed of trailing `/` (empty string means root).
fn prefix_base(base_url: &str, path: &str) -> String {
    if base_url.is_empty() {
        path.to_string()
    } else {
        format!("{}{}", base_url, path)
    }
}

/// Collect all unique $0 values from an export context -- used for navigation.
pub fn collection_keys(export: &ExportContext) -> Vec<String> {
    export.by_key.keys().cloned().collect()
}

/// Build synthetic ContentPage entries from template routes so that
/// export-based pages are discoverable by the content_nav shortcode.
///
/// Template routes generate pages from export $0 values (e.g., "DO",
/// "Temperature"). These are content pages and should be navigable
/// via the sidebar just like explicit content pages.
pub fn build_template_content_pages(
    config: &SiteConfig,
    exports: &BTreeMap<String, ExportContext>,
) -> BTreeMap<String, Vec<ContentPage>> {
    let mut result: BTreeMap<String, Vec<ContentPage>> = BTreeMap::new();

    fn collect_template_pages(
        route: &RouteConfig,
        parent_path: &str,
        exports: &BTreeMap<String, ExportContext>,
        result: &mut BTreeMap<String, Vec<ContentPage>>,
    ) {
        match route.route_type {
            RouteType::Static => {
                let url_path = if route.slug.is_empty() {
                    parent_path.to_string()
                } else {
                    format!("{}/{}", parent_path, route.slug)
                };
                for child in &route.routes {
                    collect_template_pages(child, &url_path, exports, result);
                }
            }
            RouteType::Template => {
                let export_name = route.export.as_deref().unwrap_or("");
                if let Some(export_ctx) = exports.get(export_name) {
                    let pages: Vec<ContentPage> = export_ctx
                        .by_key
                        .keys()
                        .map(|key| {
                            let slug = route.slug.replace("$0", key);
                            let url_path = format!("{}/{}", parent_path, slug);
                            ContentPage {
                                title: key.clone(),
                                slug: url_path.trim_start_matches('/').to_string(),
                                weight: 100,
                                hidden: false,
                                section: None,
                                source_path: route.page.clone().unwrap_or_default(),
                                date: None,
                                summary: None,
                                image: None,
                                url_path,
                            }
                        })
                        .collect();
                    result
                        .entry(export_name.to_string())
                        .or_default()
                        .extend(pages);
                }
            }
            RouteType::Content => {}
        }
    }

    for route in &config.routes {
        collect_template_pages(route, "", exports, &mut result);
    }
    result
}

/// Build the collections map for shortcode context (collection name -> list of keys).
///
/// Merges export collections (keyed by $0 values) with content collections
/// (keyed by page titles, ordered by weight).
pub fn build_collections(
    exports: &BTreeMap<String, ExportContext>,
    content: &BTreeMap<String, ContentContext>,
) -> BTreeMap<String, Vec<String>> {
    let mut collections: BTreeMap<String, Vec<String>> = exports
        .iter()
        .map(|(name, ctx)| (name.clone(), collection_keys(ctx)))
        .collect();

    // Content collections use titles (ordered by weight) as the values
    for (name, ctx) in content {
        let titles: Vec<String> = ctx
            .pages
            .iter()
            .filter(|p| !p.hidden)
            .map(|p| p.title.clone())
            .collect();
        collections.insert(name.clone(), titles);
    }

    collections
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn test_config() -> SiteConfig {
        SiteConfig {
            site: SiteMeta {
                title: "Test".to_string(),
                base_url: "/test/".to_string(),
                site_url: None,
                github_url: None,
                default_range: None,
            },
            content: vec![],
            exports: vec![ExportStage {
                name: "params".to_string(),
                pattern: "/reduced/single_param/*/*.series".to_string(),
                target_points: 1500,
                timestamp_column: "timestamp".to_string(),
            }],
            reports: BTreeMap::new(),
            routes: vec![RouteConfig {
                name: "home".to_string(),
                route_type: RouteType::Static,
                slug: "".to_string(),
                page: Some("/site/index.md".to_string()),
                export: None,
                annotations: None,
                content: None,
                routes: vec![RouteConfig {
                    name: "params".to_string(),
                    route_type: RouteType::Static,
                    slug: "params".to_string(),
                    page: Some("/site/params.md".to_string()),
                    export: None,
                    annotations: None,
                    content: None,
                    routes: vec![RouteConfig {
                        name: "param".to_string(),
                        route_type: RouteType::Template,
                        slug: "$0".to_string(),
                        page: Some("/site/param.md".to_string()),
                        export: Some("params".to_string()),
                        annotations: None,
                        content: None,
                        routes: vec![],
                    }],
                }],
            }],
            partials: BTreeMap::new(),
            static_assets: vec![],
            sidebar: vec![],
            feed: None,
            theme: std::collections::BTreeMap::new(),
            subsites: vec![],
            footer: None,
            header: None,
            metric_registry: std::collections::BTreeMap::new(),
            metric_captions: std::collections::BTreeMap::new(),
            labels: std::collections::BTreeMap::new(),
            explore: None,
            status_grid: None,
        }
    }

    fn test_exports() -> BTreeMap<String, ExportContext> {
        let mut by_key = BTreeMap::new();
        by_key.insert(
            "Temperature".to_string(),
            vec![ExportedFile {
                path: "params/Temperature/data.parquet".to_string(),
                file: "data/Temperature/data.parquet".to_string(),
                captures: vec!["Temperature".to_string()],
                temporal: BTreeMap::new(),
                start_time: 0,
                end_time: 0,
            }],
        );
        by_key.insert(
            "DO".to_string(),
            vec![ExportedFile {
                path: "params/DO/data.parquet".to_string(),
                file: "data/DO/data.parquet".to_string(),
                captures: vec!["DO".to_string()],
                temporal: BTreeMap::new(),
                start_time: 0,
                end_time: 0,
            }],
        );

        let mut exports = BTreeMap::new();
        exports.insert(
            "params".to_string(),
            ExportContext {
                by_key,
                columns: vec![],
            },
        );
        exports
    }

    #[test]
    fn test_expand_static_route() {
        let config = test_config();
        let exports = test_exports();
        let content = BTreeMap::new();
        let jobs = expand_routes(&config, &exports, &content).jobs;

        // Should have: index.html, params/index.html, params/Temperature.html, params/DO.html
        assert_eq!(jobs.len(), 4);
        assert_eq!(jobs[0].output_path, "index.html");
        assert_eq!(jobs[1].output_path, "params/index.html");
    }

    #[test]
    fn test_expand_template_route() {
        let config = test_config();
        let exports = test_exports();
        let content = BTreeMap::new();
        let jobs = expand_routes(&config, &exports, &content).jobs;

        let template_jobs: Vec<_> = jobs.iter().filter(|j| !j.captures.is_empty()).collect();
        assert_eq!(template_jobs.len(), 2);

        let paths: Vec<_> = template_jobs
            .iter()
            .map(|j| j.output_path.as_str())
            .collect();
        assert!(paths.contains(&"params/DO.html"));
        assert!(paths.contains(&"params/Temperature.html"));
    }

    #[test]
    fn test_breadcrumbs() {
        let config = test_config();
        let exports = test_exports();
        let content = BTreeMap::new();
        let jobs = expand_routes(&config, &exports, &content).jobs;

        // Template route should have: Home > params > Temperature
        let temp_job = jobs
            .iter()
            .find(|j| j.output_path == "params/Temperature.html")
            .unwrap();
        assert_eq!(temp_job.breadcrumbs.len(), 3);
        assert_eq!(temp_job.breadcrumbs[0].0, "Home");
        assert_eq!(temp_job.breadcrumbs[1].0, "params");
        assert_eq!(temp_job.breadcrumbs[2].0, "Temperature");
    }

    #[test]
    fn test_build_collections() {
        let exports = test_exports();
        let content = BTreeMap::new();
        let collections = build_collections(&exports, &content);
        assert_eq!(collections.len(), 1);
        let params = collections.get("params").unwrap();
        assert!(params.contains(&"Temperature".to_string()));
        assert!(params.contains(&"DO".to_string()));
    }

    #[test]
    fn test_expand_content_route() {
        let mut content = BTreeMap::new();
        content.insert(
            "pages".to_string(),
            ContentContext {
                pages: vec![
                    ContentPage {
                        title: "Water".to_string(),
                        slug: "water".to_string(),
                        weight: 10,
                        hidden: false,
                        section: None,
                        source_path: "/pages/water.md".to_string(),
                        date: None,
                        summary: None,
                        image: None,
                        url_path: String::new(),
                    },
                    ContentPage {
                        title: "History".to_string(),
                        slug: "history".to_string(),
                        weight: 30,
                        hidden: false,
                        section: None,
                        source_path: "/pages/history.md".to_string(),
                        date: None,
                        summary: None,
                        image: None,
                        url_path: String::new(),
                    },
                ],
            },
        );

        let config = SiteConfig {
            site: SiteMeta {
                title: "Test".to_string(),
                base_url: "/".to_string(),
                site_url: None,
                github_url: None,
                default_range: None,
            },
            content: vec![],
            exports: vec![],
            reports: BTreeMap::new(),
            routes: vec![RouteConfig {
                name: "home".to_string(),
                route_type: RouteType::Static,
                slug: "".to_string(),
                page: Some("/site/index.md".to_string()),
                export: None,
                annotations: None,
                content: None,
                routes: vec![RouteConfig {
                    name: "pages".to_string(),
                    route_type: RouteType::Content,
                    slug: "".to_string(),
                    page: None,
                    export: None,
                    annotations: None,
                    content: Some("pages".to_string()),
                    routes: vec![],
                }],
            }],
            partials: BTreeMap::new(),
            static_assets: vec![],
            sidebar: vec![],
            feed: None,
            theme: std::collections::BTreeMap::new(),
            subsites: vec![],
            footer: None,
            header: None,
            metric_registry: std::collections::BTreeMap::new(),
            metric_captions: std::collections::BTreeMap::new(),
            labels: std::collections::BTreeMap::new(),
            explore: None,
            status_grid: None,
        };

        let exports = BTreeMap::new();
        let jobs = expand_routes(&config, &exports, &content).jobs;

        // index.html + water.html + history.html
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0].output_path, "index.html");
        assert_eq!(jobs[1].output_path, "water.html");
        assert_eq!(jobs[1].page_source, "/pages/water.md");
        assert_eq!(jobs[2].output_path, "history.html");

        // Breadcrumbs: Home > Water
        assert_eq!(jobs[1].breadcrumbs.len(), 2);
        assert_eq!(jobs[1].breadcrumbs[1].0, "Water");
    }

    #[test]
    fn test_content_collections() {
        let mut content = BTreeMap::new();
        content.insert(
            "pages".to_string(),
            ContentContext {
                pages: vec![
                    ContentPage {
                        title: "Water".to_string(),
                        slug: "water".to_string(),
                        weight: 10,
                        hidden: false,
                        section: None,
                        source_path: "/pages/water.md".to_string(),
                        date: None,
                        summary: None,
                        image: None,
                        url_path: String::new(),
                    },
                    ContentPage {
                        title: "Secret".to_string(),
                        slug: "secret".to_string(),
                        weight: 50,
                        hidden: true,
                        section: None,
                        source_path: "/pages/secret.md".to_string(),
                        date: None,
                        summary: None,
                        image: None,
                        url_path: String::new(),
                    },
                ],
            },
        );

        let exports = BTreeMap::new();
        let collections = build_collections(&exports, &content);
        let pages = collections.get("pages").unwrap();
        assert_eq!(pages.len(), 1); // hidden page excluded
        assert_eq!(pages[0], "Water");
    }
}
