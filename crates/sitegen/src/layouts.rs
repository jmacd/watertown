// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Maud HTML layouts for sitegen pages.
//!
//! Layouts wrap rendered markdown content in a complete HTML document.
//! Selected by frontmatter `layout: data|default` in each markdown page.

use maud::{DOCTYPE, Markup, PreEscaped, html};

/// Watertown version baked into generated HTML as `<meta name="generator">`.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Git short SHA captured at build time for dev identification.
const GIT_SHA: &str = env!("WATERTOWN_GIT_SHA");

/// Build identifier string for the page footer.
///
/// For release builds (version != "0.0.0"), shows "Watertown v0.38.0".
/// For dev builds, shows "Watertown abc1234".
fn build_id() -> String {
    if VERSION.starts_with("0.0.") || VERSION.contains("dev") {
        format!("Watertown {}", GIT_SHA)
    } else {
        format!("Watertown v{}", VERSION)
    }
}

/// Footer markup from the configured footer text.
///
/// Supports variable expansion in the text:
/// - `${DATE}` — generation date (e.g. "April 20, 2026")
/// - `${BUILD}` — build identifier (e.g. "Watertown v0.47.0")
///
/// If no footer text is configured, the footer element is omitted entirely.
fn build_footer(custom: Option<&str>) -> Markup {
    let expanded = custom.map(|text| {
        let mut result = text.to_string();
        if result.contains("${DATE}") {
            let date = chrono::Local::now().format("%B %-d, %Y").to_string();
            result = result.replace("${DATE}", &date);
        }
        if result.contains("${BUILD}") {
            result = result.replace("${BUILD}", &build_id());
        }
        result
    });
    html! {
        @if let Some(ref text) = expanded {
            footer class="build-footer" {
                small {
                    (text)
                }
            }
        }
    }
}

/// Header banner markup showing a centered full-width logo image.
///
/// The image path is relative to `base_url` (e.g., `"img/logo.svg"`).
fn build_header(image: Option<&str>, base_url: &str) -> Markup {
    html! {
        @if let Some(src) = image {
            header class="site-banner" {
                img src=(format!("{}{}", base_url, src)) alt="Site banner";
            }
        }
    }
}

/// GitHub icon SVG, used in the top bar of all layouts.
const GITHUB_SVG: &str = r#"<svg width="22" height="22" viewBox="0 0 16 16" fill="currentColor"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>"#;

/// RSS feed icon SVG, used in the top bar when a feed URL is configured.
const RSS_SVG: &str = r#"<svg width="22" height="22" viewBox="0 0 16 16" fill="currentColor"><circle cx="3.5" cy="12.5" r="2"/><path d="M1.5 6.5c0-.28.22-.5.5-.5 4.14 0 7.5 3.36 7.5 7.5 0 .28-.22.5-.5.5h-1c-.28 0-.5-.22-.5-.5C7.5 10.46 5.04 8 2 8c-.28 0-.5-.22-.5-.5v-1z"/><path d="M1.5 1.5c0-.28.22-.5.5-.5C9.96.5 15.5 6.04 15.5 14c0 .28-.22.5-.5.5h-1c-.28 0-.5-.22-.5-.5C13.5 7.1 8.9 2.5 2 2.5c-.28 0-.5-.22-.5-.5v-1z"/></svg>"#;

/// Render the top bar with optional back link and right-justified icons.
fn top_bar(
    back_label: Option<&str>,
    back_href: Option<&str>,
    feed_url: Option<&str>,
    github_url: Option<&str>,
) -> Markup {
    html! {
        nav class="top-bar" {
            @if let (Some(label), Some(href)) = (back_label, back_href) {
                a class="top-bar-back" href=(href) {
                    span class="blog-back-arrow" { "\u{2190}" }
                    " " (label)
                }
            } @else {
                span {}
            }
            div class="top-bar-icons" {
                @if let Some(url) = feed_url {
                    a href=(url) target="_blank" title="RSS Feed" {
                        (PreEscaped(RSS_SVG))
                    }
                }
                @if let Some(url) = github_url {
                    a href=(url) target="_blank" title="Source on GitHub" {
                        (PreEscaped(GITHUB_SVG))
                    }
                }
            }
        }
    }
}

/// Context passed to layout functions.
pub struct LayoutContext<'a> {
    /// Page title (from frontmatter)
    pub title: &'a str,
    /// Site title (from site.yaml)
    pub site_title: &'a str,
    /// Base URL path prefix (from site.yaml, e.g. "/" or "/noyo-harbor/")
    pub base_url: &'a str,
    /// Root base URL for shared assets (style.css, chart.js, vendor/).
    /// For standalone sites this equals base_url. For subsites this is
    /// the top-level site's base_url.
    pub root_base_url: &'a str,
    /// Rendered HTML content (from markdown + shortcodes)
    pub content: &'a str,
    /// Rendered sidebar HTML (from sidebar partial, if any)
    pub sidebar: Option<&'a str>,
    /// Publication date for blog posts (ISO 8601, e.g. "2024-06-15")
    pub date: Option<&'a str>,
    /// URL to the RSS feed (e.g. "/feed.xml"), shown as icon in top bar
    pub feed_url: Option<&'a str>,
    /// GitHub repository URL, shown as icon in top bar. Omitted if None.
    pub github_url: Option<&'a str>,
    /// Custom footer text (e.g., "Made in Mendocino"). Shown before build ID.
    pub footer: Option<&'a str>,
    /// Image path for a full-width centered banner at the top of every page.
    /// Rendered relative to `base_url` (e.g., `"img/logo.svg"`).
    pub header: Option<&'a str>,
}

/// Apply a named layout to rendered content.
///
/// Layout names come from markdown frontmatter:
/// ```yaml
/// ---
/// title: "Temperature"
/// layout: data
/// ---
/// ```
pub fn apply_layout(name: &str, ctx: &LayoutContext) -> String {
    let markup = match name {
        "data" => data_layout(ctx),
        "logs" => logs_layout(ctx),
        "explore" => explore_layout(ctx),
        "page" => page_layout(ctx),
        "blog" => blog_layout(ctx),
        other => {
            if other != "default" {
                log::warn!("Unknown layout '{}', using default layout", other);
            }
            default_layout(ctx)
        }
    };
    markup.into_string()
}

/// Common `<head>` elements shared by all layouts.
fn common_head(ctx: &LayoutContext) -> Markup {
    html! {
        meta charset="utf-8";
        meta name="viewport" content="width=device-width, initial-scale=1";
        meta name="generator" content=(format!("Watertown v{}", VERSION));
        title { (ctx.title) " -- " (ctx.site_title) }
        link rel="preconnect" href="https://fonts.googleapis.com";
        link rel="preconnect" href="https://fonts.gstatic.com" crossorigin;
        link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap";
        link rel="stylesheet" href=(format!("{}style.css", ctx.root_base_url));
        link rel="stylesheet" href=(format!("{}theme.css", ctx.base_url));
        @if let Some(feed) = ctx.feed_url {
            link rel="alternate" type="application/rss+xml" title="RSS Feed" href=(feed);
        }
    }
}

/// Layout for data pages (parameter/site detail pages).
///
/// Includes CDN scripts for DuckDB-WASM and Observable Plot,
/// a sidebar for navigation, and a main content area.
fn data_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                (common_head(ctx))
            }
            body {
                (build_header(ctx.header, ctx.base_url))
                @if let Some(sidebar_html) = ctx.sidebar {
                    nav class="sidebar" {
                        (PreEscaped(sidebar_html))
                    }
                }
                main class="content-page" {
                    (top_bar(Some("Home"), Some(ctx.base_url), ctx.feed_url, ctx.github_url))
                    article class="blog-post" {
                        div class="blog-post-content" {
                            (PreEscaped(ctx.content))
                        }
                    }
                }
                (build_footer(ctx.footer))
                script src=(format!("{}chart.js", ctx.root_base_url)) type="module" {}
                script src=(format!("{}overlay.js", ctx.root_base_url)) type="module" {}
                script src=(format!("{}relative-time.js", ctx.root_base_url)) type="module" {}
            }
        }
    }
}

/// Layout for log viewer pages.
///
/// Similar to data_layout but loads log-viewer.js instead of chart.js.
/// Sidebar navigation + card-wrapped content with the log viewer container.
fn logs_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                (common_head(ctx))
            }
            body {
                (build_header(ctx.header, ctx.base_url))
                @if let Some(sidebar_html) = ctx.sidebar {
                    nav class="sidebar" {
                        (PreEscaped(sidebar_html))
                    }
                }
                main class="content-page" {
                    (top_bar(Some("Home"), Some(ctx.base_url), ctx.feed_url, ctx.github_url))
                    article class="blog-post" {
                        div class="blog-post-content" {
                            (PreEscaped(ctx.content))
                        }
                    }
                }
                (build_footer(ctx.footer))
                script src=(format!("{}log-viewer.js", ctx.root_base_url)) type="module" {}
            }
        }
    }
}

/// Layout for the browser data explorer page.
///
/// Similar to logs_layout but loads explore.js. The page content (from
/// markdown + the `explore` shortcode) supplies the `<script class="datasets">`
/// manifest and the `<div id="explore">` container that explore.js drives.
fn explore_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                (common_head(ctx))
            }
            body {
                (build_header(ctx.header, ctx.base_url))
                @if let Some(sidebar_html) = ctx.sidebar {
                    nav class="sidebar" {
                        (PreEscaped(sidebar_html))
                    }
                }
                main class="content-page" {
                    (top_bar(Some("Home"), Some(ctx.base_url), ctx.feed_url, ctx.github_url))
                    article class="blog-post" {
                        div class="blog-post-content" {
                            (PreEscaped(ctx.content))
                        }
                    }
                }
                (build_footer(ctx.footer))
                script src=(format!("{}explore.js", ctx.root_base_url)) type="module" {}
            }
        }
    }
}

/// Layout for content pages (articles, documentation, blog index).
///
/// Sidebar navigation + card-wrapped article with back-to-home link.
/// Content is wrapped in a card container matching the blog post style.
fn page_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                (common_head(ctx))
            }
            body {
                (build_header(ctx.header, ctx.base_url))
                @if let Some(sidebar_html) = ctx.sidebar {
                    nav class="sidebar" {
                        (PreEscaped(sidebar_html))
                    }
                }
                main class="content-page" {
                    (top_bar(Some("Home"), Some(ctx.base_url), ctx.feed_url, ctx.github_url))
                    article class="blog-post" {
                        div class="blog-post-content" {
                            (PreEscaped(ctx.content))
                        }
                    }
                }
                (build_footer(ctx.footer))
            }
        }
    }
}

/// Layout for individual blog posts.
///
/// Renders the post inside a card container matching the blog grid card
/// aesthetic: lighter background, rounded edges, date/title header, with
/// a back-arrow link to the blog index.
fn blog_layout(ctx: &LayoutContext) -> Markup {
    let date_display = ctx.date.map(format_date);
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                (common_head(ctx))
            }
            body {
                (build_header(ctx.header, ctx.base_url))
                @if let Some(sidebar_html) = ctx.sidebar {
                    nav class="sidebar" {
                        (PreEscaped(sidebar_html))
                    }
                }
                main class="content-page" {
                    (top_bar(Some("Blog"), Some(&format!("{}blog.html", ctx.base_url)), ctx.feed_url, ctx.github_url))
                    article class="blog-post" {
                        header class="blog-post-header" {
                            @if let Some(ref date) = date_display {
                                time class="blog-post-date" { (date) }
                            }
                            h1 class="blog-post-title" { (ctx.title) }
                        }
                        div class="blog-post-content" {
                            (PreEscaped(ctx.content))
                        }
                    }
                }
                (build_footer(ctx.footer))
            }
        }
    }
}

/// Format an ISO 8601 date string for display (e.g. "2024-06-15" -> "June 15, 2024").
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

/// Default layout for the index/home page.
///
/// Same card structure as page layout, with a spacer (and optional
/// icons) in place of the back-arrow. Icons sit above the card,
/// over the page background.
fn default_layout(ctx: &LayoutContext) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                (common_head(ctx))
            }
            body {
                (build_header(ctx.header, ctx.base_url))
                @if let Some(sidebar_html) = ctx.sidebar {
                    nav class="sidebar" {
                        (PreEscaped(sidebar_html))
                    }
                }
                main class="content-page" {
                    (top_bar(None, None, ctx.feed_url, ctx.github_url))
                    article class="blog-post" {
                        div class="blog-post-content" {
                            (PreEscaped(ctx.content))
                        }
                    }
                }
                (build_footer(ctx.footer))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_layout() {
        let ctx = LayoutContext {
            title: "Home",
            site_title: "Test Site",
            base_url: "/",
            root_base_url: "/",
            content: "<h1>Hello</h1>",
            sidebar: None,
            date: None,
            feed_url: None,
            github_url: Some("https://github.com/test/repo"),
            footer: None,
            header: None,
        };
        let html = apply_layout("default", &ctx);
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("Home -- Test Site"));
        assert!(html.contains("<h1>Hello</h1>"));
        assert!(html.contains("top-bar"), "Expected top bar: {}", html);
        assert!(
            html.contains("github.com/test/repo"),
            "Expected GitHub link"
        );
        assert!(html.contains("blog-post"), "Expected card container");
        assert!(!html.contains("RSS Feed"), "No RSS icon without feed_url");

        // Without github_url, no GitHub icon
        let ctx_no_gh = LayoutContext {
            github_url: None,
            footer: None,
            header: None,
            ..ctx
        };
        let html2 = apply_layout("default", &ctx_no_gh);
        assert!(
            !html2.contains("github.com"),
            "No GitHub link when url is None"
        );
    }

    #[test]
    fn test_data_layout_with_sidebar() {
        let ctx = LayoutContext {
            title: "Temperature",
            site_title: "Noyo Harbor",
            base_url: "/noyo-harbor/",
            root_base_url: "/",
            content: "<p>Chart here</p>",
            sidebar: Some("<ul><li>Nav</li></ul>"),
            date: None,
            feed_url: None,
            github_url: None,
            footer: None,
            header: None,
        };
        let html = apply_layout("data", &ctx);
        assert!(html.contains("chart.js")); // Glue code present
        assert!(html.contains("type=\"module\"")); // Module script
        assert!(html.contains("class=\"sidebar\""));
        assert!(html.contains("<ul><li>Nav</li></ul>"));
        assert!(html.contains("top-bar"), "Top bar present");
        assert!(html.contains("blog-post"), "Card container");
        assert!(
            html.contains("/noyo-harbor/"),
            "Home link uses base_url: {}",
            html
        );
        assert!(
            !html.contains("github.com"),
            "No GitHub link when url is None"
        );
    }

    #[test]
    fn test_unknown_layout_falls_back_to_default() {
        let ctx = LayoutContext {
            title: "Page",
            site_title: "Site",
            base_url: "/",
            root_base_url: "/",
            content: "<p>Content</p>",
            sidebar: None,
            date: None,
            feed_url: None,
            github_url: None,
            footer: None,
            header: None,
        };
        let html = apply_layout("nonexistent", &ctx);
        assert!(html.contains("top-bar"), "Default layout uses top bar");
    }

    #[test]
    fn test_page_layout() {
        let ctx = LayoutContext {
            title: "Water System",
            site_title: "Caspar Water",
            base_url: "/",
            root_base_url: "/",
            content: "<h1>Water</h1><p>Info</p>",
            sidebar: Some("<ul><li>Nav</li></ul>"),
            date: None,
            feed_url: None,
            github_url: None,
            footer: None,
            header: None,
        };
        let html = apply_layout("page", &ctx);
        assert!(html.contains("content-page"), "Page layout class");
        assert!(html.contains("blog-post"), "Card container");
        assert!(html.contains("top-bar"), "Top bar present");
        assert!(html.contains("Home"), "Back to home link: {}", html);
        assert!(
            !html.contains("github.com"),
            "No GitHub link when url is None"
        );
        assert!(html.contains("class=\"sidebar\""), "Sidebar present");
        assert!(!html.contains("chart.js"), "No CDN scripts");
    }

    #[test]
    fn test_blog_layout() {
        let ctx = LayoutContext {
            title: "My Blog Post",
            site_title: "Test Blog",
            base_url: "/",
            root_base_url: "/",
            content: "<p>Post content here</p>",
            sidebar: Some("<ul><li>Nav</li></ul>"),
            date: Some("2025-03-10"),
            feed_url: Some("/feed.xml"),
            github_url: None,
            footer: None,
            header: None,
        };
        let html = apply_layout("blog", &ctx);
        assert!(html.contains("top-bar"), "Expected top bar: {}", html);
        assert!(
            html.contains("blog.html"),
            "Expected back link to blog: {}",
            html
        );
        assert!(
            !html.contains("github.com"),
            "No GitHub link when url is None: {}",
            html
        );
        assert!(
            html.contains("March 10, 2025"),
            "Expected formatted date: {}",
            html
        );
        assert!(html.contains("My Blog Post"), "Expected title: {}", html);
        assert!(
            html.contains("blog-post-title"),
            "Expected title class: {}",
            html
        );
        assert!(
            html.contains("Post content here"),
            "Expected content: {}",
            html
        );
        assert!(
            html.contains("class=\"sidebar\""),
            "Expected sidebar: {}",
            html
        );
        assert!(!html.contains("chart.js"), "No CDN scripts in blog layout");
        assert!(html.contains("RSS Feed"), "Expected RSS icon: {}", html);
        assert!(html.contains("/feed.xml"), "Expected feed link: {}", html);
        assert!(
            html.contains("application/rss+xml"),
            "Expected RSS autodiscovery: {}",
            html
        );
    }

    #[test]
    fn test_blog_layout_no_date() {
        let ctx = LayoutContext {
            title: "Undated Post",
            site_title: "Blog",
            base_url: "/",
            root_base_url: "/",
            content: "<p>No date</p>",
            sidebar: None,
            date: None,
            feed_url: None,
            github_url: None,
            footer: None,
            header: None,
        };
        let html = apply_layout("blog", &ctx);
        assert!(html.contains("blog-post"), "Expected blog-post: {}", html);
        assert!(
            !html.contains("blog-post-date"),
            "No date element expected: {}",
            html
        );
    }

    #[test]
    fn test_logs_layout_uses_root_base_url() {
        let ctx = LayoutContext {
            title: "System Logs",
            site_title: "Noyo Harbor",
            base_url: "/noyo-harbor/",
            root_base_url: "/",
            content: "<div id=\"log-viewer\"></div>",
            sidebar: Some("<ul><li>Nav</li></ul>"),
            date: None,
            feed_url: None,
            github_url: None,
            footer: None,
            header: None,
        };
        let html = apply_layout("logs", &ctx);
        assert!(
            html.contains("/log-viewer.js"),
            "Log viewer script present: {}",
            html
        );
        assert!(
            !html.contains("\"/noyo-harbor/log-viewer.js\""),
            "Log viewer should use root_base_url, not base_url: {}",
            html
        );
        assert!(html.contains("type=\"module\""), "Module script: {}", html);
        assert!(html.contains("class=\"sidebar\""), "Sidebar present");
        assert!(!html.contains("chart.js"), "No chart.js in logs layout");
    }

    #[test]
    fn test_explore_layout_loads_explore_js() {
        let ctx = LayoutContext {
            title: "Data Explorer",
            site_title: "Noyo Harbor",
            base_url: "/noyo-harbor/",
            root_base_url: "/",
            content: "<div id=\"explore\"></div>",
            sidebar: Some("<ul><li>Nav</li></ul>"),
            date: None,
            feed_url: None,
            github_url: None,
            footer: None,
            header: None,
        };
        let html = apply_layout("explore", &ctx);
        assert!(html.contains("/explore.js"), "explore.js present: {}", html);
        assert!(
            !html.contains("\"/noyo-harbor/explore.js\""),
            "explore.js should use root_base_url, not base_url: {}",
            html
        );
        assert!(html.contains("type=\"module\""), "Module script: {}", html);
        assert!(html.contains("class=\"sidebar\""), "Sidebar present");
        assert!(!html.contains("chart.js"), "No chart.js in explore layout");
    }
}
