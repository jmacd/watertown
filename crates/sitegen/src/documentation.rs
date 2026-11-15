// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Opt-in documentation rendering on the ordinary sitegen page pipeline.

use crate::config::{DocumentationConfig, RouteConfig, RouteType, SiteConfig};
use crate::markdown::{Shortcodes, parse_args, preprocess_shortcodes};
use crate::routes::{ContentContext, PageJob};
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd, html::push_html};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

fn options() -> Options {
    Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS | Options::ENABLE_TABLES
}

pub(crate) fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Mask parser-recognized code before either variable or shortcode expansion.
fn mask_code(content: &str) -> (String, Vec<(String, String)>) {
    let mut ranges = Vec::new();
    let mut block_start = None;
    for (event, range) in Parser::new_ext(content, options()).into_offset_iter() {
        match event {
            Event::Start(Tag::CodeBlock(_)) => block_start = Some(range.start),
            Event::End(TagEnd::CodeBlock) => {
                if let Some(start) = block_start.take() {
                    ranges.push(start..range.end);
                }
            }
            Event::Code(_) if block_start.is_none() => ranges.push(range),
            _ => {}
        }
    }
    let mut prefix = format!("SITEGEN_LITERAL_{}_", uuid::Uuid::new_v4().simple());
    while content.contains(&prefix) {
        prefix.push('_');
    }
    let mut masked = String::new();
    let mut literals = Vec::new();
    let mut previous = 0;
    for range in ranges {
        let token = format!("{}{}END", prefix, literals.len());
        masked.push_str(&content[previous..range.start]);
        masked.push_str(&token);
        literals.push((token, content[range.clone()].to_string()));
        previous = range.end;
    }
    masked.push_str(&content[previous..]);
    (masked, literals)
}

pub(crate) fn preprocess(
    content: &str,
    shortcodes: &Shortcodes,
    source: &str,
) -> Result<String, String> {
    let (masked, literals) = mask_code(content);
    let variables = crate::shortcodes::preprocess_variables(&masked);
    let mut expanded = preprocess_shortcodes(&variables, shortcodes, Some(source))?;
    for (token, literal) in literals {
        expanded = expanded.replace(&token, &literal);
    }
    Ok(expanded)
}

fn absolute_path(path: &str) -> Result<String, String> {
    if !path.starts_with('/') || path.contains(['\\', '?', '#', '%', ':']) {
        return Err(format!(
            "expected an absolute input-tree path, got '{path}'"
        ));
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => return Err(format!("parent traversal is not permitted: '{path}'")),
            _ => parts.push(part),
        }
    }
    Ok(format!("/{}", parts.join("/")))
}

pub(crate) fn validate_config(config: &DocumentationConfig) -> Result<(), String> {
    if config.version != 1 {
        return Err(format!(
            "documentation renderer version {} is unsupported; this binary supports version 1",
            config.version
        ));
    }
    let _ = absolute_path(&config.source_root)?;
    for root in &config.include_roots {
        if absolute_path(root)? == "/" {
            return Err(
                "documentation include_roots must not approve the entire input tree".into(),
            );
        }
    }
    Ok(())
}

pub(crate) fn content_slug(
    config: &DocumentationConfig,
    path: &str,
) -> Result<Option<String>, String> {
    let root = absolute_path(&config.source_root)?;
    let relative = path
        .strip_prefix(&format!("{}/", root.trim_end_matches('/')))
        .ok_or_else(|| format!("content '{path}' is outside documentation source_root '{root}'"))?;
    if relative.split('/').any(|part| part == "_generated") {
        return Ok(None);
    }
    Ok(Some(
        relative
            .strip_suffix(".md")
            .or_else(|| relative.strip_suffix(".markdown"))
            .ok_or_else(|| format!("documentation content is not Markdown: '{path}'"))?
            .to_string(),
    ))
}

fn include_path(config: &DocumentationConfig, path: &str) -> Result<String, String> {
    let path = absolute_path(path)?;
    for root in &config.include_roots {
        let root = absolute_path(root)?;
        if path.starts_with(&format!("{root}/")) {
            return Ok(path);
        }
    }
    Err(format!(
        "source '{path}' is outside documentation include_roots"
    ))
}

/// Discover only active source directives, never ones displayed inside code.
pub(crate) fn source_paths(
    content: &str,
    config: &DocumentationConfig,
) -> Result<Vec<String>, String> {
    Ok(source_requests(content, config)?
        .into_iter()
        .map(|(path, _)| path)
        .collect())
}

pub(crate) fn source_requests(
    content: &str,
    config: &DocumentationConfig,
) -> Result<Vec<(String, Option<String>)>, String> {
    let (masked, _) = mask_code(content);
    let re = regex::Regex::new(r"\{\{\s*source\s+([\s\S]*?)\s*/\}\}").unwrap();
    re.captures_iter(&masked)
        .filter(|capture| {
            let start = capture.get(0).unwrap().start();
            start == 0 || masked.as_bytes()[start - 1] != b'\\'
        })
        .map(|capture| {
            let args = parse_args(&capture[1])?;
            let path = args.get("path").ok_or("source requires path")?;
            let region = args.get("region").cloned();
            include_path(config, path)
                .map(|path| (path, region.clone()))
                .map_err(|error| {
                    format!(
                        "source '{path}' region '{}': {error}",
                        region.as_deref().unwrap_or("<complete file>")
                    )
                })
        })
        .collect()
}

fn region_text(content: &str, requested: Option<&str>) -> Result<String, String> {
    let mut regions = BTreeMap::new();
    let mut open: Option<(String, usize)> = None;
    let mut offset = 0;
    for line in content.split_inclusive('\n') {
        let marker = line.trim().strip_prefix("# ").unwrap_or(line.trim());
        if let Some(name) = marker.strip_prefix("docs:start ") {
            if name.trim().is_empty() || open.is_some() || regions.contains_key(name) {
                return Err(format!("malformed or ambiguous region start '{name}'"));
            }
            open = Some((name.to_string(), offset + line.len()));
        } else if let Some(name) = marker.strip_prefix("docs:end ") {
            let (start_name, start) = open.take().ok_or("region end without start")?;
            if name != start_name {
                return Err(format!("region '{start_name}' closed by '{name}'"));
            }
            regions.insert(start_name, content[start..offset].to_string());
        } else if marker.starts_with("docs:start") || marker.starts_with("docs:end") {
            return Err(format!("malformed region marker '{marker}'"));
        }
        offset += line.len();
    }
    if let Some((name, _)) = open {
        return Err(format!("unclosed region '{name}'"));
    }
    match requested {
        Some(name) => regions
            .remove(name)
            .ok_or_else(|| format!("missing region '{name}'")),
        None => Ok(content.to_string()),
    }
}

pub(crate) fn register_source(
    shortcodes: &mut Shortcodes,
    config: DocumentationConfig,
    sources: BTreeMap<String, String>,
) {
    shortcodes.register_fallible("source", move |args| {
        let requested = args.get_str("path").ok_or("source requires path")?;
        let region = args.get_str("region");
        let result = (|| {
            let path = include_path(&config, requested)?;
            let content = sources
                .get(&path)
                .ok_or_else(|| format!("missing source '{path}'"))?;
            let text = region_text(content, region)?;
            let language = args.get_str("language").unwrap_or("text");
            if !language
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
            {
                return Err(format!("invalid source language '{language}'"));
            }
            Ok(format!(
                "\n<pre><code class=\"language-{}\">{}</code></pre>\n",
                language,
                escape(&text)
            ))
        })();
        result.map_err(|error| {
            format!(
                "{}: source '{}' region '{}': {}",
                args.get_str("markdown_path").unwrap_or(""),
                requested,
                region.unwrap_or("<complete file>"),
                error
            )
        })
    });
}

#[derive(Default)]
pub(crate) struct PageIndex {
    sources: BTreeMap<String, String>,
}

impl PageIndex {
    pub(crate) fn new(jobs: &[PageJob]) -> Result<Self, String> {
        if jobs.is_empty() {
            return Err("strict/documentation build has no published pages".into());
        }
        let mut index = Self::default();
        let mut outputs = BTreeMap::new();
        for job in jobs {
            if job.output_path.contains(['\\', '?', '#', '%', ':'])
                || job
                    .output_path
                    .split('/')
                    .any(|part| matches!(part, "" | "." | ".."))
                || Path::new(&job.output_path)
                    .components()
                    .any(|c| !matches!(c, Component::Normal(_)))
            {
                return Err(format!(
                    "{}: invalid output path '{}'",
                    job.page_source, job.output_path
                ));
            }
            if let Some(previous) = outputs.insert(&job.output_path, &job.page_source) {
                return Err(format!(
                    "output collision '{}': '{}' and '{}'",
                    job.output_path, previous, job.page_source
                ));
            }
            if !job.captures.is_empty() {
                continue;
            }
            if let Some(previous) = index
                .sources
                .insert(job.page_source.clone(), job.output_path.clone())
                && previous != job.output_path
            {
                return Err(format!(
                    "ambiguous source '{}': '{}' and '{}'",
                    job.page_source, previous, job.output_path
                ));
            }
        }
        Ok(index)
    }

    fn link(&self, source: &str, destination: &str, base: &str) -> Result<String, String> {
        if nonlocal(destination) || destination.starts_with('#') {
            return Ok(destination.to_string());
        }
        let path = destination.split(['?', '#']).next().unwrap_or(destination);
        if !(path.ends_with(".md") || path.ends_with(".markdown")) {
            return Ok(destination.to_string());
        }
        let decoded = percent_encoding::percent_decode_str(path)
            .decode_utf8()
            .map_err(|e| format!("{source}: invalid encoded link '{destination}': {e}"))?;
        let joined = if decoded.starts_with('/') {
            decoded.to_string()
        } else {
            format!(
                "{}/{}",
                source.rsplit_once('/').map_or("", |v| v.0),
                decoded
            )
        };
        let normalized = normalize(&joined)?;
        let target = self.sources.get(&normalized).ok_or_else(|| {
            format!(
                "{source}: Markdown link '{destination}' has no published source '{normalized}'"
            )
        })?;
        Ok(format!(
            "{}/{}{}",
            base.trim_end_matches('/'),
            target,
            &destination[path.len()..]
        ))
    }
}

fn normalize(path: &str) -> Result<String, String> {
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return Err(format!("path escapes source/output root: '{path}'"));
                }
            }
            _ => parts.push(part),
        }
    }
    Ok(format!("/{}", parts.join("/")))
}

fn heading_slug(text: &str) -> String {
    let mut slug = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() {
            slug.extend(c.to_lowercase());
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug = slug.trim_end_matches('-');
    if slug.is_empty() {
        "section".into()
    } else {
        slug.into()
    }
}

fn render_contents(contents: &[(u8, String, String)]) -> String {
    fn render_level(
        html: &mut String,
        contents: &[(u8, String, String)],
        index: &mut usize,
        level: u8,
    ) {
        html.push_str("<ul>");
        while *index < contents.len() {
            let (entry_level, id, text) = &contents[*index];
            if *entry_level < level {
                break;
            }
            if *entry_level > level {
                render_level(html, contents, index, *entry_level);
                continue;
            }
            html.push_str(&format!("<li><a href=\"#{id}\">{}</a>", escape(text)));
            *index += 1;
            if *index < contents.len() && contents[*index].0 > level {
                let child_level = contents[*index].0;
                render_level(html, contents, index, child_level);
            }
            html.push_str("</li>");
        }
        html.push_str("</ul>");
    }

    let contents: Vec<_> = contents
        .iter()
        .filter(|(level, _, _)| *level >= 2)
        .cloned()
        .collect();
    if contents.is_empty() {
        return String::new();
    }
    let mut html =
        "<nav class=\"page-toc\" aria-label=\"On this page\"><strong>On this page</strong>"
            .to_string();
    let mut index = 0;
    let level = contents[0].0;
    render_level(&mut html, &contents, &mut index, level);
    html.push_str("</nav>\n");
    html
}

pub(crate) fn render(
    content: &str,
    source: &str,
    index: &PageIndex,
    base: &str,
) -> Result<String, String> {
    let mut events = Vec::new();
    let mut heading = None;
    let mut heading_events = Vec::new();
    let mut heading_text = String::new();
    let mut ids = BTreeSet::from(["manual-content".to_string()]);
    let mut contents = Vec::new();
    for event in Parser::new_ext(content, options()) {
        match event {
            Event::Start(Tag::Heading { level, .. }) => {
                heading = Some(level);
                heading_text.clear();
                heading_events.clear();
            }
            Event::End(TagEnd::Heading(level)) => {
                let stem = heading_slug(&heading_text);
                let mut id = stem.clone();
                let mut suffix = 0;
                while !ids.insert(id.clone()) {
                    suffix += 1;
                    id = format!("{stem}-{suffix}");
                }
                events.push(Event::Html(format!("<{level} id=\"{id}\">").into()));
                events.append(&mut heading_events);
                events.push(Event::Html(format!("</{level}>\n").into()));
                contents.push((level as u8, id, heading_text.clone()));
                heading = None;
            }
            event => {
                if heading.is_some()
                    && let Event::Text(text) | Event::Code(text) = &event
                {
                    heading_text.push_str(text);
                }
                let event = match event {
                    Event::Start(Tag::Link {
                        link_type,
                        dest_url,
                        title,
                        id,
                    }) => Event::Start(Tag::Link {
                        link_type,
                        dest_url: index.link(source, &dest_url, base)?.into(),
                        title,
                        id,
                    }),
                    other => other,
                };
                if heading.is_some() {
                    heading_events.push(event);
                } else {
                    events.push(event);
                }
            }
        }
    }
    let mut html = render_contents(&contents);
    push_html(&mut html, events.into_iter());
    Ok(html)
}

pub(crate) fn validate_routes(
    config: &SiteConfig,
    content: &BTreeMap<String, ContentContext>,
) -> Result<(), String> {
    fn visit(
        routes: &[RouteConfig],
        config: &SiteConfig,
        bound: &mut BTreeSet<String>,
    ) -> Result<(), String> {
        for route in routes {
            if route.route_type == RouteType::Content
                && !config
                    .content
                    .iter()
                    .any(|stage| Some(&stage.name) == route.content.as_ref())
            {
                return Err(format!(
                    "route '{}': unknown content stage {:?}",
                    route.name, route.content
                ));
            }
            if route.page.is_none()
                && (route.route_type == RouteType::Template
                    || (route.route_type == RouteType::Static && route.routes.is_empty()))
            {
                return Err(format!("route '{}': missing page", route.name));
            }
            if route.route_type == RouteType::Content && !route.routes.is_empty() {
                return Err(format!(
                    "content route '{}': child routes would be ignored",
                    route.name
                ));
            }
            for name in route.export.iter().chain(route.annotations.iter()) {
                if !config.exports.iter().any(|stage| &stage.name == name) {
                    return Err(format!("route '{}': unknown export '{name}'", route.name));
                }
            }
            if route.route_type == RouteType::Template && route.export.is_none() {
                return Err(format!("route '{}': missing export", route.name));
            }
            if route.route_type == RouteType::Content
                && let Some(stage) = &route.content
            {
                bound.insert(stage.clone());
            }
            visit(&route.routes, config, bound)?;
        }
        Ok(())
    }
    let mut bound = BTreeSet::new();
    visit(&config.routes, config, &mut bound)?;
    let mut stages = BTreeSet::new();
    for stage in &config.content {
        if !stages.insert(&stage.name) {
            return Err(format!("duplicate content stage '{}'", stage.name));
        }
        if !bound.contains(&stage.name) {
            return Err(format!(
                "content stage '{}': not bound to a content route",
                stage.name
            ));
        }
        if content
            .get(&stage.name)
            .is_none_or(|context| context.pages.is_empty())
        {
            return Err(format!("content stage '{}': no pages", stage.name));
        }
    }
    fn children(items: &[crate::config::SidebarChild]) -> Result<(), String> {
        for item in items {
            if item.href.is_none() && item.children.is_empty() {
                return Err(format!(
                    "navigation '{}': no target or children",
                    item.label
                ));
            }
            children(&item.children)?;
        }
        Ok(())
    }
    for entry in &config.sidebar {
        if matches!(entry, crate::config::SidebarEntry::WithChildren { .. })
            && entry.href().is_none()
            && entry.children().is_empty()
        {
            return Err(format!(
                "navigation '{}': no target or children",
                entry.label()
            ));
        }
        if let crate::config::SidebarEntry::Simple(label) = entry
            && !content
                .values()
                .flat_map(|ctx| &ctx.pages)
                .any(|p| p.title == *label)
        {
            return Err(format!(
                "navigation '{label}': no exact page title; use an explicit href"
            ));
        }
        children(entry.children())?;
    }
    Ok(())
}

pub(crate) fn sidebar_entries(config: &SiteConfig) -> Vec<crate::config::SidebarEntry> {
    fn prefix(href: &mut String, base: &str) {
        if nonlocal(href)
            || href.starts_with('#')
            || base.is_empty()
            || href == base
            || href.starts_with(&format!("{base}/"))
        {
            return;
        }
        *href = format!("{}/{}", base, href.trim_start_matches('/'));
    }
    fn children(items: &mut [crate::config::SidebarChild], base: &str) {
        for item in items {
            if let Some(href) = &mut item.href {
                prefix(href, base);
            }
            children(&mut item.children, base);
        }
    }
    let mut entries = config.sidebar.clone();
    if config.documentation.is_some() {
        let base = config.site.base_url.trim_end_matches('/');
        for entry in &mut entries {
            match entry {
                crate::config::SidebarEntry::DirectLink { href, .. } => prefix(href, base),
                crate::config::SidebarEntry::WithChildren {
                    href,
                    children: items,
                    ..
                } => {
                    if let Some(href) = href {
                        prefix(href, base);
                    }
                    children(items, base);
                }
                crate::config::SidebarEntry::Simple(_) => {}
            }
        }
    }
    entries
}

fn nonlocal(value: &str) -> bool {
    value.starts_with("//")
        || value
            .split('/')
            .next()
            .is_some_and(|part| part.contains(':'))
}

pub(crate) fn html_tags<'a, 'b>(dom: &'b tl::VDom<'a>) -> Vec<&'b tl::HTMLTag<'a>> {
    let mut tags = Vec::new();
    let mut pending: Vec<_> = dom.children().iter().rev().copied().collect();
    while let Some(handle) = pending.pop() {
        let Some(tag) = handle.get(dom.parser()).and_then(|node| node.as_tag()) else {
            continue;
        };
        tags.push(tag);
        if !matches!(
            tag.name().as_utf8_str().as_ref(),
            "script" | "style" | "textarea" | "title"
        ) {
            pending.extend(tag.children().top().iter().copied());
        }
    }
    tags
}

/// Validate actual rendered hrefs/srcs and IDs, including raw HTML and navigation.
pub(crate) fn validate_output(output: &Path, base: &str) -> Result<(), String> {
    fn collect(dir: &Path, files: &mut Vec<std::path::PathBuf>) -> Result<(), String> {
        for entry in std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))? {
            let entry = entry.map_err(|e| e.to_string())?;
            if entry.file_type().map_err(|e| e.to_string())?.is_dir() {
                collect(&entry.path(), files)?;
            } else if entry.path().extension().is_some_and(|ext| ext == "html") {
                files.push(entry.path());
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    collect(output, &mut files)?;
    files.sort();
    let mut pages = BTreeMap::new();
    let mut links = Vec::new();
    for file in files {
        let relative = file
            .strip_prefix(output)
            .map_err(|e| e.to_string())?
            .to_string_lossy()
            .to_string();
        let html = std::fs::read_to_string(&file).map_err(|e| e.to_string())?;
        let mut ids = BTreeSet::new();
        let dom = tl::parse(&html, tl::ParserOptions::default())
            .map_err(|e| format!("{relative}: cannot parse generated HTML: {e}"))?;
        for tag in html_tags(&dom) {
            for name in ["id", "href", "src"] {
                let Some(value) = tag.attributes().get(name).flatten() else {
                    continue;
                };
                let value = value
                    .as_utf8_str()
                    .replace("&amp;", "&")
                    .replace("&quot;", "\"")
                    .replace("&#39;", "'");
                if name == "id" {
                    if !ids.insert(value.clone()) {
                        return Err(format!(
                            "{relative}: duplicate heading/element id '{value}'"
                        ));
                    }
                } else {
                    links.push((relative.clone(), value));
                }
            }
        }
        pages.insert(relative, ids);
    }
    for (source, destination) in links {
        if nonlocal(&destination) {
            continue;
        }
        let (url, fragment) = destination
            .split_once('#')
            .map_or((destination.as_str(), None), |(u, f)| (u, Some(f)));
        let path = url.split('?').next().unwrap_or(url);
        let path = percent_encoding::percent_decode_str(path)
            .decode_utf8()
            .map_err(|e| format!("{source}: invalid encoded link '{destination}': {e}"))?;
        let target = if path.is_empty() {
            format!("/{source}")
        } else if path.starts_with('/') {
            let base = base.trim_end_matches('/');
            if !base.is_empty() {
                let suffix = path
                    .strip_prefix(base)
                    .filter(|suffix| suffix.is_empty() || suffix.starts_with('/'))
                    .ok_or_else(|| {
                        format!("{source}: link '{destination}' is outside base_url '{base}/'")
                    })?;
                normalize(suffix)?
            } else {
                normalize(&path)?
            }
        } else {
            normalize(&format!(
                "{}/{}",
                source.rsplit_once('/').map_or("", |v| v.0),
                path
            ))?
        };
        let mut target = target.trim_start_matches('/').to_string();
        if target.is_empty() || output.join(&target).is_dir() || path.ends_with('/') {
            if !target.is_empty() && !target.ends_with('/') {
                target.push('/');
            }
            target.push_str("index.html");
        }
        if !output.join(&target).is_file() {
            return Err(format!(
                "{source}: broken internal link '{destination}' (output '{target}')"
            ));
        }
        let fragment = fragment
            .map(|value| percent_encoding::percent_decode_str(value).decode_utf8())
            .transpose()
            .map_err(|e| format!("{source}: invalid encoded fragment '{destination}': {e}"))?;
        if let Some(fragment) = fragment.filter(|f| !f.is_empty())
            && pages
                .get(&target)
                .is_some_and(|ids| !ids.contains(fragment.as_ref()))
        {
            return Err(format!(
                "{source}: broken fragment '{destination}' (output '{target}')"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> DocumentationConfig {
        DocumentationConfig {
            version: 1,
            source_root: "/manual".into(),
            include_roots: vec!["/examples".into()],
        }
    }

    #[test]
    fn code_and_includes_remain_literal() {
        let text = "```text\n{{ chart /}}\n<script>bad()</script>\n```\n\n    {{ $0 }}\n\n`{{ source path=\"/missing\" /}}`\n\n{{ source path=\"/examples/run.sh\" region=\"query\" language=\"bash\" /}}\n";
        assert_eq!(source_paths(text, &config()).unwrap(), ["/examples/run.sh"]);
        let mut sc = Shortcodes::new();
        register_source(&mut sc, config(), BTreeMap::from([("/examples/run.sh".into(), "# docs:start query\n{{ chart /}} <script>bad()</script> ``` $(false)\n# docs:end query\n".into())]));
        let expanded = preprocess(text, &sc, "/manual/page.md").unwrap();
        assert_eq!(expanded, preprocess(text, &sc, "/manual/page.md").unwrap());
        let html = render(&expanded, "/manual/page.md", &PageIndex::default(), "/").unwrap();
        assert!(html.contains("{{ chart /}}"));
        assert!(html.contains("{{ $0 }}"));
        assert!(html.contains("{{ source path="));
        assert!(html.contains("&lt;script&gt;bad()&lt;/script&gt;"));
        assert!(!html.contains("<script>bad()"));
        assert!(html.contains("``` $(false)"));
        assert!(
            source_paths("\\{{ source path=\"/missing\" /}}", &config())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn rejects_bad_sources_and_regions() {
        for path in [
            "/examples/../private",
            "/examples-other/key",
            "https://example.com",
            "/examples/%2e%2e/key",
        ] {
            assert!(include_path(&config(), path).is_err(), "{path}");
        }
        for text in [
            "# docs:start\n",
            "# docs:end\n",
            "# docs:start x\n# docs:start nested\n",
            "# docs:start x\n",
            "# docs:end x\n",
            "# docs:start x\n# docs:end y\n",
            "# docs:start x\n# docs:end x\n# docs:start x\n# docs:end x\n",
        ] {
            assert!(region_text(text, None).is_err(), "{text}");
        }
        let mut sc = Shortcodes::new();
        register_source(
            &mut sc,
            config(),
            BTreeMap::from([("/examples/run.sh".into(), "echo hello\n".into())]),
        );
        let error = preprocess(
            "{{ source path=\"/examples/run.sh\" region=\"absent\" /}}",
            &sc,
            "/manual/page.md",
        )
        .unwrap_err();
        assert!(
            error.contains("/manual/page.md")
                && error.contains("/examples/run.sh")
                && error.contains("absent")
        );
    }

    #[test]
    fn headings_have_stable_unique_ids() {
        let html = render(
            "## Same\n## Same\n## Same-1\n## `Same`\n## !!!\n",
            "/manual/page.md",
            &PageIndex::default(),
            "/",
        )
        .unwrap();
        for id in ["same", "same-1", "same-1-1", "same-2", "section"] {
            assert!(html.contains(&format!("id=\"{id}\"")));
            assert!(html.contains(&format!("href=\"#{id}\"")));
        }
    }

    #[test]
    fn page_contents_preserves_heading_hierarchy() {
        let html = render(
            "## Factory\n### Configuration\n#### Variant\n### Evidence\n## Next\n",
            "/manual/page.md",
            &PageIndex::default(),
            "/",
        )
        .unwrap();
        assert!(html.contains(
            "<ul><li><a href=\"#factory\">Factory</a><ul><li><a href=\"#configuration\">Configuration</a><ul><li><a href=\"#variant\">Variant</a></li></ul></li><li><a href=\"#evidence\">Evidence</a></li></ul></li><li><a href=\"#next\">Next</a></li></ul>"
        ));
        assert!(!html.contains("toc-level-"));
    }

    #[test]
    fn canonical_links_collisions_and_prefix() {
        let job = |source: &str, output: &str| PageJob {
            output_path: output.into(),
            page_source: source.into(),
            captures: vec![],
            datafiles: vec![],
            annotations: vec![],
            breadcrumbs: vec![],
        };
        let jobs = [
            job("/manual/a.md", "a.html"),
            job("/manual/nested/b.md", "nested/b.html"),
        ];
        let index = PageIndex::new(&jobs).unwrap();
        assert_eq!(
            index
                .link("/manual/a.md", "nested/b.md#same-1", "/watertown/")
                .unwrap(),
            "/watertown/nested/b.html#same-1"
        );
        assert_eq!(
            index.link("/manual/nested/b.md", "../a.md", "/").unwrap(),
            "/a.html"
        );
        assert!(index.link("/manual/a.md", "missing.md", "/").is_err());
        assert!(PageIndex::new(&[jobs[0].clone(), job("/manual/b.md", "a.html")]).is_err());
        assert!(PageIndex::new(&[jobs[0].clone(), job("/manual/a.md", "other.html")]).is_err());
        assert!(PageIndex::new(&[job("/manual/a.md", "../outside.html")]).is_err());
        assert!(PageIndex::new(&[job("/manual/a.md", "nested/./a.html")]).is_err());
        let mut template_a = job("/template.md", "a.html");
        template_a.captures.push("a".into());
        let mut template_b = job("/template.md", "b.html");
        template_b.captures.push("b".into());
        assert!(PageIndex::new(&[template_a, template_b]).is_ok());
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.html"),
            "<h2 id=\"same\"></h2><a href=\"/watertown/a.html#same\">yes</a>",
        )
        .unwrap();
        validate_output(dir.path(), "/watertown/").unwrap();
        std::fs::write(dir.path().join("a.html"), "<h2 id=\"a-b\"></h2><a title=\">\" href=\"/watertown/a.html#a%2Db\">yes</a><script>const text = '<a href=\"ignored\">';</script>").unwrap();
        validate_output(dir.path(), "/watertown/").unwrap();
        for link in [
            "/watertown/a.html#missing",
            "/watertown/missing.html",
            "/other/a.html",
        ] {
            std::fs::write(
                dir.path().join("a.html"),
                format!("<a href=\"{link}\">no</a>"),
            )
            .unwrap();
            assert!(
                validate_output(dir.path(), "/watertown/").is_err(),
                "{link}"
            );
        }
    }
}
