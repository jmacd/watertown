// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Markdown rendering and shortcode expansion for sitegen.
//!
//! Uses pulldown-cmark directly for markdown -> HTML conversion, and a simple
//! `{{ name key="value" /}}` shortcode system for template expansion.
//!
//! ## Why not maudit?
//!
//! We originally used maudit's `render_markdown` but it added 800+ lines of
//! syntect highlighting and component abstractions we don't need.  Going
//! direct with pulldown-cmark gives us control over the parser options and
//! a smaller dependency footprint.

use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd, html::push_html};
use std::collections::HashMap;

// --- Markdown rendering ------------------------------------------------------

/// Render markdown to HTML.
///
/// Uses pulldown-cmark with GFM extensions (tables, strikethrough, task lists).
/// Raw HTML blocks (`<script>`, `<style>`, `<div>`, etc.) pass through
/// unchanged per the CommonMark spec.
///
/// ## Script block extraction
///
/// `<script>...</script>` blocks are extracted before markdown parsing and
/// re-inserted afterwards.  This works around a CommonMark spec interaction
/// where a preceding type-6 HTML block (e.g. `<link>`) absorbs a `<script>`
/// tag on the next line; the first blank line inside the script then ends
/// the type-6 block and the remaining JS is parsed as regular markdown.
pub fn render_markdown(content: &str) -> String {
    // Extract <script>...</script> blocks before markdown parsing.
    let (processed, scripts) = extract_scripts(content);

    let options =
        Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS | Options::ENABLE_TABLES;

    let parser = Parser::new_ext(&processed, options);

    // Transform heading events to add id anchors.
    let events = inject_heading_anchors(parser);

    let mut html = String::with_capacity(content.len() * 2);
    push_html(&mut html, events.into_iter());

    // Restore extracted script blocks.
    restore_scripts(&mut html, &scripts);
    html
}

/// Rewrite absolute URLs in rendered HTML to include a base path prefix.
///
/// Matches `href="/..."` and `src="/..."` attributes and prepends the
/// base path.  External URLs (starting with `//` or a scheme) and
/// already-prefixed URLs are left untouched.
///
/// When `base` is `/` or empty, the input is returned unchanged.
pub fn rewrite_absolute_urls(html: &str, base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.is_empty() {
        return html.to_string();
    }

    // Match href="/..." or src="/..." where the path starts with /
    // but not with // (protocol-relative) or the base itself.
    let re = regex::Regex::new(r#"((?:href|src|action)=")(/[^"/][^"]*")"#).unwrap();
    re.replace_all(html, |caps: &regex::Captures| {
        let attr = &caps[1]; // e.g. href="
        let path = &caps[2]; // e.g. /data/foo.html"
        // Don't double-prefix
        if path.starts_with(&format!("{}/", base)) {
            format!("{}{}", attr, path)
        } else {
            format!("{}{}{}", attr, base, path)
        }
    })
    .into_owned()
}
/// Lowercases, replaces non-alphanumeric runs with hyphens, strips
/// leading/trailing hyphens.
fn slugify(text: &str) -> String {
    let mut slug = String::with_capacity(text.len());
    let mut prev_hyphen = true; // suppress leading hyphen
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            prev_hyphen = false;
        } else if !prev_hyphen {
            slug.push('-');
            prev_hyphen = true;
        }
    }
    // Strip trailing hyphen
    while slug.ends_with('-') {
        slug.pop();
    }
    slug
}

/// Walk the pulldown-cmark event stream and inject `id` attributes on headings.
///
/// For each heading, we:
/// 1. Collect all text inside the heading to build a slug
/// 2. Replace `Start(Heading { .. })` with an `Html` event containing
///    `<hN id="slug">` so the heading gets an anchor
/// 3. Append a clickable `#` link after the heading text
fn inject_heading_anchors<'a>(parser: Parser<'a>) -> Vec<Event<'a>> {
    let mut events: Vec<Event<'a>> = Vec::new();
    let mut in_heading: Option<pulldown_cmark::HeadingLevel> = None;
    let mut heading_text = String::new();
    let mut heading_events: Vec<Event<'a>> = Vec::new();

    for event in parser {
        match &event {
            Event::Start(Tag::Heading { level, .. }) => {
                in_heading = Some(*level);
                heading_text.clear();
                heading_events.clear();
                heading_events.push(event);
            }
            Event::End(TagEnd::Heading(level)) if in_heading == Some(*level) => {
                let slug = slugify(&heading_text);
                let level_num = match level {
                    pulldown_cmark::HeadingLevel::H1 => 1,
                    pulldown_cmark::HeadingLevel::H2 => 2,
                    pulldown_cmark::HeadingLevel::H3 => 3,
                    pulldown_cmark::HeadingLevel::H4 => 4,
                    pulldown_cmark::HeadingLevel::H5 => 5,
                    pulldown_cmark::HeadingLevel::H6 => 6,
                };

                if !slug.is_empty() {
                    // Emit <hN id="slug"> instead of <hN>
                    events.push(Event::Html(
                        format!("<h{} id=\"{}\">", level_num, slug).into(),
                    ));
                    // Emit the heading's inner events (text, inline code, etc.)
                    // but skip the original Start(Heading) we buffered
                    for e in heading_events.drain(..).skip(1) {
                        events.push(e);
                    }
                    // Anchor link (h2+ only -- h1 is the page title)
                    if level_num >= 2 {
                        events.push(Event::Html(
                            format!(
                                " <a class=\"anchor\" href=\"#{}\" aria-hidden=\"true\">#</a>",
                                slug
                            )
                            .into(),
                        ));
                    }
                    events.push(Event::Html(format!("</h{}>", level_num).into()));
                } else {
                    // No slug -- pass through unchanged
                    events.append(&mut heading_events);
                    events.push(event);
                }
                in_heading = None;
            }
            Event::Text(text) if in_heading.is_some() => {
                heading_text.push_str(text);
                heading_events.push(event);
            }
            Event::Code(code) if in_heading.is_some() => {
                heading_text.push_str(code);
                heading_events.push(event);
            }
            _ if in_heading.is_some() => {
                heading_events.push(event);
            }
            _ => {
                events.push(event);
            }
        }
    }

    events
}

/// Pull out every `<script ...>...</script>` span, replacing each with an
/// HTML-comment placeholder that pulldown-cmark passes through untouched
/// (CommonMark type-2 HTML block).
fn extract_scripts(content: &str) -> (String, Vec<String>) {
    let mut result = String::with_capacity(content.len());
    let mut scripts: Vec<String> = Vec::new();
    let mut remaining = content;

    while let Some(start) = remaining.find("<script") {
        // Verify the char after "<script" is whitespace, '>', or '/' to
        // avoid matching e.g. "<scripting>".
        let after = &remaining[start + 7..];
        let next_ch = after.chars().next().unwrap_or('>');
        if !matches!(next_ch, ' ' | '\t' | '\n' | '\r' | '>' | '/') {
            result.push_str(&remaining[..start + 7]);
            remaining = after;
            continue;
        }

        if let Some(end_rel) = remaining[start..].find("</script>") {
            let end = start + end_rel + "</script>".len();
            result.push_str(&remaining[..start]);
            result.push_str(&format!("<!-- SITEGEN_SCRIPT_{} -->", scripts.len()));
            scripts.push(remaining[start..end].to_string());
            remaining = &remaining[end..];
        } else {
            // No closing tag -- pass rest through as-is.
            break;
        }
    }
    result.push_str(remaining);
    (result, scripts)
}

/// Replace comment placeholders with the original script blocks.
fn restore_scripts(html: &mut String, scripts: &[String]) {
    for (i, script) in scripts.iter().enumerate() {
        let placeholder = format!("<!-- SITEGEN_SCRIPT_{} -->", i);
        if let Some(pos) = html.find(&placeholder) {
            html.replace_range(pos..pos + placeholder.len(), script);
        }
    }
}

// --- Shortcodes --------------------------------------------------------------

/// A registered shortcode function.
pub type ShortcodeFn = Box<dyn Fn(&ShortcodeArgs) -> Result<String, String> + Send + Sync>;

/// Registry of named shortcode functions.
#[derive(Default)]
pub struct Shortcodes(HashMap<String, ShortcodeFn>);

impl Shortcodes {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    #[cfg(feature = "documentation")]
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.0.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn register<F>(&mut self, name: &str, func: F)
    where
        F: Fn(&ShortcodeArgs) -> String + Send + Sync + 'static,
    {
        self.register_fallible(name, move |args| Ok(func(args)));
    }

    pub fn register_fallible<F>(&mut self, name: &str, func: F)
    where
        F: Fn(&ShortcodeArgs) -> Result<String, String> + Send + Sync + 'static,
    {
        self.0.insert(name.to_string(), Box::new(func));
    }

    fn get(&self, name: &str) -> Option<&ShortcodeFn> {
        self.0.get(name)
    }
}

/// Parsed key=value arguments from a shortcode invocation.
pub struct ShortcodeArgs(HashMap<String, String>);

impl ShortcodeArgs {
    /// Get argument as raw string.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.as_str())
    }

    /// Create a ShortcodeArgs from a HashMap (for testing).
    #[cfg(test)]
    pub fn from_map(args: HashMap<String, String>) -> Self {
        ShortcodeArgs(args)
    }
}

/// Expand `{{ name key="value" /}}` shortcodes in markdown source.
///
/// Supports:
/// - Self-closing: `{{ chart /}}`
/// - With args: `{{ nav_list collection="params" base="/params" /}}`
/// - Block: `{{ wrap }}...{{ /wrap }}`
/// - Escaped: `\{{ literal }}`
pub fn preprocess_shortcodes(
    content: &str,
    shortcodes: &Shortcodes,
    markdown_path: Option<&str>,
) -> Result<String, String> {
    let mut output = String::new();
    let mut rest = content;

    while let Some(start) = rest.find("{{") {
        // Escaped \{{ -> literal
        if start > 0 && rest.as_bytes()[start - 1] == b'\\' {
            output.push_str(&rest[..start - 1]);
            output.push_str("{{");
            rest = &rest[start + 2..];
            continue;
        }

        output.push_str(&rest[..start]);

        let remaining = &rest[start + 2..];
        let Some(tag_end) = remaining.find("}}") else {
            output.push_str("{{");
            rest = remaining;
            continue;
        };

        let sc_content = remaining[..tag_end].trim();
        let is_self_closing = sc_content.ends_with('/');
        let sc_content = if is_self_closing {
            sc_content.trim_end_matches('/').trim()
        } else {
            sc_content
        };

        // Parse name
        let mut parts = sc_content.split_whitespace();
        let name = parts.next().ok_or("Empty shortcode")?;

        if name.starts_with('/') {
            return Err(format!("Unexpected closing tag: {}", name));
        }

        if !is_valid_name(name) {
            output.push_str("{{");
            rest = remaining;
            continue;
        }

        // Parse key="value" arguments
        let args_str = parts.collect::<Vec<_>>().join(" ");
        let mut args = parse_args(&args_str)?;
        args.insert(
            "markdown_path".to_string(),
            markdown_path.unwrap_or("").to_string(),
        );

        let after_tag = &remaining[tag_end + 2..];

        if is_self_closing {
            if let Some(func) = shortcodes.get(name) {
                output.push_str(&func(&ShortcodeArgs(args))?);
            } else {
                return Err(format!("Unknown shortcode: '{}'", name));
            }
            rest = after_tag;
        } else {
            // Block shortcode -- find closing tag
            let close_compact = format!("{{{{/{}}}}}", name);
            let close_spaced = format!("{{{{ /{} }}}}", name);

            let close_pos = after_tag
                .find(&close_compact)
                .or_else(|| after_tag.find(&close_spaced));

            if let Some(pos) = close_pos {
                let close_len = if after_tag[pos..].starts_with(&close_compact) {
                    close_compact.len()
                } else {
                    close_spaced.len()
                };

                let body = preprocess_shortcodes(&after_tag[..pos], shortcodes, markdown_path)?;
                let mut block_args = args;
                block_args.insert("body".to_string(), body);

                if let Some(func) = shortcodes.get(name) {
                    output.push_str(&func(&ShortcodeArgs(block_args))?);
                } else {
                    return Err(format!("Unknown shortcode: '{}'", name));
                }
                rest = &after_tag[pos + close_len..];
            } else {
                return Err(format!(
                    "Block shortcode '{}' missing closing tag. Use '{{{{ {} /}}}}' for self-closing.",
                    name, name
                ));
            }
        }
    }

    output.push_str(rest);
    Ok(output)
}

// --- Helpers -----------------------------------------------------------------

/// Valid shortcode name: `[A-Za-z_][A-Za-z0-9_]+`
fn is_valid_name(name: &str) -> bool {
    if name.len() < 2 {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Parse `key="value" key2="value2"` into a HashMap.
pub(crate) fn parse_args(input: &str) -> Result<HashMap<String, String>, String> {
    let mut args = HashMap::new();
    if input.is_empty() {
        return Ok(args);
    }

    let mut chars = input.chars().peekable();
    let mut key = String::new();
    let mut value = String::new();
    let mut in_key = true;
    let mut in_quotes = false;
    let mut quote_char = '"';

    while let Some(ch) = chars.next() {
        match ch {
            '=' if in_key && !in_quotes => {
                in_key = false;
                if let Some(&q) = chars.peek()
                    && (q == '"' || q == '\'')
                {
                    quote_char = q;
                    in_quotes = true;
                    chars.next();
                }
            }
            c if !in_key && in_quotes && c == quote_char => {
                in_quotes = false;
                args.insert(key.trim().to_string(), value.clone());
                key.clear();
                value.clear();
                in_key = true;
                // skip trailing whitespace
                while chars.peek() == Some(&' ') {
                    chars.next();
                }
            }
            ' ' if !in_quotes => {
                if !in_key && !value.is_empty() {
                    args.insert(key.trim().to_string(), value.trim().to_string());
                    key.clear();
                    value.clear();
                    in_key = true;
                }
                while chars.peek() == Some(&' ') {
                    chars.next();
                }
            }
            '\\' if in_quotes => {
                if let Some(esc) = chars.next() {
                    match esc {
                        '"' | '\'' | '\\' => value.push(esc),
                        'n' => value.push('\n'),
                        't' => value.push('\t'),
                        _ => {
                            value.push('\\');
                            value.push(esc);
                        }
                    }
                }
            }
            _ => {
                if in_key {
                    key.push(ch);
                } else {
                    value.push(ch);
                }
            }
        }
    }

    if !in_key && !value.is_empty() {
        if in_quotes {
            return Err("Unclosed quote in argument value".to_string());
        }
        args.insert(key.trim().to_string(), value.trim().to_string());
    }

    Ok(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_markdown_basic() {
        let html = render_markdown("# Hello\n\nWorld");
        assert!(html.contains(r#"<h1 id="hello">"#));
        assert!(html.contains("Hello"));
        assert!(html.contains("<p>World</p>"));
    }

    #[test]
    fn test_render_markdown_script_passthrough() {
        let input = r#"# Title

<script type="module">
const x = 1;

items.forEach(i => {
  console.log(`Item ${i}`);
});
</script>
"#;
        let html = render_markdown(input);
        assert!(html.contains("<script type=\"module\">"));
        assert!(html.contains("items.forEach(i =>"));
        assert!(!html.contains("<p>items"));
        assert!(!html.contains("<code>"));
    }

    /// The actual bug: `<link>` before `<script>` creates a CommonMark type-6
    /// block that absorbs the script.  Blank line inside the script then ends
    /// the type-6 block and the rest is parsed as markdown.
    #[test]
    fn test_render_markdown_link_then_script() {
        let input = r#"# Title

<link rel="stylesheet" href="https://example.com/test.css">
<script type="module">
const el = document.getElementById("test-container");
const items = [1, 2, 3];

items.forEach(i => {
  const div = document.createElement("div");
  div.textContent = `Item ${i}`;
  el.appendChild(div);
});

console.log("Script loaded successfully");
</script>
"#;
        let html = render_markdown(input);
        // Script content must survive intact
        assert!(html.contains("items.forEach(i =>"), "forEach missing");
        assert!(!html.contains("<p>items"), "forEach wrapped in <p>");
        assert!(!html.contains("<code>"), "backtick became <code>");
        assert!(!html.contains("=&gt;"), "arrow was entity-encoded");
        // Link tag preserved
        assert!(html.contains("<link rel=\"stylesheet\""), "link missing");
    }

    /// Inline `<script>` inside a `<div>` (chart shortcode pattern).
    #[test]
    fn test_render_markdown_inline_script_in_div() {
        let input = r#"# Chart

<div class="chart"><script type="application/json">{"data":1}</script></div>
"#;
        let html = render_markdown(input);
        assert!(html.contains(r#"<script type="application/json">{"data":1}</script>"#));
    }

    #[test]
    fn test_extract_scripts_no_false_match() {
        // "<scripting>" should NOT be extracted
        let input = "<scripting>test</scripting>\n<script>real</script>\n";
        let html = render_markdown(input);
        assert!(html.contains("<scripting>"));
        assert!(html.contains("<script>real</script>"));
    }

    /// Noyo index.md pattern: markdown -> div -> markdown -> link + script (no
    /// blank lines in the script body).
    #[test]
    fn test_render_markdown_noyo_index() {
        let input = r#"# Noyo Harbor

Dashboard text.

<div id="map" style="height:400px;"></div>

## About

Some paragraph text.

<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script type="module">
import * as L from "https://cdn.jsdelivr.net/npm/leaflet@1.9.4/+esm";
const map = L.map("map", { scrollWheelZoom: false }).setView([39.42, -123.80], 16);
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", { attribution: '&copy; OSM' }).addTo(map);
</script>
"#;
        let html = render_markdown(input);
        assert!(
            html.contains(r#"<script type="module">"#),
            "script tag missing"
        );
        assert!(html.contains("import * as L"), "import missing");
        assert!(html.contains("L.map("), "L.map missing");
        assert!(html.contains("L.tileLayer("), "tileLayer missing");
        assert!(!html.contains("<p>const"), "JS wrapped in <p>");
        assert!(!html.contains("<p>L."), "JS wrapped in <p>");
        assert!(html.contains("<link rel="), "link tag missing");
    }

    #[test]
    fn test_shortcode_self_closing() {
        let mut sc = Shortcodes::new();
        sc.register("hello", |_args: &ShortcodeArgs| "HELLO".to_string());

        let result = preprocess_shortcodes("before {{ hello /}} after", &sc, None);
        assert_eq!(result.unwrap(), "before HELLO after");
    }

    #[test]
    fn test_shortcode_with_args() {
        let mut sc = Shortcodes::new();
        sc.register("greet", |args: &ShortcodeArgs| {
            format!("Hi {}", args.get_str("name").unwrap_or("?"))
        });

        let result = preprocess_shortcodes("{{ greet name=\"World\" /}}", &sc, None);
        assert_eq!(result.unwrap(), "Hi World");
    }

    #[test]
    fn test_shortcode_block() {
        let mut sc = Shortcodes::new();
        sc.register("wrap", |args: &ShortcodeArgs| {
            format!("<div>{}</div>", args.get_str("body").unwrap_or(""))
        });

        let result = preprocess_shortcodes("{{ wrap }}content{{ /wrap }}", &sc, None);
        assert_eq!(result.unwrap(), "<div>content</div>");
    }

    #[test]
    fn test_parse_args() {
        let args = parse_args(r#"collection="params" base="/params""#).unwrap();
        assert_eq!(args.get("collection").unwrap(), "params");
        assert_eq!(args.get("base").unwrap(), "/params");
    }

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("Hello World"), "hello-world");
        assert_eq!(slugify("The Water System"), "the-water-system");
        assert_eq!(slugify("  Leading & Trailing  "), "leading-trailing");
        assert_eq!(slugify("CamelCase123"), "camelcase123");
    }

    #[test]
    fn test_heading_anchors() {
        let html = render_markdown("## Hello World\n\nSome text.\n");
        assert!(
            html.contains(r#"<h2 id="hello-world">"#),
            "Expected id on h2, got: {}",
            html
        );
        assert!(
            html.contains(r##"href="#hello-world""##),
            "Expected anchor link, got: {}",
            html
        );
        assert!(html.contains("Some text"), "Body text missing");
    }

    #[test]
    fn test_heading_anchors_h1() {
        let html = render_markdown("# Main Title\n");
        assert!(
            html.contains(r#"<h1 id="main-title">"#),
            "Expected id on h1, got: {}",
            html
        );
    }
}
