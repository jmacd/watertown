---
title: Shortcodes
doc_id: reference:shortcodes
---

# Sitegen shortcode pilot

Shortcodes expand declared site data into HTML during a build. They do not run
shell commands or execute Markdown fences. This page covers the pilot's
literal-source contract, not every chart/report/browser option.

## Literal examples

In documentation mode, fenced blocks, indented blocks, and inline code bypass
both variable rewriting and shortcode expansion:

```text
{{ chart /}}
{{ $0 }}
{{ source path="/testsuite/examples/synthetic-timeseries/config.yaml" language="yaml" /}}
<script>throw new Error("literal only")</script>
$(touch /must-not-run)
```

Inline `{{ nav-list collection="params" /}}` keeps its spelling too.

    {{ source path="/not-a-real-include" /}}

## Source inclusion

An active directive includes escaped source, without recursive shortcode or
Markdown expansion:

```text
{{ source path="/testsuite/examples/synthetic-timeseries/run.sh" region="create-and-query" language="bash" /}}
```

`path` is an absolute **input-root** path, restricted by
`documentation.include_roots`. It is not a host absolute path or a URL.
`region` is optional; omit it for the complete file. `language` labels a plain
code block; it does not enable syntax highlighting.

Regions use `# docs:start NAME` and `# docs:end NAME` comment lines, not line
numbers. Missing files/regions, duplicate or unbalanced markers, and paths
outside approved directories fail with the referring source and requested
input in the diagnostic. Regions cannot nest.

This fixture deliberately contains HTML, shell text, shortcodes, and fences:

{{ source path="/testsuite/examples/documentation-pilot/literal.txt" language="text" /}}

[Complete literal fixture](/testsuite/examples/documentation-pilot/literal.txt).

## Navigation

{{ breadcrumb /}}

The sidebar uses `{{ content_nav /}}` with nested `sidebar` entries in
`docs/site.yaml`. Native links remain keyboard-accessible without JavaScript.
Per-page contents use the same unique IDs as headings.

Relative Markdown links such as
[the timeseries guide](../../guides/timeseries.md#inspect-1) resolve through
the canonical source/output index, including the GitHub Pages `/watertown/`
prefix. Explicit frontmatter `slug` values override source-relative paths;
duplicate output assignments fail rather than overwrite a page.

## Strict builds

`build OUTPUT --strict` rejects missing internal targets/fragments, invalid
navigation, output collisions, missing includes, partial errors, and unknown
layouts. `documentation.version: 1` opts in to literal preprocessing,
source-relative nested slugs, `_generated` discovery exclusion, and lean
assets. Existing sites without this block retain the legacy renderer.

The pilot layout uses local CSS and system fonts only. Data layouts can
explicitly request visualization scripts and their vendor assets; ordinary
manual pages do not require them. Search, highlighting, code-copy controls,
and live reload are deferred.
