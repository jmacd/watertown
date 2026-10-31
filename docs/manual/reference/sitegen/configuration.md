---
title: Sitegen configuration
doc_id: sitegen:configuration
---

# Sitegen configuration

This page explains site-wide routes, content, exports, navigation, themes, feeds, reports, and subsites. For installation, a minimal configuration, and the complete generated schema, start with the [sitegen factory](../factories/sitegen.md).

## Routes and content

Static routes render one Markdown `page`. Template routes expand export
captures; `$0` is the first wildcard capture. Content routes reference a
named content stage and generate one page per matched Markdown file.
Nested routes accumulate slugs. Keep stage names unique and output paths
distinct; strict builds reject collisions and unresolved references.
Frontmatter `hidden` excludes navigation/feed entries, not page generation.

## Exports

Export stages resolve queryable paths or format URLs before rendering.
`timestamp_column` selects the Arrow timestamp column; `target_points`
controls temporal partition sizing, not a row limit. Quick builds
(`build OUTPUT --quick`) skip exports and subsites and are not publication
validation. See [export](../export.md) for boundaries and file layout.

## Navigation and themes

`partials.sidebar` selects a Markdown partial; `sidebar` orders the entries
used by content navigation. Strings match page titles; explicit `label` /
`href` entries and `children` support nesting. Documentation strict mode
checks targets, and all internal output URLs include `site.base_url`.
Theme keys override CSS custom properties, not arbitrary template code.
`header` is a banner asset. `footer` accompanies the binary build identifier.

## Feeds

RSS requires an absolute `site.site_url`. The default `feed.xml` includes
dated, non-hidden posts in section `Blog`, newest first. Relative links become
absolute using that origin and the project base path. `feed` overrides the
description, section, and content-stage selection. Missing dates and hidden
posts are excluded; they are not parsing failures.

## Reports and subsites

`pond run /etc/site -- report NAME OUTPUT` writes `report.html`, `report.txt`,
and requested static charts from a named report. It does not send mail.
Report windows are trailing duration strings; series selection uses exact
export captures. Numeric summaries and PNGs are not interactive charts.
The [report factories](../factories/reporting.md) have separate service
prerequisites and output contracts.

Subsites recursively run a config inside an imported mount, into its
configured subdirectory. Imported content must already exist and be readable;
this is not a fetch or replication command. Check URL prefixes at every
nested site. Status grids query journal/performance data at build time;
they are snapshots refreshed only by rebuilding.

## Documentation mode and failure behavior

`documentation.version: 1` opts into the documentation renderer contract:
literal code/source includes, source-relative Markdown links, stable heading
IDs, page contents, and lean local assets. `source_root` defines page paths;
`include_roots` restrict literal file inclusion. `_generated` fragments do not
become pages. See [shortcodes](shortcodes.md) and [browser behavior](browser.md).

Strict builds reject missing includes/regions, local links/anchors, invalid
navigation, and duplicate outputs. Ordinary builds can leave partial files
on failure; publish only a completed staging directory. `make docs-build`
already stages and atomically replaces marked preview output.

To update a pond-installed configuration, repeat `mknod` with `--overwrite`.
Rebuild explicitly; changing Markdown, config, or source data does not launch
a watcher or automatically publish.
