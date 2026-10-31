---
title: Markdown frontmatter
doc_id: sitegen:frontmatter
---

# Markdown frontmatter

A page begins with a YAML object between `---` lines. Fields below are parsed
by the actual sitegen deserializer. Unknown metadata (including `doc_id`) is
tolerated, but unknown layouts fail strict builds.

<!-- docs:reference sitegen:frontmatter -->
## Full configuration

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

### Frontmatter

Frontmatter parsed from the top of each markdown page.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `date` | string &#124; null | no | null | Publication date (ISO 8601, e.g. "2024-06-15"). |
| `hidden` | boolean | no | false | If true, page renders but does not appear in navigation. |
| `image` | string &#124; null | no | null | Image URL for blog card hero image. |
| `layout` | string | no | "default" |  |
| `section` | string &#124; null | no | null | Navigation section for grouping (e.g., "About", "Blog"). |
| `slug` | string &#124; null | no | null | URL slug override. If absent, derived from filename. |
| `summary` | string &#124; null | no | null | Short summary for blog card display. |
| `title` | string | no | "" |  |
| `weight` | integer | no | 100 | Sort weight for content pages (lower = higher in nav). Default: 100 Constraints: format="int32". |

Unknown fields: not rejected by this shape; see runtime notes.
<!-- docs:end-reference -->

In ordinary sites the default layout is `default`; documentation mode selects
the documentation layout for pages without an explicit layout. `weight`
orders navigation (lower first). `hidden` hides an entry from navigation/RSS,
not from published HTML. Never use it to protect private content.

`slug` overrides the filename-derived output path. In documentation mode the
source-relative nested path is preserved by default. Keep output assignments
unique. Headings receive unique lowercase IDs with numeric suffixes for
duplicates; link to the generated ID, not a separately maintained anchor.

`section: Blog` and an ISO date such as `"2024-06-15"` make a visible page
eligible for the default RSS feed. `summary` and `image` supply blog cards.
See the [blog package](../../guides/blog.md) for asserted ordering/exclusion.
