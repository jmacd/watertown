---
title: Watertown manual
---

# Watertown development manual

Watertown is a small data lake for querying, transforming, and publishing
industrial telemetry. This is the **current development manual**, not a
versioned stable-release contract. Public interfaces have compiled reference
coverage, but cloud/API workflows and full browser interactions are not all
end-to-end verified. Migration cleanup remains in D11.

## Start here

[Create and query a timeseries](guides/timeseries.md) with a deterministic,
offline example. Learn the exact interfaces in the
[synthetic-timeseries factory](reference/factories/synthetic-timeseries.md) and
[CSV format](reference/formats/csv.md) references.

Build a [timeseries site](guides/timeseries-site.md), publish a
[blog/RSS feed](guides/blog.md), or practice [backup/recovery](guides/operations.md).
These packages create fresh workspaces and assert results in numbered tests.

## Reference

[CLI commands](reference/cli/commands.md) and [global options](reference/cli/global-options.md)
come from the production Clap definitions. Read [shared concepts](reference/concepts.md)
before combining URLs, credentials, imports, and temporal operations.
[Export](reference/export.md) is not replication.
[Factory transforms](reference/factories/timeseries.md),
[ingestion](reference/factories/ingestion.md), [storage](reference/factories/storage.md),
and [format providers](reference/formats/providers.md) state their prerequisites
and evidence boundaries.

## Site generation

The [shortcode reference](reference/sitegen/shortcodes.md#literal-examples)
explains literal examples and source inclusion; the [configuration reference](reference/sitegen/configuration.md)
covers YAML, routes, reports, feeds, and nested settings. The manual itself is a
host-mode sitegen build: no pond, live services, external fonts, or
visualization downloads are needed.

## Contributing

See the [authoring workflow](https://github.com/jmacd/watertown/blob/main/docs/README.md)
and [migration inventory](https://github.com/jmacd/watertown/blob/main/docs/documentation-inventory.json).
Only `docs/manual/` is selected as page content; archives and design notes are
not published. Generated fragments under `_generated/` are excluded from page
discovery.

[Download compiled metadata](/docs/manual/_generated/inventory.json).
The footer records the renderer/documentation version and publication source
revision; local previews are explicitly labeled `working-tree`.
