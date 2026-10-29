---
title: Watertown manual
---

# Watertown development manual

Watertown is a small data lake for querying, transforming, and publishing
industrial telemetry. This is the **four-page documentation pilot**, not yet a
comprehensive manual. Installation, operational recovery, and the remaining
interfaces still live in the repository's existing documentation.

## Start here

[Create and query a timeseries](guides/timeseries.md) with a deterministic,
offline example. Learn the exact interfaces in the
[synthetic-timeseries factory](reference/factories/synthetic-timeseries.md) and
[CSV format](reference/formats/csv.md) references.

## Publishing

The [shortcode reference](reference/sitegen/shortcodes.md#literal-examples)
explains literal examples and source inclusion. The manual itself is a
host-mode sitegen build: no pond, live services, external fonts, or
visualization downloads are needed.

## Contributing

See the [authoring workflow](https://github.com/jmacd/watertown/blob/main/docs/README.md)
and [migration inventory](https://github.com/jmacd/watertown/blob/main/docs/documentation-inventory.json).
Only `docs/manual/` is selected as page content; archives and design notes are
not published. Generated fragments under `_generated/` are excluded from page
discovery.
