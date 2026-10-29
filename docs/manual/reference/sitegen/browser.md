---
title: Browser behavior
doc_id: sitegen:browser
---

# Browser behavior

Ordinary documentation uses local CSS, system fonts, semantic navigation, and
no JavaScript. No search, syntax highlighter, copy controls, watcher, or live
reload is installed. Downloading source packages does not execute them.

Data pages instead emit inline manifests pointing to exported Parquet.
Chart/log/explorer modules query those files in the browser through
DuckDB-WASM, then render with the vendored visualization libraries.
Build/install the matching vendor assets once (`make vendor` for source
builds); production images include them. Serve over HTTP(S), not `file://`.
Keep asset URLs under the configured base path and use hosting with WASM/
module MIME support. Export only data suitable for every site visitor:
browser queries are not server-side authorization.

`chart` draws time/value series; the range argument or
`site.default_range` chooses the initial range, clamped to available data.
`metric_registry` controls counter/rate/gauge transformations; counter plots
use differences/rates rather than raw cumulative levels. Scope alignment,
column selection, and chart captions depend on the actual exported schema.
`overlay_chart` supplies pump-cycle drawdown/recovery/Horner views.
`log_viewer` provides a queryable log presentation.
`viz renderer="chart|overlay|logs"` selects the same renderer families.

`explore` offers SQL over selected exports. `explore.datasets` defines the
multi-dataset picker; otherwise shortcode `table` / `label` name the page
dataset. Configured explorer URLs let charts hand off selected files/time
windows. Secondary route `annotations` are build-selected exports rendered
as chart backgrounds. A chart does not fetch current pond data automatically:
rebuild/republish to update its manifest and Parquet.

Missing assets, incompatible WASM support, unavailable files, or query errors
must be investigated in browser diagnostics. CLI artifact assertions do not
prove interactive correctness in every browser. Browser integration checks
are a separate headless suite with their own prerequisites, not counted as
passed merely because the content-only manual built.
