---
title: A timeseries site
doc_id: guide:timeseries-site
---

# A timeseries site

This complete package installs the shared deterministic synthetic source,
exports its typed data, and renders a homepage plus a captured sensor route.
Prerequisites: Bash, `pond` on PATH, and matching sitegen vendor assets
(bundled in production images; `make vendor` for source builds).
No API or live sensor is contacted.

{{ source path="/testsuite/examples/timeseries-site/site.yaml" language="yaml" /}}

{{ source path="/testsuite/examples/timeseries-site/run.sh" language="bash" /}}

Run `bash testsuite/examples/timeseries-site/run.sh` from a checkout.
The [download package](/downloads/timeseries-site.tar) also includes the
shared `synthetic-timeseries` directory it references. Each invocation creates
a fresh output/pond; set `EXAMPLE_OUTPUT` to an absent directory to retain it.

Test `214` asserts the homepage and `temperature.html`, base-prefixed chart
assets/manifest, readable Parquet, four rows, and temperature sum `101.6`.
This is offline artifact/data coverage, not a headless browser interaction test.
Serve the generated directory under `/sensor-example/` using an HTTP server
to use the chart; do not open it as a `file://` document.

See [export](../reference/export.md), [configuration](../reference/sitegen/configuration.md),
and [browser behavior](../reference/sitegen/browser.md). Rebuilding regenerates
exports/pages; it does not watch sources, ingest live measurements, or publish.
