---
title: Excel HTML
doc_id: format:excelhtml
---

# Excel HTML

`excelhtml` reads the HydroVu-specific HTML export structure. It is not a
general HTML-table parser or a binary Excel reader; use [Excel](xls.md) for
`.xls` and `.xlsx` workbooks.

## URL example

```bash
# Replace the example host path with a HydroVu HTML export.
# No provider-specific options are required or supported.
pond cat 'host+excelhtml:///tmp/hydrovu-export.html' --format=table
```

Use `excelhtml:///exports/hydrovu.html` for pond input. An explicit decoder
can be added, for example `host+excelhtml+gzip:///tmp/hydrovu-export.html.gz`.
See [shared URL rules](../concepts.md) for host roots and reader layers.

## Full reference

The input must contain table `id="isi-report"` and the recognized data
header/row markers. Output has nullable Utf8 `Date Time` and nullable Float64
parameter columns. Convert the string time explicitly for downstream temporal
operations.

There is no typed provider-specific option object or YAML configuration.
Provider query options are ignored. The `+gzip`, `+zstd`, and `+bzip2` layers
are supported; filename extensions do not automatically select them.

## Behavior and limitations

The provider buffers the HTML DOM and rows, then emits batches of 1000.
Missing tables, missing headers, and invalid input fail. Parser tests do not
establish compatibility with arbitrary spreadsheet-generated HTML. The URL
example is usage guidance, not a runnable documentation fixture.
