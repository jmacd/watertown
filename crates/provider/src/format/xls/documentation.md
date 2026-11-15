---
title: Excel
doc_id: format:xls
---

# Excel

`xls` reads `.xls` and `.xlsx` workbooks through Calamine, exposing a selected
worksheet or Excel Table as a queryable table. For HydroVu HTML exports, use
[Excel HTML](excelhtml.md) instead.

## URL example

```bash
# Example input and sheet; replace both with names in your workbook.
# header=true and batch_size=8192 are defaults, written explicitly here.
# table and header_row are unset by default: omit them rather than use "null".
pond cat 'host+xls:///tmp/readings.xlsx?sheet=Readings&header=true&batch_size=8192' --format=table
```

`sheet` defaults to automatic selection when there is exactly one worksheet;
otherwise it is required. A missing named sheet reports the available names.
To select an Excel Table explicitly, add an example `&table=Measurements`.
To override header inference, add an example `&header_row=2` (one-based
worksheet row number). Neither table names nor header-row numbers have a
universal default. `table` requires `.xlsx` and cannot be combined with
`header_row` or `header=false`. When multiple Excel Tables are present in a
sheet, select one explicitly.

Use `xls:///readings.xlsx?...` for pond input. Quote URLs containing `&`.
The generated reference describes typed options; the URL decoder parses their
string representations. See [shared URL rules](../concepts.md).

<!-- docs:reference format:xls -->
<!-- docs:end-reference -->

## Types, headers, and ranges

For `.xlsx`, a declared Excel Table supplies its exact range and column names.
Without a declared table, header detection normally selects the first populated
row. It also recognizes report exports consisting of one-cell title rows, a
blank separator row, and a multi-column text header, excluding the title
material from output and inference. Use `header_row` only to override an
ambiguous or unusual layout. `header=false` retains every row as data and
generates column names.

Blank headers receive generated names when their columns contain data; unnamed
columns empty in every data row are omitted as worksheet layout. Named empty
columns remain in the schema. Duplicate names are rejected. Inference examines
data rows after the selected header: numeric columns become `Int64` or
`Float64`, booleans become `Boolean`, and Excel datetimes become timezone-free
microsecond timestamps. Other mixed-type columns become text without dropping
values. Empty cells are nulls and internal blank rows are retained.

## Errors and limitations

Spreadsheet error cells, invalid options, unreadable workbooks, and empty
worksheets fail explicitly. Header-only worksheets expose their schema with
zero data rows. Formulas use cached values; the provider does not recalculate
them. Calamine requires seekable input, so the provider buffers the workbook
and selected worksheet in memory. Parsing and inference run on a blocking
worker; batching does not make workbook reads streaming, and SQL filters do not
avoid parsing the worksheet.

Explicit `+gzip`, `+zstd`, and `+bzip2` reader layers do not remove the workbook
buffering requirement. Unit tests cover workbook/parser cases; this URL
example is guidance, not a runnable documentation fixture.
