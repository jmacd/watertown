---
title: Format providers
doc_id: reference:formats
---

# Format providers

Formats interpret raw bytes; they are not YAML factories. For example,
`pond cat host+jsonlogs:///tmp/events.jsonl --format=table` queries host input
without a pond; omit `host+` for pond paths. Quote URLs with query strings.
All support the common `+gzip`, `+zstd`, and `+bzip2` reader layers, not
automatic compression inferred from filename extensions. Layering a decoder
does not make a buffering parser constant-memory.

Each provider has its own editable page with prose, a URL example, and its
full reference. See [shared URL rules](../concepts.md) for entry-type casts
and roots.

## CSV

[CSV reference and runnable example](csv.md).

## OpenTelemetry JSON

[OpenTelemetry JSON reference](oteljson.md).

## JSON logs

[JSON logs reference](jsonlogs.md).

## Web logs

[Web logs reference](weblog.md).

## Excel HTML

[Excel HTML reference](excelhtml.md).

## Excel

[Excel reference](xls.md).

## Hledger journal

[Hledger journal reference](hledger.md).
