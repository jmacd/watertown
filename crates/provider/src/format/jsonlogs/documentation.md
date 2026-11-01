---
title: JSON logs
doc_id: format:jsonlogs
---

# JSON logs

`jsonlogs` exposes newline-delimited JSON objects as text-valued table rows.
It is useful when log fields vary between records and you want to cast and
select fields explicitly in SQL.

## URL example

```bash
# Replace the example host path with a JSON Lines log.
# No provider-specific options are required or supported.
pond cat 'host+jsonlogs:///tmp/events.jsonl' --format=table
```

Use `jsonlogs:///logs/events.jsonl` for pond input. For gzip input, use
`host+jsonlogs+gzip:///tmp/events.jsonl.gz`. Quote URLs and use `-d DIRECTORY`
when selecting an explicit host root; see [shared URL rules](../concepts.md).

## Full reference

Unioned object keys become nullable Utf8 columns, including numeric and
timestamp fields. Strings are unquoted, objects/arrays become compact JSON
text, and missing/null values are Arrow nulls. Cast values explicitly in SQL
before numeric or time analysis.

There is no typed provider-specific option object or YAML configuration.
Provider query options are ignored. The `+gzip`, `+zstd`, and `+bzip2` reader
layers are explicit; compression is not inferred from a filename extension.

## Behavior and limitations

The whole parsed input is buffered, not streamed in bounded memory.
Blank/NUL handling and malformed/non-object lines are parser-specific;
unparseable/non-object lines are skipped with a counted warning. Unit tests
cover parser cases; the URL example is not a runnable documentation fixture.
