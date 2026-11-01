---
title: OpenTelemetry JSON
doc_id: format:oteljson
---

# OpenTelemetry JSON

`oteljson` turns newline-delimited OTLP metrics into a wide queryable table.
It reads gauge points, not arbitrary JSON, OTLP logs, or traces.

## URL example

```bash
# Replace the example host path with an OTLP metrics JSON Lines file.
# No provider-specific options are required or supported.
pond cat 'host+oteljson:///tmp/metrics.jsonl' --format=table
```

For pond input use `oteljson:///metrics.jsonl`. For compressed input, select
the decoder explicitly, for example `host+oteljson+gzip:///tmp/metrics.jsonl.gz`.
Quote URLs; see the [shared URL rules](../concepts.md) for roots and layering.

## Full reference

Each line must be an OTLP `ExportMetricsServiceRequest`. Only gauge points
are extracted; attributes and other metric kinds are not retained. Output is
`timestamp` (nanoseconds, no Arrow timezone) plus nullable Float64 metric-name
columns. Points sharing a timestamp form a wide row; repeated metric/timestamp
values overwrite in encounter order.

There is no typed provider-specific option object or YAML configuration.
Provider query options are ignored. Supported reader layers are `+gzip`,
`+zstd`, and `+bzip2`; filename extensions do not select a decoder.

## Behavior and limitations

The implementation buffers observations and schema before emitting a batch.
Corrupt lines are skipped with a warning; a completed query does not certify
that every source record was accepted. Unit tests cover parser cases, but the
example above is usage guidance, not a runnable documentation fixture.
