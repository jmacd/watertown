---
title: Web logs
doc_id: format:weblog
---

# Web logs

`weblog` reads HTTP access logs in Combined or Common Log Format and exposes
typed request fields for SQL analysis.

## URL example

```bash
# Replace the example host path with a CLF/Combined access log.
# No provider-specific options are required or supported.
pond cat 'host+weblog:///var/log/access.log' --format=table
```

Use `weblog:///logs/access.log` for pond input. Compressed input needs an
explicit layer, such as `host+weblog+gzip:///var/log/access.log.gz`.
See [shared URL rules](../concepts.md) for roots and layering.

## Full reference

| Column | Type | Meaning |
|---|---|---|
| `timestamp` | UTC microsecond timestamp | Request time; CLF timestamps include an offset. |
| `remote_addr` | string | Client address. |
| `remote_user` | nullable string | Authenticated user, when present. |
| `method` | string | HTTP method. |
| `path` | string | Requested path. |
| `protocol` | string | HTTP protocol version. |
| `status` | UInt16 | HTTP response code. |
| `body_bytes_sent` | UInt64 | Response body size. |
| `http_referer` | nullable string | Combined-log referrer. |
| `http_user_agent` | nullable string | Combined-log user agent. |

There are no typed provider-specific query options or YAML settings.
Provider query options are ignored. Reader layers are `+gzip`, `+zstd`, and
`+bzip2`; filename extensions do not select compression.

## Behavior and limitations

`-` in nullable text becomes null. Input is buffered into parsed entries.
Malformed lines warn and are skipped, not returned as placeholder rows.
Parser unit tests exist; the example is not a runnable documentation fixture.
