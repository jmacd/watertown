---
title: HydroVu ingestion
doc_id: factory:hydrovu
---

# HydroVu ingestion

## Overview

`hydrovu` collects per-device physical series from the HydroVu API. Source API metadata determines parameter and unit column names.

## Example configuration

```yaml
client_id: "${env:HYDROVU_CLIENT_ID}"
client_secret: "${env:HYDROVU_CLIENT_SECRET}"
max_points_per_run: 1000
hydrovu_path: /hydrovu
devices:
  - name: example-device
    id: 123
```

## Use and behavior

Install with `pond mknod hydrovu /etc/hydrovu --config-path hydrovu.yaml`, then run `pond run /etc/hydrovu -- collect`. Incremental collection resumes from existing temporal data. Inactive devices are skipped.

An optional `archive_path` supplies read-only seed snapshots only when the writable device has no temporal data; it is not an output. OAuth credentials, API access, and real device IDs are live prerequisites.

<!-- docs:reference factory:hydrovu -->
## Full configuration

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | no |
| initialize | yes |
| execute | yes |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `ReadWrite`.

### HydroVuConfig

Configuration for HydroVu OAuth credentials and device list

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `archive_path` | string &#124; null | no | null | Optional path within pond holding read-only seed archives (e.g. "/hydrovu-archive"), typically git-ingested FileDynamic Parquet .series snapshots laid out as {archive_path}/devices/{id}/*.series. Consulted by find_youngest_timestamp ONLY when the writeable device directory has no temporal data (a freshly reset pond), so live collection resumes from the archive's youngest timestamp instead of crawling from epoch.  These archives are NOT written to. |
| `client_id` | string | yes | not specified | OAuth client ID (sensitive - redacted from debug output) |
| `client_secret` | string | yes | not specified | OAuth client secret (sensitive - redacted from debug output) |
| `devices` | array of HydroVuDevice | yes | not specified | Device list |
| `hydrovu_path` | string | yes | not specified | Path within pond for hydrovu data (e.g., "/hydrovu") |
| `max_points_per_run` | integer | yes | not specified | Constraints: format="uint"; minimum=0. |

Unknown fields: **rejected**.

### HydroVuDevice

Device configuration specifying which HydroVu location to collect

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `active` | boolean | no | true | When false, device is skipped during collect. Default: true. |
| `comment` | string &#124; null | no | not specified |  |
| `id` | integer | yes | not specified | Constraints: format="int64". |
| `name` | string | yes | not specified |  |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
