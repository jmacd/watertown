---
title: HydroVu ingestion
doc_id: factory:hydrovu
---

# HydroVu ingestion

## Overview

`hydrovu` collects per-device physical series from the HydroVu API. Source API metadata determines parameter and unit column names.

## Example configuration

{{ source path="/docs/examples/hydrovu/default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod hydrovu /etc/hydrovu --config-path hydrovu.yaml`, then run `pond run /etc/hydrovu -- collect`. Incremental collection resumes from existing temporal data. Inactive devices are skipped.

An optional `archive_path` supplies read-only seed snapshots only when the writable device has no temporal data; it is not an output. OAuth credentials, API access, and real device IDs are live prerequisites.

<!-- docs:reference factory:hydrovu -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `archive_path` | string &#124; null | no | null | Optional path within pond holding read-only seed archives (e.g. "/hydrovu-archive"), typically git-ingested FileDynamic Parquet .series snapshots laid out as {archive_path}/devices/{id}/*.series. Consulted by find_youngest_timestamp ONLY when the writeable device directory has no temporal data (a freshly reset pond), so live collection resumes from the archive's youngest timestamp instead of crawling from epoch.  These archives are NOT written to. |
| `client_id` | string | yes | not specified | OAuth client ID (sensitive - redacted from debug output) |
| `client_secret` | string | yes | not specified | OAuth client secret (sensitive - redacted from debug output) |
| `devices` | array of Hydro Vu Device | yes | not specified | Device list |
| `hydrovu_path` | string | yes | not specified | Path within pond for hydrovu data (e.g., "/hydrovu") |
| `max_points_per_run` | integer | yes | not specified | Constraints: format="uint"; minimum=0. |

### Hydro Vu Device

Device configuration specifying which HydroVu location to collect

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `active` | boolean | no | true | When false, device is skipped during collect. Default: true. |
| `comment` | string &#124; null | no | not specified |  |
| `id` | integer | yes | not specified | Constraints: format="int64". |
| `name` | string | yes | not specified |  |
<!-- docs:end-reference -->
