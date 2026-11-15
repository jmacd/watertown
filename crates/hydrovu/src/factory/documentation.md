---
title: HydroVu ingestion
doc_id: factory:hydrovu
---

# HydroVu ingestion

## Overview

`hydrovu` collects per-device physical series from the HydroVu API. Source API metadata determines parameter and unit column names.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod hydrovu /etc/hydrovu --config-path hydrovu.yaml`, then run `pond run /etc/hydrovu -- collect`. Incremental collection resumes from existing temporal data. Inactive devices are skipped.

An optional `archive_path` supplies read-only seed snapshots only when the writable device has no temporal data; it is not an output. OAuth credentials, API access, and real device IDs are live prerequisites.

<!-- docs:reference factory:hydrovu -->
<!-- docs:end-reference -->
