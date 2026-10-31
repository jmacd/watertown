---
title: Series materialization
doc_id: factory:materialize-series
---

# Series materialization

## Overview

`materialize-series` copies new rows from a queryable source into a physical append-only pond series.

## Example configuration

```yaml
source: series:///derived/temperature
target: /stored/temperature
time_column: timestamp
```

## Use and behavior

Install with `pond mknod materialize-series /etc/materialize --config-path materialize.yaml`, then run `pond run /etc/materialize -- push`. `pull` is accepted for scheduling symmetry but does not reverse data flow.

Each run selects rows beyond the target event-time watermark and appends a version when needed. It is not arbitrary-key deduplication or snapshot replacement and does not repair old rows arriving behind the watermark.

<!-- docs:reference factory:materialize-series -->
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

### MaterializeSeriesConfig

Configuration for the materialize-series factory.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `source` | WatertownUrl | yes | not specified | URL of the signal to materialize, e.g. series:///derived/p-water-prod.  Anything the provider can turn into a table works, so a derived node, a physical series, or a raw log read through a format scheme are all valid sources. |
| `target` | string | yes | not specified | Pond path of the TablePhysicalSeries to append to.  Created on the first run. |
| `time_column` | string | yes | not specified | Event-time column, present in the source and used both as the watermark and as the series' temporal index. |

Unknown fields: **rejected**.

### WatertownUrl

Accepted value: string.
<!-- docs:end-reference -->

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
