---
title: Series materialization
doc_id: factory:materialize-series
---

# Series materialization

## Overview

`materialize-series` copies new rows from a queryable source into a physical append-only pond series.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

All three fields are required; this component has no optional configuration fields.

## Use and behavior

Install with `pond mknod materialize-series /etc/materialize --config-path materialize.yaml`, then run `pond run /etc/materialize -- push`. `pull` is accepted for scheduling symmetry but does not reverse data flow.

Each run selects rows beyond the target event-time watermark and appends a version when needed. It is not arbitrary-key deduplication or snapshot replacement and does not repair old rows arriving behind the watermark.

<!-- docs:reference factory:materialize-series -->
<!-- docs:end-reference -->
