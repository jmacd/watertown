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

Each run selects rows beyond the target event-time watermark and appends a version when needed. It is not arbitrary-key deduplication or snapshot replacement.

Before filtering out historical rows, the materializer checks source changes.
Changes that alter already-materialized output, including an additional row or
group contribution exactly at the watermark, fail explicitly without publishing
target rows or advancing progress. The error requires explicit reconciliation
or rebuilding the target; this append-only component does not repair history.

A shared-cache checkpoint records the source snapshot and exact target identity.
It is an optimization, not authoritative progress. Identical snapshots require
no source execution. Proven timestamp-local appends can use bounded reads;
potential late changes are checked against the affected historical prefix,
preserving duplicate multiplicity.

An existing target without a matching checkpoint must pass a full historical
consistency check before adoption. Cache loss, changed recipes, removed or
replaced source versions, and unproven change locality also require full
verification. These non-incremental checks are logged and counted rather than
silently treated as bounded work. An already-inconsistent target fails the
check; deleting the checkpoint does not bypass verification.

<!-- docs:reference factory:materialize-series -->
<!-- docs:end-reference -->
