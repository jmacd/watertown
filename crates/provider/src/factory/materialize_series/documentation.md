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
consistency check before adoption. Cache loss, changed recipes, and unproven
change locality also require full verification. These non-incremental checks
are logged and counted rather than silently treated as bounded work.

A removed or replaced source version, for example one repacked under a new
identity or renamed out of a source pattern by log rotation, is never treated
as an append. For a timestamp-local source with recorded event-time bounds, the
check starts at the earliest affected time rather than covering all history;
a version without bounds still requires full verification. If rows the target
already holds are gone, the run is rejected, and the error names each removed
version with its event-time range. An already-inconsistent target fails the
check; deleting the checkpoint does not bypass verification.

### Recovering from a rejection

A rejection repeats on every run until an operator acts. Its usual causes are a
source that was re-ingested or corrected after its rows were materialized, a
source version that left the source (for example a rotated log file whose
archived name no longer matches the source pattern), or a row added exactly at
the target watermark. Pond has no command that removes a
materialized target, and this component never rebuilds one implicitly, so
recovery is an explicit retarget:

1. Decide which history is correct. If the source change was a mistake, restore
   the source instead; the next run then passes verification.
2. Change `target` in the factory config to a new, unused series path, for
   example `/metrics/perf.r2.series`.
3. Re-apply the config, either with
   `pond mknod materialize-series <factory-node> --config-path <config> --overwrite`
   or through the `pond apply` document that declares the node.
4. Repoint every consumer of the old target, such as a `temporal-reduce`
   `in_pattern`, to the new path.
5. Run the factory. With no target watermark, the first run materializes the
   whole source and establishes a new checkpoint. Expect a one-time full source
   read; downstream rollup caches rebuild because their input changed.

The old target is left as inert history; nothing writes to it again.

<!-- docs:reference factory:materialize-series -->
<!-- docs:end-reference -->
