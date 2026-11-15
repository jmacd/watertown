---
title: Synthetic timeseries
doc_id: factory:synthetic-timeseries
---

# Synthetic timeseries

`synthetic-timeseries` creates a read-only, SQL-queryable dynamic table
(`TableDynamic`). Values are computed lazily in batches, not collected from a
sensor or stored as an append-only physical series. It is a dynamic file
factory, not a `pond run` executable or automatic collector.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

This is complete for the `line` variant. A periodic component instead has
`type: sine`, `triangle`, or `square`, a required example `period: 24h`,
and optional `amplitude: 0.0`, `offset: 0.0`, and `phase: 0.0` (all defaults).
Choose a nonzero amplitude to produce a varying periodic signal; phase is
in radians. The full reference below lists each variant separately.

## Runnable example

Prerequisites: Bash and `pond` on `PATH`; no credentials, services, or existing
pond. Run the complete script from any directory. It creates a new scratch
workspace and overrides `POND` locally; it never resets an existing pond.

{{ source path="/testsuite/examples/synthetic-timeseries/config.yaml" language="yaml" /}}

{{ source path="/testsuite/examples/synthetic-timeseries/run.sh" language="bash" /}}

The exact files above are executed by test `100`. It asserts four rows,
inclusive boundaries `00:00:00Z` and `03:00:00Z`, and temperatures
`20`, `23.6`, `27.2`, `30.8`. See the [timeseries guide](../../guides/timeseries.md#inspect).

[Complete configuration](/testsuite/examples/synthetic-timeseries/config.yaml),
[complete script](/testsuite/examples/synthetic-timeseries/run.sh),
[download package](/downloads/synthetic-timeseries.tar).

<!-- docs:reference factory:synthetic-timeseries -->
<!-- docs:end-reference -->

## Semantic constraints

`start` and `end` are RFC 3339 strings; the end is inclusive if reached by
the positive interval. Require nonempty points/components and unique point
names distinct from `time_column`. These constraints exceed JSON shape checks.

Timestamp output is Arrow `Timestamp(Millisecond, UTC)`. Value columns are
non-null Float64. Top-level and point objects reject unknown fields.

### Waveforms

Each component has a required `type`: `line`, `sine`, `triangle`, or `square`.
All numeric fields default to `0`. Wave components require a duration-string
`period`; use a positive duration. `phase` is radians, default `0`.
Waveform variant objects do not currently reject unknown fields.

| Type | Additional fields | Evaluation with `t` in seconds since `start` |
|---|---|---|
| `line` | `offset`, `slope` | `offset + slope * t`; slope is units per second |
| `sine` | `period`, `amplitude`, `offset`, `phase` | `offset + amplitude * sin(2*pi*t/period + phase)` |
| `triangle` | same as sine | Starts at `offset - amplitude` at zero phase; rises then falls each period |
| `square` | same as sine | `offset + amplitude * sign(sin(2*pi*t/period + phase))` |

**Current limitation:** malformed durations warn and fall back to one hour.
Period positivity is not fully validated. Do not rely on that fallback;
configuration schemas and validation corrections are tracked separately from
this pilot.

## Inspect

The script demonstrates `pond mknod`, then `pond cat --sql`. Use
`pond describe /sensors/synthetic` to inspect its schema. Direct byte reads are
unsupported: use SQL and `--format=table`, or export queryable data.

## Update

To replace a configuration in a pond you own, use
`pond mknod synthetic-timeseries /sensors/synthetic --config-path CONFIG --overwrite`.
Subsequent queries regenerate values from that configuration; there is no
ingestion cursor or incremental history to advance. No external API is called.
Host-mounted dynamic factories are a separate execution path; this example
tests explicit pond installation only.
