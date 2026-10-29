---
title: Synthetic timeseries
doc_id: factory:synthetic-timeseries
---

# Synthetic timeseries

`synthetic-timeseries` creates a read-only, SQL-queryable dynamic table
(`TableDynamic`). Values are computed lazily in batches, not collected from a
sensor or stored as an append-only physical series. It is a dynamic file
factory, not a `pond run` executable or automatic collector.

## Example

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

## Configuration

| Field | Type / default | Meaning and constraints |
|---|---|---|
| `start` | required string | RFC 3339 timestamp, inclusive |
| `end` | required string | RFC 3339 timestamp, after `start`; included if reached by the interval |
| `interval` | required duration string | Positive sampling interval, e.g. `1h`; timestamp resolution is milliseconds |
| `time_column` | string, `timestamp` | Output timestamp column name |
| `points` | required nonempty list | Named output Float64 columns |
| `points[].name` | required nonempty string | Unique among point names; choose a name distinct from `time_column` |
| `points[].components` | required nonempty list | Components summed at each timestamp |

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
