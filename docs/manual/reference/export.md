---
title: Export
doc_id: reference:export
---

# Export

Export writes queryable pond data as external Parquet files. It does not create
a replica, backup attachment, capsule, or restoreable copy of all pond metadata.
Keep authoritative source data and replication/recovery procedures separately.

`pond export --pattern '/sensors/*' --dir OUTPUT` accepts repeated patterns;
quote globs to prevent shell expansion. Inputs must be queryable tables/series,
not arbitrary raw file bytes. A format URL supplies the decoder for raw input.
An output directory is a host side effect, not an in-pond path.

## CLI and sitegen are separate contracts

The [CLI reference](cli/commands.md#export) describes `--temporal`,
`--start-time`, `--end-time`, and `--rebuild`. CLI temporal levels are
comma-separated year/month/day/hour/minute names. CLI time filtering uses
the `timestamp` column with **inclusive** start/end comparisons; do not assume
a configurable timestamp column just because sitegen has one.

[Sitegen export stages](sitegen/configuration.md#exports) instead specify
`name`, `pattern`, `target_points`, and `timestamp_column`. They discover
resolutions and choose Hive-style partition levels so a viewport needs few
files. `target_points` is sizing guidance, not sampling/aggregation itself.
Template routes use the export's wildcard captures and file manifest.

## Time, partitions, and schemas

Parquet contains typed Arrow data. Time partition directory labels describe
the partition interval, not a promise that both endpoint samples exist.
Different sources can have different timestamp units/timezones; inspect
schema and exported values. Filter boundaries and aggregation bucket boundaries
are separate concepts. Do not infer the result from filename count alone.

The [timeseries site package](../guides/timeseries-site.md) asserts page URLs,
the referenced Parquet manifest, and exported source row counts/values.
Failures can leave partial output; publish a fresh completed directory.
`--rebuild` drops derived reduction cache state for recomputation; it is not
a data recovery or source mutation command.
