---
title: Timeseries
doc_id: guide:timeseries
---

# Create and inspect timeseries

This offline workflow starts with a host-file query and creates a deterministic
dynamic table in a new pond. Prerequisites: Bash and `pond` on `PATH`.
Keep original sensor/source data independently recoverable.

## Query a host file

Run the [complete CSV script](../reference/formats/csv.md#example). It requires
no pond and selects the two readings at or above 25 degrees.

{{ source path="/testsuite/examples/csv-query/run.sh" region="query" language="bash" /}}

This is a region, not a standalone script: the complete script initializes
`SOURCE_DIR` to its fixture directory and `WORK` to a new scratch directory.
The [CSV options](../reference/formats/csv.md#url-parameters) explain the
semicolon parameter example.

## Creating a timeseries

Run the complete [synthetic example](../reference/factories/synthetic-timeseries.md#example)
to initialize a new scratch pond and install its dynamic table.
The input is fixed at four hourly samples on January 1, 2024.

{{ source path="/testsuite/examples/synthetic-timeseries/run.sh" region="create-and-query" language="bash" /}}

Setup is required: the complete script sets `POND` inside a newly created
workspace and defines `SOURCE_DIR` and `WORK`. Never point this setup at a pond
containing data you need.

## Inspect

The numbered test asserts exactly four rows from `2024-01-01T00:00:00Z` through
`2024-01-01T03:00:00Z`. The line component has offset `20` and slope `0.001`
per second, so the ordered temperatures are `20`, `23.6`, `27.2`, and `30.8`.

### Inspect

Repeated heading names have unique anchors. This
[second inspect section](#inspect-1) and the
[factory inspection section](../reference/factories/synthetic-timeseries.md#inspect)
resolve independently.

The result is a computed table, not a durable append-only series. Export,
timeseries-to-site, and blog workflows follow this pilot and are not yet
migrated into this manual.
