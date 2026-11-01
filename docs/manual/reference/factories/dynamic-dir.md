---
title: Dynamic directory
doc_id: factory:dynamic-dir
---

# Dynamic directory

## Overview

`dynamic-dir` creates a dynamic directory whose children each use a named factory and independent configuration.

## Example configuration

This directory contains a `temperature` child that generates four hourly
readings. It needs no existing source data or external services.

{{ source path="/docs/examples/dynamic-dir/default.yaml" language="yaml" /}}

## Use and behavior

In an initialized pond, save the configuration as `directory.yaml`, then run:

```bash
pond mknod dynamic-dir /demo --config-path directory.yaml
pond list '/demo/*'
pond cat /demo/temperature --format=table
```

The directory lists `temperature`; reading that child produces timestamps
from midnight through 03:00 UTC with temperatures `20`, `23.6`, `27.2`, and
`30.8`. The child is generated on demand, not copied from a source file.

Each entry needs a `name`, a `factory`, and that factory's `config`. The example
uses [synthetic-timeseries](synthetic-timeseries.md), but another child can use
a different factory with its own settings. The selected factory validates its
own settings. Directory enumeration does not execute every child factory.

Child identities derive from the parent identity, entry name, factory, and
configuration.

<!-- docs:reference factory:dynamic-dir -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `entries` | array of Dynamic Dir Entry | yes | not specified | List of directory entries to create |

### Dynamic Dir Entry

Configuration for a single directory entry

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `config` | any JSON value | yes | not specified | Configuration for the specific factory (as raw Value for flexibility) |
| `factory` | string | yes | not specified | Factory type to use for creating this entry |
| `name` | string | yes | not specified | Name of the entry in the directory |
<!-- docs:end-reference -->
