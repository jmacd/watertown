---
title: Dynamic directory
doc_id: factory:dynamic-dir
---

# Dynamic directory

## Overview

`dynamic-dir` creates a dynamic directory whose children each use a named factory and independent configuration.

## Example configuration

{{ source path="/docs/examples/dynamic-dir/default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod dynamic-dir /derived --config-path directory.yaml`, list `/derived`, then describe or query a child. Child identities derive from the parent identity, entry name, factory, and configuration.

The nested `config` value is intentionally open JSON; the selected child factory validates it. Directory enumeration does not execute every child factory.

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
