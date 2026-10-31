---
title: Dynamic directory
doc_id: factory:dynamic-dir
---

# Dynamic directory

## Overview

`dynamic-dir` creates a dynamic directory whose children each use a named factory and independent configuration.

## Example configuration

```yaml
entries:
  - name: readings
    factory: sql-derived-series
    config:
      patterns:
        source: series:///sensors/*
```

## Use and behavior

Install with `pond mknod dynamic-dir /derived --config-path directory.yaml`, list `/derived`, then describe or query a child. Child identities derive from the parent identity, entry name, factory, and configuration.

The nested `config` value is intentionally open JSON; the selected child factory validates it. Directory enumeration does not execute every child factory.

<!-- docs:reference factory:dynamic-dir -->
## Full configuration

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | yes |
| file | no |
| initialize | no |
| execute | no |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `not registered`.

### DynamicDirConfig

Configuration for the dynamic directory

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `entries` | array of DynamicDirEntry | yes | not specified | List of directory entries to create |

Unknown fields: **rejected**.

### DynamicDirEntry

Configuration for a single directory entry

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `config` | any JSON value | yes | not specified | Configuration for the specific factory (as raw Value for flexibility) |
| `factory` | string | yes | not specified | Factory type to use for creating this entry |
| `name` | string | yes | not specified | Name of the entry in the directory |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
