---
title: Column rename transform
doc_id: factory:column-rename
---

# Column rename transform

## Overview

`column-rename` registers a table transform used by another factory. It changes a queryable view's names and types, not stored source data.

## Example configuration

```yaml
rules:
  - type: direct
    from: temperature
    to: temperature_c
  - type: pattern
    pattern: "^sensor_(.*)$"
    replacement: "$1"
```

## Use and behavior

Install the transform with `pond mknod column-rename /etc/rename --config-path rename.yaml`, then reference `/etc/rename` in a consuming factory's `transforms` list.

Rules apply first-match-wins. Direct rules can request a cast. Invalid types or regular expressions and conflicting output schemas fail. Inspect the consuming node's schema to observe the result.

<!-- docs:reference factory:column-rename -->
## Full configuration

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | no |
| initialize | no |
| execute | no |
| table transform | yes |
| raw validation | no |

Automatic post-commit access: `not registered`.

### ColumnRenameConfig

Configuration for column rename transformation

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `rules` | array of RenameRule | yes | not specified | List of rename rules applied in order |

Unknown fields: **rejected**.

### RenameRule

A single column rename rule

### RenameRule variant 1

Direct column name mapping  Example: yaml - type: direct   from: "DateTime"   to: "timestamp"   cast: "timestamp"

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `cast` | string &#124; null | no | not specified |  |
| `from` | string | yes | not specified |  |
| `to` | string | yes | not specified |  |
| `type` | "direct" | yes | not specified |  |

Unknown fields: **rejected**.

### RenameRule variant 2

Regex pattern-based renaming  Example: yaml - type: pattern   pattern: "^(.+) \\((.+)\\)$"   replacement: "$1.$2"  This transforms "Temperature (C)" -&gt; "Temperature.C"

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `pattern` | string | yes | not specified |  |
| `replacement` | string | yes | not specified |  |
| `type` | "pattern" | yes | not specified |  |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
