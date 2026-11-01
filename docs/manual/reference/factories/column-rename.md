---
title: Column rename transform
doc_id: factory:column-rename
---

# Column rename transform

## Overview

`column-rename` registers a table transform used by another factory. It changes a queryable view's names and types, not stored source data.

## Example configuration

{{ source path="/docs/examples/column-rename/default.yaml" language="yaml" /}}

## Use and behavior

Install the transform with `pond mknod column-rename /etc/rename --config-path rename.yaml`, then reference `/etc/rename` in a consuming factory's `transforms` list.

Rules apply first-match-wins. Direct rules can request a cast. Invalid types or regular expressions and conflicting output schemas fail. Inspect the consuming node's schema to observe the result.

<!-- docs:reference factory:column-rename -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `rules` | array of Rename Rule | yes | not specified | List of rename rules applied in order |

### Rename Rule

A single column rename rule

Choose one of the following:

#### Direct column name mapping

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `cast` | string &#124; null | no | not specified |  |
| `from` | string | yes | not specified |  |
| `to` | string | yes | not specified |  |
| `type` | "direct" | yes | not specified |  |

#### Regex pattern-based renaming

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `pattern` | string | yes | not specified |  |
| `replacement` | string | yes | not specified |  |
| `type` | "pattern" | yes | not specified |  |
<!-- docs:end-reference -->
