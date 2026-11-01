---
title: Column rename transform
doc_id: factory:column-rename
---

# Column rename transform

## Overview

`column-rename` registers a table transform used by another factory. It changes a queryable view's names and types, not stored source data.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

## Use and behavior

Install the transform with `pond mknod column-rename /etc/rename --config-path rename.yaml`, then reference `/etc/rename` in a consuming factory's `transforms` list.

Rules apply first-match-wins. Direct rules can request a cast. Invalid types or regular expressions and conflicting output schemas fail. Inspect the consuming node's schema to observe the result.

<!-- docs:reference factory:column-rename -->
<!-- docs:end-reference -->
