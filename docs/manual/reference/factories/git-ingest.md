---
title: Git ingestion
doc_id: factory:git-ingest
---

# Git ingestion

## Overview

`git-ingest` exposes a dynamic directory backed by a tracked Git tree. Files remain backed by the repository cache rather than being physically copied into pond storage.

## Example configuration

{{ source path="/docs/examples/git-ingest/default.yaml" language="yaml" /}}

Set `prefix: docs` to expose only that subtree, stripping `docs/` from paths.

## Use and behavior

Install with `pond mknod git-ingest /etc/git --config-path git.yaml`, then run `pond run /etc/git -- pull`. `status` reports synchronization state; `push` is a no-op, not an upstream Git push. Reads can lazily initialize the bare repository cache.

`prefix` is optional and must not begin or end with `/` or contain `..`. The external key is `ref`, never `git_ref`. Network or SSH access and credentials are operator prerequisites.

<!-- docs:reference factory:git-ingest -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `prefix` | string &#124; null | no | null | Optional path prefix filter.  When set, only files under this directory in the git tree are synced, with the prefix stripped from their paths. |
| `ref` | string | yes | not specified | Git ref to track (branch name, tag, or SHA) |
| `url` | string | yes | not specified | Git remote URL (HTTPS or SSH) |
<!-- docs:end-reference -->
