---
title: Git ingestion
doc_id: factory:git-ingest
---

# Git ingestion

## Overview

`git-ingest` exposes a dynamic directory backed by a tracked Git tree. Files remain backed by the repository cache rather than being physically copied into pond storage.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

Set `prefix: docs` to expose only that subtree, stripping `docs/` from paths.

## Use and behavior

Install with `pond mknod git-ingest /etc/git --config-path git.yaml`, then run `pond run /etc/git -- pull`. `status` reports synchronization state; `push` is a no-op, not an upstream Git push. Reads can lazily initialize the bare repository cache.

`prefix` is optional and must not begin or end with `/` or contain `..`. The external key is `ref`, never `git_ref`. Network or SSH access and credentials are operator prerequisites.

<!-- docs:reference factory:git-ingest -->
<!-- docs:end-reference -->
