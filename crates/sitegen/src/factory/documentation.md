---
title: Site generation
doc_id: factory:sitegen
---

# Site generation

## Overview

`sitegen` builds a static directory from Markdown, routes, exports, reports, and local assets. It is executable and does not create a queryable data node.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

This includes every top-level field and every field of the selected static
route. Empty collections and null values mean those optional features are
disabled; they are not placeholders for required values. The home route is
an intentional override of the empty default. Create `/site/index.md` in the
selected input root before building. The full reference below describes the
nested configuration when enabling exports, reports, feeds, or other features.

## Use and behavior

Install with `pond mknod sitegen /etc/site --config-path site.yaml`, then run `pond run /etc/site -- build OUTPUT --strict`. The same YAML can run without a pond using `pond run -d SOURCE host+sitegen:///site.yaml -- build OUTPUT --strict`.

Paths are rooted in the selected input filesystem, not the YAML file's directory. See the [configuration topics](../sitegen/configuration.md), [blog guide](../../guides/blog.md), and [timeseries-site guide](../../guides/timeseries-site.md).

<!-- docs:reference factory:sitegen -->
<!-- docs:end-reference -->
