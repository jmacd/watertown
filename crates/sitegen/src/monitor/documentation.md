---
title: Monitor report
doc_id: factory:monitor-report
---

# Monitor report

## Overview

`monitor-report` evaluates configured checks against a selected pond and writes static report artifacts. Supported checks cover observed values, staleness, ranges, graded scores, and conditional rates.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

This is a full configuration for the `stale-after` check variant, not a
collection of mutually incompatible check options. The reference below
lists every other supported check shape and its required fields.

## Use and behavior

Install with `pond mknod monitor-report /etc/monitor --config-path monitor.yaml`, then run `pond run /etc/monitor -- push`. Replace the example paths and check with sources and criteria meaningful for your pond.

Nonempty checks require real sources and semantic validation. Missing or stale data follows each check's contract and must not be interpreted as a healthy zero. Rendered reports are snapshots, not proof of collector health or alert delivery.

<!-- docs:reference factory:monitor-report -->
<!-- docs:end-reference -->
