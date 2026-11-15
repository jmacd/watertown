---
title: Systemd journal ingestion
doc_id: factory:journal-ingest
---

# Systemd journal ingestion

## Overview

`journal-ingest` collects JSON Lines from the systemd journal into per-unit streams. It requires Linux, `journalctl`, and permission to read the selected journals.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

For example, `extra_args: [--merge]` includes journals from other namespaces
available to the host command.

## Use and behavior

Install with `pond mknod journal-ingest /etc/journal --config-path journal.yaml`, then run `pond run /etc/journal -- push`. `status` reports the persisted cursor and per-unit counts; `pull` is a no-op.

The cursor is stored beneath the output path as `.journal-cursor`. Removing it changes incremental behavior. Default timestamps are journald microseconds. Query collected files using `jsonlogs`, explicitly casting timestamp and numeric fields as needed.

<!-- docs:reference factory:journal-ingest -->
<!-- docs:end-reference -->
