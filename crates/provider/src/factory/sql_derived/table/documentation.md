---
title: SQL-derived table
doc_id: factory:sql-derived-table
---

# SQL-derived table

## Overview

`sql-derived-table` creates a read-time SQL view over single-file table inputs. It changes no source bytes and is queried rather than executed with `pond run`.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

All fields have omission behavior, but an empty `patterns` map does not make
the default query useful: it needs an input named `source`. Set `query` to your
own SQL to select, filter, or calculate columns. For the restricted local
variant, replace `locality` with `{type: timestamp-local, event_time: timestamp}`;
the event-time field is required for that variant.

## Use and behavior

Install with `pond mknod sql-derived-table /derived/readings --config-path view.yaml`. Inspect it with `pond describe /derived/readings` and query it with `pond cat /derived/readings --format=table --sql 'SELECT * FROM source'`.

Table inputs reject multi-file matches. Output schema is the SQL query schema. `global` is the conservative locality default; `timestamp-local` is limited to a single-source immutable projection/filter that preserves its configured event-time column.

<!-- docs:reference factory:sql-derived-table -->
<!-- docs:end-reference -->
