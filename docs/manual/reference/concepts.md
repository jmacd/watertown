---
title: Shared concepts
doc_id: reference:concepts
---

# Shared concepts

## Pond and host roots

`POND` or global `--pond` selects the local transactional storage directory,
not a path inside its virtual filesystem. `pond init --birthplace LABEL`
creates a new identity; the label is permanent provenance.
Pond `/sensors/readings` paths are not host paths.

`host+` selects host input without requiring a pond. Global `-d DIRECTORY`
roots host URL paths at that directory; `host+csv:///readings.csv` then reads
`DIRECTORY/readings.csv`. Host reads do not imply arbitrary factories support
host execution or can mutate a pond. A host sitegen build explicitly writes
the chosen output directory.

## URL layers and entry types

URLs have empty authority: `scheme:///path`, not `scheme://hostname/path`.
`file` and `data` address raw data; `table` and `series` select queryable
entry types backed by Parquet/query providers. Entry-type casting does not
magically parse a raw CSV: select its format, e.g.
`host+csv+series:///readings.csv`. A physical series has versions read as a
logical sequence; a dynamic table/series computes a view.

Formats compose with `host+` and compression suffixes `+gzip`, `+zstd`,
`+bzip2`, e.g. `host+csv+gzip:///readings.csv.gz`. Compression is explicit,
not selected by extension. Query parameters belong to the format provider.
Quote `?`, `&`, and glob-containing URLs in the shell.

`host+FACTORY:///config.yaml` is the factory-hostmount interface. The YAML
config is a host input; it is not a storage remote. Global `--hostmount
/mount=host+FACTORY:///config.yaml` mounts supported dynamic factory output
for host queries. Old `host+remote` examples are removed interfaces.

## Patterns and effective roots

Patterns select virtual paths, with `*` captures and recursive `**` where
supported. Quotes keep the shell from expanding them first. `$0` is the first
capture in factories/sitegen, not a shell positional parameter; retain quotes
around authored YAML replacement strings. Imported mounts use their
effective root so absolute source paths remain within the imported tree.
Exec inputs/outputs use exact paths or declared directory prefixes, **not**
this pattern language.

## Substitution and credentials

Factory configs can reference `${env:NAME}` and `${env:NAME:-default}`.
Validation expands a copy; persisted raw config retains references, and
materialization resolves against the current process environment. Missing
required values fail. Do not assume YAML strings stand in for every typed
scalar without the factory's expansion path.

Storage profile credentials require raw environment references. Attachment
configuration is a separate typed surface: its URL may be expanded when
applied, while credential references resolve per replica at use time.
Do not persist literal secrets, put them in command history, or expose them
through generated output. Email report `_env` fields are names, not templates.

## Time, ordering, and evidence

Pond mutations are transactional. Physical versions, logical series content,
control-table lifecycle history, and replicated content commits are different
views of that history. A logical series can concatenate multiple versions;
maintenance can repack physical storage without rewriting its logical rows.
Do not use local transaction sequence numbers or cross-replica `fsck` roots
as universal content identities. Use the replication content tip and verify
each local structure independently.

Prefer explicit RFC 3339 UTC timestamps. Duration strings use each component's
documented grammar (usually humantime, such as `1h30m`); rate limits have their
own amount/period grammar. Millisecond, microsecond, and nanosecond Arrow
timestamps are distinct. Inclusive query filters are not the same as
half-open aggregation buckets or version watermark comparisons.

`offline-e2e` means asserted artifacts/results with no live service;
`local-service-e2e` requires a local service; `live-service-e2e` contacts the
declared service. `validation-only` proves parsing/schema/semantic checks,
not execution. `unverified` makes no runnable-evidence claim. Missing
credentials and skips never become successful coverage. Generated fields
describe deserialization, not all semantic acceptance or operational safety.
