---
title: Exec factory
doc_id: factory:exec
---

# Exec factory

`exec` stages declared pond inputs, runs an explicitly named host program in
a Linux Bubblewrap sandbox, and commits approved outputs on clean completion.
It requires a real writable pond, Linux user-namespace support, `bwrap`, and
the program/runtime. It is not supported by host-only sitegen mode or macOS.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

## Use and behavior

This prerequisite-dependent sample prints a checksum; it does not persist
stdout. Install with `pond mknod exec /etc/checksum --config-path exec.yaml`,
then `pond run /etc/checksum`. Inspect inputs/committed outputs with `cat`;
update configuration with `mknod ... --overwrite`.

For an explicit host input, replace `host_inputs: []` with a list of
`{source: /input.txt, target: /data/input.txt}` mappings and supply `pond -d`.
Both paths are required; `source` is inside the operator-selected host root,
not an arbitrary ambient host path. Set `timeout_seconds: 0` only to explicitly
disable the timeout.

<!-- docs:reference factory:exec -->
<!-- docs:end-reference -->

## Boundaries and commit semantics

`program` must be absolute; arguments are literal, not shell-expanded. Use an
explicit shell only when its semantics are intended. `inputs` are exact pond
paths; `outputs` can be exact files or directory prefixes ending `/`. They
are not globs. Environment is explicitly configured, not inherited wholesale.
Network is disabled by default; enabling it permits external side effects
which a pond rollback cannot undo.

At least one input/output/series-output is required. Ordinary outputs commit
approved changes; writes outside approved output paths are discarded.
Deletion of an existing approved output is an error. `series_outputs` are
exact append-only raw file series: prior content must remain a byte-for-byte
prefix and only the suffix becomes the next version. Editing, truncating,
deleting, or conflicting output declarations rejects the run.

A nonzero exit, timeout, or invalid output leaves pond outputs uncommitted.
The default wall timeout is 60 seconds; interactive runs default to unlimited;
explicit zero is unlimited in either mode. Interactive mode relaxes session
isolation for job control and is an attended operator decision, not a daemon
default. Existing 740/744 tests are Linux execution evidence; this page does
not claim its sample ran on a non-Linux workstation.
