---
title: Exec factory
doc_id: factory:exec
---

# Exec factory

`exec` stages declared pond inputs, runs an explicitly named host program in
a Linux Bubblewrap sandbox, and commits approved outputs on clean completion.
It requires a real writable pond, Linux user-namespace support, `bwrap`, and
the program/runtime. It is not supported by host-only sitegen mode or macOS.

```yaml
program: /usr/bin/sha256sum
args: [data/input.txt]
inputs: [/data/input.txt]
outputs: []
```

This prerequisite-dependent sample prints a checksum; it does not persist
stdout. Install with `pond mknod exec /etc/checksum --config-path exec.yaml`,
then `pond run /etc/checksum`. Inspect inputs/committed outputs with `cat`;
update configuration with `mknod ... --overwrite`.

<!-- docs:reference factory:exec -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | no |
| initialize | yes |
| execute | yes |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `ReadWrite`.

### ExecConfig

Configuration for one exec factory mount.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `args` | array of string | no | [] | Literal argument vector passed to the program. Never shell-interpreted; if shell semantics are wanted, spell them out explicitly, e.g. program: /bin/bash, args: ["-c", "..."]. |
| `env` | map of string | no | {} | Explicit environment variables set inside the sandbox. Empty by default -- nothing is inherited from the pond process environment. |
| `host_inputs` | array of HostInput | no | [] | Read-only paths from the explicit pond -d hostmount root. The exec run fails unless the operator supplied -d; pond-stored configuration can never select an ambient host filesystem root by itself. |
| `inputs` | array of string | yes | not specified | Exact pond paths staged read-only into the sandbox before exec. |
| `interactive` | boolean | no | false | Run the program without bwrap's --new-session isolation, so a real interactive program (a REPL, hledger add, etc.) keeps normal terminal job control (Ctrl-C, Ctrl-Z) instead of losing it to a detached session. This trades away --new-session's defense against the sandboxed program injecting fake keystrokes back at the controlling terminal (TIOCSTI) -- acceptable only because running a program interactively is already an explicit, attended choice by the operator. See docs/exec-factory-design.md's interactive sessions section. |
| `network` | boolean | no | false | Allow network access inside the sandbox. false unless a concrete use case needs it; bookkeeping tools are local-file-only. |
| `outputs` | array of string | no | [] | Pond paths (exact files) or directory prefixes (trailing /) that may be created or modified by the program. Anything written outside these paths in the staging directory is discarded, not committed. Deletions of a path that matched outputs before exec are always treated as an error (see design doc, "Deletions, failure, and commit semantics"). |
| `program` | string | yes | not specified | Absolute path to the program to execute. Never searched on $PATH: the config is pond-versioned, so the exact binary invoked should be an explicit, auditable value. |
| `series_outputs` | array of string | no | [] | Exact pond paths (no directory prefixes) treated as **append-only** tinyfs::EntryType::FilePhysicalSeries files: the program sees the path's full prior content staged read-write (series versions are concatenated on read), and on clean exit only the *new* suffix bytes -- not the whole file -- are committed as the series' next version. A natural fit for append-only formats like Ledger journals. If the program leaves behind anything that isn't a byte-for-byte extension of what it started with (edited, truncated, or deleted), the whole run is rejected and the pond is left unchanged, same as any other output error. |
| `timeout_seconds` | integer &#124; null | no | null | Kill the program if it hasn't exited within this many seconds. None resolves via [ExecConfig::effective_timeout]: 60s for ordinary runs, no timeout at all for interactive: true runs. Some(0) always means "no timeout", for either kind of run. Constraints: format="uint64"; minimum=0. |

Unknown fields: **rejected**.

### HostInput

A read-only path projected from the operator-selected hostmount root into the external program's bubblewrap sandbox.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `source` | string | yes | not specified | Absolute path within the pond -d hostmount namespace. |
| `target` | string | yes | not specified | Absolute path at which the source is visible inside the sandbox. |

Unknown fields: **rejected**.
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
