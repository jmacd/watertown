# Exec Factory: Running External Programs Against Pond Files

> **Status:** Design proposal (unimplemented). Scoped to Linux only for v1
> (Watershop + Azure, per `README.operations.md`); macOS is dev-only and is
> validated through Docker, never treated as a security boundary.

---

## 0. Problem statement

`crates/billing` is a hand-written, from-scratch double-entry bookkeeping
system -- an "executable factory" in the existing sense
(`crates/provider/src/registry.rs`, `register_executable_factory!`): ~30
clap subcommands, its own journal/ledger tables, its own invariant checker
(`billing/src/cli/verify.rs`, 11 checks). It works, but it is bespoke
accounting software that we now have to maintain forever: schema
migrations, policy edge cases, audit correctness -- all of it ours to own.

The plaintext-accounting ecosystem (`ledger`, `hledger`, `beancount`, and
others) already solves double-entry bookkeeping, with mature CLIs, reports,
and a shared file format (the Ledger journal format) that several programs
can read and write. What Watertown contributes that those tools don't have
on their own is: transactional commits, content-addressed versioning,
automatic remote backup, and DataFusion queryability over the result. The
gap is that these tools are **ordinary programs that do raw file I/O against
a real filesystem** -- `open()`, `read()`, `write()`, `rename()` -- not
TinyFS API calls. Every existing factory (`billing`, `sitegen`, `hydrovu`,
`materialize-series`, ...) is Rust code linked into the `pond` binary and
compiled against `FactoryContext`/`WD`. None of them can host an arbitrary
external program.

This document proposes **`exec`**: a factory that materializes a declared
set of pond paths into a real directory, runs an external program against
that directory inside a sandbox, and -- only if the program exits cleanly --
diffs the result back into the pond as one transaction. The first consumer
is intentionally trivial (`cat`, a small `bash` script) to validate the
plumbing before pointing it at `hledger` or teaching `cmd/billing` (the Go
PDF generator, `cmd/billing/main.go`) to read Ledger-format journals instead
of its four CSV files.

---

## 1. Where this fits the existing factory system

`crates/provider/src/registry.rs` already defines the extension point this
needs. A `DynamicFactory` (registered via `register_executable_factory!`,
e.g. `crates/provider/src/factory/materialize_series.rs:316-321` or
`crates/billing/src/factory.rs`) provides:

```rust
pub execute: Option<
    fn(config: Value, context: FactoryContext, ctx: ExecutionContext)
        -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send>>,
>,
```

`execute()` already runs **inside an open write transaction**
(`crates/cmd/src/commands/run.rs: run_pond_command_impl`) with a
`FactoryContext` that can resolve pond paths via `context.root().await?`
(`crates/tinyfs/src/context.rs:441-540`). Every existing factory does its
work by calling TinyFS methods directly on that `WD`.

**`exec` needs no new `ExecutionMode` and no registry changes.** It is just
another executable factory whose `execute()` body happens to (a) copy bytes
out to a real directory, (b) run a subprocess, (c) copy bytes back in via
the same `WD::write_file_path_from_slice` / `create_file_path_streaming`
calls (`crates/tinyfs/src/wd.rs:260-269,1387`) that every other factory
already uses. The novelty is entirely in steps (a)-(b); the commit path is
unremarkable.

Concretely, in `crates/exec-factory` (new crate, wired into `crates/cmd`
exactly like `billing`/`hydrovu`/`sitegen` are today -- see root
`Cargo.toml:9-14,34-40` and `crates/cmd/Cargo.toml:83,97-98,110`; linkme's
`DYNAMIC_FACTORIES` distributed slice only sees factories from crates that
are actually linked into the `pond` binary):

```rust
async fn execute(config: Value, context: FactoryContext, _ctx: ExecutionContext)
    -> Result<(), tinyfs::Error>
{
    let cfg: ExecConfig = serde_json::from_value(config)?;
    let root = context.root().await?;

    let staging = tempfile::tempdir()?;
    stage_inputs(&root, &cfg.inputs, staging.path()).await?;
    let before = snapshot_outputs(staging.path(), &cfg.outputs)?;   // pre-exec hashes

    let status = run_sandboxed(&cfg, staging.path()).await?;
    if !status.success() {
        // staging dir is dropped on scope exit; nothing was ever written
        // back into the pond, so there is nothing to abort beyond the
        // already-open (unmodified) transaction.
        return Err(tinyfs::Error::Other(format!(
            "exec factory '{}': program exited with {status}", cfg.program
        )));
    }

    let changes = diff_outputs(staging.path(), &cfg.outputs, &before)?;  // new/modified only
    for change in changes {
        root.write_file_path_from_slice(&change.pond_path, &change.bytes).await?;
    }
    Ok(())
}

register_executable_factory!(
    name: "exec",
    description: "Run an external program against materialized pond files, commit its output",
    validate: validate_exec_config,
    initialize: |_config, _context| async move { Ok(()) },
    execute: execute
);
```

---

## 2. Config schema

```yaml
version: v1
kind: mknod
metadata:
  path: /system/etc/ledger-smoke-test
spec:
  factory: exec
  config:
    program: /bin/cat                   # absolute path, never PATH-searched
    args: ["journal.ledger"]
    inputs:
      - /data/billing/journal.ledger     # pond path -> staged read-only
    outputs:
      - /data/billing/reports/**         # pond paths this run may create/modify
    on_missing_output: error             # error | ignore (deletions: always error, see §5)
    env: {}                              # explicit allow-list; empty by default
    network: false                       # bwrap --unshare-net unless true
    timeout_seconds: 60
```

`inputs`/`outputs` are pond-path globs, resolved relative to
`context.root()` (which, per `FactoryContext::root()`
(`crates/tinyfs/src/context.rs:522-534`), already honors an
`effective_root` chroot when the factory config itself lives inside a
foreign mount -- `exec` gets that scoping for free).

`program` is always an absolute, explicit path -- there is no `$PATH`
lookup, and `args` is always a literal `Vec<String>`, never a shell string,
so there is no shell-injection surface. If a program genuinely needs shell
semantics, `program: /bin/bash, args: ["-c", "..."]` is spelled out
explicitly in the config, which is pond-versioned and auditable.

---

## 3. Staging and the input/output boundary

1. **Stage inputs**: for each `inputs` glob, read the matching pond files
   (`root.async_reader_path` / existing streaming read path) and write them
   into `staging/<relative-path>` with the same relative layout they have
   in the pond. Inputs are staged read-only (`chmod 0o444`) so a
   misbehaving program gets `EACCES`, not a silent accepted write, if it
   tries to modify something outside its declared `outputs`.
2. **Snapshot outputs**: before exec, hash (BLAKE3, matching TinyFS's own
   content hashing -- `tinyfs::NodeMetadata::blake3`) every path that
   currently matches an `outputs` glob, whether or not it exists yet
   (nonexistent = `None`).
3. **Exec** (see §4).
4. **Re-snapshot and diff**: walk `outputs` globs again. Any path whose
   hash changed from step 2 is a pending write. Any path that
   newly matches `outputs` (and didn't in step 2) is a pending create. Any
   path that existed in step 2 but is now missing is a **deletion** --
   see §5.
5. **Commit**: write every pending create/modify into TinyFS via the
   existing `WD` write methods, inside the transaction `execute()` is
   already running in. A failure partway through this loop fails the whole
   `pond run` the same way any other factory error does -- nothing commits
   until the surrounding transaction commits.

Paths outside every declared `outputs` glob are never written back, even if
the program changed them in the staging directory -- they are silently
discarded with the rest of the temp dir. Paths inside `inputs` globs are
read-only at the OS level; the program simply cannot change them.

---

## 4. Sandbox: bubblewrap, Linux-only, Docker for dev verification

Literal `chroot(2)` was considered and rejected: it requires root, does not
block network access, and does not stop a program from reaching other
mounts the host process can see. **bubblewrap (`bwrap`)** gives a real
mount + network namespace without root, is already packaged for Debian/apt
(matches the `selfmon-deb-pipeline-design.md` packaging story), and is a
single well-audited binary rather than a library we'd have to vet.

Representative invocation:

```
bwrap \
  --ro-bind /bin/cat /bin/cat \
  --ro-bind /lib /lib --ro-bind /lib64 /lib64 --ro-bind /usr/lib /usr/lib \
  --bind <staging> <staging> \
  --chdir <staging> \
  --unshare-net \
  --unshare-pid \
  --die-with-parent \
  --new-session \
  -- /bin/cat journal.ledger
```

- `--unshare-net` unless `config.network: true` is explicitly set (expected
  to stay `false` for every bookkeeping use case -- these are local-file
  tools).
- `--unshare-pid` + `--die-with-parent` so a runaway child can't outlive
  the factory call or fork-bomb the host.
- Read-only binds for the program binary and the minimal set of shared
  libraries it needs (resolved once via `ldd`, or simply bind-mount `/usr`,
  `/lib*` read-only wholesale for v1 -- simplicity over minimality to
  start; tightening the bind set is a later hardening pass, not a
  blocker).
- `timeout_seconds` enforced from the Rust side (`tokio::time::timeout`
  around the child wait), killing the process group on expiry.

**v1 is Linux-only by design** -- `bwrap` needs Linux namespace support.
Since the deployment targets (Watershop, Azure, per stored project
knowledge) are both Linux, this isn't a production gap. For local
development on macOS, the validation path is: run the test suite for
`crates/exec-factory` inside a Linux container.

```bash
docker run --rm -v "$PWD":/work -w /work rust:slim bash -c '
  apt-get update && apt-get install -y bubblewrap && \
  cargo test -p exec-factory
'
```

Docker Desktop's Linux VM supports the unprivileged user namespaces
`bwrap` needs for its default (non-setuid) mode in current kernels; if that
ever proves flaky in CI, the fallback is `--security-opt
seccomp=unconfined` on the `docker run` (or `--cap-add SYS_ADMIN`) for the
test container specifically -- a CI-only workaround, never a statement
about the sandbox itself. This container-based loop is also the natural
place to pin down the minimal shared-library bind set mentioned above,
since it's the only environment where we can be sure we're testing the
real Linux sandbox path rather than the macOS no-op.

On macOS outside Docker, `exec` should refuse to run rather than silently
fall back to an unsandboxed subprocess -- a clear error beats a quiet
security downgrade. (The earlier draft of this proposal sketched a
"no sandbox, cwd-only" macOS dev mode; dropped in favor of "test via Docker,
refuse to run natively," since per-user preference we do not want silent
degradation of a safety property.)

---

## 5. Deletions, failure, and commit semantics

- **Deletions are always an error for v1**, not configurable: if a path
  that matched `outputs` before exec is gone afterward, `exec` fails the
  whole run rather than deleting pond history. This matches the
  `billing-v2` philosophy of "always reverse + reissue explicitly" rather
  than quietly mutating the record -- an operator who wants a file gone
  can delete it with an explicit, auditable pond command afterward.
- **Non-zero exit, signal, or timeout**: the staging directory is dropped,
  `execute()` returns an error, and (per `run_pond_command`,
  `crates/cmd/src/commands/run.rs: Err(e) => Err(tx.abort(&e).await.into())`)
  the whole transaction aborts. No partial state ever reaches the pond.
- **A program that writes outside its declared `outputs`** inside the
  staging dir has those writes silently discarded with the temp directory
  -- not an error, since the sandbox already prevented it from reaching
  anything outside staging; the pond-path diff is simply scoped to
  `outputs` regardless.
- There is no notion of a long-running or interactive session in v1: one
  `pond run` invocation is one program invocation, start to clean exit,
  same as every other executable factory. A REPL-style editing session is
  out of scope (see §7).

---

## 6. Worked example: the smoke test

Before anything ledger-shaped, validate the mechanism with the simplest
possible program:

```yaml
spec:
  factory: exec
  config:
    program: /bin/bash
    args: ["-c", "cat in.txt > out.txt"]
    inputs:  ["/tmp-exec-test/in.txt"]
    outputs: ["/tmp-exec-test/out.txt"]
```

`pond run /system/etc/exec-smoke-test` should: stage `in.txt` read-only,
run `bash -c 'cat in.txt > out.txt'` under `bwrap` with network and PID
namespace isolation, see `out.txt` appear in the post-exec snapshot, and
commit it into the pond at `/tmp-exec-test/out.txt`. A second test should
assert that a program exiting non-zero (`bash -c 'exit 1'`) leaves the pond
completely unchanged, and a third should assert that a program which
deletes a previously-existing output file fails the run per §5.

---

## 7. Explicitly out of scope for v1

- **macOS production support.** Linux + bwrap only; macOS is Docker-tested
  dev, nothing more.
- **FUSE-based materialization.** Heavier and slower than stage-copy-diff;
  revisit only if real workloads need the external program to see pond
  files larger than comfortably fit in a staging copy.
- **Automatic post-commit execution** (`/system/run/*`). Running an
  arbitrary external program automatically after every commit is a much
  larger trust decision than running it on explicit operator request via
  `pond run /system/etc/...`. `exec` factories should be mounted under
  `/system/etc`, invoked manually, same as `accounts` is today. Revisit
  only with a much narrower, explicitly-opted-in allowlist story.
- **Interactive/long-running programs.** One invocation, one clean exit,
  one commit -- no daemons, no REPLs.
- **Teaching `cmd/billing` to read Ledger format, or evaluating
  `hledger`/`ledger`/`beancount` specifically.** Deliberately deferred
  until the plumbing in this document is proven with trivial programs.

---

## 8. Implementation plan

1. New workspace member `crates/exec-factory`; add to root `Cargo.toml`
   members + workspace deps, and as a dependency of `crates/cmd` (mirrors
   `billing`'s wiring exactly).
2. `ExecConfig` (serde) + `validate_exec_config`, following the
   `#[serde(deny_unknown_fields)]` convention used by every other factory
   config (e.g. `TestConfig` in `test_factory.rs:21`).
3. `stage_inputs` / `snapshot_outputs` / `diff_outputs` as small,
   independently unit-testable functions operating on `std::path::Path`,
   with no TinyFS or subprocess dependency -- so the diffing logic can be
   tested without an actual pond or an actual `bwrap`.
4. `run_sandboxed`: builds the `bwrap` argv from `ExecConfig`, wraps the
   child in `tokio::time::timeout`, returns `ExitStatus`.
5. `execute()` wiring the above together, `register_executable_factory!`.
6. Tests per §6, run via the Docker loop in §4; wire that `docker run` as
   the `cargo test -p exec-factory` recipe documented in this crate's
   README, not as a new CI job (existing `rust-ci.yml` already runs on
   Linux runners, so `cargo test -p exec-factory` there exercises the real
   sandbox path with no Docker indirection needed -- Docker is purely a
   macOS dev convenience).
