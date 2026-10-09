# Exec Factory: Running External Programs Against Pond Files

> **Status:** Implemented (`crates/exec-factory`). Scoped to Linux only for
> v1 (Watershop + Azure, per `README.operations.md`); macOS is dev-only and
> is validated through Docker, never treated as a security boundary.

---

## 0. Problem statement

Watertown previously contained `crates/billing`, a hand-written,
from-scratch double-entry bookkeeping system implemented as an executable
factory. It had roughly 30 clap subcommands, its own journal/ledger tables,
and its own invariant checker. It worked, but keeping it meant maintaining
bespoke accounting software forever: schema migrations, policy edge cases,
and audit correctness -- all of it ours to own. That crate was removed once
the exec-factory and hledger path became viable.

The plaintext-accounting ecosystem (`ledger`, `hledger`, `beancount`, and
others) already solves double-entry bookkeeping, with mature CLIs, reports,
and a shared file format (the Ledger journal format) that several programs
can read and write. What Watertown contributes that those tools don't have
on their own is: transactional commits, content-addressed versioning,
automatic remote backup, and DataFusion queryability over the result. The
gap is that these tools are **ordinary programs that do raw file I/O against
a real filesystem** -- `open()`, `read()`, `write()`, `rename()` -- not
TinyFS API calls. Existing factories (`sitegen`, `hydrovu`,
`materialize-series`, ...) are Rust code linked into the `pond` binary and
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
for example `crates/provider/src/factory/materialize_series.rs`) provides:

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

Concretely, `crates/exec-factory` is wired into `crates/cmd` like
`hydrovu` and `sitegen`; linkme's
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
      - /data/billing/journal.ledger     # exact pond path -> staged read-only
    outputs:
      - /data/billing/reports/           # trailing "/" = directory prefix;
                                          # otherwise an exact file path
    series_outputs:
      - /data/billing/journal.ledger     # exact path, append-only (see §5b)
    env: {}                              # explicit allow-list; empty by default
    network: false                       # bwrap --unshare-net unless true
    interactive: false                   # true: keep terminal job control (§5c)
    timeout_seconds: 60                  # 0 = no timeout; default depends on
                                          # `interactive` (see below)
```

`inputs` and `outputs` are exact pond paths, or (for `outputs` only) a
directory prefix written with a trailing `/`; `series_outputs` entries must
always be exact file paths (validated at `mknod` time). None of these are
glob patterns -- see this module's doc comment in `config.rs` for why that
simplification was made instead of reinventing a glob engine. Every path
listed in `series_outputs` must *not* also appear in `outputs`: each output
path picks exactly one commit semantics (whole-file diff vs. append-only),
and listing it in both is rejected at `mknod` time.

Paths are resolved relative to `context.root()` (which, per
`FactoryContext::root()` (`crates/tinyfs/src/context.rs:522-534`), already
honors an `effective_root` chroot when the factory config itself lives
inside a foreign mount -- `exec` gets that scoping for free).

`program` is always an absolute, explicit path -- there is no `$PATH`
lookup, and `args` is always a literal `Vec<String>`, never a shell string,
so there is no shell-injection surface. If a program genuinely needs shell
semantics, `program: /bin/bash, args: ["-c", "..."]` is spelled out
explicitly in the config, which is pond-versioned and auditable.

`timeout_seconds` defaults to 60s for ordinary runs and to "no timeout" for
`interactive: true` runs (an attended session shouldn't be killed out from
under the operator); `Some(0)` always means "no timeout" explicitly,
either way. See `ExecConfig::effective_timeout`.

---

## 3. Staging and the input/output boundary

1. **Stage inputs**: for each exact `inputs` path, read the pond file
   (`root.read_file_path_to_vec`) and write it into
   `staging/<relative-path>` with the same relative layout it has in the
   pond. Inputs are staged read-only (`chmod 0o444`) so a misbehaving
   program gets `EACCES`, not a silent accepted write, if it tries to
   modify something outside its declared `outputs`/`series_outputs`.
2. **Stage existing outputs**: any pond content already present at an
   `outputs` path (exact or directory-prefix) is also staged -- writable
   this time -- so the deletion check in §5 has something to compare
   against, and so a program that only partially rewrites a file (e.g. a
   report generator re-reading its own prior output) sees it.
3. **Stage series outputs**: for each `series_outputs` path, load the
   pond's current committed state via `provider::series_append` (§5b) and
   write its full existing content into staging, writable. A path with no
   prior committed version stages as an empty file -- the program's first
   run creates the series' first version.
4. **Snapshot outputs**: before exec, hash (BLAKE3, matching TinyFS's own
   content hashing -- `tinyfs::NodeMetadata::blake3`) every path that
   currently matches an `outputs` entry, whether or not it exists yet
   (nonexistent = `None`). `series_outputs` paths are handled separately
   (§5b), not through this whole-file hash/diff path.
5. **Exec** (see §4).
6. **Re-snapshot and diff**: walk `outputs` paths again. Any path whose
   hash changed from step 4 is a pending write. Any path that newly
   matches `outputs` (and didn't in step 4) is a pending create. Any path
   that existed in step 4 but is now missing is a **deletion** -- see §5.
7. **Commit**: write every pending create/modify into TinyFS via the
   existing `WD` write methods, and commit every `series_outputs` suffix
   via `provider::series_append::commit_series_append` (§5b) -- all inside
   the transaction `execute()` is already running in. A failure partway
   through this loop fails the whole `pond run` the same way any other
   factory error does -- nothing commits until the surrounding transaction
   commits.

Paths outside every declared `outputs`/`series_outputs` entry are never
written back, even if the program changed them in the staging directory --
they are silently discarded with the rest of the temp dir. Paths inside
`inputs` are read-only at the OS level; the program simply cannot change
them.

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
  --ro-bind /usr /usr --ro-bind /lib /lib --ro-bind /lib64 /lib64 \
  --ro-bind /bin /bin --ro-bind /sbin /sbin --ro-bind /etc /etc \
  --dev /dev --proc /proc \
  --bind <staging> <staging> \
  --chdir <staging> \
  --unshare-pid \
  --die-with-parent \
  --new-session \
  --unshare-net \
  --setenv PATH /usr/local/bin:/usr/bin:/bin \
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
- `--new-session` is **omitted** when `config.interactive: true` -- see
  §5c for why an attended session trades away the `TIOCSTI` hardening that
  flag provides.
- `timeout_seconds` enforced from the Rust side (`tokio::time::timeout`
  around the child wait), killing the process group on expiry; `None` for
  `interactive: true` runs by default (§2).

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
  whole run rather than deleting pond history. This follows the accounting
  principle of "always reverse + reissue explicitly" rather than quietly
  mutating the record -- an operator who wants a file gone
  can delete it with an explicit, auditable pond command afterward.
  To make this enforceable, `execute()` pre-stages any already-existing
  pond content at every declared output -- both exact-path
  (`OutputSpec::File`) and directory-prefix (`OutputSpec::DirPrefix`,
  recursively) -- *before* the "before" snapshot, not just `inputs`;
  otherwise a path the program deletes without ever having been staged
  would be invisible to the diff and the deletion would silently go
  undetected.
- **Non-zero exit, signal, or timeout**: the staging directory is dropped,
  `execute()` returns an error, and (per `run_pond_command`,
  `crates/cmd/src/commands/run.rs: Err(e) => Err(tx.abort(&e).await.into())`)
  the whole transaction aborts. No partial state ever reaches the pond.
- **A program that writes outside its declared `outputs`/`series_outputs`**
  inside the staging dir has those writes silently discarded with the
  temp directory -- not an error, since the sandbox already prevented it
  from reaching anything outside staging; the pond-path diff is simply
  scoped to the declared paths regardless.

### 5b. `series_outputs`: append-only `FilePhysicalSeries` paths

Some tools -- a Ledger-format journal chief among them -- only ever
*append* new records to an existing file; the file's prior bytes are
immutable history, not a snapshot to be freely rewritten. `series_outputs`
maps a pond path onto `tinyfs::EntryType::FilePhysicalSeries` instead of
the ordinary whole-file diff in §3: the program sees the path's full prior
content (all committed versions concatenated), and on clean exit only the
**new suffix bytes** -- not the whole file -- are committed as the next
version, via `root.async_writer_path_with_type(path,
EntryType::FilePhysicalSeries)`.

This is exactly the problem `logfile_ingest` already solves (tailing an
actively-written host log file into a pond series), so `exec-factory`
doesn't reimplement it: both factories share
**`provider::series_append`** (`crates/provider/src/series_append.rs`):

- `PondSeriesState` / `load_series_state`: the pond's committed cumulative
  state for a series path -- cumulative BLAKE3, cumulative byte count, and
  (when available) the bao-tree frontier from the stored
  `SeriesOutboard`.
- `verify_prefix_matches`: confirms a host file's current bytes still
  start with exactly the pond's committed prefix. When a frontier is
  available this costs at most one `BLOCK_SIZE` read of the trailing
  partial block, not a full re-hash of the whole file -- the same
  incremental-bao-tree trick `logfile_ingest` uses to tail multi-GB log
  files cheaply.
- `read_new_suffix`: a TOCTOU-safe exact read of only the bytes past the
  verified prefix.
- `commit_series_append`: writes just that suffix as the series' next
  version.

If the staged file isn't a byte-for-byte extension of what was staged
(the program edited, truncated, or deleted a prefix instead of only
appending), `verify_prefix_matches` fails and the whole run is rejected --
pond is left unchanged, same as any other output error. A path with no
prior committed version (first run) always passes trivially and the whole
staged file becomes the series' first version.

### 5c. Interactive sessions

`interactive: true` drops `bwrap`'s `--new-session` flag (§4) so a real
interactive program -- an `hledger-ui` session, `hledger add`'s prompts,
an editor -- keeps normal terminal job control (Ctrl-C, Ctrl-Z)
instead of losing it to a detached session. This trades away
`--new-session`'s defense against the sandboxed program injecting fake
keystrokes back at the controlling terminal (`TIOCSTI`) -- acceptable
only because running a program interactively is already an explicit,
attended choice by the operator at a real terminal, not something that
runs unattended or from automation. It also defaults `timeout_seconds` to
"no timeout" (§2), since an attended session shouldn't be killed out from
under the operator mid-conversation.

stdin/stdout/stderr are inherited from the `pond` process by default
(standard `tokio::process::Command` behavior), so `pond run
/system/etc/<interactive-exec-node>` at a real terminal just works --
no separate plumbing needed for v1.

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

This exact scenario (as `/bin/sh -c 'cat data/in.txt > data/out.txt'`) is
checked in as `scripts/examples/cat-demo.yaml`, and the Rust/testsuite
tests listed in §8 cover it along with the nonzero-exit and
deletion-rejection cases. To run it yourself locally -- no Docker *image*
build, no remote pond, just `cargo run` -- use:

```
./scripts/try-exec-factory.sh
```

On Linux this runs natively; on macOS (or anywhere else) it transparently
runs the same `cargo run` inside a throwaway privileged Linux container
(build/registry caches live in named Docker volumes, so repeated runs are
incremental). With no arguments it builds a fresh temp pond, seeds an
input file, mknods and runs the demo config above, and prints the
committed output. Any arguments are passed straight through to
`cargo run --bin pond --`, e.g.:

```
./scripts/try-exec-factory.sh pond --pond /tmp/my-pond init --birthplace me
./scripts/try-exec-factory.sh pond --pond /tmp/my-pond mknod exec /system/etc/my-job --config-path scripts/examples/my-job.yaml
```

---

## 6b. Worked example: interactive `hledger-ui` against a pond journal

`scripts/examples/hledger-journal.yaml`:

```yaml
program: /usr/bin/hledger-ui
args: ["-f", "accounting/journal.ledger"]
inputs: []
series_outputs: ["/accounting/journal.ledger"]
interactive: true
```

This mounts the journal as a `series_outputs` path (§5b), so the pond keeps
every prior version's bytes immutable and commits only bytes appended by
hledger-ui's `a` (add transaction) workflow. `interactive: true` (§5c)
keeps real terminal job control for the full-window curses interface. The
stable Debian bookworm packages provide `hledger` and `hledger-ui`, but not
the newer `hledger repl` command; using the packaged TUI avoids making a
preview hledger build part of the accounting image.

End to end -- the first run against a not-yet-existing
`/accounting/journal.ledger` stages an empty file and whatever the session
writes becomes the series' first version, so there's no separate seeding
step (and no need for `pond copy`, which would create a plain
`file:physical:version` that collides with `file:physical:series` on the
next run):

```
pond mknod exec /system/etc/hledger --config-path scripts/examples/hledger-journal.yaml
pond run /system/etc/hledger
```

`pond run` inherits the controlling terminal's stdin/stdout/stderr, so this
drops the operator into `hledger-ui` against the pond's current journal. On
clean exit (`q`), transactions entered with `a` are committed as the
journal's next version. A separate exec node running `hledger add -f
accounting/journal.ledger` with the same `series_outputs` declaration is
also possible, but is not required for ordinary entry from the TUI.

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
  `/system/etc` and invoked manually. Revisit only with a much narrower,
  explicitly-opted-in allowlist story.
- **Teaching `cmd/billing` to read Ledger format.** `series_outputs` plus
  interactive `hledger-ui`/`hledger add` sessions (§6b) cover browsing,
  entering, and storing journal data; generating PDF statements from that
  journal (today `cmd/billing` reads its own four CSV files) is still
  deliberately deferred to a later pass.

---

## 8. Accounting deployment roadmap

The exec factory, append-only `series_outputs`, interactive terminal mode,
and hledger smoke/integration tests described above are implemented. The next
phase is to replace the bespoke billing ledger with ordinary Ledger-format
files while retaining Watertown's transactions, history, and remote backups.

This phase deliberately stops before statement generation. Its deliverable is
a complete, reconciled set of historical and current books that hledger can
operate interactively and DataFusion can read through Ledger format
providers.

### 8a. Canonical journal layout

The accounting pond will hold:

```text
/accounting/
  books.journal                 # includes the files below
  accounts.journal              # chart of accounts and shared directives
  cycles/
    YYYY-MM.journal             # one immutable six-month billing cycle
  current.journal               # the one writable FilePhysicalSeries
  current-cycle.yaml            # closing month/year and cycle metadata
```

`YYYY-MM` is the closing month and year of a six-month billing cycle. The
active cycle is therefore named logically by its closing month/year even
while its writable file remains at the stable `/accounting/current.journal`
path. On close, that file becomes `/accounting/cycles/YYYY-MM.journal` and a
new empty `current.journal` is created for the next declared closing month.

`books.journal` is the hledger entry point. It includes `accounts.journal`,
all closed journals under `cycles/`, and `current.journal`, so interactive
and reporting commands always see the complete accounting history. Closed
cycle files and `accounts.journal` are read-only inputs to exec factories;
only `current.journal` is a `series_outputs` path.

### 8b. Historical CSV migration

A Go migration program will live beside and reuse the existing Caspar Water
billing software and its CSV models. The original CSVs remain authoritative
source material; generated journals are reproducible migration artifacts,
not the only recoverable copy.

The program will:

1. Read the existing customer, connection, cycle, payment, and business
   inputs using the same parsing and identity rules as the current billing
   program.
2. Generate one deterministic `accounts.journal` containing the chart of
   accounts and shared commodity/account declarations.
3. Generate one deterministic `cycles/YYYY-MM.journal` for every historical
   six-month billing cycle.
4. Record the already-tallied total for each expense account in that cycle
   rather than attempting to reconstruct individual historical expenses that
   were never captured.
5. Record every payment received on or before the cycle's close cutoff in the
   applicable historical journal, with stable source identifiers in comments
   or tags so duplicate or omitted payments can be diagnosed.
6. Fail loudly on unknown identities, ambiguous cycle assignment, unbalanced
   transactions, duplicate source records, or reconciliation differences.
   It must never silently omit or coerce a historical row.

Migration acceptance is reconciliation, not merely successful parsing:

- every generated journal passes hledger's strict validation;
- every transaction balances;
- per-cycle expense totals match the source billing totals;
- per-cycle and cumulative payments match the source payment data;
- customer/account balances at every cycle boundary match the existing
  billing system; and
- rerunning the migration from identical inputs produces byte-identical
  journal files.

The migration first writes ordinary host files for review and reconciliation.
Only an explicitly accepted output set is loaded into a fresh accounting
pond.

### 8c. Interactive current-cycle operation

The accounting runtime image will be a separately published Watertown image
containing `bubblewrap`, stable Debian `hledger`, and `hledger-ui`. It will
not add accounting tools or elevated container privileges to water, septic,
noyo, or site instances.

The ordinary attended workflow is:

1. Start the accounting pond against `books.journal`.
2. Stage `accounts.journal`, closed cycle journals, and the include file
   read-only; stage `current.journal` read-write as a `series_outputs` file.
3. Run `hledger-ui` interactively inside the exec sandbox.
4. Enter new expenses and payments with the TUI's add-transaction workflow.
5. Exit cleanly with `q`; exec-factory validates the append-only boundary and
   commits the new journal suffix as one Watertown transaction.
6. Let the pond's configured post-commit backup push that transaction. A
   sandbox or validation failure aborts the pond transaction and leaves both
   the journal and remote publication unchanged.

Every entry session is explicit and attended; accounting exec nodes remain
under `/system/etc`, not `/system/run`.

### 8d. Closing a billing cycle

Cycle close will be a separate explicit command, not an hledger-ui side
effect. It will perform one atomic Watertown transaction that:

1. validates all journals with hledger;
2. verifies that `current-cycle.yaml` names the expected closing month/year;
3. refuses to overwrite an existing `cycles/YYYY-MM.journal`;
4. finalizes `current.journal` at that dated cycle path without rewriting its
   committed bytes;
5. creates a new empty `current.journal`;
6. advances `current-cycle.yaml` to the next six-month close; and
7. updates `books.journal` if its include structure requires an explicit
   cycle list.

The command must be idempotent or fail with a precise already-closed
diagnostic. Any validation, naming, or filesystem error aborts the entire
transaction. Remote backup occurs only after the close transaction commits.

The implementation must preserve `FilePhysicalSeries` identity and history;
whether close is represented internally as a TinyFS rename or as a
transactional finalization operation will be decided from the filesystem's
existing rename and series semantics, not by copying bytes through the host.

### 8e. Staging and production ponds

Accounting follows the existing producer pattern but remains independent of
sitegen for now:

- `accounting-staging` runs on Watershop and backs up to its own MinIO bucket;
- `accounting-prod` uses its own production storage and Azure backup;
- each tier has a distinct pond volume, remote URL, credentials, rate limits,
  and immutable recovery path;
- neither pond is imported by the public site pond; and
- restore drills must prove that a fresh local pond can recover the complete
  journal set and open it successfully with hledger-ui.

The deployment wrapper selects the accounting image only for
`accounting-*`. Because nested bubblewrap requires container privileges on
the current runtime, those privileges are also scoped only to accounting
instances and must be re-verified on the actual Watershop and production
hosts.

### 8f. Ledger format providers

After migration, interactive operation, close, backup, and restore are
working, add Ledger-format providers that expose journal records as typed
DataFusion tables. At minimum the provider model must represent transactions,
postings, accounts, commodities, dates, descriptions/payees, amounts, and
source journal/cycle identity without losing Ledger semantics.

The provider reads the canonical journal files; it does not create a second
accounting authority or materialize an independently editable ledger. Tests
will compare provider output with hledger reports for the same fixture books.

Generating customer statements from those tables is a later phase and is not
part of this roadmap's exit criteria.

### 8g. Exit criteria

This phase is complete when:

1. historical CSVs deterministically produce reconciled Ledger journals;
2. the accepted journals load into fresh staging and production accounting
   ponds;
3. an attended hledger-ui session can append a balanced transaction and
   commit it through exec-factory;
4. cycle close atomically archives the current journal and starts the next
   six-month cycle;
5. post-commit remote backup and full restore are verified in both tiers; and
6. DataFusion can query the journals through tested Ledger format providers.
