# Pond as the analysis tool: a scriptable + interactive query CLI

**Status:** draft / design record. Motivated by using `pond cat --sql` as the
primary analysis tool while iterating on the Caspar leak- and pump-analysis
derived series. No code yet — this proposes the surface we want before we build.

## 1. Motivation

Watertown should be **all-encompassing**: a user (or an agent) analyzing pond
data should never need to reach for `duckdb`, `datafusion-cli`, or an ad-hoc
Python/pandas script. Those tools force two bad things:

1. **Dialect drift.** We validate against a *different* engine than the one that
   runs in production (sitegen / derived series run DataFusion 51). Queries that
   pass in DuckDB then fail in the pond (`median()`, `arg_max`,
   `INTERVAL (expr) MINUTE`, `date - 1`, …). Every such surprise is a wasted
   iteration and a latent deploy bug.
2. **Format reconstruction.** To use an outside tool we must first export/convert
   the real ingest into something it can read, then test the *reconstruction*
   rather than the real data path.

`pond cat --sql` already avoids both — it runs the app's own DataFusion over the
real source format. The gap is purely **ergonomics**: it is a one-shot,
single-input, human-formatted command, when for analysis we also want a
**scriptable machine contract** and a **warm interactive session**.

Guiding principle (from the operator): *if our ability to analyze pond data with
our own tool is weak, slow down and improve the CLI — do not bolt on a bespoke
path or send users to another tool.*

## 2. Current surface (what exists today)

```
pond cat <path>
    [--format auto|raw|table]      # raw = parquet bytes, table = ASCII
    [--time-start <ms>] [--time-end <ms>]
    [--sql | --query <SQL>]
    [--explain]                    # show pattern resolution + schema, no exec
```

- The `<path>` (a URL + glob, e.g. `oteljson:///ingest/casparwater*.json`) is
  resolved to **one** table, always registered under the fixed name `source`
  (`crates/cmd/src/commands/cat.rs` `register_table("source", …)`).
- Diagnostics (`Peak memory usage`, `large allocations`) already go to **stderr**
  (`main.rs` `log::info!`, `panic_alloc.rs` `eprintln!`), so `raw` output on
  stdout is already pipe-clean.
- `table` output, however, **appends a trailing `"<N> rows in <M> batches"`
  summary into the returned string** (`format_query_results`), so it is *not*
  clean for a machine consumer.
- There is **no** REPL, **no** CSV/NDJSON/JSON output, and **no** way to register
  more than one named input for a join.

## 3. Friction observed in real use

From validating the drawdown / Horner series against staging ingest:

- **F1 — No machine output format.** `raw` (parquet) can't be eyeballed or piped
  to `wc`/`jq`/`diff`; `table` is human-only and carries a summary line. There is
  no "give me exactly the rows as CSV/NDJSON on clean stdout."
- **F2 — Single fixed table name.** Everything must be one query over one glob.
  Any join across two series (e.g. drawdown vs recovery, or a series vs a lookup)
  forces contortions into a single-source CTE. DataFusion joins are otherwise a
  core strength we can't reach.
- **F3 — Cold start per query.** Each `pond cat` re-opens the pond and re-plans.
  During query development I ran the same open+scan dozens of times; this is
  exactly the loop a REPL removes.
- **F4 — Discoverability.** The `source` table name, the `POND`-at-podman-volume
  trick, and available functions were learned from source code and memory, not
  from `--help` or an introspection command. There is no `SHOW TABLES` /
  `DESCRIBE` / `SHOW FUNCTIONS`.

## 4. Design

**Split of responsibility (operator direction):** the **CLI is the scriptable /
machine surface**; the **interactive, exploratory surface lives in sitegen (the
browser)**, not in a terminal REPL. So this design invests the CLI work in a
clean batch contract over **one shared query core** (`cat` / `pond query`), and
treats interactive querying as a *sitegen* feature (§4.3), reusing the same core.

### 4.1 A clean output contract (addresses F1)

Extend `--format` and add explicit stream discipline:

```
--format table            # human ASCII (default for queryable content)
--format csv              # RFC-4180, header row
--format ndjson           # one JSON object per row (default when piped?)
--format json             # single JSON array
--format parquet          # today's `raw`; keep as alias
```

Rules:

- **stdout carries only result rows.** The `table` summary line moves to
  **stderr** (or behind `--stats`). Nothing but data on stdout in machine formats.
- **All diagnostics/timers/progress stay on stderr** (already true — codify it).
- **`--quiet`** silences the stderr summary/timers too, for fully silent scripts.
- Exit non-zero on query/plan error with the DataFusion message on stderr, so
  `pond sql -c '…' || fail` works.

This alone lets an agent build assertions (`pond … --format ndjson | jq length`,
row-count diffs, golden-file comparisons) instead of eyeballing tables.

### 4.2 Multiple named inputs (addresses F2)

Let a query register N tables, each a URL+glob bound to a name:

```
pond query \
    --table draw='series:///analysis/drawdown-by-month/**/*.series' \
    --table rec='series:///analysis/horner-by-month/**/*.series' \
    --sql "SELECT d.month, d.s_p50, r.s_p50
           FROM draw d JOIN rec r USING (month)
           ORDER BY d.month"
```

- Backward-compatible: a bare positional `<path>` still registers as `source`, so
  every existing `pond cat --sql "… FROM source"` invocation is unchanged.
- `--table name=<url>` may be repeated; the name is the SQL identifier.
- Same `--table` flag works in the REPL (`\attach name url`) — see §4.3.

### 4.3 Interactive querying lives in sitegen, not a CLI REPL (addresses F3)

We deliberately **do not** build a terminal REPL (`pond sql`). The interactive,
exploratory loop — pick inputs, edit a query, see results, iterate — belongs in
**sitegen (the browser)**, where results are already rendered richly (Vega
charts, tables) and a warm engine can back the page. The CLI's job is to be the
**scriptable batch surface**; interactivity is a *sitegen* concern that reuses
the exact same query core (§4.1 formatters, §4.2 multi-input, §4.4 introspection).

What the CLI still owns (scriptable, non-interactive):

```
pond query -c "<SQL>"            # one-shot, honors §4.1 --format / --quiet
pond query -f script.sql         # batch of statements from a file
```

- No readline/history/meta-commands, no warm-context REPL prompt in the CLI.
- `-c` / `-f` exist purely so scripts and tests can drive the engine
  deterministically; they share formatters with `cat` so output is identical.
- Cold-start-per-query (F3) is therefore addressed **in sitegen** (one warm
  `SessionContext` behind the interactive page), not by a CLI daemon.

Sitegen interactive surface (separate design; sketched here for context):

- A page (or dev endpoint) that registers pond inputs and runs user SQL against
  the same DataFusion core, returning rows to the existing chart/table renderers.
- Warm context across queries lives server-side in sitegen, giving the fast
  iterate loop without re-opening the pond per query.
- Introspection (§4.4) surfaces in the UI: available tables, schemas, and the
  functions compiled into this build.

### 4.4 Introspection & discoverability (addresses F4)

Make the engine self-describing so no one reads Rust to use it:

- `SHOW TABLES` — registered inputs and their resolved URL/glob.
- `DESCRIBE <name>` — column names + Arrow types (what `--explain` shows today,
  promoted to a first-class verb / statement).
- `SHOW FUNCTIONS [LIKE …]` — enumerate the DataFusion scalar/agg/window
  functions actually compiled into this build. This directly prevents the
  DuckDB-ism traps (a user sees `approx_percentile_cont` exists and `median`
  does not, *before* writing the query).
- `pond query --list-functions` / `pond cat --explain` reuse the same registry;
  the sitegen interactive surface (§4.3) exposes the same three over the UI.
- Document the canonical local-preview recipe (POND → podman volume `_data`) in
  `docs/operator-guide.md` and surface it in `pond query --help`.

### 4.5 Scriptability niceties (nice-to-have)

- **Parameters:** `--param month='2026-07'` bound as a scalar, so scripts don't
  string-interpolate SQL. (Maps to DataFusion prepared-statement params.)
- **`--explain-analyze`** to get the physical plan + row counts for perf triage.
- Stable, documented **exit codes** (0 ok, non-zero on plan/exec error).

## 5. Compatibility & non-goals

- **Compatible:** positional `<path>` ⇒ `source`; `--sql`/`--query` alias;
  `raw` kept as an alias for `--format parquet`. No existing invocation breaks.
- **CLI is scriptable-only:** no terminal REPL/editor/notebook in the CLI; the
  interactive loop is a **sitegen** surface (§4.3) over the same engine.
- **Not** adding a second engine. Everything is the one DataFusion we already
  ship, so "validate" and "deploy" stay the same dialect.

## 6. Suggested phasing

Priority order reflects the operator direction: **machine-format CLI work first;
interactive querying is a later, separate sitegen track.**

1. **Output contract (§4.1) — top priority.** Add `csv` / `ndjson` / `json`, move
   the `table` summary line to stderr, add `--quiet`. Smallest change, biggest
   leverage: unlocks scripted assertions immediately and benefits `cat` as-is.
2. **Introspection (§4.4).** `DESCRIBE` / `SHOW TABLES` / `SHOW FUNCTIONS` over
   the existing single-source path.
3. **Multi-input (§4.2).** `--table name=url` (repeatable) plus `-c` / `-f` batch
   drive on `cat` / `pond query`.
4. **Sitegen interactive surface (§4.3) — separate track/design.** Warm-context
   query page in the browser, reusing the phase 1–3 core. Not a CLI REPL.

Phases 1–3 are pure CLI and each independently useful/testable; phase 4 is a
sitegen feature and gets its own design doc.

## 7. Open questions

- Do we keep growing `cat`, or introduce `pond query` as the multi-input +
  `-c`/`-f` batch verb and leave `cat` as the single-file reader?
- Should `SHOW FUNCTIONS` also print each function's signature/arg types?
- Sitegen interactive (§4.3): dev-only endpoint vs a published page? How is the
  warm `SessionContext` scoped/invalidated as the pond advances?

Until a dedicated machine-text format is implemented, `auto` keeps queryable
output as a text table through pipes; binary Parquet output requires explicit
`--format=raw`.
