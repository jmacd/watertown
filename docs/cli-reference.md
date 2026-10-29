# Watertown CLI Reference

> **Compatibility index.** Current interfaces are authoritative in the [development manual](manual/index.md). Migrated headings remain for old links; residual project recipes/troubleshooting await D11 review.

## Quick Reference

See the [authoritative development manual](manual/reference/cli/commands.md).

### Two Operating Modes

See the [authoritative development manual](manual/reference/concepts.md#pond-and-host-roots).

## Environment

See the [authoritative development manual](manual/reference/cli/commands.md).

## Commands in Detail

See the [authoritative development manual](manual/reference/cli/commands.md).

### pond init

See the [authoritative development manual](manual/reference/cli/commands.md#init).

### pond list

See the [authoritative development manual](manual/reference/cli/commands.md#list).

### pond mkdir

See the [authoritative development manual](manual/reference/cli/commands.md#mkdir).

### pond copy

See the [authoritative development manual](manual/reference/cli/commands.md#copy).

#### Copy IN (host → pond)

See the [authoritative development manual](manual/reference/cli/commands.md#copy).

#### Copy OUT (pond → host)

See the [authoritative development manual](manual/reference/cli/commands.md#copy).

#### --strip-prefix

See the [authoritative development manual](manual/reference/cli/commands.md#copy).

### pond cat

See the [authoritative development manual](manual/reference/cli/commands.md#cat).

#### Output Format (`--format`)

See the [authoritative development manual](manual/reference/cli/commands.md#cat).

#### URL Schemes

See the [authoritative development manual](manual/reference/concepts.md#url-layers-and-entry-types).

#### Hledger journal provider

See the [authoritative development manual](manual/reference/formats/providers.md#hledger-journal).

#### Binary Excel (`xls`)

See the [authoritative development manual](manual/reference/formats/providers.md#binary-excel).

#### Schema Discovery with `information_schema`

See the [authoritative development manual](manual/reference/cli/commands.md#cat).

### pond describe

See the [authoritative development manual](manual/reference/cli/commands.md#describe).

### pond mknod

See the [authoritative development manual](manual/reference/cli/commands.md#mknod).

### pond run

See the [authoritative development manual](manual/reference/cli/commands.md#run).

#### Host Mode (`host+factory://`)

See the [authoritative development manual](manual/reference/cli/commands.md#run).

### pond log

See the [authoritative development manual](manual/reference/cli/commands.md#log).

### pond remote / pond backup

See the [authoritative development manual](manual/guides/operations.md#backup-versus-import).

### pond push

See the [authoritative development manual](manual/reference/cli/commands.md#push).

### pond capsule

See the [authoritative development manual](manual/reference/cli/commands.md#capsule).

### pond freeze

See the [authoritative development manual](manual/reference/cli/commands.md#freeze).

### pond pull

See the [authoritative development manual](manual/reference/cli/commands.md#pull).

### pond restore

See the [authoritative development manual](manual/reference/cli/commands.md#restore).

### pond verify (D6.1)

See the [authoritative development manual](manual/reference/cli/commands.md#verify).

### pond fsck

See the [authoritative development manual](manual/reference/cli/commands.md#fsck).

### pond status (D6.2)

See the [authoritative development manual](manual/reference/cli/commands.md#status).

### pond tlog (D5)

See the [authoritative development manual](manual/reference/cli/commands.md#tlog).

### pond rebuild-control (D6.3)

See the [authoritative development manual](manual/reference/cli/commands.md#rebuild-control).

### pond maintain

See the [authoritative development manual](manual/reference/cli/commands.md#maintain).

### pond config

See the [authoritative development manual](manual/reference/cli/commands.md#config).

## Filesystem Conventions

Several well-known paths under the pond have special meaning to the
steward and CLI.

### `/sys/remotes/<name>` -- remote attachments (D4)

Each file under `/sys/remotes/<name>` is a small YAML document recording
one remote's URL + mode + (S3) credentials.  Managed via `pond remote
add`, `pond remote remove`, `pond remote list`.  After every write
transaction the steward iterates these and auto-pushes any whose mode is
`push` or `both`.

### `/system/{run,etc,site}/` -- factory nodes

Factory nodes live under `/system/` in directories that determine their
auto-execution behavior:

| Directory | Purpose | Post-commit | Examples |
|-----------|---------|------------|----------|
| `/system/run/` | Auto-executing factories | Yes (default: `push`) | `monitor-report` |
| `/system/etc/` | Manually triggered or passive | No | `hydrovu`, `sitegen`, `column-rename`, `logfile-ingest`, `journal-ingest` |
| `/system/site/` | Static content (templates) | No | Markdown page templates |

> **Note (D4+)**: the legacy `remote` factory used to live in
> `/system/run/<N>-remote`; that pathway is gone.  Remote sync is now
> handled by `/sys/remotes/<name>` + `pond push|pull` (see above).
> `/system/run/` remains the post-commit factory namespace; remote publication
> is a separate phase that starts after this queue finishes.

**Post-commit auto-execution:** After every write transaction, the
steward scans `/system/run/*` and executes each factory with its
configured mode (default: `push`) in lexical path order. Factories declare
whether automatic execution needs a new write transaction or can share the
fresh committed read snapshot. `monitor-report` is read-only; existing
mutating factories remain read-write. Failures do not stop later factories or
remote auto-push, but the command returns an error after backup so a failed
post-commit action cannot look successful.

**Short name resolution:** `pond run` resolves bare names (no leading
`/`) by checking `/system/run/{name}` then `/system/etc/{name}`.

**Naming convention:** Use numeric prefixes for ordering. Monitoring uses
`00-monitor` so host status is generated before other post-commit work.

`monitor-report` supports the original all-observed-below check and a typed
`rate-while` check. `rate-while` correlates a raw measurement with a condition
series using previous-state alignment, sums `positive-deltas`, divides by
actual active time, and returns `unknown` until `minimum_active_time` is met.
Its DataFusion query uses
`counter_delta(value) OVER (ORDER BY timestamp)`; decreases are treated as
reset/refill events and contribute zero. The condition predicate currently
supports `eq`, alignment supports `previous`, and the alarm comparison
supports `lt`. See
[live-monitoring-projection-design.md](live-monitoring-projection-design.md)
for the complete configuration and evidence semantics.

**Setup pattern:**
```bash
pond mkdir -p /system/etc
pond mkdir -p /system/site

# Manually triggered factories
pond mknod column-rename /system/etc/10-hrename --config-path hrename.yaml
pond mknod hydrovu /system/etc/20-hydrovu --config-path hydrovu.yaml
pond mknod sitegen /system/etc/90-sitegen --config-path site.yaml

# Static content
pond copy host:///path/to/templates /system/site --overwrite

# Backup attachment (managed by `pond backup add`, not `pond mknod`)
pond backup add origin s3://my-bucket \
    --region us-east-1 --access-key-id ... --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}'
```

---

## Factory Types

### sql-derived-table

See the [authoritative factory reference](manual/reference/factories/sql.md#sql-derived-table-and-series).

### sql-derived-series

See the [authoritative factory reference](manual/reference/factories/sql.md#sql-derived-table-and-series).

### synthetic-timeseries

See the [authoritative factory reference](manual/reference/factories/synthetic-timeseries.md).

### dynamic-dir

See the [authoritative factory reference](manual/reference/factories/sql.md#dynamic-directory).

### timeseries-join

See the [authoritative factory reference](manual/reference/factories/timeseries.md#join).

### timeseries-pivot

See the [authoritative factory reference](manual/reference/factories/timeseries.md#pivot).

### temporal-reduce

See the [authoritative factory reference](manual/reference/factories/timeseries.md#temporal-reduction).

### temporal-reduce-series

See the [authoritative factory reference](manual/reference/factories/timeseries.md#temporal-reduction).

### pump-state-series

See the [authoritative factory reference](manual/reference/factories/timeseries.md#pump-state).

### sitegen

See the [authoritative factory reference](manual/reference/factories/../sitegen/configuration.md).

### column-rename

See the [authoritative factory reference](manual/reference/factories/sql.md#column-rename).

### remote (REMOVED in D4)

Historical removed interface. Use current [backup/import operations](manual/guides/operations.md), not a remote factory.

### hydrovu

See the [authoritative factory reference](manual/reference/factories/ingestion.md#hydrovu).

### logfile-ingest

See the [authoritative factory reference](manual/reference/factories/ingestion.md#rotating-log-files).

### journal-ingest

See the [authoritative factory reference](manual/reference/factories/ingestion.md#systemd-journal).

## Glob Patterns

See the [authoritative development manual](manual/reference/concepts.md#patterns-and-effective-roots).

## End-to-End Pipeline Examples

These historical project-specific recipes are retained pending D11 review, not
current executable documentation evidence. Start with the asserted
[timeseries site](manual/guides/timeseries-site.md) and
[blog/RSS](manual/guides/blog.md) packages.

Watertown factories compose into data pipelines. Here are two real-world patterns.

### Multi-Site / Multi-Parameter (Noyo)

The Noyo Harbor example monitors water quality at multiple sites, each with multiple
sensor parameters. The pipeline fans out then fans in:

```
hydrovu (collector)
    → /hydrovu/devices/**/*.series       (raw per-device series)

timeseries-join (/combined)
    → /combined/NorthDock                (all params at one site, wide table)
    → /combined/SouthDock

timeseries-pivot (/singled)
    → /singled/Temperature               (one param across all sites, wide table)
    → /singled/DO

temporal-reduce (/reduced)
    → /reduced/single_site/NorthDock/res={1h,6h,1d}.series
    → /reduced/single_param/Temperature/res={1h,6h,1d}.series

sitegen (/system/etc/90-sitegen)
    → dist/ (HTML + Parquet, served as static site)
```

**Key configs:** `noyo/combine.yaml` (join), `noyo/single.yaml` (pivot),
`noyo/reduce.yaml` (reduce), `noyo/site.yaml` (sitegen).

**Setup pattern:**
```bash
pond init
pond mkdir -p /system/etc
pond mkdir -p /system/site
pond copy host:///path/to/templates /system/site    # Markdown templates
pond mknod column-rename /system/etc/10-hrename --config-path hrename.yaml
pond mknod dynamic-dir /combined --config-path combine.yaml
pond mknod dynamic-dir /singled  --config-path single.yaml
pond mknod dynamic-dir /reduced  --config-path reduce.yaml
pond mknod hydrovu /system/etc/20-hydrovu --config-path hydrovu.yaml
pond mknod sitegen /system/etc/90-sitegen --config-path site.yaml
pond backup add origin s3://my-bucket \
    --region us-east-1 --access-key-id ... --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}'

# Operational cycle:
pond run 20-hydrovu collect              # Collect data
pond run 90-sitegen build ./dist         # Build site
# `pond push origin` runs automatically post-commit (mode=push)
```

### Single-Source (Septic)

The septic station example has a single OTelJSON data source with ~40 metrics from two
scopes (BME280 environment sensor + Orenco Modbus registers) at different sample rates.
There's no need for join/pivot — the `oteljson://` URL scheme provides a single wide table.

```
logfile-ingest (/system/etc/10-ingest)
    → /ingest/septicstation.json         (raw OTelJSON Lines file)

temporal-reduce (/reduced)                using oteljson:// URL scheme
    → /reduced/septic/res={1h,6h,1d}.series

sitegen (/system/etc/90-sitegen)
    → dist/ (HTML + Parquet)
```

**Key difference from Noyo:** The `in_pattern` in `temporal-reduce` uses the
`oteljson://` URL scheme directly, skipping the join/pivot stages:

```yaml
factory: "temporal-reduce"
config:
  in_pattern: "oteljson:///ingest/septicstation.json"
  out_pattern: "septic"
  time_column: "timestamp"
  resolutions: [1h, 6h, 1d]
  aggregations:
    - type: "avg"
      columns:
        - "septicstation_temperature"
        - "orenco_RT_Pump1_Amps"
        # ... all gauge metrics
```

**Setup pattern:**
```bash
pond init
pond mkdir -p /system/etc
pond mkdir -p /system/site
pond mkdir -p /ingest
pond copy host:///path/to/templates /system/site
pond mknod logfile-ingest /system/etc/10-ingest --config-path ingest.yaml
pond backup add origin s3://my-bucket \
    --region us-east-1 --access-key-id ... --secret-access-key '${env:AWS_SECRET_ACCESS_KEY}'
pond mknod temporal-reduce /reduced --config-path reduce.yaml
pond mknod sitegen /system/etc/90-sitegen --config-path site.yaml

# Operational cycle:
pond run 10-ingest                          # ingest new log data
pond run 90-sitegen build ./dist            # build site (reduce is dynamic)
# `pond push origin` runs automatically post-commit (mode=push)
```

**Notes on single-source pipelines:**
- `oteljson://` discovers all metric names as columns automatically (two-pass parser).
- Different sample rates (e.g., 5min Modbus, 10min BME280) produce NULL-padded rows
  in the merged table. `temporal-reduce` aggregations handle NULLs correctly — `avg`,
  `min`, `max` skip NULLs per SQL semantics.
- No glob in `in_pattern` means `out_pattern` is a literal name (not `$0`).
- `temporal-reduce` is a dynamic factory — its output is computed on read. No
  explicit `pond run /reduced update` step is needed; sitegen triggers computation
  when it reads the export data during `build`.

### Choosing a Pipeline Shape

| Scenario | Pipeline | Why |
|----------|----------|-----|
| Multiple sites, multiple params | join → pivot → reduce → sitegen | Need cross-site and cross-param views |
| Single source, many metrics | reduce → sitegen | oteljson:// or csv:// provides the wide table |
| Single source, few metrics | reduce → sitegen | Same, but simpler reduce config |
| Raw file archive only | logfile-ingest + remote | No analysis, just backup |

---

## Factory Type Quick Reference

See the [authoritative development manual](manual/index.md).

## Troubleshooting

### "No matching files" when files exist

**Cause**: Pattern doesn't match actual paths.
**Fix**: Use `pond list '**/*'` first to see what exists, then refine pattern.

### `pond cat` outputs binary garbage

**Cause**: You're catting a table/series entry without `--format=table` or `--sql`.
The default output format is Parquet bytes (binary).
**Fix**: Add `--format=table` for human-readable output, or `--sql "SELECT * FROM source LIMIT 10"`.

### `--format` does the wrong thing

**Cause**: `--format` on `pond cat` controls output display (raw vs table).
`pond copy` does not use `--format` — entry type is in the source URL.
**Fix**: For `pond cat`, use `--format=raw` (default, binary) or `--format=table` (ASCII).
For `pond copy`, use `host:///` (data), `host+table:///` (Parquet), or `host+series:///` (time-series).

### `host+table:///` errors on CSV

**Cause**: `host+table:///` validates PAR1 magic bytes — only Parquet input accepted.
**Fix**: Use `host:///` for CSV (stores as raw bytes). Query CSV with `pond cat csv:///path --sql "..."`.

### Export pattern matches nothing in sitegen

**Cause**: The export `pattern` in site.yaml doesn't match what temporal-reduce creates.
**Fix**: Run `pond list '/reduced/**'` to see the actual directory structure. Common
mismatch: the reduce output might be `/reduced/site/res=1h.series` but the export pattern
expects `/reduced/site/*.series` or `/reduced/site/*/*.series`.

### oteljson:// queries are slow

**Cause**: The two-pass parser reads the entire file twice — once to discover columns,
once to parse data. Large files (>100MB) can be slow.
**Fix**: For repeated queries, consider ingesting into a table:series via SQL pipeline.
For one-off exploration, use `--sql` with `LIMIT` to reduce output.

### Dynamic factory shows stale data

**Cause**: Dynamic factories are re-computed on every read, but they read from their
source which may itself be cached or stale.
**Fix**: For logfile-ingest sources, run `pond run 10-ingest` first to ingest new data.
The dynamic factory chain (temporal-reduce → sitegen) will then see the updated source.

### temporal-reduce columns not found

**Cause**: Column names in `aggregations.columns` don't match the source schema.
Column names are matched against the actual schema at runtime.
**Fix**: Discover column names first:
```bash
pond cat oteljson:///ingest/data.json --format=table --sql "
  SELECT column_name FROM information_schema.columns
  WHERE table_name = 'source' ORDER BY column_name
"
```

### sitegen build fails with "unknown shortcode"

**Cause**: Typo in Markdown template or using wrong shortcode syntax.
**Fix**: Check shortcode names: `{{ $0 }}`, `{{ chart /}}`, `{{ breadcrumb /}}`,
`{{ nav_list collection="..." base="..." /}}`. Note the self-closing `/}}` syntax.

---

## See Also

- [testsuite/](../testsuite/) - Runnable test scripts
- [watertown-overview.md](watertown-overview.md) - Architecture overview
