#!/bin/bash
# EXPERIMENT: `materialize-series` turns a derived signal into a physical
#             table:series, incrementally.
#
# DESCRIPTION:
#   Watertown's typed signals are normally DERIVED: a `sql-derived-series` node
#   recomputes its output from the ingested bytes on every read. That costs no
#   storage and can never go stale, but it means a pond built purely from log
#   ingest -- selfmon, for example -- contains no `TablePhysicalSeries` at all,
#   so half the storage engine goes unexercised by the pond whose whole job is
#   to exercise it.
#
#   `materialize-series` stores that signal instead. Each run it asks the target
#   how far it has already been materialized, takes only the source rows past
#   that watermark, and appends them as ONE new version. The shape that produces
#   -- append-only, one version per tick -- is the same one `hydrovu` produces,
#   and therefore the same one collapse and reclamation operate on.
#
#   The incrementality is the point. A snapshot-and-replace materializer would
#   rewrite the whole history every tick, which is precisely the O(N^2) write
#   amplification that size-tiered collapse exists to remove.
#
# EXPECTED:
#   - The target is a TablePhysicalSeries, created on the first run.
#   - Each tick with new source rows appends exactly ONE version holding only
#     the new rows.
#   - A tick with no new source rows appends NOTHING (no empty versions).
#   - The materialized content equals what the derived node computes.
#   - The result is a genuine collapse candidate.
#   - It works when the source lives inside a `dynamic-dir`, which is the
#     topology selfmon actually uses (config/watershop-selfmon.yaml:/derived).
#
# History:
#   Added on jmacd/analysis8 with the materialize-series factory, closing the
#   selfmon coverage gap that let a table:series collapse bug (#121) ship.
set -e
source check.sh

echo "=== Experiment: materialize a derived series into table:series ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

version_count() {
    pond describe /metrics/cpu.series 2>/dev/null | grep -cE '^ *Version [0-9]+:' || true
}

# Emit one metrics line, shaped like config/scripts/measure-pond.sh output.
emit() {
    printf '{"ts":"%s","peak_rss.bytes":"%s","list.seconds":"%s"}\n' "$1" "$2" "$3" \
        >> /var/log/metrics/pond.jsonl
}

mkdir -p /var/log/metrics
: > /var/log/metrics/pond.jsonl

pond mkdir -p /system/etc >/dev/null 2>&1
pond mkdir -p /measure >/dev/null 2>&1
pond mkdir -p /metrics >/dev/null 2>&1

# ---- The selfmon pipeline in miniature -------------------------------------
# 1. logfile-ingest mirrors the host jsonl into a FilePhysicalSeries.
cat > /tmp/730-ingest.yaml << 'EOF'
archived_pattern: /var/log/metrics/pond.jsonl.*
active_pattern: /var/log/metrics/pond.jsonl
pond_path: /measure
EOF
pond mknod logfile-ingest /system/etc/ingest --config-path /tmp/730-ingest.yaml >/dev/null 2>&1

# 2. sql-derived-series types it (jsonlogs returns all-Utf8, hence the CASTs).
cat > /tmp/730-derived.yaml << 'EOF'
patterns:
  m: "jsonlogs:///measure/pond.jsonl"
query: >-
  SELECT
    CAST(ts AS TIMESTAMP)                    as timestamp,
    CAST("peak_rss.bytes" AS BIGINT)         as "peak_rss.bytes",
    CAST("list.seconds" AS DOUBLE)           as "list.seconds"
  FROM m
  ORDER BY timestamp
EOF
pond mknod sql-derived-series /derived-cpu --config-path /tmp/730-derived.yaml >/dev/null 2>&1

# 3. materialize-series stores it as a physical table:series.
cat > /tmp/730-materialize.yaml << 'EOF'
source: "series:///derived-cpu"
target: /metrics/cpu.series
time_column: timestamp
EOF
pond mknod materialize-series /system/etc/materialize --config-path /tmp/730-materialize.yaml >/dev/null 2>&1

echo "--- Step 1: first tick creates the physical series ---"
emit "2024-01-01T00:00:00Z" 1000 0.1
emit "2024-01-01T00:01:00Z" 1100 0.2
pond run /system/etc/ingest >/dev/null 2>&1
pond run /system/etc/materialize > /tmp/730-run1.log 2>&1
cat /tmp/730-run1.log

pond describe /metrics/cpu.series > /tmp/730-describe1.txt 2>&1
cat /tmp/730-describe1.txt
check 'grep -q "Type: TablePhysicalSeries" /tmp/730-describe1.txt' \
    "materialize-series created a TablePhysicalSeries"
check '[ "$(pond describe /metrics/cpu.series | grep -cE "^ *Version [0-9]+:")" = "1" ]' \
    "first tick wrote exactly one version"
check 'grep -qE "^ *Version [0-9]+: 2 rows" /tmp/730-describe1.txt' \
    "that version holds both source rows"

echo "--- Step 2: a tick with no new rows must append nothing ---"
pond run /system/etc/ingest >/dev/null 2>&1
pond run /system/etc/materialize > /tmp/730-run2.log 2>&1
cat /tmp/730-run2.log
check '[ "$(pond describe /metrics/cpu.series | grep -cE "^ *Version [0-9]+:")" = "1" ]' \
    "no empty version was burned when the source was unchanged"

echo "--- Step 3: each new tick appends ONLY the new rows ---"
emit "2024-01-01T00:02:00Z" 1200 0.3
pond run /system/etc/ingest >/dev/null 2>&1
pond run /system/etc/materialize > /tmp/730-run3.log 2>&1
cat /tmp/730-run3.log
pond describe /metrics/cpu.series > /tmp/730-describe3.txt 2>&1
cat /tmp/730-describe3.txt
check '[ "$(pond describe /metrics/cpu.series | grep -cE "^ *Version [0-9]+:")" = "2" ]' \
    "second batch of data added exactly one more version"
check 'grep -qE "^ *Version [0-9]+: 1 rows" /tmp/730-describe3.txt' \
    "the new version holds ONLY the new row (incremental, not a rewrite)"

echo "--- Step 4: materialized content equals the derived content ---"
# --format table: the default is raw parquet bytes for a table:series.
pond cat --format table /metrics/cpu.series > /tmp/730-materialized.txt 2>&1
pond cat --format table /derived-cpu > /tmp/730-derived.txt 2>&1
cat /tmp/730-materialized.txt
MAT_ROWS=$(grep -c "2024-01-01" /tmp/730-materialized.txt || true)
DER_ROWS=$(grep -c "2024-01-01" /tmp/730-derived.txt || true)
check '[ "'"${MAT_ROWS}"'" = "3" ]' "materialized series reads back all 3 rows"
check '[ "'"${MAT_ROWS}"'" = "'"${DER_ROWS}"'" ]' \
    "materialized row count matches the derived node (${MAT_ROWS} = ${DER_ROWS})"
check 'grep -q "1200" /tmp/730-materialized.txt' "latest value is present after append"

echo "--- Step 5: the result is a real collapse candidate ---"
for i in 3 4 5 6 7 8; do
    emit "2024-01-01T00:0${i}:00Z" "$((1200 + i * 100))" "0.${i}"
    pond run /system/etc/ingest >/dev/null 2>&1
    pond run /system/etc/materialize >/dev/null 2>&1
done
BEFORE_VERSIONS=$(version_count)
check '[ "'"${BEFORE_VERSIONS}"'" -ge 6 ]' "accumulated ${BEFORE_VERSIONS} versions to collapse"

pond maintain --collapse-versions 1 > /tmp/730-collapse.log 2>&1
cat /tmp/730-collapse.log
check 'grep -qE "collapse: [1-9][0-9]* file\(s\) collapsed" /tmp/730-collapse.log' \
    "the materialized series is collapsible like any other table:series"

AFTER_ROWS=$(pond cat --format table /metrics/cpu.series 2>/dev/null | grep -c "2024-01-01" || true)
check '[ "'"${AFTER_ROWS}"'" = "9" ]' \
    "collapse preserved every materialized row (${AFTER_ROWS})"

echo "--- Step 6: source inside a dynamic-dir (selfmon's real topology) ---"
# selfmon does not put its derived nodes at the pond root: they are entries of
# a `dynamic-dir` at /derived, addressed as series:///derived/p-<pond>. Those
# children are synthesized on directory read rather than being real nodes, so
# path resolution reaches them differently. Materializing a root-level node
# proves nothing about the configuration we are actually going to deploy.
cat > /tmp/730-dyndir.yaml << 'EOF'
entries:
  - name: "p-testpond"
    factory: "sql-derived-series"
    config:
      patterns:
        m: "jsonlogs:///measure/pond.jsonl"
      query: >-
        SELECT
          CAST(ts AS TIMESTAMP)                    as timestamp,
          CAST("peak_rss.bytes" AS BIGINT)         as "peak_rss.bytes",
          CAST("list.seconds" AS DOUBLE)           as "list.seconds"
        FROM m
        ORDER BY timestamp
EOF
pond mknod dynamic-dir /derived --config-path /tmp/730-dyndir.yaml >/dev/null 2>&1

pond cat --format table /derived/p-testpond > /tmp/730-dyn-read.txt 2>&1
check 'grep -q "2024-01-01" /tmp/730-dyn-read.txt' \
    "the dynamic-dir child is readable before we try to materialize it"

cat > /tmp/730-mat2.yaml << 'EOF'
source: "series:///derived/p-testpond"
target: /metrics/dyn.series
time_column: timestamp
EOF
pond mknod materialize-series /system/etc/materialize-dyn --config-path /tmp/730-mat2.yaml >/dev/null 2>&1

pond run /system/etc/materialize-dyn > /tmp/730-run-dyn.log 2>&1 || true
cat /tmp/730-run-dyn.log
check '! grep -qi "error" /tmp/730-run-dyn.log' \
    "materializing a dynamic-dir child does not error"

pond describe /metrics/dyn.series > /tmp/730-describe-dyn.txt 2>&1 || true
cat /tmp/730-describe-dyn.txt
check 'grep -q "Type: TablePhysicalSeries" /tmp/730-describe-dyn.txt' \
    "a dynamic-dir source yields a TablePhysicalSeries too"

DYN_ROWS=$(pond cat --format table /metrics/dyn.series 2>/dev/null | grep -c "2024-01-01" || true)
check '[ "'"${DYN_ROWS}"'" = "9" ]' \
    "all 9 rows materialized through the dynamic-dir path (${DYN_ROWS})"

# Incrementality must survive the indirection, not just the happy path.
emit "2024-01-01T00:09:00Z" 2100 0.9
pond run /system/etc/ingest >/dev/null 2>&1
pond run /system/etc/materialize-dyn > /tmp/730-run-dyn2.log 2>&1
pond describe /metrics/dyn.series > /tmp/730-describe-dyn2.txt 2>&1
cat /tmp/730-describe-dyn2.txt
check 'grep -qE "^ *Version [0-9]+: 1 rows" /tmp/730-describe-dyn2.txt' \
    "the dynamic-dir path appends only the new row"

check_finish
