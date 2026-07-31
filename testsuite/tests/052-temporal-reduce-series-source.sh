#!/bin/bash
# EXPERIMENT: a temporal-reduce over a pond-native `series:///` source uses the
#             partial-aggregate cache, instead of silently rescanning history.
#
# DESCRIPTION:
#   The rollup's whole point is that a new source version costs O(new rows)
#   instead of O(history): decomposable partials are folded per time bucket and
#   sealed below a watermark, so an incremental read never re-reads old data.
#
#   Until this test, that path was reachable only from a FORMAT provider
#   (`oteljson:///`, `jsonlogs:///`, ...). The precondition asked
#   `FormatRegistry::get_provider(scheme)`, which is a question about text
#   parsing, and used the answer to decide something else entirely: whether the
#   source's rows are reachable per version. For a format provider those two
#   coincide, because the format cache memoizes the parse per version. For a
#   pond-native `series:///` source they do not -- it is ALREADY columnar
#   Parquet, one file per version, which is the ideal input -- so it failed the
#   test and fell back to the single-pass delegate that reads everything, every
#   time.
#
#   That is exactly the shape selfmon runs: `materialize-series` stores a
#   derived signal as a physical table:series, and the reductions over it are
#   the ones that grow without bound. This test is that pipeline in miniature
#   (it extends test 730 by one node).
#
#   The failure being guarded is silent. The reduce returns CORRECT numbers
#   either way -- the delegate computes the same aggregate, just by rescanning
#   all history -- so no assertion on values can distinguish the two paths.
#   Only the cache artifacts can: the rollup writes `merged_<cfg>_<node>/res<N>/`
#   under the pond cache, and the delegate writes nothing. Asserting on totals
#   alone would pass on both branches and prove nothing.
#
# EXPECTED:
#   - The reduce over `series:///metrics/cpu.series` returns one bucket per
#     minute, totalling every emitted sample.
#   - A `merged_*/res60` directory exists afterwards, proving the rollup path
#     (not the single-pass delegate) served the read.
#   - After a second tick appends a new source version, the totals include the
#     new samples -- the incremental path stays correct, not just fast.
set -e
source check.sh

echo "=== Experiment: temporal-reduce over a pond-native series source ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

emit() {
    printf '{"ts":"%s","peak_rss.bytes":"%s","list.seconds":"%s"}\n' "$1" "$2" "$3" \
        >> /var/log/metrics/pond.jsonl
}

mkdir -p /var/log/metrics
: > /var/log/metrics/pond.jsonl

pond mkdir -p /system/etc >/dev/null 2>&1
pond mkdir -p /measure >/dev/null 2>&1
pond mkdir -p /metrics >/dev/null 2>&1

# ---- The selfmon pipeline in miniature, as in test 730 ----------------------
cat > /tmp/052-ingest.yaml << 'EOF'
archived_pattern: /var/log/metrics/pond.jsonl.*
active_pattern: /var/log/metrics/pond.jsonl
pond_path: /measure
EOF
pond mknod logfile-ingest /system/etc/ingest --config-path /tmp/052-ingest.yaml >/dev/null 2>&1

cat > /tmp/052-derived.yaml << 'EOF'
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
pond mknod sql-derived-series /derived-cpu --config-path /tmp/052-derived.yaml >/dev/null 2>&1

cat > /tmp/052-materialize.yaml << 'EOF'
source: "series:///derived-cpu"
target: /metrics/cpu.series
time_column: timestamp
EOF
pond mknod materialize-series /system/etc/materialize --config-path /tmp/052-materialize.yaml >/dev/null 2>&1

# ---- The node under test: a reduce whose source is the physical series ------
# `seal_target_bytes: 0` forces a seal on every build, so a handful of rows is
# enough to produce segment files. It is a cache-layout knob and changes
# nothing about the answer.
cat > /tmp/052-reduce.yaml << 'EOF'
in_pattern: "series:///metrics/cpu.series"
out_pattern: "data"
time_column: "timestamp"
resolutions: ["1m"]
seal_target_bytes: 0
aggregations:
  - type: "sum"
    columns: ["peak_rss.bytes"]
  - type: "count"
    columns: ["peak_rss.bytes"]
EOF
pond mknod temporal-reduce /reduced --config-path /tmp/052-reduce.yaml >/dev/null 2>&1

totals() {
    pond cat /reduced/data/res=1m.series --format=table --sql \
      'SELECT CAST(COUNT(*) AS BIGINT) AS b, CAST(SUM("peak_rss.bytes.count") AS BIGINT) AS n FROM source' 2>&1
}

field() {
    echo "$1" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$'"$2"'); print $'"$2"'}'
}

echo ""
echo "--- Tick 1: two samples, one minute apart ---"
emit "2024-01-01T00:00:00Z" 1000 0.1
emit "2024-01-01T00:01:00Z" 1100 0.2
pond run /system/etc/ingest >/dev/null 2>&1
pond run /system/etc/materialize >/dev/null 2>&1

OUT1=$(totals)
echo "$OUT1"
B1=$(field "$OUT1" 2)
N1=$(field "$OUT1" 3)

check '[ "'"${B1}"'" = "2" ]' "two one-minute buckets"
check '[ "'"${N1}"'" = "2" ]' "totalling both samples"

# The load-bearing assertion. `merged_*` is written ONLY by the rollup path;
# the single-pass delegate leaves no cache behind, so this distinguishes the
# two implementations where the numbers above cannot.
MERGED=$(find /pond -type d -path '*/merged_*/res60' 2>/dev/null | wc -l | tr -d ' ')
echo "partial-aggregate cache directories: ${MERGED}"
check '[ "'"${MERGED}"'" -ge 1 ]' \
    "series source served by the rollup, not the single-pass delegate"

echo ""
echo "--- Tick 2: a new source version must be folded in, not lost ---"
emit "2024-01-01T00:02:00Z" 1200 0.3
emit "2024-01-01T00:03:00Z" 1300 0.4
pond run /system/etc/ingest >/dev/null 2>&1
pond run /system/etc/materialize >/dev/null 2>&1

OUT2=$(totals)
echo "$OUT2"
B2=$(field "$OUT2" 2)
N2=$(field "$OUT2" 3)

check '[ "'"${B2}"'" = "4" ]' "four one-minute buckets after the second tick"
check '[ "'"${N2}"'" = "4" ]' \
    "every sample counted exactly once across the incremental rebuild"

echo ""
echo "=== PASS ==="
