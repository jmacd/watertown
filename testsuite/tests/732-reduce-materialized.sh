#!/bin/bash
# EXPERIMENT: temporal-reduce over a materialized table:series.
#
# DESCRIPTION:
#   selfmon exports its `perf` timeseries-join at raw 1-minute resolution:
#   525k rows x 94 columns in a single parquet row group, which the browser
#   must decode in one shot before it can draw anything. Every other dashboard
#   (water, septic) exports a temporal-reduce tree instead and lets chart.js
#   pick a resolution, so this is the missing step.
#
#   selfmon cannot reduce its raw source directly: jsonlogs returns all-Utf8,
#   and the CASTs live in the sql-derived-series layer. The materialized
#   table:series is the first TYPED, PHYSICAL thing in that pipeline, so it is
#   the natural reduce input.
#
#   Note what this does NOT get: try_rollup_table_provider (temporal_reduce.rs
#   ~:450) bails unless the input scheme has a FormatProvider, and series/file/
#   table/data are builtin schemes, not format providers. So reducing over a
#   pond-native series never engages the rollup cache. The win here is that the
#   source scan is bytes off a parquet series instead of a recomputed join.
#
# EXPECTED:
#   - temporal-reduce accepts a `series:///` physical table:series input.
#   - It emits one series per configured resolution.
#   - Coarser resolutions really do hold fewer rows than the source.
#   - Values are aggregated, not truncated: a 1h bucket averages its samples.
#
# History:
#   Added on jmacd/analysis8 to check the selfmon metrics.html load-time fix
#   before touching caspar.water config.
set -e
source check.sh

echo "=== Experiment: reduce a materialized series ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

mkdir -p /var/log/metrics
: > /var/log/metrics/pond.jsonl
emit() {
    printf '{"ts":"%s","peak_rss.bytes":"%s","list.seconds":"%s"}\n' "$1" "$2" "$3" \
        >> /var/log/metrics/pond.jsonl
}

pond mkdir -p /system/etc >/dev/null 2>&1
pond mkdir -p /measure >/dev/null 2>&1
pond mkdir -p /metrics >/dev/null 2>&1

cat > /tmp/732-ingest.yaml << 'EOF'
archived_pattern: /var/log/metrics/pond.jsonl.*
active_pattern: /var/log/metrics/pond.jsonl
pond_path: /measure
EOF
pond mknod logfile-ingest /system/etc/ingest --config-path /tmp/732-ingest.yaml >/dev/null 2>&1

cat > /tmp/732-derived.yaml << 'EOF'
patterns:
  m: "jsonlogs:///measure/pond.jsonl"
query: >-
  SELECT
    CAST(ts AS TIMESTAMP)            as timestamp,
    CAST("peak_rss.bytes" AS BIGINT) as "peak_rss.bytes",
    CAST("list.seconds" AS DOUBLE)   as "list.seconds"
  FROM m
  ORDER BY timestamp
EOF
pond mknod sql-derived-series /derived-cpu --config-path /tmp/732-derived.yaml >/dev/null 2>&1

cat > /tmp/732-materialize.yaml << 'EOF'
source: "series:///derived-cpu"
target: /metrics/perf.series
time_column: timestamp
EOF
pond mknod materialize-series /system/etc/materialize --config-path /tmp/732-materialize.yaml >/dev/null 2>&1

# 120 samples one minute apart = two full hours, so 1h buckets are unambiguous.
MIN=0
while [ $MIN -lt 120 ]; do
    H=$((MIN / 60)); M=$((MIN % 60))
    emit "$(printf '2024-01-01T%02d:%02d:00Z' $H $M)" "$((1000 + MIN))" "0.5"
    MIN=$((MIN + 1))
done
pond run /system/etc/ingest >/dev/null 2>&1
pond run /system/etc/materialize >/dev/null 2>&1

SRC_ROWS=$(pond cat --format table /metrics/perf.series 2>/dev/null | grep -c "2024-01-01" || true)
check '[ "'"${SRC_ROWS}"'" = "120" ]' "materialized source holds 120 one-minute samples (${SRC_ROWS})"

echo "--- reduce over the materialized series ---"
cat > /tmp/732-reduce.yaml << 'EOF'
entries:
  - name: "perf"
    factory: "temporal-reduce"
    config:
      in_pattern: "series:///metrics/perf.series"
      out_pattern: "data"
      time_column: "timestamp"
      resolutions: [1m, 10m, 1h]
      aggregations:
        - type: "avg"
EOF
pond mknod dynamic-dir /reduced --config-path /tmp/732-reduce.yaml >/dev/null 2>&1

pond list "/reduced/perf/data/*" > /tmp/732-list.txt 2>&1 || true
cat /tmp/732-list.txt
check 'grep -q "res=1h" /tmp/732-list.txt' "temporal-reduce accepted the series:/// input and emitted res=1h"
check 'grep -q "res=10m" /tmp/732-list.txt' "res=10m emitted"
check 'grep -q "res=1m" /tmp/732-list.txt' "res=1m emitted"

count_rows() {
    pond cat --format table "$1" 2>/dev/null | grep -c "2024-01-01" || true
}
R1M=$(count_rows /reduced/perf/data/res=1m.series)
R10M=$(count_rows /reduced/perf/data/res=10m.series)
R1H=$(count_rows /reduced/perf/data/res=1h.series)
echo "rows: 1m=${R1M} 10m=${R10M} 1h=${R1H}"

check '[ "'"${R1M}"'" = "120" ]' "res=1m keeps every sample (${R1M})"
check '[ "'"${R10M}"'" = "12" ]' "res=10m collapses to 12 buckets (${R10M})"
check '[ "'"${R1H}"'" = "2" ]' "res=1h collapses to 2 buckets (${R1H})"
check '[ "'"${R1H}"'" -lt "'"${R1M}"'" ]' "coarser resolution really is smaller"

# The point of reducing is aggregation, not sampling: minute k carries
# peak_rss 1000+k, so hour 0 must average to 1000+29.5 = 1029.5.
pond cat --format table /reduced/perf/data/res=1h.series > /tmp/732-1h.txt 2>&1
cat /tmp/732-1h.txt
check 'grep -q "1029.5" /tmp/732-1h.txt' "the 1h bucket AVERAGES its 60 samples (1029.5)"

echo "--- Step 2: the real selfmon shape (join -> materialize -> reduce) ---"
# selfmon does not export a single pond's series: it exports `perf`, a
# timeseries-join FULL OUTER joining every pond on timestamp. That join is the
# node we would materialize in production, and a join is a different factory
# from the sql-derived-series covered above -- so cover it here rather than
# assume the two behave alike.
: > /var/log/metrics/other.jsonl
emit_other() {
    printf '{"ts":"%s","peak_rss.bytes":"%s","list.seconds":"%s"}\n' "$1" "$2" "$3" \
        >> /var/log/metrics/other.jsonl
}
MIN=0
while [ $MIN -lt 120 ]; do
    H=$((MIN / 60)); M=$((MIN % 60))
    emit_other "$(printf '2024-01-01T%02d:%02d:00Z' $H $M)" "$((5000 + MIN))" "0.9"
    MIN=$((MIN + 1))
done

cat > /tmp/732-ingest2.yaml << 'EOF'
archived_pattern: /var/log/metrics/other.jsonl.*
active_pattern: /var/log/metrics/other.jsonl
pond_path: /measure
EOF
pond mknod logfile-ingest /system/etc/ingest2 --config-path /tmp/732-ingest2.yaml >/dev/null 2>&1
pond run /system/etc/ingest2 >/dev/null 2>&1

cat > /tmp/732-derived2.yaml << 'EOF'
patterns:
  m: "jsonlogs:///measure/other.jsonl"
query: >-
  SELECT
    CAST(ts AS TIMESTAMP)            as timestamp,
    CAST("peak_rss.bytes" AS BIGINT) as "peak_rss.bytes",
    CAST("list.seconds" AS DOUBLE)   as "list.seconds"
  FROM m
  ORDER BY timestamp
EOF
pond mknod sql-derived-series /derived-other --config-path /tmp/732-derived2.yaml >/dev/null 2>&1

cat > /tmp/732-join.yaml << 'EOF'
inputs:
  - pattern: "series:///derived-cpu"
    scope: "pond-a"
  - pattern: "series:///derived-other"
    scope: "pond-b"
EOF
pond mknod timeseries-join /perf --config-path /tmp/732-join.yaml >/dev/null 2>&1

pond cat --format table /perf > /tmp/732-join.txt 2>&1
head -5 /tmp/732-join.txt
check 'grep -q "pond-a" /tmp/732-join.txt' "the join scopes columns per pond"
check 'grep -q "pond-b" /tmp/732-join.txt' "both ponds are present in the join"

cat > /tmp/732-mat-join.yaml << 'EOF'
source: "series:///perf"
target: /metrics/joined.series
time_column: timestamp
EOF
pond mknod materialize-series /system/etc/materialize-join --config-path /tmp/732-mat-join.yaml >/dev/null 2>&1
pond run /system/etc/materialize-join > /tmp/732-matjoin.log 2>&1
cat /tmp/732-matjoin.log
check '! grep -qi "error" /tmp/732-matjoin.log' "a timeseries-join can be materialized"

pond describe /metrics/joined.series > /tmp/732-desc-join.txt 2>&1
check 'grep -q "Type: TablePhysicalSeries" /tmp/732-desc-join.txt' \
    "the materialized join is a TablePhysicalSeries"
JOIN_ROWS=$(count_rows /metrics/joined.series)
check '[ "'"${JOIN_ROWS}"'" = "120" ]' "the materialized join holds all 120 joined rows (${JOIN_ROWS})"

cat > /tmp/732-reduce2.yaml << 'EOF'
entries:
  - name: "perf"
    factory: "temporal-reduce"
    config:
      in_pattern: "series:///metrics/joined.series"
      out_pattern: "data"
      time_column: "timestamp"
      resolutions: [1m, 10m, 1h]
      aggregations:
        - type: "avg"
EOF
pond mknod dynamic-dir /reduced-join --config-path /tmp/732-reduce2.yaml >/dev/null 2>&1

pond cat --format table "/reduced-join/perf/data/res=1h.series" > /tmp/732-join-1h.txt 2>&1
cat /tmp/732-join-1h.txt
JR1H=$(grep -c "2024-01-01" /tmp/732-join-1h.txt || true)
check '[ "'"${JR1H}"'" = "2" ]' "the joined+materialized series reduces to 2 hourly buckets (${JR1H})"
check 'grep -q "1029.5" /tmp/732-join-1h.txt' "pond-a averages correctly through the whole chain"
check 'grep -q "5029.5" /tmp/732-join-1h.txt' "pond-b averages correctly through the whole chain"

check_finish
