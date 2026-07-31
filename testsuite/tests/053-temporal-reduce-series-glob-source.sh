#!/bin/bash
# EXPERIMENT: a temporal-reduce whose glob matches SEVERAL pond-native series
#             must aggregate all of them into one output, exactly once.
#
# DESCRIPTION:
#   Test 052 covers the single-source case, which is what selfmon's
#   `/reduced/perf` runs. But a reduce with a glob `in_pattern` and a CONSTANT
#   `out_pattern` folds every matched file into ONE output (test 046 does this
#   for `oteljson:///`), and that takes a different branch: instead of one
#   node's version parquets, the scan is the union of the pruned version
#   parquets of every matched node.
#
#   That union is where a series source can go wrong in ways the single-source
#   case cannot:
#     - listing only the first matched node, silently halving the totals;
#     - listing a node's whole history instead of its pruned versions, which is
#       correct but defeats the incremental property;
#     - failing to merge schemas across nodes, so the aggregation references a
#       column that is absent from one member.
#
#   Both series carry the same schema and the same timestamps here, so each
#   bucket must show exactly TWO samples. One is an undercount (a dropped
#   source), three or four is a double-count (a version listed twice, which is
#   the specific failure mode version collapse creates).
#
# EXPECTED:
#   - Two one-minute buckets, each totalling 2 samples, 4 in all.
#   - The partial-aggregate cache is engaged (`merged_*/res60` exists), so this
#     exercises the union branch of the rollup and not the single-pass delegate.
#   - After appending to BOTH sources, the totals grow to 8 with no double
#     counting across the incremental rebuild.
set -e
source check.sh

echo "=== Experiment: reduce a glob over several pond-native series ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

emit() {
    printf '{"ts":"%s","peak_rss.bytes":"%s"}\n' "$1" "$2" \
        >> /var/log/metrics/pond.jsonl
}

mkdir -p /var/log/metrics
: > /var/log/metrics/pond.jsonl

pond mkdir -p /system/etc >/dev/null 2>&1
pond mkdir -p /measure >/dev/null 2>&1
pond mkdir -p /metrics >/dev/null 2>&1

cat > /tmp/053-ingest.yaml << 'EOF'
archived_pattern: /var/log/metrics/pond.jsonl.*
active_pattern: /var/log/metrics/pond.jsonl
pond_path: /measure
EOF
pond mknod logfile-ingest /system/etc/ingest --config-path /tmp/053-ingest.yaml >/dev/null 2>&1

# Two derived signals over the same rows, with identical schemas, so the union
# of the two materialized series is exactly twice the samples.
for n in a b; do
    cat > /tmp/053-derived-$n.yaml << EOF
patterns:
  m: "jsonlogs:///measure/pond.jsonl"
query: >-
  SELECT
    CAST(ts AS TIMESTAMP)            as timestamp,
    CAST("peak_rss.bytes" AS BIGINT) as "peak_rss.bytes"
  FROM m
  ORDER BY timestamp
EOF
    pond mknod sql-derived-series /derived-$n --config-path /tmp/053-derived-$n.yaml >/dev/null 2>&1

    cat > /tmp/053-materialize-$n.yaml << EOF
source: "series:///derived-$n"
target: /metrics/$n.series
time_column: timestamp
EOF
    pond mknod materialize-series /system/etc/materialize-$n \
        --config-path /tmp/053-materialize-$n.yaml >/dev/null 2>&1
done

# The node under test: a GLOB over both series, folded into one output.
cat > /tmp/053-reduce.yaml << 'EOF'
in_pattern: "series:///metrics/*.series"
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
pond mknod temporal-reduce /reduced --config-path /tmp/053-reduce.yaml >/dev/null 2>&1

totals() {
    pond cat /reduced/data/res=1m.series --format=table --sql \
      'SELECT CAST(COUNT(*) AS BIGINT) AS b, CAST(SUM("peak_rss.bytes.count") AS BIGINT) AS n FROM source' 2>&1
}

field() {
    echo "$1" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$'"$2"'); print $'"$2"'}'
}

tick() {
    pond run /system/etc/ingest >/dev/null 2>&1
    pond run /system/etc/materialize-a >/dev/null 2>&1
    pond run /system/etc/materialize-b >/dev/null 2>&1
}

echo ""
echo "--- Tick 1: two samples, present in BOTH series ---"
emit "2024-01-01T00:00:00Z" 1000
emit "2024-01-01T00:01:00Z" 1100
tick

OUT1=$(totals)
echo "$OUT1"
check '[ "'"$(field "$OUT1" 2)"'" = "2" ]' "two one-minute buckets"
check '[ "'"$(field "$OUT1" 3)"'" = "4" ]' \
    "both series counted, each exactly once (2 buckets x 2 sources)"

MERGED=$(find /pond -type d -path '*/merged_*/res60' 2>/dev/null | wc -l | tr -d ' ')
echo "partial-aggregate cache directories: ${MERGED}"
check '[ "'"${MERGED}"'" -ge 1 ]' \
    "glob over series sources served by the rollup union, not the delegate"

echo ""
echo "--- Tick 2: append to both, incrementally ---"
emit "2024-01-01T00:02:00Z" 1200
emit "2024-01-01T00:03:00Z" 1300
tick

OUT2=$(totals)
echo "$OUT2"
check '[ "'"$(field "$OUT2" 2)"'" = "4" ]' "four one-minute buckets"
check '[ "'"$(field "$OUT2" 3)"'" = "8" ]' \
    "no source dropped and no version double-counted after the rebuild"

echo ""
echo "=== PASS ==="
