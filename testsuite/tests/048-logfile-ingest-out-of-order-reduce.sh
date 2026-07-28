#!/bin/bash
# EXPERIMENT: logfile-ingest of OUT-OF-ORDER data feeding temporal-reduce.
# DESCRIPTION: Reproduces the recurring watershop production failure where a
#   collector flushes a buffered older sample AFTER a newer one, so the active
#   log file grows with a late, earlier-timestamped record. logfile-ingest is
#   strictly append-only: it stores each run's new tail as a new
#   FilePhysicalSeries version, faithfully preserving the collector's write
#   order. The late record therefore lands in a LATER source version whose
#   earliest time bucket PRECEDES a bucket already sealed by an earlier version.
#
#   The temporal-reduce incremental rollup used to reject this with:
#
#     temporal-reduce rollup: source node ... backfills already-sealed buckets
#     (earliest bucket ... precedes frontier ...); ... Re-run the export with
#     --rebuild ...
#
#   which froze every reduced read at the frontier until a manual
#   `pond export --rebuild`. The fix recognizes that FilePhysicalSeries versions
#   are DISJOINT append deltas, so per-version partials merge by GROUP BY
#   time_bucket regardless of arrival order; the sequentiality guard now applies
#   only to overlapping (non-series) sources.
#
# EXPECTED: Reading the reduced 1h series succeeds (no sealed-bucket error) and
#   the late-arriving earlier bucket is present with correct aggregates, exactly
#   as a single-pass GROUP BY would compute.
set -e
source check.sh

echo "=== Experiment: logfile-ingest out-of-order data -> temporal-reduce ==="

pond init --birthplace test-host

# ---- Setup: host active log file (newline-delimited OTelJSON) ----
#
# Two hourly buckets, two samples each. timeUnixNano values:
#   08:00:00Z = 1704096000000000000   08:15:00Z = 1704096900000000000
#   09:00:00Z = 1704099600000000000   09:15:00Z = 1704100500000000000
#
# Run 1 writes the NEWER (09:00) bucket first, so the rollup seals it. Run 2
# appends the OLDER (08:00) bucket -- the out-of-order / late data.

mkdir -p /var/log/well

cat > /var/log/well/well.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704099600000000000","asDouble":44.0}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704100500000000000","asDouble":44.2}]}}]}]}]}
OTELJSON

cat > /tmp/ingest.yaml << 'EOF'
archived_pattern: /var/log/well/well.json.*
active_pattern: /var/log/well/well.json
pond_path: /ingest
EOF

pond mkdir -p /system/run
pond mkdir -p /ingest
pond mknod logfile-ingest /system/run/10-well --config-path /tmp/ingest.yaml

# ---- Run 1: ingest the newer bucket (version 1) ----
echo ""
echo "--- Run 1: ingest newer (09:00) bucket ---"
RUST_LOG=info pond run /system/run/10-well 2>&1 | tee /tmp/run1.log

# ---- Append OUT-OF-ORDER older data, then Run 2 (version 2) ----
echo ""
echo "--- Appending late (08:00) bucket to the active file ---"
cat >> /var/log/well/well.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704096000000000000","asDouble":42.0}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704096900000000000","asDouble":42.4}]}}]}]}]}
OTELJSON

echo ""
echo "--- Run 2: append late (08:00) bucket as a second version ---"
RUST_LOG=info pond run /system/run/10-well 2>&1 | tee /tmp/run2.log

# ---- Confirm the out-of-order data really entered via the append path ----
echo ""
echo "--- Ingested file ---"
pond list '/ingest/*'

# All four points must be present (both versions read through oteljson://).
ROWS=$(pond cat oteljson:///ingest/well.json --format=table \
  --sql "SELECT COUNT(*) AS c FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

# ---- temporal-reduce over the append-only series source ----
cat > /tmp/reduce.yaml << 'YAML'
in_pattern: "oteljson:///ingest/well.json"
out_pattern: "data"
time_column: "timestamp"
resolutions: ["1h"]
aggregations:
  - type: "avg"
    columns: ["well_depth_value"]
  - type: "min"
    columns: ["well_depth_value"]
  - type: "max"
    columns: ["well_depth_value"]
  - type: "count"
    columns: ["well_depth_value"]
YAML

pond mknod temporal-reduce /reduced --config-path /tmp/reduce.yaml

# Reading the series drives the incremental per-version rollup partial path that
# the sealed-bucket guard used to reject.
echo ""
echo "--- Read reduced 1h series (drives the rollup) ---"
CAT_OUT=$(pond cat /reduced/data/res=1h.series --format=table --sql "
  SELECT \"well_depth_value.avg\", \"well_depth_value.min\", \"well_depth_value.max\"
  FROM source ORDER BY timestamp" 2>&1 || true)
echo "$CAT_OUT"

BUCKETS=$(pond cat /reduced/data/res=1h.series --format=table \
  --sql "SELECT COUNT(*) AS c FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

# Earliest bucket (08:00) is the LATE-arriving one; it must be present & correct.
EARLY_AVG=$(pond cat /reduced/data/res=1h.series --format=table --sql "
  SELECT \"well_depth_value.avg\" AS v FROM source ORDER BY timestamp LIMIT 1" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+\.[0-9]+' | head -1)

LATE_AVG=$(pond cat /reduced/data/res=1h.series --format=table --sql "
  SELECT \"well_depth_value.avg\" AS v FROM source ORDER BY timestamp DESC LIMIT 1" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+\.[0-9]+' | head -1)

# Totals, not averages: avg = SUM(sum)/SUM(count) is invariant under a
# double-count, so it cannot distinguish "counted once" from "counted twice"
# (see test 733). Only a total sample count can.
TOTAL_N=$(pond cat /reduced/data/res=1h.series --format=table \
  --sql "SELECT CAST(SUM(\"well_depth_value.count\") AS BIGINT) AS n FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

# ---- Verify ----
echo ""
echo "--- Verification ---"

check 'grep -qiE "append" /tmp/run2.log' \
  "run 2 ingested the late data via the append path (new series version)"

check '[ "${ROWS}" = "4" ]' \
  "all four points readable through oteljson:// (both versions), got ${ROWS}"

check '! echo "$CAT_OUT" | grep -qiE "backfills|precedes frontier|--rebuild|sealed"' \
  "out-of-order series data does NOT trip the sealed-bucket guard"

check '! echo "$CAT_OUT" | grep -qi "error"' \
  "reading the reduced series raises no error"

check '[ "${BUCKETS}" = "2" ]' \
  "two hourly buckets produced (08:00 and 09:00), got ${BUCKETS}"

check '[ "${EARLY_AVG}" = "42.2" ]' \
  "late-arriving 08:00 bucket reconstructs avg(42.0,42.4)=42.2, got ${EARLY_AVG}"

check '[ "${LATE_AVG}" = "44.1" ]' \
  "09:00 bucket reconstructs avg(44.0,44.2)=44.1, got ${LATE_AVG}"

check '[ "${TOTAL_N}" = "4" ]' \
  "each of the 4 source samples is counted exactly once after the late append, got ${TOTAL_N}"

check_finish
