#!/bin/bash
# EXPERIMENT: temporal-reduce rollup vs. pack-only version maintenance
#
# DESCRIPTION:
#   Native logical-series-v2 maintenance is physical and pack-only:
#   `pond maintain --collapse-versions` publishes bounded content-addressed
#   packs but does not rewrite source Oplog rows, change the logical content
#   tip, or present a synthetic backfill to temporal-reduce. Both reduced nodes
#   below read the same maintained source; only their allowed_lateness differs.
#   Their already-built rollups must remain readable and unchanged.
#
# EXPECTED:
#   - Maintenance reports a real repack and creates local pack storage.
#   - Neither lateness setting errors after maintenance: allowed_lateness
#     governs genuinely late data, not physical packing.
#   - Both read every bucket exactly once -- 96 buckets, and a total sample
#     count equal to the 96 points ingested, not 192.
set -e
source check.sh

echo "=== Experiment: temporal-reduce collapse vs allowed_lateness ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

PACKS=/pond/data/_packs
count_pack_objects() {
    find "${PACKS}/objects" -type f 2>/dev/null | wc -l | tr -d ' '
}

# ---- Source: 96 hourly OTelJSON well_depth samples spanning 4 days ----------
# 2026-07-15T00:00Z (epoch 1784073600) .. 2026-07-18T23:00Z, one point/hour.
# Four days comfortably exceeds the default 1-day lateness, so a collapsed
# full-range version backfills buckets already sealed by the first read.
mkdir -p /var/log/well
awk 'BEGIN {
    start = 1784073600;
    for (i = 0; i < 96; i++) {
        e = start + i * 3600;
        v = 44.0 + (i % 5) * 0.1;
        printf "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[{\"name\":\"well_depth_value\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"%d000000000\",\"asDouble\":%.1f}]}}]}]}]}\n", e, v;
    }
}' > /tmp/050-all.jsonl

cat > /tmp/050-ingest.yaml << 'EOF'
archived_pattern: /var/log/well/well.json.*
active_pattern: /var/log/well/well.json
pond_path: /ingest
EOF

pond mkdir -p /system/run >/dev/null
pond mkdir -p /ingest >/dev/null
pond mknod logfile-ingest /system/run/10-well --config-path /tmp/050-ingest.yaml >/dev/null

# ---- Accumulate multiple source versions (24 samples per ingest run) --------
# Multiple append versions give pack maintenance a genuine over-threshold
# native series, mirroring the many hourly ingest versions on water-staging.
: > /var/log/well/well.json
split -l 24 /tmp/050-all.jsonl /tmp/050-chunk.
for c in /tmp/050-chunk.*; do
    cat "$c" >> /var/log/well/well.json
    pond run /system/run/10-well >/dev/null 2>&1
done

ROWS=$(pond cat oteljson:///ingest/well.json --format=table \
  --sql "SELECT COUNT(*) AS c FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

# ---- Two reduced nodes over the same source, differing only in lateness -----
cat > /tmp/050-reduce-default.yaml << 'YAML'
in_pattern: "oteljson:///ingest/well.json"
out_pattern: "data"
time_column: "timestamp"
resolutions: ["1h"]
aggregations:
  - type: "avg"
    columns: ["well_depth_value"]
  - type: "count"
    columns: ["well_depth_value"]
YAML

cat > /tmp/050-reduce-late.yaml << 'YAML'
in_pattern: "oteljson:///ingest/well.json"
out_pattern: "data"
time_column: "timestamp"
resolutions: ["1h"]
allowed_lateness: 14d
aggregations:
  - type: "avg"
    columns: ["well_depth_value"]
  - type: "count"
    columns: ["well_depth_value"]
YAML

pond mknod temporal-reduce /reduced-default --config-path /tmp/050-reduce-default.yaml >/dev/null
pond mknod temporal-reduce /reduced-late --config-path /tmp/050-reduce-late.yaml >/dev/null

# ---- Seal both rollups by reading them BEFORE maintenance ------------------
# The first read builds the rollup cache and seals buckets older than
# newest - allowed_lateness.
echo ""
echo "--- Seal both rollups (first read) ---"
SEAL_DEFAULT=$(pond cat /reduced-default/data/res=1h.series --format=table \
  --sql "SELECT COUNT(*) AS c FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)
SEAL_LATE=$(pond cat /reduced-late/data/res=1h.series --format=table \
  --sql "SELECT COUNT(*) AS c FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

# ---- Repack the source versions without changing logical history ------------
echo ""
echo "--- maintain --collapse-versions 1 (pack-only physical maintenance) ---"
BEFORE_PACKS=$(count_pack_objects)
pond maintain --collapse-versions 1 > /tmp/050-collapse.log 2>&1
grep -iE "pack maintenance|packs:" /tmp/050-collapse.log || true
AFTER_PACKS=$(count_pack_objects)

# ---- Second read: neither lateness may error, neither may double-count ------
echo ""
echo "--- Second read: default (1d) lateness ---"
DEFAULT_OUT=$(pond cat /reduced-default/data/res=1h.series --format=table \
  --sql "SELECT COUNT(*) AS c, SUM(\"well_depth_value.count\") AS n FROM source" 2>&1 || true)
echo "$DEFAULT_OUT"
DEFAULT_ROWS=$(echo "$DEFAULT_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$2); print $2}')
DEFAULT_N=$(echo "$DEFAULT_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')

echo ""
echo "--- Second read: allowed_lateness=14d ---"
LATE_OUT=$(pond cat /reduced-late/data/res=1h.series --format=table \
  --sql "SELECT COUNT(*) AS c, SUM(\"well_depth_value.count\") AS n FROM source" 2>&1 || true)
echo "$LATE_OUT"
LATE_ROWS=$(echo "$LATE_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$2); print $2}')
LATE_N=$(echo "$LATE_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')

# ---- Verify -----------------------------------------------------------------
echo ""
echo "--- Verification ---"

check '[ "${ROWS}" = "96" ]' \
  "source ingested 96 hourly points across versions, got ${ROWS}"

check 'grep -qE "pack maintenance: [1-9][0-9]* candidate\(s\), [1-9][0-9]* repacked" /tmp/050-collapse.log' \
  "maintain repacked the source into a bounded physical layout"

check 'grep -qE "packs: [1-9][0-9]* object\(s\) written" /tmp/050-collapse.log' \
  "maintain reports new physical pack objects"

check '[ "'"${AFTER_PACKS}"'" -gt "'"${BEFORE_PACKS}"'" ]' \
  "pack maintenance created local pack objects (${BEFORE_PACKS} -> ${AFTER_PACKS})"

check '[ "${SEAL_DEFAULT}" = "96" ] && [ "${SEAL_LATE}" = "96" ]' \
  "both rollups read all 96 buckets on the sealing pass"

check '! echo "$DEFAULT_OUT" | grep -qiE "backfills|--rebuild"' \
  "default 1d lateness remains readable after physical maintenance"

check '! echo "$LATE_OUT" | grep -qiE "backfills|--rebuild"' \
  "allowed_lateness=14d remains readable after physical maintenance"

check '[ "${DEFAULT_ROWS}" = "96" ] && [ "${LATE_ROWS}" = "96" ]' \
  "both reduced series still read all 96 buckets after maintenance, got ${DEFAULT_ROWS} / ${LATE_ROWS}"

# Avg and bucket count alone can hide duplication, so retain a total-count
# invariant across physical maintenance as the stronger cache-integrity check.
check '[ "${DEFAULT_N}" = "96" ] && [ "${LATE_N}" = "96" ]' \
  "each source point is counted exactly once after maintenance, got ${DEFAULT_N} / ${LATE_N}"

check_finish
