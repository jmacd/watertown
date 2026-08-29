#!/bin/bash
# EXPERIMENT: pack-only version maintenance preserves temporal-reduce rollups.
#
# DESCRIPTION:
#   temporal-reduce's rollup cache stores one partial per immutable source
#   logical leaf. Native logical-series-v2 maintenance is pack-only: it creates
#   a bounded physical pack but does not add, merge, supersede, or delete those
#   source leaves. The cached whole-series sum and count must remain unchanged,
#   and a repeated maintenance pass must recognize the source as already
#   physically bounded.
#
# EXPECTED:
#   - Before maintenance, the reduced series totals 96 samples summing to 96.0.
#   - `maintain --collapse-versions` reports a real repack and creates pack
#     objects without changing the source's Oplog version.
#   - After maintenance those totals are unchanged.
#   - A second pass reports 0 repacked / already bounded and writes no new pack.
#
# History:
#   Added on jmacd/analysis8 while reviewing the size-tiered collapse work. The
#   defect predates that branch -- temporal_reduce.rs and rollup_cache.rs are
#   untouched by it -- but tiering makes collapse fire more often, and the
#   branch fixed the sibling instance of this bug in the format cache only.
set -e
source check.sh

echo "=== Experiment: collapse must not double-count rollup partials ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

PACKS=/pond/data/_packs
count_pack_objects() {
    find "${PACKS}/objects" -type f 2>/dev/null | wc -l | tr -d ' '
}

# ---- Source: 96 hourly OTelJSON samples, every value exactly 1.0 ------------
# A constant 1.0 makes the arithmetic unambiguous: over the whole series
# SUM(well_depth_value.sum) must be 96.0 and SUM(well_depth_value.count) must
# be 96, whatever the bucketing. Any deviation is double-counting, not rounding.
mkdir -p /var/log/well
awk 'BEGIN {
    start = 1784073600;   # 2026-07-15T00:00:00Z
    for (i = 0; i < 96; i++) {
        e = start + i * 3600;
        printf "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[{\"name\":\"well_depth_value\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"%d000000000\",\"asDouble\":1.0}]}}]}]}]}\n", e;
    }
}' > /tmp/733-all.jsonl

cat > /tmp/733-ingest.yaml << 'EOF'
archived_pattern: /var/log/well/well.json.*
active_pattern: /var/log/well/well.json
pond_path: /ingest
EOF

pond mkdir -p /system/run >/dev/null
pond mkdir -p /ingest >/dev/null
pond mknod logfile-ingest /system/run/10-well --config-path /tmp/733-ingest.yaml >/dev/null

# ---- Accumulate several source versions (24 samples per ingest run) ---------
# One append version per run, so `maintain --collapse-versions` has a genuine
# over-threshold series to pack -- the hourly-ingest shape of water/septic.
: > /var/log/well/well.json
split -l 24 /tmp/733-all.jsonl /tmp/733-chunk.
for c in /tmp/733-chunk.*; do
    cat "$c" >> /var/log/well/well.json
    pond run /system/run/10-well >/dev/null 2>&1
done

SRC_ROWS=$(pond cat oteljson:///ingest/well.json --format=table \
  --sql "SELECT COUNT(*) AS c FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)
check '[ "'"${SRC_ROWS}"'" = "96" ]' "source ingested 96 points across versions (${SRC_ROWS})"

# `pond describe` reports a FilePhysicalSeries' highest Oplog version, and each
# ingest run appends exactly one, so >1 confirms a genuine maintenance
# candidate. (This differs from 730's per-version table listing.)
SRC_VERSIONS=$(pond describe /ingest/well.json 2>/dev/null \
  | grep -E '^ *Version:' | grep -oE '[0-9]+' | head -1)
echo "source series is at version ${SRC_VERSIONS}"
check '[ "'"${SRC_VERSIONS}"'" -ge 2 ]' \
    "the source really has multiple logical versions to repack (v${SRC_VERSIONS})"

# ---- A rollup that uses sum and count, the aggregations that expose it ------
# allowed_lateness=14d mirrors production while sum and count make any accidental
# duplication visible.
cat > /tmp/733-reduce.yaml << 'YAML'
in_pattern: "oteljson:///ingest/well.json"
out_pattern: "data"
time_column: "timestamp"
resolutions: ["1h"]
allowed_lateness: 14d
aggregations:
  - type: "sum"
    columns: ["well_depth_value"]
  - type: "count"
    columns: ["well_depth_value"]
YAML
pond mknod temporal-reduce /reduced --config-path /tmp/733-reduce.yaml >/dev/null

totals() {
    # Whole-series totals, independent of how many buckets the rollup emits.
    pond cat /reduced/data/res=1h.series --format=table \
      --sql 'SELECT CAST(SUM("well_depth_value.sum") AS BIGINT) AS s, CAST(SUM("well_depth_value.count") AS BIGINT) AS n FROM source' 2>&1
}

# ---- First read: builds the rollup partials, one per live source version ----
echo ""
echo "--- Before maintenance ---"
BEFORE_OUT=$(totals)
echo "$BEFORE_OUT"
BEFORE_SUM=$(echo "$BEFORE_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$2); print $2}')
BEFORE_N=$(echo "$BEFORE_OUT"   | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')

check '[ "'"${BEFORE_SUM}"'" = "96" ]'  "before collapse the rollup sums 96 samples of 1.0 (${BEFORE_SUM})"
check '[ "'"${BEFORE_N}"'" = "96" ]'    "before collapse the rollup counts 96 samples (${BEFORE_N})"

# ---- Repack the source versions (the hourly production trigger) -------------
echo ""
echo "--- maintain --collapse-versions 1 ---"
BEFORE_PACKS=$(count_pack_objects)
pond maintain --collapse-versions 1 > /tmp/733-collapse.log 2>&1
grep -iE "pack maintenance|packs:|reclaim" /tmp/733-collapse.log || true
check 'grep -qE "pack maintenance: [1-9][0-9]* candidate\(s\), [1-9][0-9]* repacked" /tmp/733-collapse.log' \
    "maintain physically repacked the source series"
check 'grep -qE "packs: [1-9][0-9]* object\(s\) written" /tmp/733-collapse.log' \
    "maintain reports bounded pack objects written"
AFTER_PACKS=$(count_pack_objects)
check '[ "'"${AFTER_PACKS}"'" -gt "'"${BEFORE_PACKS}"'" ]' \
    "maintenance created local pack objects (${BEFORE_PACKS} -> ${AFTER_PACKS})"
SRC_VERSIONS_AFTER=$(pond describe /ingest/well.json 2>/dev/null \
  | grep -E '^ *Version:' | grep -oE '[0-9]+' | head -1)
check '[ "'"${SRC_VERSIONS_AFTER}"'" = "'"${SRC_VERSIONS}"'" ]' \
    "pack-only maintenance leaves the source Oplog version unchanged (v${SRC_VERSIONS_AFTER})"

# ---- Second read: physical maintenance is invisible to rollup content -------
echo ""
echo "--- After maintenance ---"
AFTER_OUT=$(totals)
echo "$AFTER_OUT"
AFTER_SUM=$(echo "$AFTER_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$2); print $2}')
AFTER_N=$(echo "$AFTER_OUT"   | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')

check '[ "'"${AFTER_SUM}"'" = "96" ]' \
    "pack maintenance must not change the rollup sum (want 96, got ${AFTER_SUM})"
check '[ "'"${AFTER_N}"'" = "96" ]' \
    "pack maintenance must not change the rollup count (want 96, got ${AFTER_N})"

# ---- A second pass must settle without changing pack or query state ----------
pond maintain --collapse-versions 1 > /tmp/733-collapse2.log 2>&1
cat /tmp/733-collapse2.log
check 'grep -qE "pack maintenance: [1-9][0-9]* candidate\(s\), 0 repacked, [1-9][0-9]* already bounded" /tmp/733-collapse2.log' \
    "the repeated maintenance pass finds the source already bounded"
check 'grep -qE "packs: 0 object\(s\) written" /tmp/733-collapse2.log' \
    "the repeated maintenance pass writes no new pack objects"
check '[ "$(count_pack_objects)" = "'"${AFTER_PACKS}"'" ]' \
    "pack object count is stable across repeated maintenance"
AGAIN_OUT=$(totals)
AGAIN_N=$(echo "$AGAIN_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')
check '[ "'"${AGAIN_N}"'" = "96" ]' \
    "a second maintenance pass keeps the count at 96 (${AGAIN_N})"

# ---- Control: avg is why this went unnoticed --------------------------------
# Every sample is 1.0, so the mean is 1.0 before and after -- doubling sum and
# count together leaves it untouched. Recorded here so the next reader does not
# "fix" this test by asserting on avg.
AVG_OUT=$(pond cat /reduced/data/res=1h.series --format=table \
  --sql 'SELECT CAST(SUM("well_depth_value.sum") / SUM("well_depth_value.count") AS BIGINT) AS a FROM source' 2>&1 || true)
echo "$AVG_OUT"
AVG_V=$(echo "$AVG_OUT" | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)
check '[ "'"${AVG_V}"'" = "1" ]' \
    "avg stays 1.0 whether or not partials doubled -- this is the blind spot (${AVG_V})"

check_finish
