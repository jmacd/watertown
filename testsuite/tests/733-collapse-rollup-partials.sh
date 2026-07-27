#!/bin/bash
# EXPERIMENT: version collapse must not double-count temporal-reduce rollup
#   partials.
#
# DESCRIPTION:
#   This is the same defect as 731, one layer over. 731 fixed the FORMAT cache,
#   which globbed a node's cache directory and so returned both a merged
#   version and the superseded versions it stands for. The ROLLUP cache has the
#   identical structure and was not fixed:
#
#     - rollup_cache::find_uncached_members() writes one partial per LIVE source
#       version, named {node}_v{version}_{blake3}.parquet. It only ever ADDS;
#       nothing removes the partial of a version that later becomes superseded.
#     - temporal_reduce.rs registers the whole directory via
#       rollup_cache::listing_table_from_dir(&glob_dir, ctx) -- a plain glob.
#     - the read-side merge is SUM(__p_sum_i), SUM(__p_count_i) GROUP BY bucket.
#
#   So after `maintain --collapse-versions` merges source versions [lo,hi] into
#   one run, the run is a new version with a new blake3, gets its own partial,
#   and the hi-lo+1 superseded partials are still on disk and still summed.
#   Every row in the collapsed window is counted twice, and again on each later
#   collapse.
#
#   Why this survived: the rollup cache is only engaged when the input scheme
#   has a FormatProvider (temporal_reduce.rs try_rollup_table_provider), i.e.
#   oteljson/jsonlogs/csv -- NOT the builtin series:/// scheme. The existing
#   collapse-vs-rollup test (050) uses oteljson and so does reach it, but it
#   asserts only on `avg` and on bucket COUNT, and both are invariant under
#   doubling: avg = SUM(sum)/SUM(count) is unchanged when sum and count both
#   double, and GROUP BY collapses duplicate buckets back to the same row count.
#   `sum` and `count` are the aggregations that expose it, and nothing used them
#   over a collapsed source.
#
#   Live in production: config/water.yaml and config/septic.yaml reduce over
#   "oteljson:///ingest/*.json" -- logfile-ingest FilePhysicalSeries that are
#   exactly what list_collapsible_series targets -- while config/scripts/run.sh
#   runs `maintain --compact --collapse-versions 100` hourly.
#
# EXPECTED (currently FAILS -- this pins the bug before the fix):
#   - Before collapse, the reduced series totals 96 samples summing to 96.0.
#   - `maintain --collapse-versions` merges the source versions.
#   - After collapse those totals are UNCHANGED. Today they double to 192 / 192.0
#     because the superseded versions' partials are still in the glob dir.
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
# One append version per run, so `maintain --collapse-versions` has a real run
# of adjacent versions to merge -- the hourly-ingest shape of water/septic.
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

# `pond describe` reports a FilePhysicalSeries' highest version, and each ingest
# run appends exactly one, so >1 means there is a real run of adjacent versions
# for collapse to merge. (Note this differs from 730's per-version listing,
# which is the TablePhysicalSeries shape.)
SRC_VERSIONS=$(pond describe /ingest/well.json 2>/dev/null \
  | grep -E '^ *Version:' | grep -oE '[0-9]+' | head -1)
echo "source series is at version ${SRC_VERSIONS}"
check '[ "'"${SRC_VERSIONS}"'" -ge 2 ]' \
    "the source really has multiple versions to collapse (v${SRC_VERSIONS})"

# ---- A rollup that uses sum and count, the aggregations that expose it ------
# allowed_lateness=14d keeps the collapsed tail inside the unsealed hot window,
# so this test isolates double-counting rather than re-testing the sealed-bucket
# rejection already covered by 050.
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
echo "--- Before collapse ---"
BEFORE_OUT=$(totals)
echo "$BEFORE_OUT"
BEFORE_SUM=$(echo "$BEFORE_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$2); print $2}')
BEFORE_N=$(echo "$BEFORE_OUT"   | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')

check '[ "'"${BEFORE_SUM}"'" = "96" ]'  "before collapse the rollup sums 96 samples of 1.0 (${BEFORE_SUM})"
check '[ "'"${BEFORE_N}"'" = "96" ]'    "before collapse the rollup counts 96 samples (${BEFORE_N})"

# ---- Collapse the source versions (the hourly production trigger) -----------
echo ""
echo "--- maintain --collapse-versions 1 ---"
pond maintain --collapse-versions 1 > /tmp/733-collapse.log 2>&1
grep -iE "collapse|reclaim" /tmp/733-collapse.log || true
check 'grep -qE "collapse: [1-9][0-9]* file\(s\) collapsed" /tmp/733-collapse.log' \
    "maintain actually collapsed the source series"

# ---- Second read: the merged run adds a partial; the old ones remain --------
echo ""
echo "--- After collapse ---"
AFTER_OUT=$(totals)
echo "$AFTER_OUT"
AFTER_SUM=$(echo "$AFTER_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$2); print $2}')
AFTER_N=$(echo "$AFTER_OUT"   | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')

check '[ "'"${AFTER_SUM}"'" = "96" ]' \
    "collapse must not change the rollup sum (want 96, got ${AFTER_SUM})"
check '[ "'"${AFTER_N}"'" = "96" ]' \
    "collapse must not change the rollup count (want 96, got ${AFTER_N})"

# ---- A second collapse must not compound the error either -------------------
pond maintain --collapse-versions 1 >/dev/null 2>&1 || true
AGAIN_OUT=$(totals)
AGAIN_N=$(echo "$AGAIN_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')
check '[ "'"${AGAIN_N}"'" = "96" ]' \
    "a second collapse pass keeps the count at 96 (${AGAIN_N})"

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
