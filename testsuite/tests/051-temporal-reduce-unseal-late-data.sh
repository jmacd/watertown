#!/bin/bash
# EXPERIMENT: genuinely late data must UNSEAL the segments that cover it.
#
# DESCRIPTION:
#   Every other rollup test either never seals (the data is too small to reach
#   `seal_target_bytes`) or exercises version COLLAPSE, where the source content
#   is unchanged and the correct answer is to reuse the cache untouched. Neither
#   reaches the path this branch introduced for real backfill:
#
#     a new source version whose event range dips BELOW the sealed watermark
#     drops the segments covering it and lets the ordinary seal path recompute
#     them from source, instead of hard-erroring with "--rebuild".
#
#   The unit tests cannot reach it either: MemoryPersistence records no event
#   bounds, so every source version reads as SourceRange::UNKNOWN, the dirty
#   range is unbounded and EVERY build unseals EVERYTHING. Only a tlogfs pond
#   records real bounds, and only then is a segment ever partially retained.
#
#   `seal_target_bytes: 0` forces a seal on every build so a handful of rows is
#   enough to produce segments; this is a cache-layout knob, so setting it
#   changes nothing about the answer.
#
#   Two things can go wrong and both are silent:
#     - unsealing too little: a bucket keeps its pre-backfill value, or the
#       watermark stays above a dirty point that no segment covers, so buckets
#       vanish entirely (the empty-span watermark hole).
#     - unsealing too much, or failing to drop superseded segment files: the
#       recomputed rows are summed alongside the stale ones and every affected
#       sample is counted twice.
#
#   Totals expose both. avg does not: avg = SUM(sum)/SUM(count) is invariant
#   under doubling, which is exactly how the original defect survived review
#   (see test 733).
#
# EXPECTED:
#   - The first pass seals; day 1..3 (72 hourly points) read back as 72 buckets
#     summing to 72 samples.
#   - A late point at day 1 12:30, far behind the sealed watermark, is accepted
#     with no error and no "--rebuild" demand.
#   - The rollup then totals 73 samples over 72 buckets: the late sample lands
#     in the EXISTING 12:00 bucket, which must be recomputed from 1 sample to 2
#     -- not left stale at 1, and not double-counted to 3.
#   - A CASCADED rollup (1h then 1d) shows the same total. A coarse level is
#     folded from the finer one and keys its freshness on the finer level's
#     digest plus whether it was REBUILT; late data is not a rebuild, so unless
#     the dirty point is propagated the coarse level keeps its own sealed
#     segments and reports the pre-backfill total forever.
set -e
source check.sh

echo "=== Experiment: late data unseals the segments covering it ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

# ---- Source: 72 hourly samples, 2026-07-15T00:00Z .. 2026-07-17T23:00Z ------
# Each sample is exactly 1.0, so SUM over the rollup equals the sample COUNT
# and any deviation is a miscount rather than a rounding difference.
mkdir -p /var/log/well
awk 'BEGIN {
    start = 1784073600;
    for (i = 0; i < 72; i++) {
        e = start + i * 3600;
        printf "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[{\"name\":\"well_depth_value\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"%d000000000\",\"asDouble\":1.0}]}}]}]}]}\n", e;
    }
}' > /tmp/051-all.jsonl

# The late sample: day 1 at 12:30, i.e. inside the 12:00 bucket, three days
# behind the newest sample and far below any 1d sealed watermark.
awk 'BEGIN {
    e = 1784073600 + 12 * 3600 + 1800;
    printf "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[{\"name\":\"well_depth_value\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"%d000000000\",\"asDouble\":1.0}]}}]}]}]}\n", e;
}' > /tmp/051-late.jsonl

cat > /tmp/051-ingest.yaml << 'EOF'
archived_pattern: /var/log/well/well.json.*
active_pattern: /var/log/well/well.json
pond_path: /ingest
EOF

pond mkdir -p /system/run >/dev/null
pond mkdir -p /ingest >/dev/null
pond mknod logfile-ingest /system/run/10-well --config-path /tmp/051-ingest.yaml >/dev/null

# Several ingest runs, so the source has several live versions and the rollup
# must reconcile a set rather than a single file.
: > /var/log/well/well.json
split -l 18 /tmp/051-all.jsonl /tmp/051-chunk.
for c in /tmp/051-chunk.*; do
    cat "$c" >> /var/log/well/well.json
    pond run /system/run/10-well >/dev/null 2>&1
done

# ---- The rollup, forced to seal on every build ------------------------------
cat > /tmp/051-reduce.yaml << 'YAML'
in_pattern: "oteljson:///ingest/well.json"
out_pattern: "data"
time_column: "timestamp"
resolutions: ["1h"]
seal_target_bytes: 0
aggregations:
  - type: "sum"
    columns: ["well_depth_value"]
  - type: "count"
    columns: ["well_depth_value"]
YAML
pond mknod temporal-reduce /reduced --config-path /tmp/051-reduce.yaml >/dev/null

# The same rollup, cascaded: 1d is folded from 1h rather than from source.
sed 's/resolutions: \["1h"\]/resolutions: ["1h", "1d"]/' /tmp/051-reduce.yaml \
  > /tmp/051-reduce-cascade.yaml
pond mknod temporal-reduce /reduced-cascade --config-path /tmp/051-reduce-cascade.yaml >/dev/null

totals() {
    pond cat /reduced/data/res=1h.series --format=table --sql \
      'SELECT CAST(COUNT(*) AS BIGINT) AS b, CAST(SUM("well_depth_value.count") AS BIGINT) AS n FROM source' 2>&1
}

# The coarsest level of the cascade, folded from 1h rather than from source.
daily_total() {
    pond cat /reduced-cascade/data/res=1d.series --format=table --sql \
      'SELECT CAST(SUM("well_depth_value.count") AS BIGINT) AS n FROM source' 2>&1 \
      | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1
}

# One sample per bucket, so the 12:00 bucket is the one the late point joins.
noon_count() {
    pond cat /reduced/data/res=1h.series --format=table --sql \
      'SELECT CAST("well_depth_value.count" AS BIGINT) AS c FROM source
       WHERE timestamp = arrow_cast(1784116800000, '"'"'Timestamp(Millisecond, None)'"'"')' 2>&1 \
      | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1
}

echo ""
echo "--- First read: builds and seals ---"
BEFORE_OUT=$(totals)
echo "$BEFORE_OUT"
BEFORE_B=$(echo "$BEFORE_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$2); print $2}')
BEFORE_N=$(echo "$BEFORE_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')
BEFORE_NOON=$(noon_count)
BEFORE_DAILY=$(daily_total)

# The seal must actually have happened, or this test proves nothing: a rollup
# with zero segments cannot exercise unsealing at all.
# 'seg-*' specifically: hot.parquet lives in the same directory and always
# exists, so a plain '*.parquet' count would pass even if nothing ever sealed
# and this whole test degenerated into "does a rollup work".
SEGMENTS=$(find /pond -path '*/merged_*/res3600/*' -name 'seg-*.parquet' 2>/dev/null | wc -l | tr -d ' ')
echo "segment files after the sealing pass: ${SEGMENTS}"

echo ""
echo "--- Append the late sample (day 1 12:30) ---"
cat /tmp/051-late.jsonl >> /var/log/well/well.json
pond run /system/run/10-well 2>&1 | tee /tmp/051-late-run.log | tail -3

echo ""
echo "--- Second read: must unseal and recompute, not error ---"
AFTER_OUT=$(totals || true)
echo "$AFTER_OUT"
AFTER_B=$(echo "$AFTER_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$2); print $2}')
AFTER_N=$(echo "$AFTER_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')
AFTER_NOON=$(noon_count)
AFTER_DAILY=$(daily_total)

# A third read exercises the steady state: nothing changed, so the rollup must
# reuse what it just wrote and still answer identically.
THIRD_OUT=$(totals || true)
THIRD_N=$(echo "$THIRD_OUT" | grep -E '^\| *[0-9]' | head -1 | awk -F'|' '{gsub(/ /,"",$3); print $3}')

echo ""
echo "--- Verification ---"

check '[ "${BEFORE_B}" = "72" ]' \
  "the first pass reads 72 hourly buckets, got ${BEFORE_B}"

check '[ "${BEFORE_N}" = "72" ]' \
  "the first pass counts each of the 72 samples once, got ${BEFORE_N}"

check '[ "${SEGMENTS}" -ge 1 ]' \
  "seal_target_bytes: 0 actually sealed a segment, so unsealing has something to do (${SEGMENTS} file(s))"

check '[ "${BEFORE_NOON}" = "1" ]' \
  "the 12:00 bucket starts with a single sample, got ${BEFORE_NOON}"

check '! echo "$AFTER_OUT" | grep -qiE "backfills|precedes|--rebuild|error"' \
  "data landing behind the sealed watermark is no longer an error"

check '[ "${AFTER_B}" = "72" ]' \
  "the late sample joins an existing bucket, so the bucket count is unchanged at 72, got ${AFTER_B}"

check '[ "${AFTER_N}" = "73" ]' \
  "the late sample is counted exactly once (72 = it was dropped, 74+ = double-counted), got ${AFTER_N}"

check '[ "${AFTER_NOON}" = "2" ]' \
  "the sealed 12:00 bucket was recomputed from 1 sample to 2 (1 = stale segment kept, 3 = stale and fresh both summed), got ${AFTER_NOON}"

check '[ "${BEFORE_DAILY}" = "72" ]' \
  "the cascaded 1d level totals 72 before the backfill, got ${BEFORE_DAILY}"

check '[ "${AFTER_DAILY}" = "73" ]' \
  "the backfill propagates to the cascaded 1d level, which is folded from 1h and would otherwise keep its own sealed segments (72 = the dirty point stopped at the finest level), got ${AFTER_DAILY}"

check '[ "${THIRD_N}" = "73" ]' \
  "a repeat read with no new data still totals 73, got ${THIRD_N}"

check_finish
