#!/bin/bash
# EXPERIMENT: logfile-ingest records event-time bounds, so temporal-reduce can
#   prune its cache instead of rebuilding it.
# DESCRIPTION: temporal-reduce keys its partial-aggregate cache on the event-time
#   range of each source version (watertown #123). It reads that range from the
#   min/max_event_time tlogfs records on the version; a version without them is
#   taken to span ALL of time (`SourceRange::UNKNOWN` = i64::MIN..i64::MAX).
#
#   logfile-ingest never recorded those bounds. Every version it wrote was
#   therefore unbounded, so on every build the dirty range reached the beginning
#   of time, `unseal_from(None)` dropped every sealed segment, `sealed_hi_secs`
#   reset to null, and the rollup recomputed the entire history from source.
#   In production (watershop site-staging) that turned a ~450 MB incremental
#   site build into a pinned ~2.05 GB full rebuild on every single run -- the
#   cost of a cold cache, paid three times an hour, forever.
#
#   The fix is the optional `timestamp_field` config below, which makes
#   logfile-ingest record each version's real bounds the way journal-ingest
#   already did.
#
# EXPECTED: With `timestamp_field` set, the ingested versions carry a time range
#   and the rollup manifest records real per-source ranges; an incremental
#   append leaves the sealed segments standing. Without it (the control), the
#   manifest is full of the i64::MIN sentinel and the append unseals everything.
set -e
source check.sh

echo "=== Experiment: logfile-ingest event-time bounds bound the rollup ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

# ---- Source: 48 hourly samples, then one more recent sample as the append ---
# Every sample is 1.0, so SUM over the rollup equals the sample COUNT and any
# deviation is a miscount rather than a rounding difference.
mkdir -p /var/log/well
awk 'BEGIN {
    start = 1784073600;
    for (i = 0; i < 48; i++) {
        e = start + i * 3600;
        printf "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[{\"name\":\"well_depth_value\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"%d000000000\",\"asDouble\":1.0}]}}]}]}]}\n", e;
    }
}' > /tmp/054-all.jsonl

# The append: one sample AFTER the newest, i.e. ordinary forward progress. This
# is the case that must stay incremental.
awk 'BEGIN {
    e = 1784073600 + 48 * 3600;
    printf "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[{\"name\":\"well_depth_value\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"%d000000000\",\"asDouble\":1.0}]}}]}]}]}\n", e;
}' > /tmp/054-next.jsonl

cat > /tmp/054-reduce.yaml << 'YAML'
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

# `seal_target_bytes: 0` seals on every build, so segments exist to be dropped.
# Without it the size gate would leave everything hot and the two arms below
# would be indistinguishable.

# Count the unbounded sentinel across every manifest in a pond. This is the
# exact quantity measured on the production pond, where it was 7526 of 7526.
count_unbounded() {
    find "$1/cache" -name manifest.json -exec cat {} + 2>/dev/null \
        | grep -o -- '-9223372036854775808' | wc -l | tr -d ' '
}

# Is any resolution still sealed?  `unseal_from(None)` resets sealed_hi_secs to
# null, so a null everywhere means the whole cache was thrown away.
count_sealed() {
    find "$1/cache" -name manifest.json -exec cat {} + 2>/dev/null \
        | grep -oE '"sealed_hi_secs":[[:space:]]*[0-9]' | wc -l | tr -d ' '
}

# Guard against a vacuous pass: "no sentinels" means nothing if no manifest was
# ever written.
count_manifests() {
    find "$1/cache" -name manifest.json 2>/dev/null | wc -l | tr -d ' '
}

manifests() {
    find "$1/cache" -name manifest.json -exec cat {} + 2>/dev/null
}

# The sealed segments' digests. `sealed_hi_secs` cannot distinguish the two arms
# because a build always re-seals before writing the manifest, so the end state
# looks sealed either way. What differs is whether the segments that existed
# before an append SURVIVE it: pruning preserves them, while unsealing destroys
# and recomputes them. That survival is precisely the work the fix avoids.
seg_digests() {
    manifests "$1" | grep -oE '"digest":[[:space:]]*"[0-9a-f]{64}"' \
        | grep -oE '[0-9a-f]{64}' | sort -u
}

# How many of the digests in $1 are still present in $2.
survivors() {
    comm -12 <(printf '%s\n' "$1" | sort -u) <(printf '%s\n' "$2" | sort -u) \
        | grep -c . || true
}

# ---- Arm A: bounds recorded (the fix) --------------------------------------
cat > /tmp/054-ingest-bounded.yaml << 'EOF'
archived_pattern: /var/log/well/well.json.*
active_pattern: /var/log/well/well.json
pond_path: /ingest
timestamp_field: timeUnixNano
timestamp_unit: nanoseconds
EOF

pond mkdir -p /system/run >/dev/null
pond mkdir -p /ingest >/dev/null
pond mknod logfile-ingest /system/run/10-well --config-path /tmp/054-ingest-bounded.yaml >/dev/null

: > /var/log/well/well.json
split -l 12 /tmp/054-all.jsonl /tmp/054-chunk.
for c in /tmp/054-chunk.*; do
    cat "$c" >> /var/log/well/well.json
    pond run /system/run/10-well >/dev/null 2>&1
done

pond mknod temporal-reduce /reduced --config-path /tmp/054-reduce.yaml >/dev/null

echo ""
echo "--- Build the rollup ---"
SUM_BEFORE=$(pond cat /reduced/data/res=1h.series --format=table \
  --sql "SELECT CAST(SUM(\"well_depth_value.count\") AS BIGINT) AS n FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

UNBOUNDED_FIXED=$(count_unbounded "$POND")
SEALED_FIXED=$(count_sealed "$POND")
MANIFESTS_FIXED=$(count_manifests "$POND")

echo ""
echo "--- Manifest after the initial build ---"
manifests "$POND"

# The four ingest runs each wrote 12 hourly samples, so the recorded ranges must
# tile the source exactly: the earliest is the first sample and the latest is
# the 48th.  Checking the values, not merely their presence, is what
# distinguishes a correct conversion from a plausible-looking wrong one --
# nanoseconds read as microseconds would still be non-sentinel.
FIRST_US=1784073600000000
LAST_US=1784242800000000
MIN_SEEN=$(manifests "$POND" | grep -oE '"min_us":[[:space:]]*-?[0-9]+' \
    | grep -oE '\-?[0-9]+$' | sort -n | head -1)
MAX_SEEN=$(manifests "$POND" | grep -oE '"max_us":[[:space:]]*-?[0-9]+' \
    | grep -oE '\-?[0-9]+$' | sort -n | tail -1)

SEG_FIXED_BEFORE=$(seg_digests "$POND")

# The incremental append.
cat /tmp/054-next.jsonl >> /var/log/well/well.json
pond run /system/run/10-well >/dev/null 2>&1

SUM_AFTER=$(pond cat /reduced/data/res=1h.series --format=table \
  --sql "SELECT CAST(SUM(\"well_depth_value.count\") AS BIGINT) AS n FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

SEALED_FIXED_AFTER=$(count_sealed "$POND")
SEG_FIXED_AFTER=$(seg_digests "$POND")
SURVIVED_FIXED=$(survivors "$SEG_FIXED_BEFORE" "$SEG_FIXED_AFTER")
SEG_FIXED_BEFORE_N=$(printf '%s\n' "$SEG_FIXED_BEFORE" | grep -c . || true)

# ---- Arm B: control, no timestamp_field (today's behaviour) -----------------
export POND=/pond-control
pond init --birthplace test-host >/dev/null

cat > /tmp/054-ingest-plain.yaml << 'EOF'
archived_pattern: /var/log/well2/well.json.*
active_pattern: /var/log/well2/well.json
pond_path: /ingest
EOF

mkdir -p /var/log/well2
pond mkdir -p /system/run >/dev/null
pond mkdir -p /ingest >/dev/null
pond mknod logfile-ingest /system/run/10-well --config-path /tmp/054-ingest-plain.yaml >/dev/null

: > /var/log/well2/well.json
for c in /tmp/054-chunk.*; do
    cat "$c" >> /var/log/well2/well.json
    pond run /system/run/10-well >/dev/null 2>&1
done

pond mknod temporal-reduce /reduced --config-path /tmp/054-reduce.yaml >/dev/null

pond cat /reduced/data/res=1h.series --format=table \
  --sql "SELECT COUNT(*) FROM source" >/dev/null 2>&1 || true

UNBOUNDED_CONTROL=$(count_unbounded "$POND")
SEALED_CONTROL=$(count_sealed "$POND")
SEG_CONTROL_BEFORE=$(seg_digests "$POND")

cat /tmp/054-next.jsonl >> /var/log/well2/well.json
pond run /system/run/10-well >/dev/null 2>&1

pond cat /reduced/data/res=1h.series --format=table \
  --sql "SELECT COUNT(*) FROM source" >/dev/null 2>&1 || true

SEALED_CONTROL_AFTER=$(count_sealed "$POND")
SEG_CONTROL_AFTER=$(seg_digests "$POND")
SURVIVED_CONTROL=$(survivors "$SEG_CONTROL_BEFORE" "$SEG_CONTROL_AFTER")
SEG_CONTROL_BEFORE_N=$(printf '%s\n' "$SEG_CONTROL_BEFORE" | grep -c . || true)

export POND=/pond

# ---- Verify ----------------------------------------------------------------
echo ""
echo "--- Verification ---"
echo "fixed:   manifests=${MANIFESTS_FIXED} unbounded=${UNBOUNDED_FIXED} sealed=${SEALED_FIXED} -> ${SEALED_FIXED_AFTER}"
echo "fixed:   min_us=${MIN_SEEN} max_us=${MAX_SEEN}"
echo "fixed:   segments ${SEG_FIXED_BEFORE_N}, surviving the append ${SURVIVED_FIXED}"
echo "control: unbounded=${UNBOUNDED_CONTROL} sealed=${SEALED_CONTROL} -> ${SEALED_CONTROL_AFTER}"
echo "control: segments ${SEG_CONTROL_BEFORE_N}, surviving the append ${SURVIVED_CONTROL}"

check '[ "${MANIFESTS_FIXED}" -gt 0 ]' \
  "the rollup wrote a manifest, so the counts below are not vacuous"

check '[ "${UNBOUNDED_FIXED}" = "0" ]' \
  "no source range is the unbounded sentinel, got ${UNBOUNDED_FIXED}"

check '[ "${MIN_SEEN}" = "${FIRST_US}" ]' \
  "earliest recorded bound is the first sample, got ${MIN_SEEN}"

check '[ "${MAX_SEEN}" = "${LAST_US}" ]' \
  "latest recorded bound is the last sample, got ${MAX_SEEN}"

check '[ "${SEALED_FIXED}" -gt 0 ]' \
  "the rollup sealed at least one resolution, got ${SEALED_FIXED}"

check '[ "${SEG_FIXED_BEFORE_N}" -gt 0 ] && [ "${SURVIVED_FIXED}" = "${SEG_FIXED_BEFORE_N}" ]' \
  "every sealed segment survives an in-order append, ${SURVIVED_FIXED}/${SEG_FIXED_BEFORE_N}"

check '[ "${SUM_BEFORE}" = "48" ]' \
  "all 48 samples counted exactly once before the append, got ${SUM_BEFORE}"

check '[ "${SUM_AFTER}" = "49" ]' \
  "the appended sample is counted, and none double-counted, got ${SUM_AFTER}"

# The control documents what the fix is worth: without bounds every source looks
# unbounded, so the append recomputes the segments rather than keeping them.
check '[ "${UNBOUNDED_CONTROL}" -gt 0 ]' \
  "control (no timestamp_field) records unbounded ranges, got ${UNBOUNDED_CONTROL}"

check '[ "${SEG_CONTROL_BEFORE_N}" -gt 0 ] && [ "${SURVIVED_CONTROL}" = "0" ]' \
  "control recomputes every segment on append, ${SURVIVED_CONTROL}/${SEG_CONTROL_BEFORE_N} survived"

check_finish
