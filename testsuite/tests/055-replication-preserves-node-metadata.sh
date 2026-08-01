#!/bin/bash
# EXPERIMENT: replication carries node metadata, so a consumer that rolls up
#   REPLICATED data stays incremental.
# DESCRIPTION: Test 054 fixed the producer: logfile-ingest now records each
#   version's event-time bounds. That is only half the path actually used in
#   production. watershop's site pond does not roll up its own ingest; it rolls
#   up copies pulled from other ponds (config/scripts/attach-remotes.sh). So the
#   bounds have to survive replication too.
#
#   They did not. Before the content-addressed rewrite, replication shipped
#   whole OplogEntry rows and min/max_event_time rode along for free. The
#   rewrite reduced each node to a child_hash and let the consumer re-mint the
#   rest locally -- which silently dropped every column that is not a function
#   of the bytes. A consumer therefore saw NULL bounds on every replicated
#   version, read them back as SourceRange::UNKNOWN (i64::MIN..i64::MAX), and
#   rebuilt its entire rollup from source on every single build. That is the
#   ~2.05 GB pinned build measured on site-staging, reproduced here.
#
#   The fix puts node metadata back on the wire, on the DIRECTORY ENTRY: a
#   directory holds names that refer to nodes, and the metadata belongs beside
#   the name. Metadata is content, so the entry's hash commits to it -- which
#   also means a successful cross-pond import is itself a proof of fidelity,
#   since the import re-folds the mirrored tree and rejects any mismatch
#   against the source's root hash.
#
# EXPECTED: The consumer's rollup over replicated data records real per-source
#   ranges (no i64::MIN sentinel), the replicated node keeps the producer's own
#   mtime, and an in-order append leaves the consumer's sealed segments standing
#   instead of recomputing them.
set -e
source check.sh

echo "=== Experiment: replication preserves node metadata ==="

P1=/tmp/055-producer
P2=/tmp/055-consumer
REMOTE=/tmp/055-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

# ---- Source data: 48 hourly samples, then one later sample as the append ----
# Every sample is 1.0, so SUM over the rollup equals the sample COUNT and any
# deviation is a miscount rather than a rounding difference.
mkdir -p /var/log/well
awk 'BEGIN {
    start = 1784073600;
    for (i = 0; i < 48; i++) {
        e = start + i * 3600;
        printf "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[{\"name\":\"well_depth_value\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"%d000000000\",\"asDouble\":1.0}]}}]}]}]}\n", e;
    }
}' > /tmp/055-all.jsonl

awk 'BEGIN {
    e = 1784073600 + 48 * 3600;
    printf "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[{\"name\":\"well_depth_value\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"%d000000000\",\"asDouble\":1.0}]}}]}]}]}\n", e;
}' > /tmp/055-next.jsonl

cat > /tmp/055-ingest.yaml << 'EOF'
archived_pattern: /var/log/well/well.json.*
active_pattern: /var/log/well/well.json
pond_path: /ingest
timestamp_field: timeUnixNano
timestamp_unit: nanoseconds
EOF

# The consumer rolls up the REPLICATED copy, exactly as the site pond does.
cat > /tmp/055-reduce.yaml << 'YAML'
in_pattern: "oteljson:///imports/up/ingest/well.json"
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

# `seal_target_bytes: 0` seals on every build, so segments exist to survive (or
# not) the append; the default size gate would leave everything hot and make the
# two outcomes indistinguishable.

count_unbounded() {
    find "$1/cache" -name manifest.json -exec cat {} + 2>/dev/null \
        | grep -o -- '-9223372036854775808' | wc -l | tr -d ' '
}

count_manifests() {
    find "$1/cache" -name manifest.json 2>/dev/null | wc -l | tr -d ' '
}

manifests() {
    find "$1/cache" -name manifest.json -exec cat {} + 2>/dev/null
}

# The sealed segments' digests. What distinguishes an incremental build from a
# full rebuild is whether the segments that existed before an append SURVIVE it:
# pruning preserves them, unsealing destroys and recomputes them.
seg_digests() {
    manifests "$1" | grep -oE '"digest":[[:space:]]*"[0-9a-f]{64}"' \
        | grep -oE '[0-9a-f]{64}' | sort -u
}

survivors() {
    comm -12 <(printf '%s\n' "$1" | sort -u) <(printf '%s\n' "$2" | sort -u) \
        | grep -c . || true
}

rollup_sum() {
    pond cat /reduced/data/res=1h.series --format=table \
        --sql "SELECT CAST(SUM(\"well_depth_value.count\") AS BIGINT) AS n FROM source" 2>&1 \
        | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1
}

echo ""
echo "--- Step 1: producer ingests with event-time bounds ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null
pond mkdir -p /system/run >/dev/null
pond mkdir -p /ingest >/dev/null
pond mknod logfile-ingest /system/run/10-well --config-path /tmp/055-ingest.yaml >/dev/null

: > /var/log/well/well.json
split -l 12 /tmp/055-all.jsonl /tmp/055-chunk.
for c in /tmp/055-chunk.*; do
    cat "$c" >> /var/log/well/well.json
    pond run /system/run/10-well >/dev/null 2>&1
done

# `pond list -l` renders each node's mtime, which is the plainest observable
# form of replicated node metadata.
pond list -l /ingest/well.json > /tmp/055-p1-list.txt 2>&1
P1_MTIME=$(grep 'well.json' /tmp/055-p1-list.txt \
    | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' | head -1)

echo ""
echo "--- Step 2: publish to a file:// remote ---"
pond backup add origin "file://${REMOTE}" > /tmp/055-backup.log 2>&1
check 'grep -q "added remote origin" /tmp/055-backup.log' "backup add origin succeeded"

echo ""
echo "--- Step 3: consumer imports the producer's pond ---"
export POND="$P2"
pond init --birthplace test-host >/dev/null
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
# Pull well after the producer wrote, so a consumer that re-minted the mtime
# locally would record a visibly different one.
sleep 2
pond pull upstream > /tmp/055-pull.log 2>&1
check 'grep -q "pull upstream complete" /tmp/055-pull.log' "consumer completed cross-pond import"

# The import re-folds the mirrored tree and compares it to the source's root
# hash. Because metadata is part of the entry, that fold only matches if every
# replicated version's metadata matches too -- so a clean pull is itself the
# fidelity check. (When the consumer re-minted metadata locally, this is exactly
# where the mismatch surfaced.)
check '! grep -qi "folds to" /tmp/055-pull.log' \
    "mirrored tree folds to the source root (metadata replicated exactly)"

# The replica must report the producer's own mtime, not the time of the pull --
# the same thing `rsync -t` and `cp -p` do.
pond list -l /imports/up/ingest/well.json > /tmp/055-p2-list.txt 2>&1
P2_MTIME=$(grep 'well.json' /tmp/055-p2-list.txt \
    | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' | head -1)

echo ""
echo "--- Step 4: consumer rolls up the REPLICATED copy ---"
pond mknod temporal-reduce /reduced --config-path /tmp/055-reduce.yaml >/dev/null
SUM_BEFORE=$(rollup_sum)

UNBOUNDED=$(count_unbounded "$POND")
MANIFESTS=$(count_manifests "$POND")
SEG_BEFORE=$(seg_digests "$POND")
SEG_BEFORE_N=$(printf '%s\n' "$SEG_BEFORE" | grep -c . || true)

echo ""
echo "--- Consumer rollup manifest ---"
manifests "$POND"

echo ""
echo "--- Step 5: producer appends, consumer re-pulls and rebuilds ---"
export POND="$P1"
cat /tmp/055-next.jsonl >> /var/log/well/well.json
pond run /system/run/10-well >/dev/null 2>&1

export POND="$P2"
pond pull upstream > /tmp/055-pull2.log 2>&1
check 'grep -q "pull upstream complete" /tmp/055-pull2.log' "consumer pulled the append"

SUM_AFTER=$(rollup_sum)
SEG_AFTER=$(seg_digests "$POND")
SURVIVED=$(survivors "$SEG_BEFORE" "$SEG_AFTER")
UNBOUNDED_AFTER=$(count_unbounded "$POND")

echo ""
echo "--- Verification ---"
echo "producer mtime: ${P1_MTIME}"
echo "consumer mtime: ${P2_MTIME}"
echo "consumer: manifests=${MANIFESTS} unbounded=${UNBOUNDED} -> ${UNBOUNDED_AFTER}"
echo "consumer: segments ${SEG_BEFORE_N}, surviving the append ${SURVIVED}"
echo "consumer: rollup sum ${SUM_BEFORE} -> ${SUM_AFTER}"

check '[ "${MANIFESTS}" -gt 0 ]' \
    "the consumer's rollup wrote a manifest, so the counts below are not vacuous"

check '[ -n "${P1_MTIME}" ]' \
    "the producer's series reports an mtime"

check '[ "${P1_MTIME}" = "${P2_MTIME}" ]' \
    "the replica keeps the producer's mtime rather than the time of the pull"

check '[ "${UNBOUNDED}" = "0" ]' \
    "no replicated source is the unbounded sentinel, got ${UNBOUNDED}"

check '[ "${SUM_BEFORE}" = "48" ]' \
    "all 48 replicated samples counted exactly once, got ${SUM_BEFORE}"

check '[ "${SUM_AFTER}" = "49" ]' \
    "the appended sample is counted, and none double-counted, got ${SUM_AFTER}"

check '[ "${UNBOUNDED_AFTER}" = "0" ]' \
    "the append introduces no unbounded source, got ${UNBOUNDED_AFTER}"

# The payoff: with bounds carried across the wire, the consumer prunes its cache
# instead of discarding it. Without them every replicated source spanned all of
# time, so this number was 0 and the whole rollup was recomputed every build.
check '[ "${SEG_BEFORE_N}" -gt 0 ] && [ "${SURVIVED}" = "${SEG_BEFORE_N}" ]' \
    "every sealed segment survives the replicated append, ${SURVIVED}/${SEG_BEFORE_N}"

check_finish
