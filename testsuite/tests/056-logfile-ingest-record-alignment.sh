#!/bin/bash
# EXPERIMENT: a log declared to carry timestamps always ends up with event-time
#   bounds -- a partially-written record can no longer strand a version without
#   them, and a field that does not match the data fails loudly.
# DESCRIPTION: test 054 made logfile-ingest RECORD bounds; this one makes that
#   recording total.
#
#   logfile-ingest tails an actively-written file by byte offset, so a slice
#   routinely ends mid-record: the station is still writing that line. Stored
#   verbatim, one record straddles two versions and NEITHER can read its own
#   timestamps -- the first cannot parse its trailing fragment, the second
#   cannot parse its leading one. The scan then abandons (correctly: bounds
#   covering only the lines that parsed would leave the rest of the slice
#   outside its own range) and the version was written unbounded. One unbounded
#   version is all it takes: temporal-reduce treats it as spanning all of time
#   and rebuilds its entire cache, which is the ~2.05 GB failure of 054
#   reappearing through a different door.
#
#   Two changes close it. Slices from an active file are cut at the last
#   newline, so every stored version holds a whole number of records and the
#   next one starts at a record boundary; the withheld bytes are picked up on
#   the next tick, since appends are detected by size. And naming a
#   timestamp_field now declares the node temporal, so a version that still
#   cannot produce bounds fails the write instead of being silently stored
#   unbounded.
#
#   Both are gated on timestamp_field being set. Without it the file is an
#   opaque byte stream that may contain no newline at all, and withholding
#   would strand it forever rather than merely until the next tick.
#
# EXPECTED: ingesting a torn tail withholds the partial record and yields zero
#   unbounded versions; the withheld bytes arrive once complete, byte-exact; a
#   mismatched timestamp_field fails the run and names the field; and a node
#   without timestamp_field still ingests a torn tail verbatim.
set -e
source check.sh

echo "=== Experiment: record alignment makes temporal bounds total ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

mkdir -p /var/log/torn

# One OTLP record per line, same shape the stations emit.
record() {
    printf '{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"%d000000000","asDouble":1.0}]}}]}]}]}\n' "$1"
}

# The unbounded sentinel (i64::MIN) as it appears in a manifest or listing.
count_unbounded() {
    find "$1" -name '*.json' -exec cat {} + 2>/dev/null \
        | grep -o -- '-9223372036854775808' | wc -l | tr -d ' '
}

cat > /tmp/056-ingest.yaml << 'EOF'
archived_pattern: /var/log/torn/well.json.*
active_pattern: /var/log/torn/well.json
pond_path: /ingest
timestamp_field: timeUnixNano
timestamp_unit: nanoseconds
EOF

pond mkdir -p /system/run >/dev/null
pond mkdir -p /ingest >/dev/null
pond mknod logfile-ingest /system/run/10-well --config-path /tmp/056-ingest.yaml >/dev/null

# ---- A torn tail: two whole records plus a half-written third --------------
: > /var/log/torn/well.json
record 1784073600 >> /var/log/torn/well.json
record 1784077200 >> /var/log/torn/well.json
COMPLETE_BYTES=$(wc -c < /var/log/torn/well.json | tr -d ' ')

# The station is mid-write: a record with no terminating newline.
printf '{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metr' \
    >> /var/log/torn/well.json
TORN_BYTES=$(wc -c < /var/log/torn/well.json | tr -d ' ')

echo ""
echo "--- Ingest with a half-written record at the tail ---"
pond run /system/run/10-well >/dev/null 2>&1

pond cat /ingest/well.json > /tmp/056-after-torn.txt 2>/dev/null || true
STORED=$(wc -c < /tmp/056-after-torn.txt | tr -d ' ')

check "[ '$STORED' -eq '$COMPLETE_BYTES' ]" \
  "stores only the $COMPLETE_BYTES complete bytes, not the $TORN_BYTES on disk"
check "[ \"\$(tail -c 1 /tmp/056-after-torn.txt)\" = '' ]" \
  "stored content ends on a record boundary"
check "[ \"\$(grep -c timeUnixNano /tmp/056-after-torn.txt)\" -eq 2 ]" \
  "exactly the 2 complete records are stored, not the half-written third"

UNBOUNDED_TORN=$(count_unbounded "$POND")
check "[ '$UNBOUNDED_TORN' -eq 0 ]" \
  "no unbounded version after a torn tail (found $UNBOUNDED_TORN sentinels)"

# The version must actually carry a range -- "no sentinel" is vacuous if no
# version was written at all.
pond list -l /ingest/well.json > /tmp/056-list.txt 2>&1 || true
check "grep -qE '[0-9]{4}-[0-9]{2}-[0-9]{2}' /tmp/056-list.txt" \
  "the stored version reports a real time range"

# ---- The withheld bytes arrive once the record is finished -----------------
echo ""
echo "--- Complete the record and re-run ---"
printf 'ics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1784080800000000000","asDouble":1.0}]}}]}]}]}\n' \
    >> /var/log/torn/well.json
FULL_BYTES=$(wc -c < /var/log/torn/well.json | tr -d ' ')

pond run /system/run/10-well >/dev/null 2>&1

pond cat /ingest/well.json > /tmp/056-after-complete.txt 2>/dev/null || true
STORED_FULL=$(wc -c < /tmp/056-after-complete.txt | tr -d ' ')

check "[ '$STORED_FULL' -eq '$FULL_BYTES' ]" \
  "withheld bytes are picked up on the next tick ($STORED_FULL of $FULL_BYTES)"
check "diff -q /tmp/056-after-complete.txt /var/log/torn/well.json" \
  "pond content is byte-identical to the host file"
check "[ \"\$(grep -c timeUnixNano /tmp/056-after-complete.txt)\" -eq 3 ]" \
  "all three records are present exactly once"

UNBOUNDED_FULL=$(count_unbounded "$POND")
check "[ '$UNBOUNDED_FULL' -eq 0 ]" \
  "still no unbounded version after the append (found $UNBOUNDED_FULL sentinels)"

# ---- A field that does not match the data must fail loudly -----------------
echo ""
echo "--- A mismatched timestamp_field fails the run ---"
mkdir -p /var/log/wrong
record 1784073600 > /var/log/wrong/other.json

cat > /tmp/056-wrong.yaml << 'EOF'
archived_pattern: /var/log/wrong/other.json.*
active_pattern: /var/log/wrong/other.json
pond_path: /wrongfield
timestamp_field: notATimestampField
timestamp_unit: nanoseconds
EOF

pond mkdir -p /wrongfield >/dev/null
pond mknod logfile-ingest /system/run/20-wrong --config-path /tmp/056-wrong.yaml >/dev/null

RUN_RC=0
pond run /system/run/20-wrong > /tmp/056-wrong-out.txt 2>&1 || RUN_RC=$?

check "[ '$RUN_RC' -ne 0 ]" \
  "declaring a field the records lack fails the run (rc=$RUN_RC)"
check_contains /tmp/056-wrong-out.txt \
  "the failure names the field it looked for" 'notATimestampField'

# The silent alternative is the whole point: an unbounded version stored here
# would surface only as a slow, memory-hungry rollup weeks later.
UNBOUNDED_WRONG=$(count_unbounded "$POND")
check "[ '$UNBOUNDED_WRONG' -eq 0 ]" \
  "the rejected write left no unbounded version behind"

# ---- Without timestamp_field, a byte stream is still stored verbatim -------
echo ""
echo "--- An opaque stream is not withheld ---"
mkdir -p /var/log/opaque
# No newline anywhere: aligning this would withhold it forever.
printf 'no-newline-anywhere-at-all' > /var/log/opaque/blob.log

cat > /tmp/056-opaque.yaml << 'EOF'
archived_pattern: /var/log/opaque/blob.log.*
active_pattern: /var/log/opaque/blob.log
pond_path: /opaque
EOF

pond mkdir -p /opaque >/dev/null
pond mknod logfile-ingest /system/run/30-opaque --config-path /tmp/056-opaque.yaml >/dev/null
pond run /system/run/30-opaque >/dev/null 2>&1

pond cat /opaque/blob.log > /tmp/056-opaque.txt 2>/dev/null || true
check "diff -q /tmp/056-opaque.txt /var/log/opaque/blob.log" \
  "a stream with no newline and no timestamp_field is stored byte-exact"

check_finish
