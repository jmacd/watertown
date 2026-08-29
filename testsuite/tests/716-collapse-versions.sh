#!/bin/bash
# EXPERIMENT: Multi-version data:series pack maintenance via `pond maintain`
#
# DESCRIPTION:
#   A FilePhysicalSeries file (data:series) gains one OplogEntry version per
#   ingest. This test grows a host log and re-runs logfile-ingest several times
#   to accumulate multiple logical leaves, then runs
#   `pond maintain --collapse-versions 1`. For native logical-series-v2 data,
#   maintenance is pack-only: it publishes a bounded content-addressed pack
#   without rewriting or deleting those Oplog rows or changing logical content.
#
# EXPECTED:
#   - Repeated ingest produces a multi-version data:series file.
#   - `pond maintain --collapse-versions 1` reports a real repack and writes a
#     physical pack object.
#   - File content is byte-identical before and after pack maintenance.
#   - A re-run reports 0 repacked / already bounded and writes no new pack.
#
# History:
#   Added on jmacd/56 alongside Ship::collapse_versions / the
#   --collapse-versions flag, which had no testsuite coverage.
set -e
source check.sh

echo "=== Experiment: data:series version collapse ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

PACKS=/pond/data/_packs
count_pack_objects() {
    find "${PACKS}/objects" -type f 2>/dev/null | wc -l | tr -d ' '
}

mkdir -p /var/log/app
cat > /tmp/716-ingest.yaml << 'EOF'
archived_pattern: /var/log/app/events.log.*
active_pattern: /var/log/app/events.log
pond_path: /logs/app
EOF
pond mkdir -p /system/run >/dev/null 2>&1
pond mkdir -p /logs/app >/dev/null 2>&1
pond mknod logfile-ingest /system/run/10-events --config-path /tmp/716-ingest.yaml >/dev/null 2>&1

echo "--- Step 1: accumulate versions by growing the active log ---"
: > /var/log/app/events.log
for i in 1 2 3 4 5; do
    printf 'event-%d at line %d\n' "$i" "$i" >> /var/log/app/events.log
    pond run /system/run/10-events >/dev/null 2>&1
done

POND_SIZE=$(pond cat /logs/app/events.log 2>/dev/null | wc -c | tr -d ' ')
HOST_SIZE=$(wc -c < /var/log/app/events.log | tr -d ' ')
check '[ "'"${POND_SIZE}"'" = "'"${HOST_SIZE}"'" ]' "ingested series matches host log (${POND_SIZE} bytes)"

BEFORE_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${BEFORE_MD5}"'" ]' "pre-collapse content md5 computed"
BEFORE_PACKS=$(count_pack_objects)
check '[ "'"${BEFORE_PACKS}"'" = "0" ]' "no pack objects exist before maintenance"

echo "--- Step 2: repack the multi-version series ---"
pond maintain --collapse-versions 1 > /tmp/716-collapse.log 2>&1
cat /tmp/716-collapse.log
check 'grep -qE "pack maintenance: [1-9][0-9]* candidate\(s\), [1-9][0-9]* repacked" /tmp/716-collapse.log' \
    "maintain repacked at least one candidate"
check 'grep -qE "packs: [1-9][0-9]* object\(s\) written" /tmp/716-collapse.log' \
    "maintain reports new bounded pack objects"
AFTER_PACKS=$(count_pack_objects)
check '[ "'"${AFTER_PACKS}"'" -gt "'"${BEFORE_PACKS}"'" ]' \
    "pack maintenance durably created physical pack objects (${BEFORE_PACKS} -> ${AFTER_PACKS})"

echo "--- Step 3: logical content is unchanged after repacking ---"
AFTER_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${AFTER_MD5}"'" = "'"${BEFORE_MD5}"'" ]' "content byte-identical after repacking"
check 'pond cat /logs/app/events.log | grep -q "event-1 at line 1"' "first append remains readable"
check 'pond cat /logs/app/events.log | grep -q "event-5 at line 5"' "last append remains readable"

echo "--- Step 4: second maintenance pass is already bounded ---"
pond maintain --collapse-versions 1 > /tmp/716-collapse2.log 2>&1
cat /tmp/716-collapse2.log
check 'grep -qE "pack maintenance: [1-9][0-9]* candidate\(s\), 0 repacked, [1-9][0-9]* already bounded" /tmp/716-collapse2.log' \
    "repeated maintenance finds the series already bounded"
check 'grep -qE "packs: 0 object\(s\) written" /tmp/716-collapse2.log' \
    "repeated maintenance writes no new pack objects"
check '[ "$(count_pack_objects)" = "'"${AFTER_PACKS}"'" ]' \
    "pack object count is stable across repeated maintenance"

echo "--- Step 5: append after maintenance still reads correctly ---"
printf 'event-6 at line 6\n' >> /var/log/app/events.log
pond run /system/run/10-events >/dev/null 2>&1
EXPECT_MD5=$(md5sum < /var/log/app/events.log | awk '{print $1}')
APPEND_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${APPEND_MD5}"'" = "'"${EXPECT_MD5}"'" ]' "post-maintenance append reads full logical content"

check_finish
