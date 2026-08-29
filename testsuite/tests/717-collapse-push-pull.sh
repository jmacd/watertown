#!/bin/bash
# EXPERIMENT: data:series pack maintenance is transparent to replication
#
# DESCRIPTION:
#   A producer accumulates many versions of one data:series file and publishes
#   them to a file:// remote. `pond maintain --collapse-versions` then performs
#   pack-only local maintenance: it creates a bounded physical pack but does
#   not create a content commit, advance/auto-push the content tip, or change
#   what a fresh consumer pulls from the remote.
#
# EXPECTED:
#   - Producer repacks >=1 candidate into local pack storage.
#   - Producer content root and pushed tip remain unchanged; verify stays clean
#     without an auto-push.
#   - Consumer reproduces the producer's unchanged logical content exactly.
#
# History:
#   Added on jmacd/56 to cover collapse x push/pull replication, a gap left by
#   716 (single-pond collapse) and 712 (compact push).
set -e
source check.sh

echo "=== Experiment: collapse + push/pull replication (file://) ==="

P1=/tmp/717-p1
P2=/tmp/717-p2
REMOTE=/tmp/717-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

count_pack_objects() {
    find "$1/data/_packs/objects" -type f 2>/dev/null | wc -l | tr -d ' '
}

echo "--- Step 1: producer accumulates a multi-version series file ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null

mkdir -p /var/log/app717
cat > /tmp/717-ingest.yaml << 'EOF'
archived_pattern: /var/log/app717/events.log.*
active_pattern: /var/log/app717/events.log
pond_path: /logs/app
EOF
pond mkdir -p /system/run >/dev/null 2>&1
pond mkdir -p /logs/app >/dev/null 2>&1
pond mknod logfile-ingest /system/run/10-events --config-path /tmp/717-ingest.yaml >/dev/null 2>&1

: > /var/log/app717/events.log
for i in 1 2 3 4 5; do
    printf 'event-%d at line %d\n' "$i" "$i" >> /var/log/app717/events.log
    pond run /system/run/10-events >/dev/null 2>&1
done

SRC_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${SRC_MD5}"'" ]' "producer series md5 computed"

echo "--- Step 2: backup add (pushes existing history) ---"
pond backup add origin "file://${REMOTE}" > /tmp/717-backup.log 2>&1
check 'grep -q "added remote origin" /tmp/717-backup.log' "backup add origin succeeded"
TIP_BEFORE=$(pond status 2>/dev/null | awk '/last pushed:/ {print $NF}')
ROOT_BEFORE=$(pond fsck 2>/dev/null)
PACKS_BEFORE=$(count_pack_objects "$P1")
check '[ ${#TIP_BEFORE} -eq 64 ]' "producer pushed tip is a 64-hex content hash"
check '[ ${#ROOT_BEFORE} -eq 64 ]' "producer content root is a 64-hex hash"
check '[ "'"${PACKS_BEFORE}"'" = "0" ]' "no local pack objects exist before maintenance"

echo "--- Step 3: pack-only maintenance ---"
pond maintain --collapse-versions 1 > /tmp/717-collapse.log 2>&1
cat /tmp/717-collapse.log
check 'grep -qE "pack maintenance: [1-9][0-9]* candidate\(s\), [1-9][0-9]* repacked" /tmp/717-collapse.log' \
    "producer repacked at least one candidate"
check 'grep -qE "packs: [1-9][0-9]* object\(s\) written" /tmp/717-collapse.log' \
    "maintenance reports local pack objects written"
PACKS_AFTER=$(count_pack_objects "$P1")
check '[ "'"${PACKS_AFTER}"'" -gt "'"${PACKS_BEFORE}"'" ]' \
    "maintenance created local physical pack objects (${PACKS_BEFORE} -> ${PACKS_AFTER})"

echo "--- Step 4: maintenance leaves content and remote tips unchanged ---"
TIP_AFTER=$(pond status 2>/dev/null | awk '/last pushed:/ {print $NF}')
ROOT_AFTER=$(pond fsck 2>/dev/null)
check '[ "'"${TIP_AFTER}"'" = "'"${TIP_BEFORE}"'" ]' \
    "pushed content tip is unchanged by local pack maintenance"
check '[ "'"${ROOT_AFTER}"'" = "'"${ROOT_BEFORE}"'" ]' \
    "logical content root is unchanged by local pack maintenance"
check '! grep -q "post-commit auto-push" /tmp/717-collapse.log' \
    "pack-only maintenance does not create or auto-push a content commit"

echo "--- Step 5: verify clean after local maintenance ---"
pond verify origin > /tmp/717-verify.log 2>&1
check_contains /tmp/717-verify.log "verify clean after pack-only maintenance" "live data matches remote"

echo "--- Step 6: fresh consumer pulls and reproduces content ---"
export POND="$P2"
pond init --birthplace test-host >/dev/null
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
pond pull upstream > /tmp/717-pull.log 2>&1
check 'grep -q "pull upstream complete" /tmp/717-pull.log' "consumer completed cross-pond import"

IMPORTED_MD5=$(pond cat /imports/up/logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${IMPORTED_MD5}"'" = "'"${SRC_MD5}"'" ]' "consumer content matches producer after local maintenance"
check 'pond cat /imports/up/logs/app/events.log | grep -q "event-1 at line 1"' "first version survives replication"
check 'pond cat /imports/up/logs/app/events.log | grep -q "event-5 at line 5"' "last version survives replication"

check_finish
