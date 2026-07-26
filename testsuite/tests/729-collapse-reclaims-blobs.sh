#!/bin/bash
# EXPERIMENT: `pond maintain --collapse-versions` actually reclaims disk
#
# DESCRIPTION:
#   Collapse merges a window of series versions into one run row, but the rows
#   it superseded survive -- and each one still references its `_large_files`
#   blob. Until those rows are deleted, collapse bounds a pond's growth RATE
#   without ever returning a byte. This test drives the whole reclamation path
#   through the CLI: it grows a series whose every version exceeds the
#   large-file threshold (64 KiB) so each lands as its own blob, collapses, and
#   asserts that blobs and BYTES on disk actually go down.
#
#   The dangerous failure mode is the opposite one: sweeping a blob that is
#   still live. That is silent -- the pond looks fine until something reads it
#   -- so the test re-reads the content and runs `pond fsck`, whose content
#   pass re-validates every surviving row against its blob.
#
# EXPECTED:
#   - Each ingest version externalizes its own blob under data/_large_files.
#   - `pond maintain --collapse-versions 1` reports collapsed files AND a
#     reclaim line with deleted rows and freed blobs.
#   - Blob count and total bytes both fall.
#   - Series content is byte-identical afterwards and `pond fsck` is clean.
#   - Once settled, a further pass reclaims nothing (a sweep that keeps
#     finding garbage is a sweep that is eating live data).
#
# History:
#   Added on jmacd/analysis8 with reclamation, the second half of collapse.
set -e
source check.sh

echo "=== Experiment: collapse reclaims large-file blobs ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

LARGE_FILES=/pond/data/_large_files

count_blobs() {
    find "${LARGE_FILES}" -name 'blake3=*.parquet' 2>/dev/null | wc -l | tr -d ' '
}
blob_bytes() {
    find "${LARGE_FILES}" -name 'blake3=*.parquet' -printf '%s\n' 2>/dev/null |
        awk '{t+=$1} END {print t+0}'
}

mkdir -p /var/log/app
cat > /tmp/729-ingest.yaml << 'EOF'
archived_pattern: /var/log/app/events.log.*
active_pattern: /var/log/app/events.log
pond_path: /logs/app
EOF
pond mkdir -p /system/run >/dev/null 2>&1
pond mkdir -p /logs/app >/dev/null 2>&1
pond mknod logfile-ingest /system/run/10-events --config-path /tmp/729-ingest.yaml >/dev/null 2>&1

echo "--- Step 1: 12 versions, each above the 64 KiB large-file threshold ---"
: > /var/log/app/events.log
for i in $(seq 1 12); do
    # ~96 KiB of DISTINCT bytes per version: identical content would dedup to a
    # single content-addressed blob and quietly weaken every assertion below.
    head -c 98304 /dev/urandom | base64 | tr -d '\n' | head -c 98304 >> /var/log/app/events.log
    printf '\nversion %d\n' "$i" >> /var/log/app/events.log
    pond run /system/run/10-events >/dev/null 2>&1
done

BEFORE_BLOBS=$(count_blobs)
BEFORE_BYTES=$(blob_bytes)
echo "before: ${BEFORE_BLOBS} blobs, ${BEFORE_BYTES} bytes"
check '[ "'"${BEFORE_BLOBS}"'" -ge 12 ]' "each version externalized its own blob (${BEFORE_BLOBS})"

POND_SIZE=$(pond cat /logs/app/events.log 2>/dev/null | wc -c | tr -d ' ')
HOST_SIZE=$(wc -c < /var/log/app/events.log | tr -d ' ')
check '[ "'"${POND_SIZE}"'" = "'"${HOST_SIZE}"'" ]' "ingested series matches host log (${POND_SIZE} bytes)"

BEFORE_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${BEFORE_MD5}"'" ]' "pre-collapse content md5 computed"

echo "--- Step 2: collapse, which must also reclaim ---"
pond maintain --collapse-versions 1 > /tmp/729-collapse.log 2>&1
cat /tmp/729-collapse.log
check 'grep -qE "collapse: [1-9][0-9]* file\(s\) collapsed" /tmp/729-collapse.log' "maintain collapsed >=1 file"
check 'grep -qE "reclaim: [1-9][0-9]* superseded row\(s\) deleted" /tmp/729-collapse.log' "reclaim deleted superseded rows"
check 'grep -qE "reclaim: .* [1-9][0-9]* blob\(s\) freed" /tmp/729-collapse.log' "reclaim freed blobs"

AFTER_BLOBS=$(count_blobs)
AFTER_BYTES=$(blob_bytes)
echo "after: ${AFTER_BLOBS} blobs, ${AFTER_BYTES} bytes"
check '[ "'"${AFTER_BLOBS}"'" -lt "'"${BEFORE_BLOBS}"'" ]' "blob count fell ${BEFORE_BLOBS} -> ${AFTER_BLOBS}"
check '[ "'"${AFTER_BYTES}"'" -lt "'"${BEFORE_BYTES}"'" ]' "bytes on disk fell ${BEFORE_BYTES} -> ${AFTER_BYTES}"

echo "--- Step 3: nothing live was swept ---"
AFTER_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${AFTER_MD5}"'" = "'"${BEFORE_MD5}"'" ]' "content byte-identical after reclamation"

# A clean fsck exits 0 and prints its root hash; a content failure exits
# non-zero naming the missing blob.
if pond fsck > /tmp/729-fsck.log 2>&1; then FSCK_RC=0; else FSCK_RC=1; fi
cat /tmp/729-fsck.log
check '[ "'"${FSCK_RC}"'" = "0" ]' "fsck exits clean after reclamation"
check 'grep -qE "^[0-9a-f]{64}$" /tmp/729-fsck.log' "fsck printed a root hash over the surviving rows"

echo "--- Step 4: settle, then a further pass must reclaim nothing ---"
for _ in 1 2 3 4 5; do
    pond maintain --collapse-versions 1 > /tmp/729-settle.log 2>&1
    grep -qE "collapse: 0 file\(s\) collapsed" /tmp/729-settle.log && break
done
SETTLED_BLOBS=$(count_blobs)
pond maintain --collapse-versions 1 > /tmp/729-again.log 2>&1
cat /tmp/729-again.log
check '! grep -q "reclaim:" /tmp/729-again.log' "settled pond reclaims nothing"
check '[ "$(find '"${LARGE_FILES}"' -name "blake3=*.parquet" | wc -l | tr -d " ")" = "'"${SETTLED_BLOBS}"'" ]' \
    "blob set is stable once settled (${SETTLED_BLOBS})"

FINAL_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${FINAL_MD5}"'" = "'"${BEFORE_MD5}"'" ]' "content still intact after repeated passes"

check_finish
