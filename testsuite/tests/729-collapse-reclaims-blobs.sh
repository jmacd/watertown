#!/bin/bash
# EXPERIMENT: `pond maintain --collapse-versions` runs real pack-only maintenance
#
# DESCRIPTION:
#   Collapse used to merge a window of series versions into one run row and
#   reclaim the rows it superseded (and the `_large_files` blobs those rows
#   were the last referrers of). Native writes are now always
#   logical-series-v2 (delivery gate 7 of the logical-series-identity
#   design), so rewriting Oplog rows into a merged run cannot represent each
#   merged row's immutable per-append logical leaf hash. Instead,
#   `Ship::collapse_versions` -- and therefore `pond maintain
#   --collapse-versions` -- now performs production, pack-only physical
#   maintenance (`docs/logical-series-identity-design.md`,
#   `steward::pack_maintenance`): it repacks an over-threshold native v2
#   series into a smaller, bounded set of content-addressed physical pack
#   objects published under this pond's own local `data/_packs` namespace,
#   without ever rewriting an Oplog row, superseding a row that is still
#   live, or changing a series' `dp.series.2` manifest, content root, Delta
#   version, or txn sequence. This test drives that through the CLI: it
#   grows a series whose every version exceeds the large-file threshold
#   (64 KiB) so each lands as its own blob, then asserts that a dry run
#   previews the repack, a real run performs and reports it, content and the
#   original `_large_files` blobs are unchanged (a repack is purely
#   additive), and a repeated run settles (nothing left to repack).
#
#   The dangerous failure mode would be sweeping a blob that is still live.
#   That is silent -- the pond looks fine until something reads it -- so the
#   test re-reads the content and runs `pond fsck`, whose content pass
#   re-validates every surviving row against its blob, after every step.
#
# EXPECTED:
#   - Each ingest version externalizes its own blob under data/_large_files.
#   - A dry run reports the one repack candidate (12 physical objects ->
#     1 proposed) without changing content, blobs, or pack state.
#   - `pond maintain --collapse-versions 1` (no --dry-run) exits 0, reports
#     one series repacked into one new physical pack object under
#     data/_packs, and leaves the original `_large_files` blobs and series
#     content completely unchanged (a repack is additive: the new pack is
#     one more way to *read* the series, not a replacement for its rows).
#   - Series content is byte-identical afterwards and `pond fsck` is clean.
#   - A repeated attempt settles: the series is already at its achievable
#     bounded layout, so nothing new is written and the candidate no longer
#     needs a repack.
#
# History:
#   Added on jmacd/analysis8 with reclamation, the second half of collapse.
#   Updated on jmacd/incremental1 when logical-series-v2 (delivery gate 7)
#   made collapse's row-rewriting merge unavailable in production; see
#   docs/logical-series-identity-design.md.
#   Updated again on jmacd/incremental1 when pack-only local maintenance
#   (`steward::pack_maintenance`) replaced the gated no-op with a real,
#   safe repack of over-threshold native v2 series.
set -e
source check.sh

echo "=== Experiment: collapse performs pack-only maintenance for logical-series-v2 series ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

LARGE_FILES=/pond/data/_large_files
PACKS=/pond/data/_packs

count_blobs() {
    find "${LARGE_FILES}" -name 'blake3=*.parquet' 2>/dev/null | wc -l | tr -d ' '
}
blob_bytes() {
    find "${LARGE_FILES}" -name 'blake3=*.parquet' -printf '%s\n' 2>/dev/null |
        awk '{t+=$1} END {print t+0}'
}
count_pack_objects() {
    find "${PACKS}/objects" -type f 2>/dev/null | wc -l | tr -d ' '
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
check '[ "$(count_pack_objects)" = "0" ]' "no pack objects exist before any collapse"

POND_SIZE=$(pond cat /logs/app/events.log 2>/dev/null | wc -c | tr -d ' ')
HOST_SIZE=$(wc -c < /var/log/app/events.log | tr -d ' ')
check '[ "'"${POND_SIZE}"'" = "'"${HOST_SIZE}"'" ]' "ingested series matches host log (${POND_SIZE} bytes)"

BEFORE_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${BEFORE_MD5}"'" ]' "pre-collapse content md5 computed"

echo "--- Step 2: dry run previews the repack without doing it ---"
pond maintain --dry-run --collapse-versions 1 > /tmp/729-dry-run.log 2>&1
cat /tmp/729-dry-run.log
check 'grep -q "Dry run: nothing was modified." /tmp/729-dry-run.log' "dry run identifies itself"
check 'grep -qE "collapse: 1 series exceed 1 live versions \(1 would be repacked, 0 already at their achievable bounded layout, 0 pre-v2/unsupported\)" /tmp/729-dry-run.log' \
    "dry run reports one repack candidate"
check 'grep -qE "/logs/app/events.log: node .* \(FilePhysicalSeries\): 12 leaf/leaves, 1179783 logical byte\(s\), 12 physical object\(s\) -> 1 proposed \(needs repack\)" /tmp/729-dry-run.log' \
    "dry run reports the candidate path, current fanout, and proposed bounded layout"
check '[ "$(count_blobs)" = "'"${BEFORE_BLOBS}"'" ]' "dry run leaves the blob set unchanged"
check '[ "$(count_pack_objects)" = "0" ]' "dry run publishes no pack objects"
DRY_RUN_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${DRY_RUN_MD5}"'" = "'"${BEFORE_MD5}"'" ]' "dry run leaves series content unchanged"

echo "--- Step 3: a real collapse repacks the series without touching its blobs ---"
if pond maintain --collapse-versions 1 > /tmp/729-collapse.log 2>&1; then
    COLLAPSE_RC=0
else
    COLLAPSE_RC=1
fi
cat /tmp/729-collapse.log
check '[ "'"${COLLAPSE_RC}"'" = "0" ]' "maintain exits 0 when a real repack happens"
check 'grep -qE "pack maintenance: 1 candidate\(s\), 1 repacked, 0 already bounded, 0 unsupported legacy series" /tmp/729-collapse.log' \
    "maintain reports the one candidate repacked"
check 'grep -qE "packs: 1 object\(s\) written \(1179783 byte\(s\)\), 0 object\(s\) removed \(0 byte\(s\) freed\)" /tmp/729-collapse.log' \
    "maintain reports exactly one new bounded pack object written"

AFTER_BLOBS=$(count_blobs)
AFTER_BYTES=$(blob_bytes)
echo "after: ${AFTER_BLOBS} blobs, ${AFTER_BYTES} bytes"
check '[ "'"${AFTER_BLOBS}"'" = "'"${BEFORE_BLOBS}"'" ]' "original blob count unchanged ${BEFORE_BLOBS} -> ${AFTER_BLOBS} (a repack is additive)"
check '[ "'"${AFTER_BYTES}"'" = "'"${BEFORE_BYTES}"'" ]' "original blob bytes on disk unchanged ${BEFORE_BYTES} -> ${AFTER_BYTES}"
check '[ "$(count_pack_objects)" = "1" ]' "exactly one new physical pack object was durably written"

echo "--- Step 4: nothing about the series' logical content was touched ---"
AFTER_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${AFTER_MD5}"'" = "'"${BEFORE_MD5}"'" ]' "content byte-identical after a real repack"

# A clean fsck exits 0 and prints its root hash; a content failure exits
# non-zero naming the missing blob.
if pond fsck > /tmp/729-fsck.log 2>&1; then FSCK_RC=0; else FSCK_RC=1; fi
cat /tmp/729-fsck.log
check '[ "'"${FSCK_RC}"'" = "0" ]' "fsck exits clean after a repack"
check 'grep -qE "^[0-9a-f]{64}$" /tmp/729-fsck.log' "fsck printed a root hash over the surviving rows"

echo "--- Step 5: pack-only maintenance settles across repeated attempts ---"
if pond maintain --collapse-versions 1 > /tmp/729-again.log 2>&1; then
    AGAIN_RC=0
else
    AGAIN_RC=1
fi
cat /tmp/729-again.log
check '[ "'"${AGAIN_RC}"'" = "0" ]' "a repeated attempt still exits 0"
check 'grep -qE "pack maintenance: 1 candidate\(s\), 0 repacked, 1 already bounded, 0 unsupported legacy series" /tmp/729-again.log' \
    "the repeated attempt finds the series already at its bounded floor"
check 'grep -qE "packs: 0 object\(s\) written \(0 byte\(s\)\), 0 object\(s\) removed \(0 byte\(s\) freed\)" /tmp/729-again.log' \
    "the repeated attempt writes no new pack objects"
check '[ "$(count_blobs)" = "'"${BEFORE_BLOBS}"'" ]' "blob set is stable across repeated settlement"
check '[ "$(count_pack_objects)" = "1" ]' "pack object count is stable (no duplicate repack) across repeated settlement"

FINAL_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${FINAL_MD5}"'" = "'"${BEFORE_MD5}"'" ]' "content still intact after repeated settlement"

pond maintain --dry-run --collapse-versions 1 > /tmp/729-settled-dry-run.log 2>&1
cat /tmp/729-settled-dry-run.log
check 'grep -qE "collapse: 1 series exceed 1 live versions \(0 would be repacked, 1 already at their achievable bounded layout, 0 pre-v2/unsupported\)" /tmp/729-settled-dry-run.log' \
    "a settled series' dry run reports it as already bounded, not needing a repack"
check '[ "$(count_blobs)" = "'"${BEFORE_BLOBS}"'" ]' "final dry run leaves blobs unchanged"
check '[ "$(count_pack_objects)" = "1" ]' "final dry run leaves pack objects unchanged"
SETTLED_DRY_RUN_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${SETTLED_DRY_RUN_MD5}"'" = "'"${BEFORE_MD5}"'" ]' \
    "settled dry run leaves content unchanged"

if pond fsck > /tmp/729-final-fsck.log 2>&1; then FINAL_FSCK_RC=0; else FINAL_FSCK_RC=1; fi
cat /tmp/729-final-fsck.log
check '[ "'"${FINAL_FSCK_RC}"'" = "0" ]' "fsck still exits clean after settlement"

check_finish
