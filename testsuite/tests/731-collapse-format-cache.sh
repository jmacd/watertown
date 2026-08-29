#!/bin/bash
# EXPERIMENT: pack-only version maintenance preserves format-cached reads.
#
# DESCRIPTION:
#   External-format URLs (jsonlogs, oteljson, csv, weblog, excelhtml) are read
#   through the format cache: each version of the source file is converted once
#   to Parquet under {POND}/cache/{scheme}_{node}/v{N}_{hash}.parquet, and the
#   query runs over those Parquets.
#
#   Native logical-series-v2 maintenance is pack-only: it publishes a bounded
#   physical pack without replacing or deleting the immutable Oplog versions
#   that key this cache. Raw and format-cached reads must therefore remain
#   logically identical, and a repeated maintenance pass must settle without
#   creating another pack.
#
# EXPECTED:
#   - A derived series over a jsonlogs source reads the same row count before
#     and after `pond maintain --collapse-versions`.
#   - The raw bytes are unaffected either way.
#   - First maintenance repacks the source; the second reports 0 repacked /
#     already bounded and leaves the pack object count stable.
#
# History:
#   Found on jmacd/analysis8 while testing materialize-series against selfmon's
#   topology: the materialized output held 18 rows for 9 distinct timestamps.
#   Regression for the fix that names live version Parquets explicitly instead
#   of globbing the cache directory.
set -e
source check.sh

echo "=== Experiment: collapse + format cache must not duplicate rows ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

PACKS=/pond/data/_packs
count_pack_objects() {
    find "${PACKS}/objects" -type f 2>/dev/null | wc -l | tr -d ' '
}

mkdir -p /var/log/metrics
: > /var/log/metrics/pond.jsonl
emit() {
    printf '{"ts":"%s","peak_rss.bytes":"%s","list.seconds":"%s"}\n' "$1" "$2" "$3" \
        >> /var/log/metrics/pond.jsonl
}

pond mkdir -p /system/etc >/dev/null 2>&1
pond mkdir -p /measure >/dev/null 2>&1

cat > /tmp/731-ingest.yaml << 'EOF'
archived_pattern: /var/log/metrics/pond.jsonl.*
active_pattern: /var/log/metrics/pond.jsonl
pond_path: /measure
EOF
pond mknod logfile-ingest /system/etc/ingest --config-path /tmp/731-ingest.yaml >/dev/null 2>&1

cat > /tmp/731-derived.yaml << 'EOF'
patterns:
  m: "jsonlogs:///measure/pond.jsonl"
query: >-
  SELECT
    CAST(ts AS TIMESTAMP)            as timestamp,
    CAST("peak_rss.bytes" AS BIGINT) as "peak_rss.bytes"
  FROM m
  ORDER BY timestamp
EOF
pond mknod sql-derived-series /derived-cpu --config-path /tmp/731-derived.yaml >/dev/null 2>&1

# One version per tick is what makes this a collapse candidate.
for i in 0 1 2 3 4 5 6 7 8; do
    emit "2024-01-01T00:0${i}:00Z" "$((1000 + i))" "0.${i}"
    pond run /system/etc/ingest >/dev/null 2>&1
done

echo "--- before collapse ---"
RAW_BEFORE=$(pond cat /measure/pond.jsonl 2>/dev/null | grep -c "2024-01-01" || true)
DERIVED_BEFORE=$(pond cat --format table /derived-cpu 2>/dev/null | grep -c "2024-01-01" || true)
check '[ "'"${RAW_BEFORE}"'" = "9" ]' "raw series holds 9 lines (${RAW_BEFORE})"
check '[ "'"${DERIVED_BEFORE}"'" = "9" ]' "derived series reads 9 rows (${DERIVED_BEFORE})"

echo "--- pack maintenance ---"
BEFORE_PACKS=$(count_pack_objects)
pond maintain --collapse-versions 1 > /tmp/731-collapse.log 2>&1
grep -E "pack maintenance|packs:|reclaim" /tmp/731-collapse.log
check 'grep -qE "pack maintenance: [1-9][0-9]* candidate\(s\), [1-9][0-9]* repacked" /tmp/731-collapse.log' \
    "the ingested series was physically repacked"
check 'grep -qE "packs: [1-9][0-9]* object\(s\) written" /tmp/731-collapse.log' \
    "maintenance reports bounded pack objects written"
AFTER_PACKS=$(count_pack_objects)
check '[ "'"${AFTER_PACKS}"'" -gt "'"${BEFORE_PACKS}"'" ]' \
    "maintenance created local pack objects (${BEFORE_PACKS} -> ${AFTER_PACKS})"

echo "--- after collapse ---"
RAW_AFTER=$(pond cat /measure/pond.jsonl 2>/dev/null | grep -c "2024-01-01" || true)
DERIVED_AFTER=$(pond cat --format table /derived-cpu 2>/dev/null | grep -c "2024-01-01" || true)
check '[ "'"${RAW_AFTER}"'" = "9" ]' "raw series still holds 9 lines (${RAW_AFTER})"
check '[ "'"${DERIVED_AFTER}"'" = "9" ]' \
    "derived series still reads 9 rows, not 18 (${DERIVED_AFTER})"

# A second pass must settle without changing either pack or query state.
pond maintain --collapse-versions 1 > /tmp/731-collapse2.log 2>&1
cat /tmp/731-collapse2.log
check 'grep -qE "pack maintenance: [1-9][0-9]* candidate\(s\), 0 repacked, [1-9][0-9]* already bounded" /tmp/731-collapse2.log' \
    "the repeated maintenance pass finds the source already bounded"
check 'grep -qE "packs: 0 object\(s\) written" /tmp/731-collapse2.log' \
    "the repeated maintenance pass writes no new pack objects"
check '[ "$(count_pack_objects)" = "'"${AFTER_PACKS}"'" ]' \
    "pack object count is stable across repeated maintenance"
DERIVED_AGAIN=$(pond cat --format table /derived-cpu 2>/dev/null | grep -c "2024-01-01" || true)
check '[ "'"${DERIVED_AGAIN}"'" = "9" ]' \
    "a second maintenance pass keeps the count at 9 (${DERIVED_AGAIN})"

# Capture the status immediately: `check` evaluates its argument later, by
# which point `$?` reflects check's own machinery, not fsck -- so an inline
# `[ $? -eq 0 ]` is vacuously true and would pass even when fsck fails.
pond fsck > /tmp/731-fsck.log 2>&1
FSCK_RC=$?
check '[ "'"${FSCK_RC}"'" -eq 0 ]' "pond fsck passes after pack maintenance (rc=${FSCK_RC})"

check_finish
