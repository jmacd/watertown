#!/bin/bash
# EXPERIMENT: version collapse must not double-count format-cached reads.
#
# DESCRIPTION:
#   External-format URLs (jsonlogs, oteljson, csv, weblog, excelhtml) are read
#   through the format cache: each version of the source file is converted once
#   to Parquet under {POND}/cache/{scheme}_{node}/v{N}_{hash}.parquet, and the
#   query runs over those Parquets.
#
#   That cache was read by GLOBBING the node's cache directory, which is only
#   correct while "every Parquet ever written" equals "every version that is
#   live". Version collapse breaks exactly that equality: it replaces a run of
#   versions with ONE merged version carrying the same bytes. The superseded
#   versions' Parquets stay on disk, so the glob returned the merged version
#   AND the versions it stands for -- every row counted twice.
#
#   The bug is silent. `pond cat` reads the pond directly and stays correct, so
#   the raw file looks fine; only the typed/derived read doubles. It scales with
#   collapse, so it grows precisely on the ponds that ingest the most.
#
# EXPECTED:
#   - A derived series over a jsonlogs source reads the same row count before
#     and after `pond maintain --collapse-versions`.
#   - The raw bytes are unaffected either way.
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

echo "--- collapse ---"
pond maintain --collapse-versions 1 > /tmp/731-collapse.log 2>&1
grep -E "collapse|reclaim" /tmp/731-collapse.log
check 'grep -qE "collapse: [1-9][0-9]* file\(s\) collapsed" /tmp/731-collapse.log' \
    "the ingested series was actually collapsed"

echo "--- after collapse ---"
RAW_AFTER=$(pond cat /measure/pond.jsonl 2>/dev/null | grep -c "2024-01-01" || true)
DERIVED_AFTER=$(pond cat --format table /derived-cpu 2>/dev/null | grep -c "2024-01-01" || true)
check '[ "'"${RAW_AFTER}"'" = "9" ]' "raw series still holds 9 lines (${RAW_AFTER})"
check '[ "'"${DERIVED_AFTER}"'" = "9" ]' \
    "derived series still reads 9 rows, not 18 (${DERIVED_AFTER})"

# A second collapse pass must not compound the error either.
pond maintain --collapse-versions 1 >/dev/null 2>&1 || true
DERIVED_AGAIN=$(pond cat --format table /derived-cpu 2>/dev/null | grep -c "2024-01-01" || true)
check '[ "'"${DERIVED_AGAIN}"'" = "9" ]' \
    "a second collapse pass keeps the count at 9 (${DERIVED_AGAIN})"

pond fsck > /tmp/731-fsck.log 2>&1
check '[ $? -eq 0 ]' "pond fsck passes after collapse"

check_finish
