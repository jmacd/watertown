#!/bin/bash
# EXPERIMENT: Multi-version table:series pack maintenance via `pond maintain`
#
# DESCRIPTION:
#   A TablePhysicalSeries appends one immutable logical leaf/Oplog row per
#   ingest. Native logical-series-v2 maintenance does not re-encode those rows
#   into one replacement version. Instead it publishes a bounded set of
#   content-addressed Parquet pack objects under data/_packs while preserving
#   the series manifest, Oplog rows, logical content, and query results.
#
# EXPECTED:
#   - Three `host+series://` copies onto one path build a 3-version
#     table:series (21 rows, three distinct days).
#   - `pond maintain --collapse-versions 1` reports a real repack and creates
#     physical pack storage.
#   - After maintenance the series still reads all 21 rows with every day
#     intact, preserving its logical content.
#   - Oplog version rows remain intact, and a second maintain reports
#     0 repacked / already bounded without writing another pack.
#   - Appending a 4th version extends the logical series and makes it a repack
#     candidate again.
#
# History:
#   Added alongside table:series collapse support, then updated for the v2
#   pack-only maintenance contract.
set -e
source check.sh

echo "=== Experiment: table:series version collapse ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

PACKS=/pond/data/_packs
count_pack_objects() {
    find "${PACKS}/objects" -type f 2>/dev/null | wc -l | tr -d ' '
}

# ---- Setup: four parquet files, one per distinct day ------------------------
# Distinct days keep the versions non-overlapping so row counts add cleanly,
# and a distinct constant `temperature` per day tags which version each row
# came from -- that is what proves collapse preserved every version's rows.
mkdir -p /tmp/728-exp
pond mkdir /gen >/dev/null
pond mkdir /data >/dev/null

for d in 1 2 3 4; do
    cat > "/tmp/728-day${d}.yaml" << YAML
start: "2024-01-0${d}T00:00:00Z"
end: "2024-01-0${d}T06:00:00Z"
interval: "1h"
time_column: "timestamp"
points:
  - name: "temperature"
    components:
      - type: line
        slope: 0.0
        offset: ${d}0.0
YAML
    pond mknod synthetic-timeseries "/gen/day${d}" \
        --config-path "/tmp/728-day${d}.yaml" >/dev/null
    pond copy "/gen/day${d}" "host:///tmp/728-exp" >/dev/null 2>&1
    cp "/tmp/728-exp/gen/day${d}" "/tmp/728-v${d}.parquet"
done

# Helper: number of immutable Oplog versions. Pack-only maintenance deliberately
# leaves these logical rows intact and bounds their physical read layout instead.
oplog_versions() {
    pond describe /data/well.series 2>/dev/null | grep -cE '^ *Version [0-9]+:'
}
series_rows() {
    local where="${1:-1=1}"
    pond cat --sql "SELECT count(*) AS n FROM source WHERE ${where}" \
        --format table /data/well.series 2>/dev/null \
        | grep -E '^\| *[0-9]+ *\|' | grep -oE '[0-9]+' | head -1
}

# ---- Step 1: build a 3-version table:series --------------------------------
# `host+series://` onto an existing path appends a NEW VERSION rather than
# overwriting, which is exactly the per-version parquet fan-out under test.
echo "--- Step 1: accumulate three table:series versions ---"
for d in 1 2 3; do
    pond copy "host+series:///tmp/728-v${d}.parquet" /data/well.series >/dev/null 2>&1
done

BEFORE_ROWS=$(series_rows)
check '[ "'"${BEFORE_ROWS}"'" = "21" ]' \
    "three versions merge on read into 21 rows, got ${BEFORE_ROWS}"

BEFORE_D1=$(series_rows "temperature = 10.0")
BEFORE_D2=$(series_rows "temperature = 20.0")
BEFORE_D3=$(series_rows "temperature = 30.0")
check '[ "'"${BEFORE_D1}"'" = "7" ] && [ "'"${BEFORE_D2}"'" = "7" ] && [ "'"${BEFORE_D3}"'" = "7" ]' \
    "each version contributes 7 rows before collapse (${BEFORE_D1}/${BEFORE_D2}/${BEFORE_D3})"

# Distinguishes this test from 716: collapse here must handle parquet, not
# an opaque byte stream, so confirm the node really is a table series -- the
# entry type that both collapse gates used to reject.
pond describe /data/well.series > /tmp/728-describe-before.txt 2>&1
cat /tmp/728-describe-before.txt
check 'grep -q "Type: TablePhysicalSeries" /tmp/728-describe-before.txt' \
    "node is a TablePhysicalSeries (the type collapse used to skip)"

BEFORE_VERSIONS=$(oplog_versions)
check '[ "'"${BEFORE_VERSIONS}"'" = "3" ]' \
    "three immutable Oplog versions were appended, got ${BEFORE_VERSIONS}"
BEFORE_PACKS=$(count_pack_objects)
check '[ "'"${BEFORE_PACKS}"'" = "0" ]' \
    "no pack objects exist before maintenance"

# ---- Step 2: pack maintenance -----------------------------------------------
echo "--- Step 2: publish a bounded physical pack layout ---"
pond maintain --dry-run --collapse-versions 1 > /tmp/728-dry-run.log 2>&1
cat /tmp/728-dry-run.log
check 'grep -q "Dry run: nothing was modified." /tmp/728-dry-run.log' \
    "dry run identifies itself"
check 'grep -qE "^ +/data/well\.series: node .* \(TablePhysicalSeries\): 3 leaf/leaves, 21 logical row\(s\), 3 physical object\(s\) -> 1 proposed \(needs repack\)$" /tmp/728-dry-run.log' \
    "dry run identifies the table:series path, leaf fanout, and required repack"
check '[ "$(count_pack_objects)" = "'"${BEFORE_PACKS}"'" ]' \
    "dry run publishes no pack objects"

pond maintain --collapse-versions 1 > /tmp/728-collapse.log 2>&1
cat /tmp/728-collapse.log

check 'grep -qE "pack maintenance: [1-9][0-9]* candidate\(s\), [1-9][0-9]* repacked" /tmp/728-collapse.log' \
    "maintain repacked the table:series"
check 'grep -qE "packs: [1-9][0-9]* object\(s\) written" /tmp/728-collapse.log' \
    "maintain reports bounded pack objects written"
AFTER_PACKS=$(count_pack_objects)
check '[ "'"${AFTER_PACKS}"'" -gt "'"${BEFORE_PACKS}"'" ]' \
    "pack maintenance durably created physical pack objects (${BEFORE_PACKS} -> ${AFTER_PACKS})"

# ---- Step 3: logical rows and Oplog history are unchanged -------------------
echo "--- Step 3: content survives pack-only maintenance ---"

pond describe /data/well.series > /tmp/728-describe-after.txt 2>&1
cat /tmp/728-describe-after.txt
AFTER_VERSIONS=$(oplog_versions)
check '[ "'"${AFTER_VERSIONS}"'" = "'"${BEFORE_VERSIONS}"'" ]' \
    "pack-only maintenance leaves all ${BEFORE_VERSIONS} Oplog versions intact"

AFTER_ROWS=$(series_rows)
check '[ "'"${AFTER_ROWS}"'" = "'"${BEFORE_ROWS}"'" ]' \
    "row count unchanged after maintenance (${BEFORE_ROWS} -> ${AFTER_ROWS})"

AFTER_D1=$(series_rows "temperature = 10.0")
AFTER_D3=$(series_rows "temperature = 30.0")
check '[ "'"${AFTER_D1}"'" = "7" ]' \
    "oldest version's logical rows remain present, got ${AFTER_D1}"
check '[ "'"${AFTER_D3}"'" = "7" ]' \
    "newest version's logical rows remain present, got ${AFTER_D3}"

# Temporal coverage must still span all three original days.
SPAN=$(pond cat --sql "SELECT count(DISTINCT date_trunc('day', timestamp)) AS n FROM source" \
    --format table /data/well.series 2>/dev/null \
    | grep -E '^\| *[0-9]+ *\|' | grep -oE '[0-9]+' | head -1)
check '[ "'"${SPAN}"'" = "3" ]' \
    "maintained series still spans all three days, got ${SPAN}"

# ---- Step 4: idempotence ----------------------------------------------------
echo "--- Step 4: second maintenance pass is already bounded ---"
pond maintain --collapse-versions 1 > /tmp/728-collapse2.log 2>&1
cat /tmp/728-collapse2.log
check 'grep -qE "pack maintenance: [1-9][0-9]* candidate\(s\), 0 repacked, [1-9][0-9]* already bounded" /tmp/728-collapse2.log' \
    "the repeated attempt finds the table:series already bounded"
check 'grep -qE "packs: 0 object\(s\) written" /tmp/728-collapse2.log' \
    "the repeated attempt writes no new pack objects"
check '[ "$(count_pack_objects)" = "'"${AFTER_PACKS}"'" ]' \
    "pack object count is stable across repeated maintenance"
check '[ "$(oplog_versions)" = "'"${BEFORE_VERSIONS}"'" ]' \
    "repeated maintenance still leaves Oplog history intact"

# ---- Step 5: appends still extend the logical series -------------------------
echo "--- Step 5: append after pack maintenance ---"
pond copy "host+series:///tmp/728-v4.parquet" /data/well.series >/dev/null 2>&1

APPEND_ROWS=$(series_rows)
check '[ "'"${APPEND_ROWS}"'" = "28" ]' \
    "post-maintenance append adds exactly its own 7 rows, got ${APPEND_ROWS}"

APPEND_VERSIONS=$(oplog_versions)
EXPECTED_APPEND_VERSIONS=$((BEFORE_VERSIONS + 1))
check '[ "'"${APPEND_VERSIONS}"'" = "'"${EXPECTED_APPEND_VERSIONS}"'" ]' \
    "append adds one immutable Oplog version (${BEFORE_VERSIONS} -> ${APPEND_VERSIONS})"

APPEND_D4=$(series_rows "temperature = 40.0")
APPEND_D1=$(series_rows "temperature = 10.0")
check '[ "'"${APPEND_D4}"'" = "7" ]' \
    "appended version is readable, got ${APPEND_D4}"
check '[ "'"${APPEND_D1}"'" = "7" ]' \
    "pre-maintenance rows still readable after the append, got ${APPEND_D1}"

# ---- Step 6: the new leaf makes the physical layout repackable again --------
echo "--- Step 6: the extended series repacks again ---"
pond maintain --collapse-versions 1 > /tmp/728-collapse3.log 2>&1
cat /tmp/728-collapse3.log
check 'grep -qE "pack maintenance: [1-9][0-9]* candidate\(s\), [1-9][0-9]* repacked" /tmp/728-collapse3.log' \
    "pack maintenance re-arms once a new logical leaf lands"
check 'grep -qE "packs: [1-9][0-9]* object\(s\) written" /tmp/728-collapse3.log' \
    "the extended logical series publishes a new bounded pack"
FINAL_ROWS=$(series_rows)
check '[ "'"${FINAL_ROWS}"'" = "28" ]' \
    "repeated pack maintenance is lossless, got ${FINAL_ROWS}"
FINAL_VERSIONS=$(oplog_versions)
check '[ "'"${FINAL_VERSIONS}"'" = "'"${APPEND_VERSIONS}"'" ]' \
    "repacking leaves all ${FINAL_VERSIONS} Oplog versions intact"
FINAL_PACKS=$(count_pack_objects)
check '[ "'"${FINAL_PACKS}"'" -ge "'"${AFTER_PACKS}"'" ]' \
    "a renewed repack preserves a bounded physical pack set (${AFTER_PACKS} -> ${FINAL_PACKS})"

check_finish
