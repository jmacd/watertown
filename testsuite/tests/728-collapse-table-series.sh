#!/bin/bash
# EXPERIMENT: Multi-version table:series collapse via `pond maintain`
#
# DESCRIPTION:
#   716 covers collapse for a FilePhysicalSeries (data:series), which merges
#   versions by BYTE-CONCATENATING them and verifies byte-identity.  A
#   TablePhysicalSeries cannot work that way: each version is a self-contained
#   parquet file with its own footer, and parquet files cannot be
#   concatenated.  Collapsing a table:series therefore has to RE-ENCODE --
#   scan the whole series through its table provider and stream every batch
#   into one new parquet file -- so only LOGICAL equality (row count, per-
#   version rows, temporal coverage) can be asserted, never a byte hash.
#
#   This distinction is why table:series was excluded from collapse entirely:
#   `list_collapsible_series` matched only 'file:physical:series' and
#   `collapse_file_series` hard-errored on anything else.  Both gates were
#   silent -- `pond maintain --collapse-versions N` simply reported
#   "0 file(s) collapsed" for ANY N, so no threshold could ever help.
#
#   That mattered because a table:series is read as a DataFusion ListingTable
#   with ONE PARQUET FILE PER LIVE VERSION, costing ~58ms per version
#   regardless of size (per-file listing + footer schema inference).  On
#   caspar.water an hourly collector accrued ~1 version/hour with 4 rows each,
#   so subsite export time grew without bound while the data did not.
#
# EXPECTED:
#   - Three `host+series://` copies onto one path build a 3-version
#     table:series (21 rows, three distinct days).
#   - `pond maintain --collapse-versions 1` reports >=1 file collapsed.
#     (Before table:series support this reported 0 -- the regression guard.)
#   - After collapse the series still reads all 21 rows with every day intact:
#     re-encoding neither drops superseded versions' rows nor double-counts
#     them.
#   - A second maintain reports 0 collapsed (already merged).
#   - Appending a 4th version after the collapse extends the merged baseline.
#
# History:
#   Added alongside State::collapse_table_series / the table:physical:series
#   arm of list_collapsible_series, which 716 did not reach.
set -e
source check.sh

echo "=== Experiment: table:series version collapse ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

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

# Helper: number of LIVE versions, i.e. the per-version parquet files the
# read path must list and open.  This is the quantity collapse exists to
# shrink -- reads cost ~58ms per live version regardless of size.
live_versions() {
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

BEFORE_VERSIONS=$(live_versions)
check '[ "'"${BEFORE_VERSIONS}"'" = "3" ]' \
    "three live versions => three parquet files to list on every read, got ${BEFORE_VERSIONS}"

# ---- Step 2: collapse -------------------------------------------------------
echo "--- Step 2: collapse the version chain ---"
pond maintain --collapse-versions 1 > /tmp/728-collapse.log 2>&1
cat /tmp/728-collapse.log

# THE REGRESSION GUARD: this line read "0 file(s) collapsed" for every
# threshold before table:series became a collapse candidate.
check 'grep -qE "collapse: [1-9][0-9]* file\(s\) collapsed" /tmp/728-collapse.log' \
    "maintain collapsed the table:series (was always 0 before support)"

# ---- Step 3: re-encoding preserved every row -------------------------------
echo "--- Step 3: content survives the re-encode ---"

# THE POINT OF THE FIX: the read path now lists ONE parquet file instead of
# three, so per-read cost stops scaling with ingest history.
pond describe /data/well.series > /tmp/728-describe-after.txt 2>&1
cat /tmp/728-describe-after.txt
AFTER_VERSIONS=$(live_versions)
check '[ "'"${AFTER_VERSIONS}"'" = "1" ]' \
    "three live versions collapsed into one parquet file, got ${AFTER_VERSIONS}"
check 'grep -qE "^ *Version [0-9]+: 21 rows" /tmp/728-describe-after.txt' \
    "the single surviving version carries all 21 rows"
check 'grep -qE "^ *Version [0-9]+: 21 rows, time range: 2024-01-01 .* to 2024-01-03 " /tmp/728-describe-after.txt' \
    "merged version's temporal bounds span the union of all three versions"

AFTER_ROWS=$(series_rows)
check '[ "'"${AFTER_ROWS}"'" = "'"${BEFORE_ROWS}"'" ]' \
    "row count unchanged after collapse (${BEFORE_ROWS} -> ${AFTER_ROWS})"

AFTER_D1=$(series_rows "temperature = 10.0")
AFTER_D3=$(series_rows "temperature = 30.0")
check '[ "'"${AFTER_D1}"'" = "7" ]' \
    "oldest version's rows survive the merge, got ${AFTER_D1}"
check '[ "'"${AFTER_D3}"'" = "7" ]' \
    "newest version's rows survive the merge, got ${AFTER_D3}"

# Superseded versions must be skipped on read, not replayed alongside the
# merged file -- that would silently double every row.
check '[ "'"${AFTER_ROWS}"'" != "42" ]' \
    "superseded versions are not double-counted after collapse"

# Temporal coverage must span all three original days, not just the last one.
SPAN=$(pond cat --sql "SELECT count(DISTINCT date_trunc('day', timestamp)) AS n FROM source" \
    --format table /data/well.series 2>/dev/null \
    | grep -E '^\| *[0-9]+ *\|' | grep -oE '[0-9]+' | head -1)
check '[ "'"${SPAN}"'" = "3" ]' \
    "merged version still spans all three days, got ${SPAN}"

# ---- Step 4: idempotence ----------------------------------------------------
echo "--- Step 4: second collapse is a no-op ---"
pond maintain --collapse-versions 1 > /tmp/728-collapse2.log 2>&1
cat /tmp/728-collapse2.log
check 'grep -qE "collapse: 0 file\(s\) collapsed" /tmp/728-collapse2.log' \
    "an already-collapsed table:series is no longer a candidate"

# ---- Step 5: appends still work on top of the merged version ----------------
# The merged file carries `collapsed_through`; a later append must extend it
# rather than resurrect the superseded versions.
echo "--- Step 5: append after collapse ---"
pond copy "host+series:///tmp/728-v4.parquet" /data/well.series >/dev/null 2>&1

APPEND_ROWS=$(series_rows)
check '[ "'"${APPEND_ROWS}"'" = "28" ]' \
    "post-collapse append adds exactly its own 7 rows, got ${APPEND_ROWS}"

APPEND_VERSIONS=$(live_versions)
check '[ "'"${APPEND_VERSIONS}"'" = "2" ]' \
    "append extends the merged baseline (1 merged + 1 new), got ${APPEND_VERSIONS}"

APPEND_D4=$(series_rows "temperature = 40.0")
APPEND_D1=$(series_rows "temperature = 10.0")
check '[ "'"${APPEND_D4}"'" = "7" ]' \
    "appended version is readable, got ${APPEND_D4}"
check '[ "'"${APPEND_D1}"'" = "7" ]' \
    "pre-collapse rows still readable after the append, got ${APPEND_D1}"

# ---- Step 6: the new tail is collapsible again ------------------------------
echo "--- Step 6: the merged+appended chain collapses again ---"
pond maintain --collapse-versions 1 > /tmp/728-collapse3.log 2>&1
cat /tmp/728-collapse3.log
check 'grep -qE "collapse: [1-9][0-9]* file\(s\) collapsed" /tmp/728-collapse3.log' \
    "collapse re-arms once a new version lands"
FINAL_ROWS=$(series_rows)
check '[ "'"${FINAL_ROWS}"'" = "28" ]' \
    "repeated collapse is lossless, got ${FINAL_ROWS}"
FINAL_VERSIONS=$(live_versions)
check '[ "'"${FINAL_VERSIONS}"'" = "1" ]' \
    "the chain is back to a single live version, got ${FINAL_VERSIONS}"

check_finish
