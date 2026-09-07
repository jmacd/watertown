#!/bin/bash
# REQUIRES: compose
# TEST: Commit-indexed MinIO pull batches lagged inline series payloads
# DESCRIPTION:
#   Build a producer with enough separately-committed history to make a full
#   ancestry walk expensive, create a small inline table:series baseline,
#   publish it, and import it into a consumer. Then append eight separately
#   committed versions to that same series, publish them in two producer
#   pushes while the consumer remains behind, and let it catch up afterward.
#   Trace that lagged pull at MinIO and require one commit-partition scan, no
#   object point queries, exactly three object batches (two current-closure
#   batches plus one exact suffix-payload batch), content convergence, and a
#   conservative physical-read ceiling.
#
#   This exercises the production CLI path that unit tests cannot:
#     durable graft pin -> pond pull -> indexed ancestry -> exact closure
#     -> validated series prefix -> exact inline suffix payload batch -> MinIO.
#   Producer lag and changed pack objects must never turn into repeated full
#   objects-partition scans.
set -euo pipefail
source check.sh

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="bounded-pull-543"
HISTORY_COMMITS=16
LAG_COMMITS=8
ROWS_PER_VERSION=7
FINAL_SERIES_ROWS=$(((LAG_COMMITS + 1) * ROWS_PER_VERSION))
REQUEST_CEILING=200
# This fixture's current closure names 33 exact metadata keys. The lagged
# suffix contributes one distinct inline physical object per appended version;
# the baseline object's span must be excluded by prefix validation.
EXPECTED_BATCH_KEYS=$((33 + LAG_COMMITS))
WORK_DIR="${PWD}/.test-543-bounded-pull"

TRACE_PID=
cleanup() {
    if [[ -n "${TRACE_PID}" ]]; then
        kill "${TRACE_PID}" 2>/dev/null || true
        wait "${TRACE_PID}" 2>/dev/null || true
    fi
}
final_cleanup() {
    cleanup
    rm -rf "${WORK_DIR}"
}
trap final_cleanup EXIT

rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"

start_trace() {
    local trace_log=$1
    : > "${trace_log}"
    mc admin trace --no-color local >"${trace_log}" 2>&1 &
    TRACE_PID=$!
    sleep 2
}

stop_trace() {
    local trace_log=$1
    sleep 1
    cleanup
    TRACE_PID=
    grep -cE 's3\.(GetObject|HeadObject|ListObjects)' "${trace_log}" || true
}

echo "=== Incremental MinIO pull bounds commit ancestry reads ==="

curl -fsS "${MINIO_ENDPOINT}/minio/health/live" >/dev/null
mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null
mc rb --force "local/${BUCKET_NAME}" >/dev/null 2>&1 || true
mc mb "local/${BUCKET_NAME}" >/dev/null

attach_origin() {
    pond backup add origin "s3://${BUCKET_NAME}" \
        --region us-east-1 \
        --endpoint "${MINIO_ENDPOINT}" \
        --access-key-id "${MINIO_ROOT_USER}" \
        --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
        --allow-http \
        --overwrite >/dev/null
}

export POND="${WORK_DIR}/producer"
pond init --birthplace bounded-pull-producer >/dev/null
pond mkdir /data >/dev/null
pond mkdir /gen >/dev/null
attach_origin
pond backup remove origin >/dev/null

echo "Building ${HISTORY_COMMITS} separate local historical commits..."
for i in $(seq 1 "${HISTORY_COMMITS}"); do
    printf 'historical commit %02d\n' "${i}" > "${WORK_DIR}/history-${i}.txt"
    pond copy "host://${WORK_DIR}/history-${i}.txt" "/data/history-${i}.txt" >/dev/null
done

echo "Building one baseline plus ${LAG_COMMITS} small inline table-series payloads..."
EXPORT_DIR="${WORK_DIR}/series-export"
mkdir -p "${EXPORT_DIR}"
for i in $(seq 0 "${LAG_COMMITS}"); do
    day=$((i + 1))
    value=$((100 + i))
    config="${WORK_DIR}/series-${i}.yaml"
    cat >"${config}" <<YAML
start: "2024-01-$(printf '%02d' "${day}")T00:00:00Z"
end: "2024-01-$(printf '%02d' "${day}")T06:00:00Z"
interval: "1h"
time_column: "timestamp"
points:
  - name: "temperature"
    components:
      - type: line
        slope: 0.0
        offset: ${value}.0
YAML
    pond mknod synthetic-timeseries "/gen/version-${i}" --config-path "${config}" >/dev/null
    pond copy "/gen/version-${i}" "host://${EXPORT_DIR}" >/dev/null 2>&1
    cp "${EXPORT_DIR}/gen/version-${i}" "${WORK_DIR}/series-${i}.parquet"
done
pond copy "host+series://${WORK_DIR}/series-0.parquet" /data/temps.series >/dev/null 2>&1
check \
    "test \"$(head -c4 "${WORK_DIR}/series-0.parquet" | od -A n -t x1 | tr -d ' ')\" = 50415231" \
    "baseline table-series payload is valid Parquet"
check \
    "test \"$(head -c4 "${WORK_DIR}/series-${LAG_COMMITS}.parquet" | od -A n -t x1 | tr -d ' ')\" = 50415231" \
    "last lagged table-series payload is valid Parquet"

# One upgraded push carries the full local commit log and atomically backfills
# the dedicated commit index before the consumer's initial import.
attach_origin

export POND="${WORK_DIR}/consumer"
pond init --birthplace bounded-pull-consumer >/dev/null
pond remote add upstream "s3://${BUCKET_NAME}" /imports/source \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null

INITIAL_TRACE_LOG="${WORK_DIR}/initial-trace.log"
start_trace "${INITIAL_TRACE_LOG}"
pond pull upstream >/dev/null
INITIAL_REQUESTS=$(stop_trace "${INITIAL_TRACE_LOG}")

check \
    "test \"$(pond cat /imports/source/data/history-${HISTORY_COMMITS}.txt)\" = \"historical commit ${HISTORY_COMMITS}\"" \
    "initial graft import converges"
INITIAL_SERIES_LOG="${WORK_DIR}/initial-series.log"
pond cat --sql "SELECT count(*) AS rows, min(temperature) AS min_value, max(temperature) AS max_value FROM source" \
    --format table /imports/source/data/temps.series >"${INITIAL_SERIES_LOG}" 2>&1
check \
    "grep -qE '\\| *${ROWS_PER_VERSION} *\\| *100(\\.0)? *\\| *100(\\.0)? *\\|' '${INITIAL_SERIES_LOG}'" \
    "initial graft contains the ${ROWS_PER_VERSION}-row table-series baseline"

export POND="${WORK_DIR}/producer"
echo "Appending ${LAG_COMMITS} separately committed series versions across two producer pushes..."
pond backup remove origin >/dev/null
for i in $(seq 1 "$((LAG_COMMITS / 2))"); do
    pond copy "host+series://${WORK_DIR}/series-${i}.parquet" /data/temps.series >/dev/null 2>&1
done
attach_origin
pond backup remove origin >/dev/null
for i in $(seq "$((LAG_COMMITS / 2 + 1))" "${LAG_COMMITS}"); do
    pond copy "host+series://${WORK_DIR}/series-${i}.parquet" /data/temps.series >/dev/null 2>&1
done
attach_origin

TRACE_LOG="${WORK_DIR}/incremental-trace.log"
start_trace "${TRACE_LOG}"

export POND="${WORK_DIR}/consumer"
COMMAND_LOG="${WORK_DIR}/pull-command.log"
pond pull upstream >"${COMMAND_LOG}" 2>&1
REQUESTS=$(stop_trace "${TRACE_LOG}")

GETS=$(grep -c 's3.GetObject' "${TRACE_LOG}" || true)
ACCESS_SUMMARY=$(grep 'storage_access_summary' "${COMMAND_LOG}" | tail -1)
POINT_QUERIES=$(printf '%s\n' "${ACCESS_SUMMARY}" | sed -n 's/.* object_point_queries=\([0-9][0-9]*\).*/\1/p')
BATCH_QUERIES=$(printf '%s\n' "${ACCESS_SUMMARY}" | sed -n 's/.* object_batch_queries=\([0-9][0-9]*\).*/\1/p')
BATCH_KEYS=$(printf '%s\n' "${ACCESS_SUMMARY}" | sed -n 's/.* object_batch_keys=\([0-9][0-9]*\).*/\1/p')
BATCH_HITS=$(printf '%s\n' "${ACCESS_SUMMARY}" | sed -n 's/.* object_batch_hits=\([0-9][0-9]*\).*/\1/p')
COMMIT_OPS=$(printf '%s\n' "${ACCESS_SUMMARY}" | sed -n 's/.* delta_commits_ops=\([0-9][0-9]*\).*/\1/p')
echo "Initial pull MinIO traffic: ${INITIAL_REQUESTS} read request(s)"
echo "Incremental pull MinIO traffic: ${REQUESTS} read request(s), ${GETS} GET(s)"
echo "Incremental pull access summary: ${ACCESS_SUMMARY#*storage_access_summary }"

FINAL_SERIES_LOG="${WORK_DIR}/final-series.log"
pond cat --sql "SELECT count(*) AS rows, count(DISTINCT temperature) AS values, CAST(sum(temperature) AS BIGINT) AS total FROM source" \
    --format table /imports/source/data/temps.series >"${FINAL_SERIES_LOG}" 2>&1
PRODUCER_CONTENT="${WORK_DIR}/producer-series-content.log"
CONSUMER_CONTENT="${WORK_DIR}/consumer-series-content.log"
POND="${WORK_DIR}/producer" pond cat \
    --sql "SELECT timestamp, temperature FROM source ORDER BY timestamp, temperature" \
    --format table /data/temps.series >"${PRODUCER_CONTENT}" 2>&1
POND="${WORK_DIR}/consumer" pond cat \
    --sql "SELECT timestamp, temperature FROM source ORDER BY timestamp, temperature" \
    --format table /imports/source/data/temps.series >"${CONSUMER_CONTENT}" 2>&1
PRODUCER_ROWS="${WORK_DIR}/producer-series-rows.log"
CONSUMER_ROWS="${WORK_DIR}/consumer-series-rows.log"
grep -E '^\| *2024-' "${PRODUCER_CONTENT}" >"${PRODUCER_ROWS}"
grep -E '^\| *2024-' "${CONSUMER_CONTENT}" >"${CONSUMER_ROWS}"
if [[ -d /output ]]; then
    cat > /output/543-metrics.txt <<EOF
initial_read_requests=${INITIAL_REQUESTS}
incremental_read_requests=${REQUESTS}
incremental_get_requests=${GETS}
object_point_queries=${POINT_QUERIES}
object_batch_queries=${BATCH_QUERIES}
object_batch_keys=${BATCH_KEYS}
object_batch_hits=${BATCH_HITS}
delta_commits_ops=${COMMIT_OPS}
EOF
fi

check \
    "grep -qE '\\| *${FINAL_SERIES_ROWS} *\\| *$((LAG_COMMITS + 1)) *\\| *6552 *\\|' '${FINAL_SERIES_LOG}'" \
    "lagged pull converges to ${FINAL_SERIES_ROWS} rows across all $((LAG_COMMITS + 1)) series versions"
check \
    "cmp -s '${PRODUCER_ROWS}' '${CONSUMER_ROWS}'" \
    "consumer table-series rows and values exactly match the producer"
check \
    "test '${REQUESTS}' -gt 0" \
    "MinIO trace captured physical reads"
check \
    "test '${REQUESTS}' -le '${REQUEST_CEILING}'" \
    "incremental pull stays within ${REQUEST_CEILING} MinIO read requests"
check \
    "test '${POINT_QUERIES}' = 0" \
    "lagged ancestry, closure, and payload reads use no object point queries"
check \
    "test '${BATCH_QUERIES}' = 3" \
    "indexed pull uses two current-closure batches plus one exact suffix-payload batch"
check \
    "test '${BATCH_KEYS}' = '${EXPECTED_BATCH_KEYS}'" \
    "exact batches request current metadata plus only the ${LAG_COMMITS} lagged payload objects"
check \
    "test '${BATCH_HITS}' = '${EXPECTED_BATCH_KEYS}'" \
    "all lagged series payload objects resolve through the inline batch"
check \
    "test '${COMMIT_OPS}' -gt 0" \
    "indexed pull reads the dedicated Delta commits partition"
check_not_contains \
    "${COMMAND_LOG}" \
    "incremental pull reports no generic error" \
    "[ERR]"
check \
    "! grep -Eiq 'rate[- ]?limit|fetch[^[:cntrl:]]*(error|failed)' '${COMMAND_LOG}'" \
    "incremental pull reports no fetch or rate-limit error"

check_finish
