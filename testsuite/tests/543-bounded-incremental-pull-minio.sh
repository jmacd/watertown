#!/bin/bash
# REQUIRES: compose
# TEST: Commit-indexed MinIO pull bounds lagged ancestry and metadata reads
# DESCRIPTION:
#   Build a producer with enough separately-committed history to make a full
#   ancestry walk expensive, publish it, and import it into a consumer. Then make eight
#   separate local commits, publish them in two producer pushes, and let the
#   consumer catch up only afterward. Trace that lagged pull at MinIO and
#   require one commit-partition scan, no object point queries, exactly two
#   object batches, convergence, and a conservative physical-read ceiling.
#   The producer's current tree contains every historical file, so this also
#   catches a regression to one Delta partition scan per current object.
#
#   This exercises the production CLI path that unit tests cannot:
#     durable graft pin -> pond pull -> indexed ancestry -> exact closure -> MinIO.
#   The commits partition is the cost invariant: producer lag must never turn
#   into one full objects-partition scan per intermediate parent hash.
set -euo pipefail
source check.sh

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="bounded-pull-543"
HISTORY_COMMITS=16
LAG_COMMITS=8
REQUEST_CEILING=200
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
attach_origin
pond backup remove origin >/dev/null

echo "Building ${HISTORY_COMMITS} separate local historical commits..."
for i in $(seq 1 "${HISTORY_COMMITS}"); do
    printf 'historical commit %02d\n' "${i}" > "${WORK_DIR}/history-${i}.txt"
    pond copy "host://${WORK_DIR}/history-${i}.txt" "/data/history-${i}.txt" >/dev/null
done
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

export POND="${WORK_DIR}/producer"
echo "Creating ${LAG_COMMITS} separate local commits across two producer pushes..."
pond backup remove origin >/dev/null
for i in $(seq 1 "$((LAG_COMMITS / 2))"); do
    printf 'lagged commit %02d\n' "${i}" > "${WORK_DIR}/lagged-${i}.txt"
    pond copy "host://${WORK_DIR}/lagged-${i}.txt" "/data/lagged-${i}.txt" >/dev/null
done
attach_origin
pond backup remove origin >/dev/null
for i in $(seq "$((LAG_COMMITS / 2 + 1))" "${LAG_COMMITS}"); do
    printf 'lagged commit %02d\n' "${i}" > "${WORK_DIR}/lagged-${i}.txt"
    pond copy "host://${WORK_DIR}/lagged-${i}.txt" "/data/lagged-${i}.txt" >/dev/null
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
COMMIT_OPS=$(printf '%s\n' "${ACCESS_SUMMARY}" | sed -n 's/.* delta_commits_ops=\([0-9][0-9]*\).*/\1/p')
echo "Initial pull MinIO traffic: ${INITIAL_REQUESTS} read request(s)"
echo "Incremental pull MinIO traffic: ${REQUESTS} read request(s), ${GETS} GET(s)"
echo "Incremental pull access summary: ${ACCESS_SUMMARY#*storage_access_summary }"

check \
    "test \"$(pond cat /imports/source/data/lagged-${LAG_COMMITS}.txt)\" = 'lagged commit 08'" \
    "lagged incremental graft import converges"
check \
    "test '${REQUESTS}' -gt 0" \
    "MinIO trace captured physical reads"
check \
    "test '${REQUESTS}' -le '${REQUEST_CEILING}'" \
    "incremental pull stays within ${REQUEST_CEILING} MinIO read requests"
check \
    "test '${POINT_QUERIES}' = 0" \
    "lagged ancestry and closure discovery use no object point queries"
check \
    "test '${BATCH_QUERIES}' = 2" \
    "indexed pull uses exactly two exact current-closure object batches"
check \
    "test '${COMMIT_OPS}' -gt 0" \
    "indexed pull reads the dedicated Delta commits partition"
check_not_contains \
    "${COMMAND_LOG}" \
    "incremental pull reports no rate-limit or fetch error" \
    "[ERR]"

check_finish
