#!/bin/bash
# REQUIRES: compose
# TEST: Incremental MinIO pull bounds commit ancestry reads
# DESCRIPTION:
#   Build a producer remote with enough separately-pushed history to make a
#   full ancestry walk expensive, import it into a consumer, then advance the
#   producer by one commit. Trace the second pull at MinIO and require both
#   convergence and a bounded number of physical requests.
#
#   This exercises the production CLI path that unit tests cannot:
#     durable graft pin -> pond pull -> bounded ancestry fetch -> MinIO.
#   Before the bounded-ancestry fix, the second pull fetched every historical
#   commit through a Delta point query and exceeded the request ceiling below.
set -euo pipefail
source check.sh

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="bounded-pull-543"
HISTORY_COMMITS=16
REQUEST_CEILING=600

TRACE_PID=
cleanup() {
    if [[ -n "${TRACE_PID}" ]]; then
        kill "${TRACE_PID}" 2>/dev/null || true
        wait "${TRACE_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

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

export POND=/tmp/bounded-pull-producer
rm -rf "${POND}"
pond init --birthplace bounded-pull-producer >/dev/null
pond mkdir /data >/dev/null
pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond push origin >/dev/null

echo "Building ${HISTORY_COMMITS} separately-pushed historical commits..."
for i in $(seq 1 "${HISTORY_COMMITS}"); do
    printf 'historical commit %02d\n' "${i}" > "/tmp/history-${i}.txt"
    pond copy "host:///tmp/history-${i}.txt" "/data/history-${i}.txt" >/dev/null
    pond push origin >/dev/null
done

export POND=/tmp/bounded-pull-consumer
rm -rf "${POND}"
pond init --birthplace bounded-pull-consumer >/dev/null
pond remote add upstream "s3://${BUCKET_NAME}" /imports/source \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null

INITIAL_TRACE_LOG=/tmp/bounded-pull-initial-trace.log
start_trace "${INITIAL_TRACE_LOG}"
pond pull upstream >/dev/null
INITIAL_REQUESTS=$(stop_trace "${INITIAL_TRACE_LOG}")

check \
    "test \"$(pond cat /imports/source/data/history-${HISTORY_COMMITS}.txt)\" = \"historical commit ${HISTORY_COMMITS}\"" \
    "initial graft import converges"

export POND=/tmp/bounded-pull-producer
printf 'one bounded append\n' > /tmp/incremental.txt
pond copy host:///tmp/incremental.txt /data/incremental.txt >/dev/null
pond push origin >/dev/null

TRACE_LOG=/tmp/bounded-pull-minio-trace.log
start_trace "${TRACE_LOG}"

export POND=/tmp/bounded-pull-consumer
pond pull upstream >/tmp/bounded-pull-command.log 2>&1
REQUESTS=$(stop_trace "${TRACE_LOG}")

GETS=$(grep -c 's3.GetObject' "${TRACE_LOG}" || true)
echo "Initial pull MinIO traffic: ${INITIAL_REQUESTS} read request(s)"
echo "Incremental pull MinIO traffic: ${REQUESTS} read request(s), ${GETS} GET(s)"

check \
    "test \"$(pond cat /imports/source/data/incremental.txt)\" = 'one bounded append'" \
    "incremental graft import converges"
check \
    "test '${REQUESTS}' -gt 0" \
    "MinIO trace captured physical reads"
check \
    "test '${REQUESTS}' -le '${REQUEST_CEILING}'" \
    "incremental pull stays within ${REQUEST_CEILING} MinIO read requests"
check \
    "test $((REQUESTS * 100)) -le $((INITIAL_REQUESTS * 70))" \
    "incremental pull uses at most 70% of the full-history initial pull"
check_not_contains \
    /tmp/bounded-pull-command.log \
    "incremental pull reports no rate-limit or fetch error" \
    "[ERR]"

check_finish
