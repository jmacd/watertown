#!/bin/bash
# SPDX-FileCopyrightText: 2025 Caspar Water Company
#
# SPDX-License-Identifier: Apache-2.0
#
# Measure the physical request cost of a pond tick as a remote accumulates
# history.  Not a testsuite test: it needs a real S3 server and a fresh
# process per tick, and it reports numbers rather than passing or failing.
#
# The question it answers: does the Nth tick cost what the Nth tick does, or
# does it cost what the remote has accumulated?  Before the checkpoint /
# app-transaction / compaction fixes, a push against a 100-commit remote cost
# 1198 requests, ~99% of which were proportional to the remote's age.
#
# A tick is measured whole -- a commit auto-pushes, so the write and its
# replication are one unit of billable work, and that is what production
# actually does on a timer.
#
# Each tick runs a fresh process on purpose: delta-rs caches table state
# in-process, so a long-lived client hides exactly the cold-open cost that
# production pays every time.
set -euo pipefail

POND_BIN="${POND_BIN:-$(cd "$(dirname "$0")/.." && pwd)/target/release/pond}"
ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET="${BUCKET:-measure-$(date +%s)}"
TICKS="${TICKS:-40}"
WORK="${WORK:-/tmp/wt-measure}"

export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin
export POND="$WORK/pond"
export RUST_LOG=warn

mc() {
    podman run --rm --network=host --entrypoint mc \
        -e MC_HOST_local="http://minioadmin:minioadmin@localhost:9000" \
        docker.io/minio/mc "$@"
}

rm -rf "$WORK"
mkdir -p "$WORK"
mc mb --ignore-existing "local/$BUCKET" >/dev/null

"$POND_BIN" init --birthplace measure-host >/dev/null 2>&1
"$POND_BIN" mkdir /data >/dev/null 2>&1
if [ -n "${LIMITS_YAML:-}" ]; then
    # Attach through the same document shape production uses, so the meter's
    # own accounting can be compared against the trace.
    export S3_URL="s3://$BUCKET" S3_ENDPOINT="$ENDPOINT" S3_REGION=us-east-1
    export S3_ACCESS_KEY="$MINIO_ROOT_USER" S3_SECRET_KEY="$MINIO_ROOT_PASSWORD"
    "$POND_BIN" apply -f "$LIMITS_YAML" >/dev/null 2>&1
    echo "[OK] pond -> s3://$BUCKET (governed by $LIMITS_YAML)"
else
    "$POND_BIN" backup add origin "s3://$BUCKET" \
        --region us-east-1 \
        --endpoint "$ENDPOINT" \
        --access-key-id "$MINIO_ROOT_USER" \
        --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
        --allow-http >/dev/null 2>&1
    echo "[OK] pond -> s3://$BUCKET"
fi

# The trace is ground truth: it counts what crossed the wire, not what the
# client believed it asked for.
podman run --rm --network=host --entrypoint mc \
    -e MC_HOST_local="http://minioadmin:minioadmin@localhost:9000" \
    docker.io/minio/mc admin trace --no-color local >"$WORK/trace.log" 2>&1 &
TRACE_JOB=$!
trap 'kill "$TRACE_JOB" 2>/dev/null || true' EXIT
sleep 4

echo ""
echo "=== $TICKS ticks, one process each ==="
printf '%-6s %-10s %-6s %-6s %-6s %s\n' tick requests GET PUT LIST sec
for i in $(seq 1 "$TICKS"); do
    printf 'timestamp,sensor_id,temperature\n2024-01-%02dT00:00:00Z,sensor-%03d,%d.5\n' \
        $(( (i % 28) + 1 )) "$i" "$i" > "$WORK/m.csv"

    before=$(wc -l < "$WORK/trace.log")
    start=$(date +%s.%N)
    "$POND_BIN" copy "host://$WORK/m.csv" "/data/m$i.csv" >/dev/null 2>&1 || echo "  tick $i FAILED"
    end=$(date +%s.%N)
    sleep 1
    after=$(wc -l < "$WORK/trace.log")

    slice=$(sed -n "$((before+1)),${after}p" "$WORK/trace.log")
    printf '%-6s %-10s %-6s %-6s %-6s %s\n' "$i" "$(( after - before ))" \
        "$(printf '%s' "$slice" | grep -c 's3.GetObject' || true)" \
        "$(printf '%s' "$slice" | grep -c 's3.PutObject' || true)" \
        "$(printf '%s' "$slice" | grep -c 's3.ListObjects' || true)" \
        "$(printf '%.2f' "$(echo "$end - $start" | bc)")"
done

kill "$TRACE_JOB" 2>/dev/null || true
sleep 1

echo ""
echo "=== bytes on the wire (whole run) ==="
python3 - "$WORK/trace.log" <<'PY'
import re, sys
mult = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}
up = down = n = 0
for line in open(sys.argv[1], errors="replace"):
    m = re.search(r"\u2191 ([\d.]+) (\w+) \u2193 ([\d.]+) (\w+)", line)
    if not m:
        continue
    n += 1
    up += float(m.group(1)) * mult.get(m.group(2), 1)
    down += float(m.group(3)) * mult.get(m.group(4), 1)
print(f"requests={n}  up={up/1024:.1f} KiB  down={down/1024:.1f} KiB")
PY

echo ""
echo "=== live objects in the bucket ==="
mc ls --recursive "local/$BUCKET/" | wc -l

echo ""
echo "trace: $WORK/trace.log   bucket: $BUCKET"
