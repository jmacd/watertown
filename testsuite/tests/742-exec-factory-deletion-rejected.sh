#!/bin/bash
# REQUIRES: privileged
# EXPERIMENT: deleting a pre-existing output (one not also declared as an
#             input) inside the sandbox must fail the run, and the prior
#             content must survive untouched -- deletions are never
#             committed in v1 (see docs/exec-factory-design.md, "Deletions,
#             failure, and commit semantics").
# EXPECTED:
#   - A `/data/out.txt` file already exists in the pond before the run.
#   - The exec factory's program removes that file inside the sandbox.
#   - `pond run` reports an error.
#   - `/data/out.txt` is still present in the pond, with its original
#     content, after the rejected run.
set -e
source check.sh

echo "=== Experiment: exec factory rejects deletion of a pre-existing output ==="

pond init --birthplace test-host >/dev/null
pond mkdir -p /system/etc >/dev/null 2>&1
pond mkdir -p /data >/dev/null 2>&1

echo -n "prior output, must survive" > /tmp/742-prior.txt
pond copy host:///tmp/742-prior.txt /data/out.txt >/dev/null 2>&1

cat > /tmp/742-exec.yaml << 'EOF'
program: /bin/sh
args: ["-c", "rm -f data/out.txt"]
inputs: []
outputs: ["/data/out.txt"]
EOF
pond mknod exec /system/etc/742-delete --config-path /tmp/742-exec.yaml >/dev/null 2>&1

RUN_OUT=$(pond run /system/etc/742-delete 2>&1 || true)
echo "$RUN_OUT"
check 'echo "$RUN_OUT" | grep -qi "error"' \
    "deleting a pre-existing declared output is reported as an error"

pond cat /data/out.txt > /tmp/742-after.txt 2>&1 || true
cat /tmp/742-after.txt
check_contains /tmp/742-after.txt "the prior output still exists in the pond, unchanged" \
    "prior output, must survive"

check_finish
