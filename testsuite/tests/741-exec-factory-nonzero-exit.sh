#!/bin/bash
# REQUIRES: privileged
# EXPERIMENT: a nonzero exit from the `exec` sandbox must fail the factory
#             run and leave the pond unchanged -- no partial commit.
# EXPECTED:
#   - `pond run` on an exec node whose program exits nonzero reports an error.
#   - The output path the program wrote to inside the sandbox is NOT present
#     in the pond: a failed run commits nothing, by design (see
#     docs/exec-factory-design.md, "Deletions, failure, and commit semantics").
set -e
source check.sh

echo "=== Experiment: exec factory discards output on nonzero exit ==="

pond init --birthplace test-host >/dev/null
pond mkdir -p /system/etc >/dev/null 2>&1
pond mkdir -p /data >/dev/null 2>&1

cat > /tmp/741-exec.yaml << 'EOF'
program: /bin/sh
args: ["-c", "echo should not be committed > data/out.txt && exit 1"]
inputs: []
outputs: ["/data/out.txt"]
EOF
pond mknod exec /system/etc/741-fail --config-path /tmp/741-exec.yaml >/dev/null 2>&1

RUN_OUT=$(pond run /system/etc/741-fail 2>&1 || true)
echo "$RUN_OUT"
check 'echo "$RUN_OUT" | grep -qi "error"' \
    "a nonzero program exit is reported as an error, not silently ignored"

CAT_OUT=$(pond cat /data/out.txt 2>&1 || true)
echo "$CAT_OUT"
check 'echo "$CAT_OUT" | grep -qi "not found\|no such\|error"' \
    "the output the failed program wrote is NOT present in the pond"

check_finish
