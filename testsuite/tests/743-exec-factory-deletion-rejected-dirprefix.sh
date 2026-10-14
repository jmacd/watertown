#!/bin/bash
# REQUIRES: privileged
# EXPERIMENT: same guarantee as 742, but for a directory-prefix output
#             (`outputs: ["/reports/"]`) with a pre-existing file nested a
#             level deep, not also listed under `inputs`. Exercises the
#             recursive pre-staging of existing directory-prefix outputs
#             (see docs/exec-factory-design.md, "Deletions, failure, and
#             commit semantics").
# EXPECTED:
#   - A `/reports/2026/january.txt` file already exists in the pond before
#     the run.
#   - The exec factory's program removes that nested file inside the
#     sandbox.
#   - `pond run` reports an error.
#   - `/reports/2026/january.txt` is still present in the pond, with its
#     original content, after the rejected run.
set -e
source check.sh

echo "=== Experiment: exec factory rejects deletion of a nested pre-existing output under a directory prefix ==="

pond init --birthplace test-host >/dev/null
pond mkdir -p /system/etc >/dev/null 2>&1
pond mkdir -p /reports/2026 >/dev/null 2>&1

echo -n "prior report, must survive" > /tmp/743-prior.txt
pond copy host:///tmp/743-prior.txt /reports/2026/january.txt >/dev/null 2>&1

cat > /tmp/743-exec.yaml << 'EOF'
program: /bin/sh
args: ["-c", "rm -f reports/2026/january.txt"]
inputs: []
outputs: ["/reports/"]
EOF
pond mknod exec /system/etc/743-delete --config-path /tmp/743-exec.yaml >/dev/null 2>&1

RUN_OUT=$(pond run /system/etc/743-delete 2>&1 || true)
echo "$RUN_OUT"
check 'echo "$RUN_OUT" | grep -qi "error"' \
    "deleting a nested pre-existing output under a directory-prefix output is reported as an error"

pond cat /reports/2026/january.txt > /tmp/743-after.txt 2>/dev/null
cat /tmp/743-after.txt
check_contains /tmp/743-after.txt "the prior nested output still exists in the pond, unchanged" \
    "prior report, must survive"

check_finish
