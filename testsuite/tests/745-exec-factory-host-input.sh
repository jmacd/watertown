#!/bin/bash
# REQUIRES: privileged
# EXPERIMENT: Explicit hostmount paths are projected read-only into exec sandboxes.
# EXPECTED: A run requires -d, copies host bytes into Pond outputs, and cannot alter the host.
set -e
source check.sh

echo "=== Experiment: exec factory read-only host input ==="

pond init --birthplace test-host >/dev/null
pond mkdir -p /system/etc >/dev/null 2>&1

HOST_ROOT=/tmp/745-host
mkdir -p "${HOST_ROOT}/statements"
printf '%%PDF-1.7\nstatement bytes\n' > "${HOST_ROOT}/statements/bank.pdf"
cp "${HOST_ROOT}/statements/bank.pdf" /tmp/745-original.pdf

cat > /tmp/745-exec.yaml << 'EOF'
program: /bin/sh
args:
  - "-c"
  - "mkdir -p accounting; cat /inbox/bank.pdf > accounting/bank.pdf; ! echo changed > /inbox/bank.pdf"
inputs: []
outputs: ["/accounting/"]
host_inputs:
  - source: /statements
    target: /inbox
EOF
pond mknod exec /system/etc/745-host-input --config-path /tmp/745-exec.yaml >/dev/null 2>&1

WITHOUT_ROOT=$(pond run /system/etc/745-host-input 2>&1 || true)
check 'echo "$WITHOUT_ROOT" | grep -q "requires an explicit pond -d"' \
    "pond-stored config cannot select an ambient host root"
check '! pond cat /accounting/bank.pdf >/dev/null 2>&1' \
    "failed run leaves Pond unchanged"

pond -d "${HOST_ROOT}" run /system/etc/745-host-input >/tmp/745-run.log 2>&1
pond cat /accounting/bank.pdf >/tmp/745-pond.pdf 2>/dev/null

check 'cmp /tmp/745-original.pdf /tmp/745-pond.pdf' \
    "sandbox copied the host statement into Pond"
check 'cmp /tmp/745-original.pdf "${HOST_ROOT}/statements/bank.pdf"' \
    "sandbox could not modify the host statement"

ln -s /tmp "${HOST_ROOT}/escape"
cat > /tmp/745-escape.yaml << 'EOF'
program: /bin/cat
args: ["/escaped/745-original.pdf"]
inputs: []
outputs: []
host_inputs:
  - source: /escape
    target: /escaped
EOF
pond mknod exec /system/etc/745-escape --config-path /tmp/745-escape.yaml >/dev/null 2>&1
ESCAPE=$(pond -d "${HOST_ROOT}" run /system/etc/745-escape 2>&1 || true)
check 'echo "$ESCAPE" | grep -q "escapes the explicit hostmount root"' \
    "host input symlink cannot escape the selected root"

check_finish
