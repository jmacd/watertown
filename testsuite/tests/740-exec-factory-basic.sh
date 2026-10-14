#!/bin/bash
# REQUIRES: privileged
# EXPERIMENT: `exec` runs an external program against staged pond files inside
#             a bwrap sandbox, and commits its declared output back into the
#             pond as one transaction.
#
# DESCRIPTION:
#   `exec` is the general-purpose escape hatch for using existing plain-file
#   tools (plaintext-accounting ledgers, report generators, etc.) without
#   reimplementing them as native factories. It stages declared `inputs` into
#   a real host directory, execs `program` inside `bwrap` (no network, no
#   shared PID namespace, read-only system dirs), and on a clean (zero) exit
#   copies any changed `outputs` back into the pond. See
#   docs/exec-factory-design.md.
#
# EXPECTED:
#   - The config is rejected at `mknod` time if `program` is not absolute.
#   - A successful run copies the program's output back into the pond with
#     the exact bytes it produced.
#   - An input staged into the sandbox is NOT itself modified in the pond
#     (it's staged read-only; this run doesn't even try to write it).
#   - Re-running with unchanged inputs still succeeds (idempotent).
set -e
source check.sh

echo "=== Experiment: exec factory runs cat/sh against staged pond files ==="

pond init --birthplace test-host >/dev/null

pond mkdir -p /system/etc >/dev/null 2>&1
pond mkdir -p /data >/dev/null 2>&1

echo "--- Step 1: an invalid config (relative program path) is rejected ---"
cat > /tmp/740-invalid.yaml << 'EOF'
program: cat
inputs: ["/data/in.txt"]
outputs: []
EOF
INVALID_OUT=$(pond mknod exec /system/etc/740-invalid --config-path /tmp/740-invalid.yaml 2>&1 || true)
echo "$INVALID_OUT"
check 'echo "$INVALID_OUT" | grep -qi "absolute path"' \
    "relative 'program' path is rejected at mknod time"

echo "--- Step 2: write the input file the sandboxed program will read ---"
echo -n "hello from the pond" > /tmp/740-in.txt
pond copy host:///tmp/740-in.txt /data/in.txt >/dev/null 2>&1
pond cat /data/in.txt > /tmp/740-in-check.txt 2>/dev/null
check_contains /tmp/740-in-check.txt "input landed in the pond before exec" "hello from the pond"

echo "--- Step 3: create the exec node ---"
cat > /tmp/740-exec.yaml << 'EOF'
program: /bin/sh
args: ["-c", "cat data/in.txt > data/out.txt"]
inputs: ["/data/in.txt"]
outputs: ["/data/out.txt"]
EOF
pond mknod exec /system/etc/740-cat --config-path /tmp/740-exec.yaml >/dev/null 2>&1

echo "--- Step 4: run it; the program's output is committed to the pond ---"
pond run /system/etc/740-cat > /tmp/740-run1.log 2>&1
cat /tmp/740-run1.log
check '! grep -qi "error" /tmp/740-run1.log' "first run reports no error"

pond cat /data/out.txt > /tmp/740-out1.txt 2>/dev/null
cat /tmp/740-out1.txt
check_contains /tmp/740-out1.txt "committed output has the program's bytes" "hello from the pond"

echo "--- Step 5: a second run with unchanged input is idempotent ---"
pond run /system/etc/740-cat > /tmp/740-run2.log 2>&1
cat /tmp/740-run2.log
check '! grep -qi "error" /tmp/740-run2.log' "second run (unchanged input) reports no error"
pond cat /data/out.txt > /tmp/740-out2.txt 2>/dev/null
check 'diff -q /tmp/740-out1.txt /tmp/740-out2.txt >/dev/null' \
    "re-running with the same input produces byte-identical output"

echo "--- Step 6: the input itself was not modified by the run ---"
pond cat /data/in.txt > /tmp/740-in-after.txt 2>/dev/null
check 'diff -q /tmp/740-in-check.txt /tmp/740-in-after.txt >/dev/null' \
    "the staged-read-only input is unchanged in the pond"

check_finish
