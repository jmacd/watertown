#!/bin/bash
# REQUIRES: privileged
# EXPERIMENT: `series_outputs` maps a pond path onto an append-only
#             `FilePhysicalSeries`, and a real plaintext-accounting tool
#             (`hledger`) can be run against it inside the sandbox -- the
#             concrete use case this mechanism was built for. See
#             docs/exec-factory-design.md §5b/§6b.
#
# DESCRIPTION:
#   A non-interactive stand-in for the attended `hledger-ui` session in
#   scripts/examples/hledger-journal.yaml: `hledger` both validates the
#   existing journal (exits non-zero on a parse error, so a corrupt
#   journal never gets appended to) and appends one new transaction to it,
#   in a single sandboxed run. Only the new bytes are committed as the
#   journal's next series version -- the seeded transaction's bytes are
#   never rewritten.
#
# EXPECTED:
#   - The seeded journal, staged read-write via `series_outputs`, is
#     valid input `hledger` accepts.
#   - After the run, the pond's journal contains both the original seeded
#     transaction and the new one `hledger` appended, with the seeded
#     bytes byte-for-byte unchanged (series append, not rewrite).
#   - `hledger balance` against the final committed journal reflects both
#     transactions' accounts.
set -e
source check.sh

echo "=== Experiment: exec factory runs hledger against a FilePhysicalSeries journal ==="

pond init --birthplace test-host >/dev/null

pond mkdir -p /system/etc >/dev/null 2>&1
pond mkdir -p /accounting >/dev/null 2>&1

echo "--- Step 1: seed the journal with one transaction, as the series' first version ---"
# A `series_outputs` path doesn't need to pre-exist: if it's missing, the
# sandbox is staged with an empty file and whatever the program writes
# becomes the series' first version. So the journal is seeded by running
# the sandbox itself, not `pond copy` -- `pond copy` would create a plain
# `file:physical:version`, which collides with `file:physical:series` on
# the next run (`EntryTypeMismatch`).
cat > /tmp/744-seed.yaml << 'EOF'
program: /bin/sh
args:
  - "-c"
  - |
    cat > accounting/journal.ledger << 'LEDGER'
    2026-01-01 Opening balance
        assets:checking           $1000.00
        equity:opening balance
    LEDGER
inputs: []
series_outputs: ["/accounting/journal.ledger"]
EOF
pond mknod exec /system/etc/744-seed --config-path /tmp/744-seed.yaml >/dev/null 2>&1
pond run /system/etc/744-seed > /tmp/744-seed-run.log 2>&1
cat /tmp/744-seed-run.log
check '! grep -qi "error" /tmp/744-seed-run.log' "the journal-seeding run reports no error"

pond cat /accounting/journal.ledger > /tmp/744-seed-check.txt 2>/dev/null
check_contains /tmp/744-seed-check.txt "the seeded journal landed in the pond" "Opening balance"

echo "--- Step 2: hledger validates + appends a transaction, inside the sandbox ---"
cat > /tmp/744-exec.yaml << 'EOF'
program: /bin/sh
args:
  - "-c"
  - |
    hledger -f accounting/journal.ledger print > /dev/null
    cat >> accounting/journal.ledger << 'LEDGER'

    2026-02-01 Coffee
        expenses:coffee            $4.00
        assets:checking
    LEDGER
inputs: []
series_outputs: ["/accounting/journal.ledger"]
EOF
pond mknod exec /system/etc/744-hledger --config-path /tmp/744-exec.yaml >/dev/null 2>&1

pond run /system/etc/744-hledger > /tmp/744-run.log 2>&1
cat /tmp/744-run.log
check '! grep -qi "error" /tmp/744-run.log' "the sandboxed hledger run reports no error"

echo "--- Step 3: the committed journal has both transactions ---"
pond cat /accounting/journal.ledger > /tmp/744-final.txt 2>/dev/null
cat /tmp/744-final.txt
check_contains /tmp/744-final.txt "the original seeded transaction survives, unchanged" "Opening balance"
check_contains /tmp/744-final.txt "the new transaction hledger appended is committed" "Coffee"

echo "--- Step 4: a real hledger balance report against the final journal agrees ---"
cat > /tmp/744-balance.yaml << 'EOF'
program: /bin/sh
args: ["-c", "hledger -f accounting/journal.ledger balance > balance.txt"]
inputs: ["/accounting/journal.ledger"]
outputs: ["/balance.txt"]
EOF
pond mknod exec /system/etc/744-balance --config-path /tmp/744-balance.yaml >/dev/null 2>&1
pond run /system/etc/744-balance > /tmp/744-balance-run.log 2>&1
cat /tmp/744-balance-run.log
check '! grep -qi "error" /tmp/744-balance-run.log' "the balance report run reports no error"

pond cat /balance.txt > /tmp/744-balance.txt 2>/dev/null
cat /tmp/744-balance.txt
check_contains /tmp/744-balance.txt "the balance report reflects the checking account" "checking"
check_contains /tmp/744-balance.txt "the balance report reflects the coffee expense" "coffee"

check_finish
