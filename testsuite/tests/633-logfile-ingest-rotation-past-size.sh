#!/bin/bash
# TEST: Logfile Ingest - Rotation where new file grew past old size
# DESCRIPTION: When a logfile is rotated and the new file grows larger than
#   the previously tracked cumulative_size before the next ingestion run,
#   the ingest must detect the rotation (prefix mismatch) rather than
#   treating it as a normal append.
#
# This is the scenario observed on watershop staging: the active file
# was rotated, a new file with different content appeared under the same
# name, and by the time ingestion ran the new file was already larger
# than what the pond had tracked.  The ingest must:
#   1. Detect that the prefix no longer matches (content changed)
#   2. Find the matching archived file
#   3. Rename the pond entry to the archived name
#   4. Ingest the new active file from scratch
#
set -e
source check.sh

echo "=== Test: Rotation Where New File Grew Past Old Size ==="
echo ""

export POND=/pond
pond init --birthplace test-host

#############################
# SETUP
#############################

mkdir -p /var/log/testapp

cat > /tmp/ingest.yaml << 'EOF'
archived_pattern: /var/log/testapp/app.log.*
active_pattern: /var/log/testapp/app.log
pond_path: /logs/app
EOF

pond mkdir -p /system/run
pond mkdir -p /logs/app
pond mknod logfile-ingest /system/run/10-logs --config-path /tmp/ingest.yaml

#############################
# PHASE 1: Create and ingest initial logfile (~20KB)
#############################

echo "=== Phase 1: Initial ingest ==="

for i in $(seq 1 200); do
    printf '{"ts":"2024-01-01T%02d:%02d:%02dZ","level":"INFO","msg":"Original entry %04d padding-xxxxxxxxx"}\n' $((i/3600%24)) $((i/60%60)) $((i%60)) $i
done > /var/log/testapp/app.log

SIZE1=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  Active file: ${SIZE1} bytes"

pond run /system/run/10-logs 2>/tmp/run1.log

POND_HASH1=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep 'app.log' | awk '{print $1}')
HOST_HASH1=$(b3sum /var/log/testapp/app.log | awk '{print $1}')

check "[ '${POND_HASH1}' = '${HOST_HASH1}' ]" \
    "phase 1: initial ingest blake3 correct (${SIZE1} bytes)"

#############################
# PHASE 2: Rotate and grow past old size BEFORE next ingest
#
# This is the critical scenario:
#   - app.log (20KB, tracked) → app.log.1 (archived)
#   - new app.log appears with DIFFERENT content
#   - new app.log grows to 30KB (> the 20KB we tracked)
#   - next ingest sees size grew: "must be an append" — WRONG
#   - prefix verification catches the mismatch
#############################

echo ""
echo "=== Phase 2: Rotate, new file grows past old tracked size ==="

# Rotate: rename active → archived
mv /var/log/testapp/app.log /var/log/testapp/app.log.1
echo "  Rotated app.log → app.log.1"

# Create new active file with DIFFERENT content, LARGER than old size
for i in $(seq 1 300); do
    printf '{"ts":"2024-02-01T%02d:%02d:%02dZ","level":"WARN","msg":"New-generation entry %04d completely-different-content-yyy"}\n' $((i/3600%24)) $((i/60%60)) $((i%60)) $i
done > /var/log/testapp/app.log

SIZE2=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  New active file: ${SIZE2} bytes (old tracked: ${SIZE1} bytes)"
echo "  New file is LARGER than old tracked size → looks like append"

# This is where the bug would manifest:
# The ingest sees host.size (30KB) > pond.cumulative_size (20KB) and
# assumes it's a normal append.  But the prefix is different content.
pond run /system/run/10-logs 2>/tmp/run2.log

# The ingest must NOT fail
check "[ $? -eq 0 ]" \
    "phase 2: ingestion succeeded (did not crash)"

# It must detect rotation
check "grep -q 'Rotation detected\|rotation\|Prefix verification' /tmp/run2.log" \
    "phase 2: rotation or prefix mismatch detected"

# It must NOT report a spurious error
check "! grep -q 'Transaction aborted' /tmp/run2.log" \
    "phase 2: no transaction abort"

# The archived file must exist in the pond
pond cat /logs/app/app.log.1 > /tmp/archived.out 2>/dev/null
check "diff /var/log/testapp/app.log.1 /tmp/archived.out" \
    "phase 2: archived file content preserved in pond"

# The new active must be in the pond with correct content
pond cat /logs/app/app.log > /tmp/active.out 2>/dev/null
check "diff /var/log/testapp/app.log /tmp/active.out" \
    "phase 2: new active file ingested correctly"

# blake3 for both files
POND_HASH_ARCHIVED=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep 'app.log.1' | awk '{print $1}')
HOST_HASH_ARCHIVED=$(b3sum /var/log/testapp/app.log.1 | awk '{print $1}')
check "[ '${POND_HASH_ARCHIVED}' = '${HOST_HASH_ARCHIVED}' ]" \
    "phase 2: archived file blake3 correct"

POND_HASH_NEW=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep -w 'app.log$' | awk '{print $1}')
HOST_HASH_NEW=$(b3sum /var/log/testapp/app.log | awk '{print $1}')
check "[ '${POND_HASH_NEW}' = '${HOST_HASH_NEW}' ]" \
    "phase 2: new active file blake3 correct"

#############################
# PHASE 3: Verify normal append still works after rotation
#############################

echo ""
echo "=== Phase 3: Normal append after rotation ==="

for i in $(seq 301 400); do
    printf '{"ts":"2024-02-02T%02d:%02d:%02dZ","level":"INFO","msg":"Post-rotation entry %04d"}\n' $((i/3600%24)) $((i/60%60)) $((i%60)) $i
done >> /var/log/testapp/app.log

SIZE3=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  Active file: ${SIZE3} bytes (grew from ${SIZE2})"

pond run /system/run/10-logs 2>/tmp/run3.log

check "! grep -q 'Prefix verification failed' /tmp/run3.log" \
    "phase 3: no prefix verification failure on normal append"

pond cat /logs/app/app.log > /tmp/pond3.out
check "diff /var/log/testapp/app.log /tmp/pond3.out" \
    "phase 3: content matches after post-rotation append"

POND_HASH3=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep -w 'app.log$' | awk '{print $1}')
HOST_HASH3=$(b3sum /var/log/testapp/app.log | awk '{print $1}')
check "[ '${POND_HASH3}' = '${HOST_HASH3}' ]" \
    "phase 3: blake3 correct after post-rotation append"

#############################
# PHASE 4: Rotation with an unseen tail
#
# Lines appended after the last ingest are rotated away before the next one.
# The ingest appends that tail to the pond file before renaming it, and its
# summary must count it once, as appended (observed on watershop staging as
# "0 appended (+0 bytes)" after a 6.5 MB tail was ingested).
#############################

echo ""
echo "=== Phase 4: Rotation with an unseen tail ==="

SIZE_TRACKED=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
for i in $(seq 401 450); do
    printf '{"ts":"2024-02-03T%02d:%02d:%02dZ","level":"INFO","msg":"Unseen tail entry %04d"}\n' $((i/3600%24)) $((i/60%60)) $((i%60)) $i
done >> /var/log/testapp/app.log
SIZE_ROTATED=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
MISSED=$((SIZE_ROTATED - SIZE_TRACKED))

mv /var/log/testapp/app.log /var/log/testapp/app.log.2
for i in $(seq 1 20); do
    printf '{"ts":"2024-02-04T00:00:%02dZ","level":"INFO","msg":"Third-generation entry %04d"}\n' $i $i
done > /var/log/testapp/app.log
SIZE_NEW=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  Rotated ${SIZE_ROTATED} bytes (${MISSED} unseen); new active ${SIZE_NEW} bytes"

pond run /system/run/10-logs 2>/tmp/run4.log

pond cat /logs/app/app.log.2 > /tmp/archived4.out
check "diff /var/log/testapp/app.log.2 /tmp/archived4.out" \
    "phase 4: rotated file includes the unseen tail"

pond cat /logs/app/app.log > /tmp/active4.out
check "diff /var/log/testapp/app.log /tmp/active4.out" \
    "phase 4: new active file ingested correctly"

check "grep -qF 'Logfile ingestion complete: 1 new (${SIZE_NEW} bytes), 1 appended (+${MISSED} bytes), 1 unchanged' /tmp/run4.log" \
    "phase 4: summary counts the unseen tail once, as appended"

echo ""
echo "=== All phases complete ==="
check_finish
