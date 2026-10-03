#!/bin/bash
# EXPERIMENT: pond apply -- initialize a pond with multiple configs
# DESCRIPTION: Test that pond apply creates nodes from k8s-style YAML
#   resources, auto-creates parent directories, and is idempotent.
# EXPECTED: All nodes created on first apply, unchanged on second apply.
set -e
source check.sh

echo "=== Experiment: pond apply -- init ==="

pond init --birthplace test-host

# ==============================================================================
# Step 1: Create config files with k8s-style format
# ==============================================================================

echo ""
echo "--- Step 1: Create apply config files ---"

cat > /tmp/setup.yaml << 'YAML'
version: v1
kind: mkdir
metadata:
  path: /data
---
version: v1
kind: mknod
metadata:
  path: /data/derived-a
spec:
  factory: sql-derived-table
  config:
    patterns:
      source: "table:///data/*.table"
    query: "SELECT 1 AS value"
---
version: v1
kind: mknod
metadata:
  path: /data/derived-b
spec:
  factory: sql-derived-table
  config:
    patterns:
      source: "table:///data/*.table"
    query: "SELECT 2 AS value"
YAML

echo "Config file created"

# ==============================================================================
# Step 2: Apply -- should create dir and both nodes
# ==============================================================================

echo ""
echo "--- Step 2: First apply (create) ---"

APPLY_OUT=$(pond apply -f /tmp/setup.yaml 2>&1)
echo "$APPLY_OUT"

check 'echo "$APPLY_OUT" | grep -q "created.*derived-a"' "derived-a created"
check 'echo "$APPLY_OUT" | grep -q "created.*derived-b"' "derived-b created"

# Verify nodes exist
LIST_OUT=$(pond list "/data/*")
echo "$LIST_OUT"
check 'echo "$LIST_OUT" | grep -q "derived-a"' "derived-a in listing"
check 'echo "$LIST_OUT" | grep -q "derived-b"' "derived-b in listing"

# ==============================================================================
# Step 3: Re-apply -- should be idempotent (unchanged)
# ==============================================================================

echo ""
echo "--- Step 3: Second apply (idempotent) ---"

REAPPLY_OUT=$(pond apply -f /tmp/setup.yaml 2>&1)
echo "$REAPPLY_OUT"

check 'echo "$REAPPLY_OUT" | grep -q "unchanged"' "reports unchanged"
check 'echo "$REAPPLY_OUT" | grep -q "no data changes"' "no data changes committed"

STATUS_OUT=$(pond status)
echo "$STATUS_OUT"
check 'echo "$STATUS_OUT" | grep -q "Recovery:.*OK (no incomplete transactions)"' "no incomplete transaction after unchanged apply"

# ==============================================================================
# Step 4: Verify transaction log
# ==============================================================================

echo ""
echo "--- Step 4: Verify transaction log ---"

LOG_OUT=$(pond log --limit 5)
echo "$LOG_OUT"

# `pond log` now reconstructs the originating CLI command from the data
# Delta commit metadata (`pond_txn`), so the apply command appears in the
# log.  Assert the write transaction committed and the command is shown.
check 'echo "$LOG_OUT" | grep -q "(write)"' "transaction log contains a write transaction"
check 'echo "$LOG_OUT" | grep -q "COMMITTED"' "transaction log contains a COMMITTED transaction"
check 'echo "$LOG_OUT" | grep -q "Command  : pond apply"' "transaction log shows the apply command"

check_finish
