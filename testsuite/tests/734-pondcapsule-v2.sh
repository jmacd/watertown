#!/bin/bash
# EXPERIMENT: pondcapsule.2 verification/import and Watertown native names
#
# EXPECTED:
#   - A minimal pondcapsule.2 is accepted by the CLI verifier.
#   - The capsule imports into a fresh pond and preserves its logical root.
#   - A native backup contains the selected watertown.*.v1 wire identifiers.
set -e
source check.sh

echo "=== Experiment: pondcapsule.2 and watertown native names ==="

CAPSULE=/tmp/734-capsule
TARGET=/tmp/734-restored
POND_DIR=/tmp/734-pond
REMOTE=/tmp/734-remote
rm -rf "$CAPSULE" "$TARGET" "$POND_DIR" "$REMOTE"
mkdir -p "$CAPSULE/recovery/refs" "$CAPSULE/recovery/manifests" \
    "$CAPSULE/recovery/objects" "$REMOTE"

echo "--- Step 1: create a canonical pondcapsule.2 manifest ---"
MANIFEST="$CAPSULE/recovery/manifests/pending.json"
# The manifest root and canonical-encoding check hash these bytes exactly, so
# the file must hold jq's compact JSON with no trailing newline (jq -cn
# otherwise appends one).
MANIFEST_JSON=$(jq -cn \
    --arg pond_id "11111111-1111-1111-1111-111111111111" \
    --arg source_tip "0000000000000000000000000000000000000000000000000000000000000000" \
    '{
      format: "pondcapsule.2",
      source: {
        pond_id: $pond_id,
        birthplace: "testsuite-734",
        source_tip: $source_tip,
        exported_at_micros: 1700000000000000,
        tool_version: "testsuite"
      },
      entries: [{
        path: "/",
        entry_type: "dir:physical",
        source_node_id: "00000000-0000-7100-8000-000000000000",
        node: {kind: "directory"}
      }, {
        path: "/data",
        entry_type: "dir:physical",
        source_node_id: "00000000-0000-7100-8000-000000000001",
        node: {kind: "directory"}
      }]
    }')
printf '%s' "$MANIFEST_JSON" > "$MANIFEST"
CAPSULE_ROOT=$({ printf 'pondcapsule.root.2\n'; cat "$MANIFEST"; } | b3sum | awk '{print $1}')
mv "$MANIFEST" "$CAPSULE/recovery/manifests/${CAPSULE_ROOT}.json"
printf '%s\n' "$CAPSULE_ROOT" > "$CAPSULE/recovery/refs/latest"

echo "--- Step 2: verify and import the v2 capsule ---"
pond capsule verify "$CAPSULE" >/tmp/734-verify.log 2>&1
check_contains /tmp/734-verify.log "pond verifies a pondcapsule.2 manifest" \
    "capsule verified"
export POND="$TARGET"
pond capsule import "$CAPSULE" --birthplace testsuite-734 --experimental \
    >/tmp/734-import.log 2>&1
check_contains /tmp/734-import.log "pond imports a pondcapsule.2 into a fresh pond" \
    "capsule imported"
check 'pond list / >/dev/null' "imported pond root is listable"
check 'pond list / | grep -q "data"' "imported pond has its /data entry"

echo "--- Step 3: publish a new-format native backup ---"
export POND="$POND_DIR"
pond init --birthplace testsuite-734 >/dev/null
printf 'native format probe\n' >/tmp/734-native.txt
pond copy host:///tmp/734-native.txt /native.txt >/dev/null 2>&1
pond backup add origin "file://${REMOTE}" >/tmp/734-backup.log 2>&1
check '[ -d "$REMOTE/_delta_log" ]' "native backup is initialized"
check 'grep -R -a -q "watertown.commit.v1" "$REMOTE"' \
    "backup contains watertown.commit.v1"
check 'grep -R -a -q "watertown.tree.v1" "$REMOTE"' \
    "backup contains watertown.tree.v1"

check_finish
