#!/bin/bash
# EXPERIMENT: Backup-remote lifecycle over a file:// remote (no S3/MinIO)
#
# DESCRIPTION:
#   Exercises the D4..D7 backup (push-side) CLI surface end-to-end against a
#   local `file:///` remote, so the test is self-contained (no docker-compose):
#     1. init pond, copy a raw data file in
#     2. `pond backup add origin file://...`  (auto-pushes on attach)
#     3. `pond backup list`                    shows the attachment + watermark
#     4. `pond push origin`                    reports "nothing to push"
#     5. `pond verify origin`                  live data matches remote
#     6. `pond status`                         identity + remote + watermark
#     7. write more data (auto-pushed post-commit), verify still clean,
#        LAST_PUSHED_SEQ advances
#     8. `pond backup add --bidirectional`     attaches a mode=both remote
#     9. negative paths: push/verify of an unknown remote fail cleanly
#    10. `pond backup remove origin`           detaches; list no longer shows it
#
# EXPECTED:
#   - Every positive step succeeds; watermark advances monotonically.
#   - Unknown-remote push/verify fail with a clear non-zero exit.
#
# History:
#   Added on jmacd/52 to cover the new push-side verbs (backup add/list/remove,
#   push, verify, status) that previously had only MinIO-gated coverage.  Uses
#   a file:// remote so it runs in the base container without compose.
set -e
source check.sh

echo "=== Experiment: backup-remote lifecycle (file://) ==="

POND_DIR=/tmp/710-pond
REMOTE_DIR=/tmp/710-remote
BIDI_DIR=/tmp/710-remote-bidi
rm -rf "$POND_DIR" "$REMOTE_DIR" "$BIDI_DIR"
mkdir -p "$REMOTE_DIR" "$BIDI_DIR"
export POND="$POND_DIR"

# Extract a column from `pond backup list` for a given remote name.
# Columns: NAME URL MODE MOUNT PUSHED_TIP PULLED_TIP
backup_field() {  # name colnum
    pond backup list 2>/dev/null | awk -v n="$1" -v c="$2" '$1==n {print $c; exit}'
}

echo "--- Step 1: init + ingest ---"
pond init --birthplace test-host >/dev/null
printf 'alpha\nbravo\ncharlie\n' > /tmp/710-a.txt
pond copy host:///tmp/710-a.txt /a.txt >/dev/null 2>&1
check 'pond cat /a.txt | grep -q bravo' "raw file ingested"

echo "--- Step 2-3: backup add (auto-push) + list ---"
pond backup add origin "file://${REMOTE_DIR}" > /tmp/710-add.log 2>&1
check_contains /tmp/710-add.log "backup add reports push mode" "mode=push"
check 'pond backup list | grep -q origin'                       "backup list shows origin"
check 'pond backup list | grep -q PUSHED_TIP'                   "backup list has tip-frontier column"
check '[ -d "'"${REMOTE_DIR}"'/_delta_log" ]'                   "remote initialized as Delta table"
PUSHED1=$(backup_field origin 5)
check '[ -n "'"${PUSHED1}"'" ] && [ "'"${PUSHED1}"'" != "-" ]'  "PUSHED_TIP set after attach (auto-push)"

echo "--- Step 4: re-push is idempotent (tip unchanged) ---"
pond push origin > /tmp/710-push.log 2>&1
check_contains /tmp/710-push.log "re-push reports acknowledged no-op" "remote already acknowledged the current content tip"
PUSHED_AGAIN=$(backup_field origin 5)
check '[ "'"${PUSHED_AGAIN}"'" = "'"${PUSHED1}"'" ]'            "re-push leaves PUSHED_TIP unchanged (no new content)"

echo "--- Step 5: verify clean ---"
pond verify origin > /tmp/710-verify.log 2>&1
check_contains /tmp/710-verify.log "verify reports live data matches" "live data matches remote"

echo "--- Step 6: status ---"
pond status > /tmp/710-status.log 2>&1
check_contains /tmp/710-status.log "status prints identity section" "Pond ID:"
check_contains /tmp/710-status.log "status lists one remote"        "Remotes (1)"
check_contains /tmp/710-status.log "status shows last pushed"       "last pushed:"

echo "--- Step 7: write more (auto-push) -> tip advances ---"
printf 'delta\necho\n' > /tmp/710-b.txt
pond copy host:///tmp/710-b.txt /b.txt >/dev/null 2>&1
PUSHED2=$(backup_field origin 5)
check '[ -n "'"${PUSHED2}"'" ] && [ "'"${PUSHED2}"'" != "'"${PUSHED1}"'" ]' "PUSHED_TIP advanced after new write"
pond verify origin > /tmp/710-verify2.log 2>&1
check_contains /tmp/710-verify2.log "verify still clean after auto-push" "live data matches remote"

echo "--- Step 8: bidirectional backup ---"
pond backup add mirror "file://${BIDI_DIR}" --bidirectional > /tmp/710-bidi.log 2>&1
check_contains /tmp/710-bidi.log "bidirectional attaches mode=both" "mode=both"
check '[ "$(backup_field mirror 3)" = "both" ]'                  "backup list shows both-mode remote"

echo "--- Step 9: negative paths ---"
check '! pond push nope'   "push of unknown remote fails"
check '! pond verify nope' "verify of unknown remote fails"

echo "--- Step 10: detach origin ---"
pond backup remove origin > /tmp/710-remove.log 2>&1
check_contains /tmp/710-remove.log "remove reports detach" "detached"
check '! pond backup list | grep -q "^origin "' "origin no longer listed"
check 'pond backup list | grep -q "^mirror "'   "mirror still listed"

check_finish
