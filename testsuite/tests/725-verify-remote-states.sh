#!/bin/bash
# EXPERIMENT: `pond verify` reports the commit-graph relationship between the
#             local tip and a remote's published tip, and compaction is
#             transparent to that relationship (D9).
#
# DESCRIPTION:
#   The content-addressed remote holds one tip ref per pond; `pond verify`
#   places the remote tip against the local content tip and reports:
#     * UpToDate     -- remote tip == local content tip.
#     * RemoteBehind -- remote tip is an ANCESTOR of the local tip (local has
#                       unpushed content commits); reports the unpushed count.
#     * Diverged     -- remote tip is not in local history (real mismatch).
#
#   Under D9 the content tip is the last CONTENT-CHANGING commit.  Compaction
#   (`pond maintain --compact`) is content-preserving: it rewrites local
#   storage but leaves the logical content byte-identical, so it adds NO commit
#   to the content graph and does NOT advance the tip.  Therefore compaction
#   must NOT move verify into RemoteBehind -- it stays UpToDate, and a push
#   after it is a harmless no-op.  This test locks that invariant in from the
#   CLI so a regression to "compaction is a pushable commit" is caught.
#
#   The RemoteBehind ancestor-walk and `local_unpushed` count are covered
#   directly by the Rust unit test `test_remote_cli.rs` (RemoteBehind by 1);
#   that state is not cleanly reachable from the CLI because every content
#   write auto-pushes to a push-mode backup.
#
# EXPECTED:
#   - After backup add (auto-push): verify reports UpToDate.
#   - After a further content write (auto-push): verify stays UpToDate.
#   - After maintain --compact: verify stays UpToDate (NOT RemoteBehind).
#   - A push after compaction completes and leaves verify UpToDate.
#
# History:
#   Added on jmacd/65 for the RemoteBehind verify state.  Rewritten under D9:
#   compaction adds no commit, so its old "RemoteBehind by 1 after compact"
#   premise is invalid; this now asserts compaction is transparent to verify.
set -e
source check.sh

echo "=== Experiment: verify states; compaction is transparent to verify (D9) ==="

P1=/tmp/725-p1
REMOTE=/tmp/725-remote
rm -rf "$P1" "$REMOTE"
mkdir -p "$REMOTE"

echo "--- Step 1: producer ingests data + attaches a backup (auto-push) ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null
pond mkdir /data >/dev/null
for i in 1 2 3 4; do
    printf 'verify-row-%d\n' "$i" > /tmp/725-f$i.txt
    pond copy host:///tmp/725-f$i.txt /data/f$i.txt >/dev/null 2>&1
done
pond backup add origin "file://${REMOTE}" > /tmp/725-backup.log 2>&1
check 'grep -q "added remote origin" /tmp/725-backup.log' "backup add origin succeeded"

echo "--- Step 2: verify is UpToDate right after auto-push ---"
pond verify origin > /tmp/725-verify1.log 2>&1
check_contains /tmp/725-verify1.log "verify clean after auto-push (UpToDate)" \
    "live data matches remote tip"

echo "--- Step 3: a further content write auto-pushes; verify stays UpToDate ---"
printf 'verify-row-5\n' > /tmp/725-f5.txt
pond copy host:///tmp/725-f5.txt /data/f5.txt >/dev/null 2>&1
pond verify origin > /tmp/725-verify2.log 2>&1
check_contains /tmp/725-verify2.log "verify clean after further write (UpToDate)" \
    "live data matches remote tip"

echo "--- Step 4: maintain --compact does NOT advance the content tip ---"
pond maintain --compact > /tmp/725-compact.log 2>&1
check_contains /tmp/725-compact.log "compaction recorded locally" "Compaction committed"

echo "--- Step 5: verify stays UpToDate after compaction (NOT RemoteBehind) ---"
set +e
pond verify origin > /tmp/725-verify3.log 2>&1
VERIFY3_EXIT=$?
set -e
check '[ '"$VERIFY3_EXIT"' -eq 0 ]' "verify exit is success after compaction"
check_contains /tmp/725-verify3.log "compaction is transparent to verify (UpToDate)" \
    "live data matches remote tip"
check '! grep -q "is behind local by" /tmp/725-verify3.log' \
    "compaction does NOT create a RemoteBehind state"

echo "--- Step 6: a push after compaction completes; verify still UpToDate ---"
pond push origin > /tmp/725-push.log 2>&1
check 'grep -q "remote already acknowledged the current content tip" /tmp/725-push.log' \
    "push after compaction reports an acknowledged no-op"
pond verify origin > /tmp/725-verify4.log 2>&1
check_contains /tmp/725-verify4.log "verify still clean after the no-op push" \
    "live data matches remote tip"

check_finish
