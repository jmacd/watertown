#!/bin/bash
# EXPERIMENT: Producer-side compaction is transparent to replication (D9)
#
# DESCRIPTION:
#   Exercises `pond maintain --compact` under the D9 content-addressed model,
#   where compaction is a LOCAL physical optimization (merging series versions
#   / rewriting parquet) that leaves the logical content -- and therefore the
#   content tip -- byte-identical.  Compaction adds no commit to the content
#   graph, exactly as `git gc`/repack adds no commits.  Consequences on a
#   file:// remote (self-contained; no compose):
#     1. Producer P1: ingest several files, `pond backup add origin`
#        (auto-pushes each write, so the remote already holds the tip).
#     2. `pond maintain --compact` reclaims local storage; it reports the
#        content is unchanged and replicas need no update.
#     3. `pond verify origin` is clean (UpToDate) AFTER compaction WITHOUT any
#        push -- the content tip is unchanged, so it was already replicated.
#     4. `pond push origin` is a harmless no-op that still completes and keeps
#        verify UpToDate (re-sending the unchanged closure is idempotent).
#     5. A fresh consumer P2 cross-pond imports and pulls and sees identical
#        file content (md5): compaction did not disturb the replicated data.
#     6. `pond status` reflects identity + the backup remote post-compact.
#
# EXPECTED:
#   - maintain --compact reports a compaction commit and that replicas need
#     no update.
#   - verify is UpToDate right after compaction, with no intervening push.
#   - a follow-up push completes and leaves verify UpToDate.
#   - the consumer reproduces producer content exactly.
#
# History:
#   Added on jmacd/52 to cover `Ship::compact` / `pond maintain --compact`.
#   Rewritten under D9: compaction is content-preserving and transparent to
#   the content graph, so it adds no commit and needs no push to replicate.
set -e
source check.sh

echo "=== Experiment: maintain --compact is transparent to replication (file://) ==="

P1=/tmp/712-p1
P2=/tmp/712-p2
REMOTE=/tmp/712-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

echo "--- Step 1: producer ingest + backup add (auto-push) ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null
pond mkdir -p /data >/dev/null 2>&1
for i in 1 2 3 4; do
    printf 'compact-row-%d\n' "$i" > /tmp/712-f$i.txt
    pond copy host:///tmp/712-f$i.txt /data/f$i.txt >/dev/null 2>&1
done
pond backup add origin "file://${REMOTE}" >/dev/null 2>&1
SRC_MD5=$(pond cat /data/f3.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${SRC_MD5}"'" ]' "producer f3 md5 computed"

echo "--- Step 2: maintain --compact (local optimization, transparent) ---"
pond maintain --compact > /tmp/712-compact.log 2>&1
check_contains /tmp/712-compact.log "compaction recorded locally" "Compaction committed"
check_contains /tmp/712-compact.log "compaction reports it needs no push" "replicas need no update"

echo "--- Step 3: verify is UpToDate after compaction WITHOUT any push ---"
pond verify origin > /tmp/712-verify1.log 2>&1
check_contains /tmp/712-verify1.log "verify clean after compact, no push" "live data matches remote"

echo "--- Step 4: push after compaction is a harmless no-op that completes ---"
pond push origin > /tmp/712-push.log 2>&1
check 'grep -q "remote already acknowledged the current content tip" /tmp/712-push.log' \
    "push after compaction reports an acknowledged no-op"
pond verify origin > /tmp/712-verify2.log 2>&1
check_contains /tmp/712-verify2.log "verify still clean after the no-op push" "live data matches remote"

echo "--- Step 5: fresh consumer pulls; content survives compaction ---"
export POND="$P2"
pond init --birthplace test-host >/dev/null
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
pond pull upstream > /tmp/712-pull.log 2>&1
check 'grep -qE "pull upstream complete" /tmp/712-pull.log' "consumer pull completed"
check 'grep -qE "files: [1-9]" /tmp/712-pull.log' "consumer imported >=1 file"
IMPORTED_MD5=$(pond cat /imports/up/data/f3.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${IMPORTED_MD5}"'" = "'"${SRC_MD5}"'" ]' "consumer f3 content matches producer post-compact"
check 'pond cat /imports/up/data/f1.txt | grep -q compact-row-1' "first file survives compaction round-trip"
check 'pond cat /imports/up/data/f4.txt | grep -q compact-row-4' "last file survives compaction round-trip"

echo "--- Step 6: producer status ---"
export POND="$P1"
pond status > /tmp/712-status.log 2>&1
check_contains /tmp/712-status.log "status prints identity" "Pond ID:"
check_contains /tmp/712-status.log "status lists the backup remote" "origin"

check_finish
