#!/bin/bash
# EXPERIMENT: `pond restore` bootstraps a whole-pond replica from a backup and
#             produces a byte-identical pond (same pond_id, same fsck root).
#
# DESCRIPTION:
#   Before this, an operator could NOT restore a whole pond from a backup:
#   `pond init` always mints a fresh pond_id, so attaching your own backup as a
#   mirror (`pond remote add ... /`) is refused on a pond_id mismatch, and the
#   only code that stamps a replica id (`Ship::create_replica`) was reachable
#   solely from Rust tests (BACKLOG: CA-MIRROR-RESTORE, P1).  `pond restore`
#   closes that gap: it discovers the source pond_id from the remote, stamps a
#   replica shell carrying that id, attaches the remote as a mirror at `/`, and
#   pulls the full content graph (mirror rebuild via `rebuild_pond`).
#
#   The KEY property that distinguishes restore from a cross-pond IMPORT (711):
#   an import keeps the consumer's OWN pond_id and mounts the foreign tree under
#   a foreign partition; a restore ADOPTS the source pond_id and rebuilds the
#   tree by node_id.  The canonical cross-replica fingerprint is the TIP COMMIT
#   HASH (per 718: the node_id/version/part_id-keyed fsck root is NOT a
#   cross-replica invariant, since replicas reach the same content via different
#   local transaction histories).  So restore asserts the restored tip equals
#   the producer's pushed tip, plus byte-level content equality and a clean
#   restored-pond fsck.
#
# EXPECTED:
#   - `pond restore` completes and rebuilds the mirror.
#   - Restored pond passes its own `pond fsck` (integrity intact).
#   - Restored content tip == producer pushed tip (canonical replica identity).
#   - File content (small inline + >64KB external blob) md5-matches producer.
#   - A second `pond pull origin` short-circuits as already up to date.
#   - Re-running `pond restore` over the now-existing pond is refused.
#
# History:
#   Added on jmacd/65 with the new `pond restore` verb to close CA-MIRROR-RESTORE.
set -e
source check.sh

echo "=== Experiment: pond restore (whole-pond replica bootstrap) ==="

P1=/tmp/724-p1
P2=/tmp/724-p2
REMOTE=/tmp/724-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

echo "--- Step 1: producer builds a pond (inline files + a >64KB blob) ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null
pond mkdir /data >/dev/null
echo "hello watertown" > /tmp/724-a.txt
printf 'row-one\n'     > /tmp/724-f1.txt
head -c 200000 /dev/urandom > /tmp/724-big.bin   # incompressible -> real blob
pond copy host:///tmp/724-a.txt  /data/a.txt   >/dev/null 2>&1
pond copy host:///tmp/724-f1.txt /data/f1.txt  >/dev/null 2>&1
pond copy host:///tmp/724-big.bin /data/big.bin >/dev/null 2>&1

P1_ROOT=$(pond fsck 2>/dev/null)
check '[ ${#P1_ROOT} -eq 64 ]' "producer fsck root is a 64-hex checksum"
A_MD5=$(md5sum /tmp/724-a.txt   | cut -d' ' -f1)
BIG_MD5=$(md5sum /tmp/724-big.bin | cut -d' ' -f1)

echo "--- Step 2: backup add (auto-push) to a file:// remote ---"
pond backup add origin "file://${REMOTE}" > /tmp/724-backup.log 2>&1
check 'grep -q "added remote origin" /tmp/724-backup.log' "backup add origin succeeded"
P1_TIP=$(POND="$P1" pond status 2>/dev/null | awk '/last pushed:/ {print $3}')
check '[ ${#P1_TIP} -eq 64 ]' "producer pushed tip is a 64-hex content hash"

echo "--- Step 3: fresh consumer restores the whole pond ---"
export POND="$P2"
# P2 does NOT exist yet: restore must bootstrap it from scratch.
pond restore origin "file://${REMOTE}" > /tmp/724-restore.log 2>&1
check 'grep -q "restore complete" /tmp/724-restore.log' "pond restore completed"

echo "--- Step 4: restored pond passes its own integrity check ---"
check 'pond fsck >/dev/null 2>&1' \
    "restored pond fsck passes (structural + content integrity intact)"
P2_ROOT=$(pond fsck 2>/dev/null)
check '[ ${#P2_ROOT} -eq 64 ]' "restored fsck root is a 64-hex checksum"

echo "--- Step 5: restored content matches the producer byte-for-byte ---"
P2_A_MD5=$(pond cat /data/a.txt 2>/dev/null | md5sum | cut -d' ' -f1)
check '[ "'"$P2_A_MD5"'" = "'"$A_MD5"'" ]' "restored /data/a.txt md5 matches producer"
P2_BIG_MD5=$(pond cat /data/big.bin 2>/dev/null | md5sum | cut -d' ' -f1)
check '[ "'"$P2_BIG_MD5"'" = "'"$BIG_MD5"'" ]' "restored >64KB /data/big.bin md5 matches producer"
check 'pond cat /data/f1.txt 2>/dev/null | grep -q row-one' "restored /data/f1.txt content intact"

echo "--- Step 6: consumer converged on the producer's content tip ---"
P2_PULLED=$(pond status 2>/dev/null | awk '/last pulled:/ {print $3}')
check '[ "'"$P2_PULLED"'" = "'"$P1_TIP"'" ]' "restored pulled tip equals producer pushed tip"

echo "--- Step 7: the mirror attachment tracks upstream incrementally ---"
pond pull origin > /tmp/724-pull2.log 2>&1
check 'grep -q "already up to date" /tmp/724-pull2.log' \
    "second pull short-circuits (mirror is attached and current)"

echo "--- Step 8: restore refuses to clobber an existing pond ---"
set +e
pond restore origin "file://${REMOTE}" > /tmp/724-restore2.log 2>&1
RESTORE2_EXIT=$?
set -e
check '[ '"$RESTORE2_EXIT"' -ne 0 ]' "second restore over an existing pond fails"
check_contains /tmp/724-restore2.log "restore refuses over an existing pond" "refusing to restore over an existing pond"

check_finish
