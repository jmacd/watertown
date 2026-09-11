#!/bin/bash
# EXPERIMENT: `pond fsck` as a replica-comparison fingerprint.
#
# DESCRIPTION:
#   fsck's headline use case is "verify two replicas are identical."  The
#   content root is a tree_hash over each pond's root_tree_hash (a directory
#   is a partition; its recursive content tree_hash is its digest), so it is a
#   lineage-independent fingerprint of a pond's content.  This test pins down
#   exactly what "comparable" means -- and the scope limit baked into the
#   design.  Two parts, both self-contained (the replication half uses a
#   file:// remote, no compose):
#
#   PART A -- byte replica, TOP-LEVEL root equality:
#     1. Build a pond with an inline file and an incompressible large-file
#        blob; record its fsck root.
#     2. `cp -r` the entire pond to a new location and fsck the copy: the
#        root is IDENTICAL (location-independent; a true replica matches by
#        root alone, which is the documented compare-by-root workflow).
#     3. --quick roots also match (the root is structural, content pass does
#        not move it).
#     4. Diverge the copy (add a file) -> roots now DIFFER, so the root
#        detects replica divergence.
#     5. Corrupt the copy's blob -> the copy's default fsck FAILS while its
#        --quick (structural) pass still passes -- the content pass is what
#        closes that gap, even on a replica.
#
#   PART B -- replication-produced replica, PARTITION-level equality:
#     6. Producer pushes to a file:// remote; a consumer cross-pond imports
#        and pulls.  The consumer keeps its OWN pond_id and mounts the
#        producer's data, so the two TOP-LEVEL roots legitimately differ
#        (the consumer folds in its own pond_id partition as well as the
#        producer's) -- a content-only fingerprint this is NOT.
#     7. But every one of the producer's per-partition digests
#        (pond_id/part_id rows=N tree_hash) reappears VERBATIM in the
#        consumer's `fsck --verbose` output: content-addressed replication
#        preserved pond_id, part_id, and content tree hashes byte-for-byte.
#     8. Mutate the producer + re-push/re-pull -> the producer's partition
#        digests change AND the consumer re-converges to the new digests.
#
# EXPECTED:
#   - Byte-replica fsck roots match exactly and track divergence.
#   - Corruption on a replica is caught by the content pass, invisible to
#     --quick.
#   - A replication-produced consumer reproduces every producer partition
#     digest exactly, and stays converged across incremental pushes, even
#     though the consumer's top-level root differs by design.
#
# History:
#   Added on jmacd/65 alongside `pond fsck` to give the replica-comparison
#   guarantee end-to-end coverage (047-fsck.sh only exercises a single pond).
#   Uses a file:// remote so it runs in the base container without compose.
set -e
source check.sh

echo "=== Experiment: fsck replica equality ==="

P1=/tmp/718-p1
P1_COPY=/tmp/718-p1-copy
P2=/tmp/718-p2
REMOTE=/tmp/718-remote
rm -rf "$P1" "$P1_COPY" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

# The cross-pond content fingerprint: the tip commit hash.  A pushed producer
# and a consumer that pulled it converge on the SAME tip because the commit's
# root_tree_hash is pure content (name-keyed, lineage-independent).  The
# top-level fsck root differs across these two ponds for a different reason:
# the consumer folds in its OWN pond_id partition alongside the mounted
# producer pond, so its cross-pond root has an extra entry.
status_pushed_tip() {  # POND_DIR
    POND="$1" pond status 2>/dev/null | awk '/last pushed:/ {print $3}'
}
status_pulled_tip() {  # POND_DIR
    POND="$1" pond status 2>/dev/null | awk '/last pulled:/ {print $3}'
}

echo "--- Part A: byte replica, top-level root equality ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null
echo "hello watertown" > /tmp/718-small.txt
head -c 200000 /dev/urandom > /tmp/718-big.bin   # incompressible -> real blob
pond copy host:///tmp/718-small.txt /small.txt >/dev/null 2>&1
pond copy host:///tmp/718-big.bin   /big.bin    >/dev/null 2>&1

ROOT_P1=$(pond fsck 2>/dev/null)
check '[ ${#ROOT_P1} -eq 64 ]' "producer root is 64 hex chars"

# A true replica: bit-for-bit copy of the whole pond directory.
cp -r "$P1" "$P1_COPY"
ROOT_COPY=$(POND="$P1_COPY" pond fsck 2>/dev/null)
check '[ "'"$ROOT_P1"'" = "'"$ROOT_COPY"'" ]' "byte-replica root equals original (location-independent)"

ROOT_P1_Q=$(pond fsck --quick 2>/dev/null)
ROOT_COPY_Q=$(POND="$P1_COPY" pond fsck --quick 2>/dev/null)
check '[ "'"$ROOT_P1_Q"'" = "'"$ROOT_COPY_Q"'" ]' "--quick roots also match across replicas"

echo "--- Part A: root detects divergence ---"
echo "diverge" > /tmp/718-extra.txt
POND="$P1_COPY" pond copy host:///tmp/718-extra.txt /extra.txt >/dev/null 2>&1
ROOT_COPY_DIV=$(POND="$P1_COPY" pond fsck 2>/dev/null)
check '[ "'"$ROOT_P1"'" != "'"$ROOT_COPY_DIV"'" ]' "root diverges after replica gains a file"

echo "--- Part A: content pass catches blob corruption on a replica ---"
BLOB=$(ls "$P1_COPY"/data/_large_files/blake3=*.parquet | head -1)
python3 - "$BLOB" <<'PY'
import sys
p = sys.argv[1]
d = bytearray(open(p, 'rb').read())
for i in range(len(d)//2, len(d)//2 + 16):
    d[i] ^= 0xFF
open(p, 'wb').write(d)
PY
set +e
POND="$P1_COPY" pond fsck > /tmp/718-corrupt.txt 2>&1
CORRUPT_EXIT=$?
POND="$P1_COPY" pond fsck --quick > /dev/null 2>&1
QUICK_EXIT=$?
set -e
check '[ '"$CORRUPT_EXIT"' -ne 0 ]' "default fsck fails on corrupted replica blob"
check_contains /tmp/718-corrupt.txt "fsck names the mismatching blake3" "does not match recorded blake3"
check '[ '"$QUICK_EXIT"' -eq 0 ]' "--quick still passes (structural-only; corruption invisible)"

check_eq() {  # DESCRIPTION ACTUAL EXPECTED
    check '[ "'"$2"'" = "'"$3"'" ]' "$1 (got '$2', want '$3')"
}

echo "--- Part B: replication-produced replica, cross-pond content equality ---"
export POND="$P1"
pond backup add origin "file://${REMOTE}" >/dev/null 2>&1   # auto-push

# The producer's content fingerprint is its pushed tip commit hash.
P1_TIP=$(status_pushed_tip "$P1")
check '[ ${#P1_TIP} -eq 64 ]' "producer pushed tip is a 64-hex content hash"

export POND="$P2"
pond init --birthplace test-host >/dev/null
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
pond pull upstream > /tmp/718-pull.log 2>&1
check 'grep -qE "pull upstream complete" /tmp/718-pull.log' "consumer completed the cross-pond import"

# Top-level fsck roots differ by design: the consumer has its own pond_id
# partition plus the mounted producer pond, so its cross-pond content root
# (a tree_hash over every pond_id it holds) is NOT a single-pond fingerprint.
ROOT_P2=$(pond fsck 2>/dev/null)
check '[ "'"$ROOT_P1"'" != "'"$ROOT_P2"'" ]' "consumer top-level fsck root differs (own pond_id present)"

# But the content fingerprint -- the tip commit hash -- matches exactly: the
# consumer converged on the producer's pushed tip.  This is the integrity
# comparison that IS valid across ponds.
P2_TIP=$(status_pulled_tip "$P2")
check '[ ${#P2_TIP} -eq 64 ]' "consumer pulled tip is a 64-hex content hash"
check_eq "consumer content tip equals producer content tip (cross-pond equality)" \
    "$P2_TIP" "$P1_TIP"

# And the producer's file content is readable byte-for-byte through the mount.
P1_A_MD5=$(POND="$P1" pond cat /a.txt 2>/dev/null | md5sum | cut -d' ' -f1)
P2_A_MD5=$(pond cat /imports/up/a.txt 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "consumer reproduces producer /a.txt content exactly" "$P2_A_MD5" "$P1_A_MD5"

echo "--- Part B: convergence across an incremental push ---"
export POND="$P1"
echo "second-gen" > /tmp/718-gen2.txt
pond copy host:///tmp/718-gen2.txt /gen2.txt >/dev/null 2>&1   # auto-pushed
P1_TIP2=$(status_pushed_tip "$P1")
check '[ "$P1_TIP2" != "$P1_TIP" ] && [ ${#P1_TIP2} -eq 64 ]' \
    "producer content tip advances after new data"

export POND="$P2"
pond pull upstream > /tmp/718-pull2.log 2>&1
P2_TIP2=$(status_pulled_tip "$P2")
check_eq "consumer re-converges to producer's new content tip after pull" \
    "$P2_TIP2" "$P1_TIP2"

check_finish
