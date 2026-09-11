#!/bin/bash
# EXPERIMENT: An incremental MIRROR pull applies an upstream delta (new file +
#             new series version) and preserves full replica identity.
#
# DESCRIPTION:
#   724 restores a whole pond, then its "incremental" second pull is a NO-OP
#   short-circuit (nothing changed upstream).  726 exercises the incremental
#   series-version diff but in cross-pond IMPORT mode (the consumer keeps its
#   OWN pond_id and mounts the foreign tree; tips never converge).  Neither
#   drives `rebuild_pond`'s incremental node_id diff in MIRROR mode with REAL
#   upstream changes: a restored replica that must apply a delta and remain a
#   byte-identical mirror (same pond_id, same content TIP).
#
#   This test closes that gap.  A producer publishes a pond; a consumer
#   `pond restore`s it (adopting the source pond_id).  The producer then makes
#   TWO kinds of change in one further push -- a brand-new file (a Create diff)
#   and a new VERSION appended to an existing series (a Version diff) -- and the
#   consumer runs a plain `pond pull` on the mirror.  The incremental
#   `plan_node_diff` must apply BOTH ops to the existing mirror, and the mirror
#   must converge back onto the producer's new content TIP (the canonical
#   cross-replica fingerprint, per 718/724).  This is the only case that proves
#   an already-materialized mirror stays a faithful replica across an
#   incremental update, not just across a from-scratch restore.
#
# EXPECTED:
#   - After restore: mirror has the file and series v1 (7 rows); tip == producer.
#   - Producer adds /data/b.txt and appends series v2; auto-push.
#   - After `pond pull origin`: mirror gains b.txt, series shows BOTH versions
#     (14 rows), the original file is unchanged, and the mirror tip again equals
#     the producer's pushed tip.
#   - Restored mirror passes its own `pond fsck`.
#
# History:
#   Added on jmacd/65 to cover incremental MIRROR-mode diff application, a gap
#   left by 724 (no-op second pull) and 726 (import mode, no tip convergence).
set -e
source check.sh

echo "=== Experiment: incremental mirror-mode pull (file://) ==="

P1=/tmp/727-p1
P2=/tmp/727-p2
REMOTE=/tmp/727-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

# Two single-column series on distinct days so versions add cleanly (as in 726).
cat > /tmp/727-day1.yaml << 'YAML'
start: "2024-03-01T00:00:00Z"
end: "2024-03-01T06:00:00Z"
interval: "1h"
time_column: "timestamp"
points:
  - name: "temperature"
    components:
      - type: line
        slope: 0.0
        offset: 11.0
YAML
cat > /tmp/727-day2.yaml << 'YAML'
start: "2024-03-02T00:00:00Z"
end: "2024-03-02T06:00:00Z"
interval: "1h"
time_column: "timestamp"
points:
  - name: "temperature"
    components:
      - type: line
        slope: 0.0
        offset: 22.0
YAML

echo "--- Step 1: producer builds a file + a series (v1) and publishes ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null
pond mkdir /gen >/dev/null
pond mkdir /data >/dev/null
mkdir -p /tmp/727-exp
echo "alpha" > /tmp/727-a.txt
pond copy host:///tmp/727-a.txt /data/a.txt >/dev/null 2>&1

pond mknod synthetic-timeseries /gen/day1 --config-path /tmp/727-day1.yaml >/dev/null
pond mknod synthetic-timeseries /gen/day2 --config-path /tmp/727-day2.yaml >/dev/null
pond copy /gen/day1 "host:///tmp/727-exp" >/dev/null 2>&1
pond copy /gen/day2 "host:///tmp/727-exp" >/dev/null 2>&1
cp /tmp/727-exp/gen/day1 /tmp/727-v1.parquet
cp /tmp/727-exp/gen/day2 /tmp/727-v2.parquet
pond copy "host+series:///tmp/727-v1.parquet" /data/temps.series >/dev/null 2>&1

pond backup add origin "file://${REMOTE}" > /tmp/727-backup.log 2>&1
check 'grep -q "added remote origin" /tmp/727-backup.log' "backup add origin succeeded"
P1_TIP1=$(pond status 2>/dev/null | awk '/last pushed:/ {print $3}')
check '[ ${#P1_TIP1} -eq 64 ]' "producer pushed tip (v1) is a 64-hex content hash"

echo "--- Step 2: fresh consumer restores the whole pond (mirror) ---"
export POND="$P2"
pond restore origin "file://${REMOTE}" > /tmp/727-restore.log 2>&1
check 'grep -q "restore complete" /tmp/727-restore.log' "pond restore completed"
check 'pond fsck >/dev/null 2>&1' "restored mirror passes its own fsck"
check 'pond cat /data/a.txt 2>/dev/null | grep -q alpha' "mirror has the file after restore"
pond cat --sql "SELECT count(*) AS n FROM source" --format table /data/temps.series \
    > /tmp/727-p2-v1.txt 2>/dev/null
check 'grep -qE "\| 7 " /tmp/727-p2-v1.txt' "mirror series has v1 only (7 rows)"
P2_PULLED1=$(pond status 2>/dev/null | awk '/last pulled:/ {print $3}')
check '[ "'"$P2_PULLED1"'" = "'"$P1_TIP1"'" ]' "mirror tip equals producer tip after restore"

echo "--- Step 3: producer makes a delta: new file + new series version ---"
export POND="$P1"
echo "beta" > /tmp/727-b.txt
pond copy host:///tmp/727-b.txt /data/b.txt >/dev/null 2>&1                 # Create diff
pond copy "host+series:///tmp/727-v2.parquet" /data/temps.series >/dev/null 2>&1  # Version diff
pond cat --sql "SELECT count(*) AS n FROM source" --format table /data/temps.series \
    > /tmp/727-p1-v2.txt 2>/dev/null
check 'grep -qE "\| 14 " /tmp/727-p1-v2.txt' "producer series now has 14 rows (2 versions)"
P1_TIP2=$(pond status 2>/dev/null | awk '/last pushed:/ {print $3}')
check '[ ${#P1_TIP2} -eq 64 ] && [ "'"$P1_TIP2"'" != "'"$P1_TIP1"'" ]' \
    "producer pushed tip advanced after the delta"

echo "--- Step 4: mirror applies the incremental diff via a plain pull ---"
export POND="$P2"
pond pull origin > /tmp/727-pull.log 2>&1
check 'grep -q "pull origin complete" /tmp/727-pull.log' "mirror completed incremental pull"
check_not_contains /tmp/727-pull.log "already up to date" "pull was NOT a no-op short-circuit"

echo "--- Step 5: the delta landed and the mirror is still a faithful replica ---"
check 'pond cat /data/b.txt 2>/dev/null | grep -q beta' "mirror gained the new file b.txt"
check 'pond cat /data/a.txt 2>/dev/null | grep -q alpha' "mirror kept the original file a.txt"
pond cat --sql "SELECT count(*) AS n FROM source" --format table /data/temps.series \
    > /tmp/727-p2-v2.txt 2>/dev/null
check 'grep -qE "\| 14 " /tmp/727-p2-v2.txt' "mirror series now has BOTH versions (14 rows)"
pond cat --sql "SELECT count(*) AS n FROM source WHERE temperature = 11.0" --format table \
    /data/temps.series > /tmp/727-p2-day1.txt 2>/dev/null
check 'grep -qE "\| 7 " /tmp/727-p2-day1.txt' "mirror still has v1 rows (day1 -- not lost)"
check 'pond fsck >/dev/null 2>&1' "mirror still passes fsck after the incremental pull"
P2_PULLED2=$(pond status 2>/dev/null | awk '/last pulled:/ {print $3}')
check '[ "'"$P2_PULLED2"'" = "'"$P1_TIP2"'" ]' \
    "mirror tip re-converged to producer tip after the incremental delta"

check_finish
