#!/bin/bash
# EXPERIMENT: Format cache reuse + correctness for DYNAMIC files (git-ingest)
#
# DESCRIPTION:
#   Companion to test 534 (which settled the PHYSICAL cross-pond case).
#   This settles the DYNAMIC case raised by @jmacd on remote-followup.md
#   C2: a materialized dynamic file should be reusable across read
#   transactions, and that reuse must never serve stale data.
#
#   Physical files key the format cache on their blake3 content hash.
#   Dynamic files (e.g. git-ingested FileDynamic blobs) have NO blake3, so
#   the cache keys on `node_id.to_short_string()` instead
#   (provider/src/format_cache.rs cache_version_path).  The safety of that
#   choice rests entirely on the node_id being CONTENT-ADDRESSED:
#   gitpond derives each blob's FileID from `name + git-blob-OID` via
#   FileID::from_content (crates/gitpond/src/tree.rs child_file_id), so:
#
#     - unchanged content -> same git OID -> same node_id -> cache REUSE
#     - changed content   -> new git OID  -> new  node_id -> cache MISS (re-derive)
#
#   This test proves both halves empirically, across separate `pond`
#   processes (the "across builds / across read transactions" scenario,
#   since the cache lives on disk in {POND}/cache/).
#
#   Cache-log signals (provider/src/provider_api.rs):
#     [SAVE] Format cache: writing dynamic file <url>   <- MISS (parse)
#     (a dynamic HIT returns silently -- no log line)   <- HIT  (reuse)
#   so reuse is asserted via ZERO [SAVE] + byte-identical results + a
#   stable on-disk cache file.
#
# EXPECTED:
#   Build #1 (cold):              >=1 [SAVE]; one cache Parquet appears.
#   Build #2 (unchanged, reuse):   0 [SAVE]; identical results; same file.
#   --- modify CSV in git, re-pull ---
#   Build #3 (changed, re-derive): >=1 [SAVE]; NEW cache file (new node_id);
#                                  results reflect new content (no stale read).
#   Build #4 (unchanged, reuse):   0 [SAVE]; identical to Build #3.
#
# FAILURE INTERPRETATION:
#   - Build #2 or #4 emits [SAVE]  ==> dynamic materializations are NOT
#     reused across read transactions (the property @jmacd intends to
#     maintain is broken).
#   - Build #3 returns the OLD MAX(temperature) ==> a STALE read: the
#     dynamic cache key is not tracking content; this would be the
#     "invalid read of cached data" risk, and here it would be hit on a
#     plain read transaction, not just a write.
set -e

echo "=== Experiment: Dynamic-file format cache reuse + correctness ==="
echo ""

CHECKS_TOTAL=0
CHECKS_FAILED=0

check_eq() {
    local description="$1" actual="$2" expected="$3"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if [[ "${actual}" == "${expected}" ]]; then
        echo "  ✓ ${description} (${actual})"
    else
        echo "  ✗ ${description}: got '${actual}', expected '${expected}'"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

check_ge() {
    local description="$1" actual="$2" floor="$3"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if [[ "${actual}" -ge "${floor}" ]]; then
        echo "  ✓ ${description} (${actual} >= ${floor})"
    else
        echo "  ✗ ${description}: got '${actual}', expected >= '${floor}'"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

check_ne() {
    local description="$1" a="$2" b="$3"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if [[ "${a}" != "${b}" ]]; then
        echo "  ✓ ${description} ('${a}' != '${b}')"
    else
        echo "  ✗ ${description}: both are '${a}' (expected different)"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

save_count() { grep -c '\[SAVE\] Format cache: writing' "$1" 2>/dev/null || true; }

export POND=/tmp/pond-535
CACHE_DIR="${POND}/cache"

# Read /sensors/sensors.csv via the csv:// format provider in a fresh
# process; results -> $1, logs -> $2.
read_dynamic() {
    local results="$1" logfile="$2"
    RUST_LOG="info,provider=debug" pond cat "csv:///sensors/sensors.csv" \
        --format=table \
        --sql "SELECT COUNT(*) AS n, MAX(temperature) AS mx FROM source" \
        > "${results}" 2> "${logfile}"
}

cache_files() { find "${CACHE_DIR}" -name '*.parquet' 2>/dev/null | sort; }
cache_count() { cache_files | wc -l | tr -d ' '; }

#############################
# GIT REPO + POND SETUP
#############################

REPO_DIR=/tmp/test-dyncache-repo
rm -rf "$REPO_DIR"
mkdir -p "$REPO_DIR/data"
cd "$REPO_DIR"
git init -b main >/dev/null
git config user.email "test@example.com"
git config user.name "Test User"

cat > data/sensors.csv << 'CSV'
timestamp,temperature,humidity
2024-01-01T00:00:00Z,20.5,45.0
2024-01-01T01:00:00Z,21.0,44.5
2024-01-01T02:00:00Z,22.1,43.0
CSV
git add -A
git commit -q -m "Initial sensor CSV (max temp 22.1)"
cd /

rm -rf "${POND}"
pond init --birthplace test-host >/dev/null
cat > /tmp/git-dyncache.yaml << EOF
url: file://${REPO_DIR}
ref: main
prefix: data
EOF
pond mknod git-ingest /sensors --config-path /tmp/git-dyncache.yaml >/dev/null
RUST_LOG=info pond run /sensors pull
echo "git-ingest pull complete (sensors.csv is a dynamic FileDynamic node)"

#############################
# BUILD #1 — cold cache
#############################

echo ""
echo "=== Build #1: first read of the dynamic file (cold cache) ==="
read_dynamic /tmp/r1.txt /tmp/b1.log
SAVE_1=$(save_count /tmp/b1.log)
grep '\[SAVE\] Format cache' /tmp/b1.log || true
check_ge "Build #1 parses the dynamic file (>=1 [SAVE])" "${SAVE_1}" 1

CACHE_1_COUNT=$(cache_count)
CACHE_1_FILES=$(cache_files)
echo "Cache files after Build #1: ${CACHE_1_COUNT}"
echo "${CACHE_1_FILES}"
check_ge "Build #1 materialized a cache Parquet for the dynamic file" \
    "${CACHE_1_COUNT}" 1
# Dynamic cache key is the node_id short string, NOT a 64-hex blake3.
check_eq "Dynamic cache file is keyed by node_id (no 64-hex blake3 in name)" \
    "$(cache_files | grep -cE 'v[0-9]+_[0-9a-f]{64}\.parquet' || true)" "0"

MX_1=$(grep -oE '[0-9]+\.[0-9]+' /tmp/r1.txt | head -1)
echo "Build #1 MAX(temperature) = ${MX_1}"
check_eq "Build #1 sees original content (max temp 22.1)" "${MX_1}" "22.1"

#############################
# BUILD #2 — unchanged: reuse
#############################

echo ""
echo "=== Build #2: re-read unchanged (warm cache — reuse property) ==="
read_dynamic /tmp/r2.txt /tmp/b2.log
SAVE_2=$(save_count /tmp/b2.log)
echo "Build #2 [SAVE] count: ${SAVE_2}"
check_eq "Build #2 re-parses NOTHING (0 [SAVE] — dynamic materialization reused)" \
    "${SAVE_2}" "0"
check_eq "Build #2 cache file set is unchanged (no new materialization)" \
    "$(cache_files)" "${CACHE_1_FILES}"
if diff -u /tmp/r1.txt /tmp/r2.txt >/dev/null 2>&1; then
    echo "  ✓ Build #1 and Build #2 results byte-identical"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
else
    echo "  ✗ Build #2 results differ from Build #1"; diff -u /tmp/r1.txt /tmp/r2.txt || true
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1)); CHECKS_FAILED=$((CHECKS_FAILED + 1))
fi

#############################
# MUTATE CONTENT + RE-PULL
#############################

echo ""
echo "=== Mutate sensors.csv in git (new max temp 99.9) and re-pull ==="
cd "$REPO_DIR"
cat > data/sensors.csv << 'CSV'
timestamp,temperature,humidity
2024-01-01T00:00:00Z,20.5,45.0
2024-01-01T01:00:00Z,21.0,44.5
2024-01-01T02:00:00Z,22.1,43.0
2024-01-01T03:00:00Z,99.9,40.0
CSV
git add -A
git commit -q -m "Add hot reading (max temp 99.9)"
cd /
RUST_LOG=info pond run /sensors pull
echo "Re-pull complete; blob OID changed, so the node_id must change too."

#############################
# BUILD #3 — changed: re-derive, no stale read
#############################

echo ""
echo "=== Build #3: read after content change (must re-derive, no stale read) ==="
read_dynamic /tmp/r3.txt /tmp/b3.log
SAVE_3=$(save_count /tmp/b3.log)
grep '\[SAVE\] Format cache' /tmp/b3.log || true
check_ge "Build #3 re-parses the changed dynamic file (>=1 [SAVE])" "${SAVE_3}" 1

MX_3=$(grep -oE '[0-9]+\.[0-9]+' /tmp/r3.txt | head -1)
echo "Build #3 MAX(temperature) = ${MX_3}"
check_eq "Build #3 sees NEW content (max temp 99.9 — NOT a stale 22.1)" \
    "${MX_3}" "99.9"

CACHE_3_FILES=$(cache_files)
check_ne "Build #3 created a NEW cache file (content-addressed node_id changed)" \
    "${CACHE_3_FILES}" "${CACHE_1_FILES}"

#############################
# BUILD #4 — unchanged again: reuse the new materialization
#############################

echo ""
echo "=== Build #4: re-read unchanged after the change (reuse again) ==="
read_dynamic /tmp/r4.txt /tmp/b4.log
SAVE_4=$(save_count /tmp/b4.log)
echo "Build #4 [SAVE] count: ${SAVE_4}"
check_eq "Build #4 re-parses NOTHING (0 [SAVE] — new materialization reused)" \
    "${SAVE_4}" "0"
if diff -u /tmp/r3.txt /tmp/r4.txt >/dev/null 2>&1; then
    echo "  ✓ Build #3 and Build #4 results byte-identical"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
else
    echo "  ✗ Build #4 results differ from Build #3"; diff -u /tmp/r3.txt /tmp/r4.txt || true
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1)); CHECKS_FAILED=$((CHECKS_FAILED + 1))
fi

#############################
# RESULTS
#############################

echo ""
echo "=== Results ==="
echo "Checks: ${CHECKS_TOTAL}, Failed: ${CHECKS_FAILED}"
if [[ "${CHECKS_FAILED}" -ne 0 ]]; then
    echo "[FAIL] one or more checks failed"
    exit 1
fi
echo "[OK] dynamic materializations are reused across read transactions AND"
echo "     re-derived on content change (content-addressed node_id) — no stale reads"
