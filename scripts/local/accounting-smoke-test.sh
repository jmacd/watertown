#!/bin/bash
# Developer smoke test: hledger (via exec-factory) + a real watertown pond
# + a local-directory remote backup, end to end, in one disposable container.
#
# This is deliberately NOT the "for real" story (that will be a published,
# hledger-capable image run the way production runs images -- see the
# notes at the bottom of this file). It's a fast dev-loop script for
# iterating on the mechanism itself, building from your checked-out tree,
# same pattern as scripts/try-exec-factory.sh.
#
# IMPORTANT FINDING (2026-10-06): bwrap needs real elevated container
# privileges to create its own user+mount namespace. Plain `docker run` /
# `podman run` (no extra flags) fails outright ("No permissions to create
# a new namespace"). Even `--cap-add=SYS_ADMIN --security-opt
# seccomp=unconfined --security-opt apparmor=unconfined` together still
# failed here ("Can't mount proc on /proc"). Only `--privileged` worked
# reliably. Production's config/scripts/pond.sh currently runs plain
# `podman run` with none of these flags -- an accounting instance will
# need `--privileged` (or whatever minimal-but-sufficient combination
# testing on the real watershop host turns up) added to pond.sh, gated
# per-instance so water/septic/noyo stay exactly as unprivileged as they
# are today.
#
# Usage:
#   ./scripts/local/accounting-smoke-test.sh
#       Run the unattended backup/restore smoke test.
#
#   ./scripts/local/accounting-smoke-test.sh --interactive
#       After seeding the journal, enter `hledger-ui` inside the exec
#       sandbox. Type `q` to exit; the script then backs up and
#       restores the pond and performs the same unattended verification.
#
# What it does, inside one throwaway --privileged container:
#   1. cargo-builds the pond binary once (incremental via named volumes, like
#      scripts/try-exec-factory.sh)
#   2. inits a fresh pond, seeds a ledger journal via a sandboxed hledger
#      run (series_outputs -- see scripts/examples/hledger-journal.yaml)
#   3. with --interactive, opens `hledger-ui` against that pond journal
#      with terminal stdin/stdout/stderr passed through the sandbox
#   4. attaches a LOCAL DIRECTORY backup (`pond backup add`) -- no S3/MinIO
#      needed for this smoke test -- and pushes
#   5. restores that backup into a second, independent pond directory
#      (`pond restore`) and verifies the journal survived the round trip
#   6. prints the restored journal and a real `hledger balance` report
#      computed from it
#
# If this passes, the three pieces (hledger, exec-factory sandboxing,
# watertown backup/restore) compose correctly. It does not validate S3/
# Azure credentials, terraform wiring, or the --privileged story above --
# those are the remaining steps before this can run "for real".

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

RUST_IMAGE="rust:slim"
CARGO_TARGET_VOLUME="exec-factory-try-target"
CARGO_REGISTRY_VOLUME="exec-factory-try-registry"

INTERACTIVE=false
case "${1:-}" in
    "")
        ;;
    --interactive)
        INTERACTIVE=true
        ;;
    *)
        echo "Usage: $0 [--interactive]" >&2
        exit 2
        ;;
esac

if [[ "${INTERACTIVE}" == "true" && ! -t 0 ]]; then
    echo "ERROR: --interactive requires a terminal on stdin" >&2
    exit 2
fi

INNER_SCRIPT='
set -euo pipefail
if ! command -v bwrap >/dev/null 2>&1 || ! command -v hledger >/dev/null 2>&1 || ! command -v hledger-ui >/dev/null 2>&1; then
    echo "=== installing bubblewrap + hledger + hledger-ui (one-time per container/host) ===" >&2
    apt-get update -qq && apt-get install -y -qq bubblewrap hledger hledger-ui pkg-config libssl-dev >/dev/null
fi
cd /work

echo "--- Building pond once (incremental cache is retained across runs) ---"
cargo build --quiet --bin pond
POND_BIN="${CARGO_TARGET_DIR}/debug/pond"

PRIMARY=$(mktemp -d /tmp/accounting-primary.XXXXXX)
BACKUP_DIR=$(mktemp -d /tmp/accounting-backup.XXXXXX)
# `pond restore` refuses to run over an EXISTING directory (even empty), so
# pick a name under a dir that exists but do not create the leaf itself.
RESTORED="$(mktemp -d /tmp/accounting-restored-parent.XXXXXX)/pond"

run() { "${POND_BIN}" --pond "${PRIMARY}" "$@"; }

echo "--- Step 1: init pond, seed journal via sandboxed hledger ---"
run init --birthplace accounting-smoke-test
run mkdir -p /accounting
run mkdir -p /system/etc

cat > /tmp/seed.yaml << "EOF"
program: /bin/sh
args:
  - "-c"
  - |
    cat > accounting/journal.ledger << "LEDGER"
    2026-01-01 Opening balance
        assets:checking           $1000.00
        equity:opening balance
    LEDGER
inputs: []
series_outputs: ["/accounting/journal.ledger"]
EOF
run mknod exec /system/etc/hledger-seed --config-path /tmp/seed.yaml
run run /system/etc/hledger-seed
run cat /accounting/journal.ledger

if [ "${ACCOUNTING_INTERACTIVE}" = "true" ]; then
    echo ""
    echo "--- Interactive milestone: hledger-ui inside the exec sandbox ---"
    echo "Browse accounts and transactions; type q to continue."
    run mknod exec /system/etc/hledger --config-path /work/scripts/examples/hledger-journal.yaml
    run run /system/etc/hledger
fi

echo ""
echo "--- Step 2: attach a local-directory backup and push ---"
run backup add origin "file://${BACKUP_DIR}"
run push origin

echo ""
echo "--- Step 3: restore the backup into a fresh pond directory ---"
"${POND_BIN}" --pond "${RESTORED}" restore origin "file://${BACKUP_DIR}"

echo ""
echo "--- Step 4: verify the restored journal, run a real hledger balance report ---"
RESTORED_RUN() { "${POND_BIN}" --pond "${RESTORED}" "$@"; }
RESTORED_RUN cat /accounting/journal.ledger

cat > /tmp/balance.yaml << "EOF"
program: /bin/sh
args: ["-c", "hledger -f accounting/journal.ledger balance > balance.txt"]
inputs: ["/accounting/journal.ledger"]
outputs: ["/balance.txt"]
EOF
RESTORED_RUN mknod exec /system/etc/hledger-balance --config-path /tmp/balance.yaml
RESTORED_RUN run /system/etc/hledger-balance
echo ""
echo "=== balance report, computed from the RESTORED pond ==="
RESTORED_RUN cat /balance.txt

echo ""
echo "=== PASS: hledger + exec-factory + backup/restore round-tripped cleanly ==="
'

echo "=== host $(uname -s): running inside a throwaway --privileged Linux container ===" >&2
echo "(see the IMPORTANT FINDING comment at the top of this script re: --privileged)" >&2
DOCKER_ARGS=(run --rm -i)
if [[ "${INTERACTIVE}" == "true" ]]; then
    DOCKER_ARGS+=(-t)
fi
docker "${DOCKER_ARGS[@]}" --privileged \
    -v "${REPO_ROOT}:/work" \
    -v "${CARGO_TARGET_VOLUME}:/cargo-target" \
    -v "${CARGO_REGISTRY_VOLUME}:/usr/local/cargo/registry" \
    -e CARGO_TARGET_DIR=/cargo-target \
    -e ACCOUNTING_INTERACTIVE="${INTERACTIVE}" \
    -w /work \
    "${RUST_IMAGE}" bash -c "${INNER_SCRIPT}"
