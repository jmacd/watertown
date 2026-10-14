#!/bin/bash
# SPDX-FileCopyrightText: 2026 Caspar Water Company
# SPDX-License-Identifier: Apache-2.0
#
# Build and run the `exec` dynamic factory locally via `cargo run` -- no
# pre-built binary, no Docker *image*, no remote/S3 pond. See
# docs/exec-factory-design.md for the mechanism this exercises.
#
# The sandbox (`bwrap`) needs Linux user namespaces:
#   - On Linux, this runs `cargo run` directly on the host.
#   - On anything else (e.g. macOS), it runs `cargo run` *inside* a
#     throwaway privileged Linux container instead -- a native build for
#     the container's own architecture, no cross-compilation involved.
# Either way, every pond this script touches is a fresh, local, throwaway
# directory; nothing is pushed or pulled from a remote.
#
# Usage:
#   ./scripts/try-exec-factory.sh
#       Run the built-in cat-passthrough demo (scripts/examples/cat-demo.yaml)
#       end to end in a fresh temp pond, and print the committed output.
#
#   ./scripts/try-exec-factory.sh pond [ARGS...]
#       Run any `pond` subcommand yourself (via cargo run), to seed inputs,
#       inspect a pond, or run your own exec config. Point `--pond <dir>` at
#       the same directory across calls to reuse it. Config files passed to
#       `mknod --config-path` must live under the repo root (anywhere under
#       /work inside the container) since that's all the container can see.
#
# Examples (your own exec config, kept across calls with one pond dir):
#   POND=/tmp/my-pond
#   ./scripts/try-exec-factory.sh pond --pond "$POND" init --birthplace me
#   ./scripts/try-exec-factory.sh pond --pond "$POND" mkdir -p /data
#   ./scripts/try-exec-factory.sh pond --pond "$POND" copy host:///tmp/journal.ledger /data/journal.ledger
#   ./scripts/try-exec-factory.sh pond --pond "$POND" mknod exec /system/etc/my-job --config-path scripts/examples/my-job.yaml
#   ./scripts/try-exec-factory.sh pond --pond "$POND" run /system/etc/my-job
#   ./scripts/try-exec-factory.sh pond --pond "$POND" cat /data/out.txt
#
# Build/registry caches live in named Docker volumes (not bind-mounted),
# so repeated calls are incremental and your host target/ dir is untouched.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

RUST_IMAGE="rust:slim"
CARGO_TARGET_VOLUME="exec-factory-try-target"
CARGO_REGISTRY_VOLUME="exec-factory-try-registry"

# Runs inside the container (or directly, on Linux). $@ is the script's own
# argv: either empty (built-in demo) or "pond <pond-subcommand-args...>".
INNER_SCRIPT='
set -euo pipefail
if ! command -v bwrap >/dev/null 2>&1 || ! command -v hledger >/dev/null 2>&1 || ! command -v hledger-ui >/dev/null 2>&1; then
    echo "=== installing bubblewrap + hledger + hledger-ui (one-time per container/host) ===" >&2
    apt-get update -qq && apt-get install -y -qq bubblewrap hledger hledger-ui pkg-config libssl-dev >/dev/null
fi
cd /work

if [ "$#" -eq 0 ]; then
    POND_DIR="$(mktemp -d /tmp/exec-factory-try.XXXXXX)"
    echo "=== fresh local pond: ${POND_DIR} ==="
    echo "hello from try-exec-factory.sh" > /tmp/try-exec-in.txt

    run() { cargo run --quiet --bin pond -- --pond "${POND_DIR}" "$@"; }

    run init --birthplace try-exec-factory
    run mkdir -p /data
    run mkdir -p /system/etc
    run copy "host:///tmp/try-exec-in.txt" /data/in.txt
    run mknod exec /system/etc/try-exec --config-path /work/scripts/examples/cat-demo.yaml
    run run /system/etc/try-exec

    echo ""
    echo "=== committed output (/data/out.txt) ==="
    run cat /data/out.txt
    echo ""
    echo "Pond directory: ${POND_DIR} (inside this throwaway container; gone on exit)"
elif [ "$1" = "pond" ]; then
    shift
    exec cargo run --quiet --bin pond -- "$@"
else
    echo "Usage: $0 [pond ARGS...]" >&2
    exit 2
fi
'

if [[ "$(uname -s)" == "Linux" ]]; then
    cd "${REPO_ROOT}"
    bash -c "${INNER_SCRIPT}" try-exec-factory "$@"
else
    echo "=== non-Linux host ($(uname -s)): running inside a throwaway privileged Linux container ===" >&2
    docker run --rm -i --privileged \
        -v "${REPO_ROOT}:/work" \
        -v "${CARGO_TARGET_VOLUME}:/cargo-target" \
        -v "${CARGO_REGISTRY_VOLUME}:/usr/local/cargo/registry" \
        -e CARGO_TARGET_DIR=/cargo-target \
        -w /work \
        "${RUST_IMAGE}" bash -c "${INNER_SCRIPT}" try-exec-factory "$@"
fi
