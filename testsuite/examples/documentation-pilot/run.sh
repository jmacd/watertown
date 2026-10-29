#!/bin/bash
set -euo pipefail
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "${SOURCE_DIR}/../../.." && pwd)}"
if [[ -n "${EXAMPLE_OUTPUT:-}" ]]; then
    mkdir "${EXAMPLE_OUTPUT}"
    WORK="$(cd "${EXAMPLE_OUTPUT}" && pwd)"
else
    WORK="$(mktemp -d "${TMPDIR:-/tmp}/watertown-manual.XXXXXX")"
fi
export POND_BIN="${POND_BIN:-$(command -v pond)}"
make -C "${REPO_ROOT}" docs-build DOCS_OUT="${WORK}/site" POND_BIN="${POND_BIN}"
echo "Example output: ${WORK}"
