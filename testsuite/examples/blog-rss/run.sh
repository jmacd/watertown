#!/bin/bash
set -euo pipefail
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -n "${EXAMPLE_OUTPUT:-}" ]]; then
    mkdir "${EXAMPLE_OUTPUT}"
    WORK="$(cd "${EXAMPLE_OUTPUT}" && pwd)"
else
    WORK="$(mktemp -d "${TMPDIR:-/tmp}/watertown-blog.XXXXXX")"
fi
pond run -d "${SOURCE_DIR}" host+sitegen:///site.yaml -- build "${WORK}/site" --strict
echo "Example output: ${WORK}"
