#!/bin/bash
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE="${DOCS_SOURCE_ROOT:-${REPO}}"
OUTPUT="${DOCS_OUT:-${REPO}/.docs-preview/watertown}"
BINARY="${POND_BIN:-${REPO}/target/debug/pond}"
if [[ ! -x "${BINARY}" ]]; then
    echo "Missing pond binary: ${BINARY}. Build once with 'make build', or set POND_BIN to a compatible prebuilt binary." >&2
    exit 1
fi
if [[ -e "${OUTPUT}" && ! -f "${OUTPUT}/.watertown-docs" ]]; then
    echo "Refusing to replace non-documentation output: ${OUTPUT}" >&2
    exit 1
fi
mkdir -p "$(dirname "${OUTPUT}")"
SOURCE="$(cd "${SOURCE}" && pwd)"
OUTPUT="$(cd "$(dirname "${OUTPUT}")" && pwd)/$(basename "${OUTPUT}")"
case "${OUTPUT}/" in
    "${SOURCE}/"|"${SOURCE}/docs/"|"${SOURCE}/docs/manual/"*|"${SOURCE}/docs/templates/"*|"${SOURCE}/testsuite/"*)
        echo "Documentation output must not replace selected source inputs: ${OUTPUT}" >&2
        exit 1
        ;;
esac
STAGE="$(mktemp -d "$(dirname "${OUTPUT}")/.docs-build.XXXXXX")"
trap 'rm -r -- "${STAGE}"' EXIT
mkdir -p "${STAGE}/site/downloads"
for example in csv-query synthetic-timeseries; do
    tar -C "${SOURCE}/testsuite/examples" -cf "${STAGE}/site/downloads/${example}.tar" "${example}"
done
if ! "${BINARY}" run -d "${SOURCE}" host+sitegen:///docs/site.yaml -- build "${STAGE}/site" --strict; then
    echo "Manual build failed. This source requires documentation renderer contract 1 and build --strict; use a binary built after D04. Original diagnostics are above." >&2
    exit 1
fi
touch "${STAGE}/site/.watertown-docs"
if [[ -e "${OUTPUT}" ]]; then
    mv "${OUTPUT}" "${STAGE}/previous"
fi
if ! mv "${STAGE}/site" "${OUTPUT}"; then
    if [[ -d "${STAGE}/previous" ]]; then
        mv "${STAGE}/previous" "${OUTPUT}"
    fi
    exit 1
fi
echo "Manual built: ${OUTPUT}"
