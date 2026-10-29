#!/bin/bash
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${DOCS_CONTAINER_IMAGE:?Set an immutable image digest or local content ID}"
: "${DOCS_OUT:?Set an absent artifact output directory}"
: "${WATERTOWN_DOCS_REVISION:?Set the exact source revision}"
: "${DOCS_EXPECTED_VERSION:?Set the expected image binary version}"
if [[ ! "${DOCS_CONTAINER_IMAGE}" =~ ^(.+@)?sha256:[0-9a-f]{64}$ ]]; then
    echo "Documentation requires an immutable image identity, not a moving tag." >&2
    exit 1
fi
if [[ -e "${DOCS_OUT}" ]]; then
    echo "Artifact output must not already exist: ${DOCS_OUT}" >&2
    exit 1
fi
DESTINATION="${DOCS_OUT}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/watertown-docs.XXXXXX")"
trap 'rm -r -- "${WORK}"' EXIT
WORK="$(cd "${WORK}" && pwd -P)"
mkdir "${WORK}/bin" "${WORK}/tmp"
export DOCS_REPO_ROOT="${REPO}" DOCS_WORKSPACE="${WORK}"
export DOCS_CONTAINER_ENGINE="${DOCS_CONTAINER_ENGINE:-docker}"
export DOCS_CONTAINER_IMAGE WATERTOWN_DOCS_REVISION
cat > "${WORK}/bin/pond" <<'SH'
#!/bin/bash
set -euo pipefail
exec "${DOCS_CONTAINER_ENGINE}" run --rm --network none --read-only \
    --entrypoint /usr/local/bin/pond --user "$(id -u):$(id -g)" \
    --tmpfs /tmp:rw,nosuid,nodev \
    --env POND --env WATERTOWN_DOCS_REVISION --env WATERTOWN_DOCS_VERSION \
    --volume "${DOCS_REPO_ROOT}:${DOCS_REPO_ROOT}:ro" \
    --volume "${DOCS_WORKSPACE}:${DOCS_WORKSPACE}:rw" \
    --workdir "${DOCS_REPO_ROOT}" "${DOCS_CONTAINER_IMAGE}" "$@"
SH
chmod +x "${WORK}/bin/pond"
export PATH="${WORK}/bin:${REPO}/testsuite/helpers:${PATH}"
export TMPDIR="${WORK}/tmp" REPO_ROOT="${REPO}" EXAMPLES_ROOT="${REPO}/testsuite/examples"
ACTUAL="$(pond --version)"
if [[ "${ACTUAL}" != "pond ${DOCS_EXPECTED_VERSION}" ]]; then
    echo "Image version mismatch: expected pond ${DOCS_EXPECTED_VERSION}; got ${ACTUAL}" >&2
    exit 1
fi
for test in 057-documentation-csv-example 100-synthetic-timeseries-basic \
    213-sitegen-documentation-pilot 214-documentation-workflows; do
    bash "${REPO}/testsuite/tests/${test}.sh"
done
POND_BIN="${WORK}/bin/pond" DOCS_OUT="${WORK}/manual" bash "${REPO}/scripts/docs-build.sh"
mkdir -p "$(dirname "${DESTINATION}")"
cp -R "${WORK}/manual" "${DESTINATION}"
echo "Validated documentation artifact: ${DESTINATION}"
