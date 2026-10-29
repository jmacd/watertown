#!/bin/bash
# EXPERIMENT: Four-page documentation readiness gate, using cached pond only
# EXPECTED: Canonical nested links, literal code, strict rejection, and lean offline output
set -euo pipefail
source check.sh
REPO_ROOT="${REPO_ROOT:-/repo}"
EXAMPLES_ROOT="${EXAMPLES_ROOT:-/examples}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/watertown-test.XXXXXX")"
trap 'rm -r -- "${WORK}"' EXIT
export EXAMPLE_OUTPUT="${WORK}/pilot"
export POND_BIN="${POND_BIN:-$(command -v pond)}"
bash "${EXAMPLES_ROOT}/documentation-pilot/run.sh"
SITE="${EXAMPLE_OUTPUT}/site"
FACTORY="${SITE}/reference/factories/synthetic-timeseries.html"
CSV="${SITE}/reference/formats/csv.html"
GUIDE="${SITE}/guides/timeseries.html"
SHORTCODES="${SITE}/reference/sitegen/shortcodes.html"
for page in "${SITE}/index.html" "${FACTORY}" "${CSV}" "${GUIDE}" "${SHORTCODES}"; do
    check '[[ -f "${page}" ]]' "pilot page $(basename "${page}") generated"
done
check_contains "${GUIDE}" "duplicate heading gets unique ID" 'id="inspect-1"'
check_contains "${GUIDE}" "page contents uses canonical ID" 'href="#inspect-1"'
check_contains "${GUIDE}" "cross-page fragment uses Pages prefix" 'href="/watertown/reference/factories/synthetic-timeseries.html#inspect"'
check_contains "${SHORTCODES}" "literal variable is not expanded" '{{ $0 }}'
check_contains "${SHORTCODES}" "literal shortcode spelling is preserved" '{{ nav-list collection='
check_contains "${SHORTCODES}" "fenced HTML remains escaped" '&lt;script&gt;throw new Error('
check_contains "${SHORTCODES}" "included HTML remains escaped" 'included source is literal'
check_not_contains "${SHORTCODES}" "included script is not executable" '<script>throw'
check_contains "${SHORTCODES}" "included fence stays literal" '~~~ text'
check_contains "${FACTORY}" "exact fixture YAML included" 'slope: 0.001'
check_contains "${FACTORY}" "exact fixture script included" 'pond init --birthplace documentation-example'
check_contains "${FACTORY}" "nested sidebar active page" 'aria-current="page">Synthetic timeseries'
check_contains "${FACTORY}" "skip navigation is available" 'href="#manual-content"'
check '! find "${SITE}" -name "*.js" -o -name "*.wasm" | grep -q .' "no unused JS/WASM bundles"
check '[[ ! -d "${SITE}/vendor" ]]' "no visualization vendor tree"
check '! grep -R -E "fonts.googleapis|fonts.gstatic" "${SITE}" --include="*.html"' "no remote fonts"
check 'cmp "${EXAMPLES_ROOT}/synthetic-timeseries/config.yaml" "${SITE}/testsuite/examples/synthetic-timeseries/config.yaml"' "downloadable raw source is identical"
mkdir "${WORK}/download"
tar -xf "${SITE}/downloads/csv-query.tar" -C "${WORK}/download"
check 'diff -r "${EXAMPLES_ROOT}/csv-query" "${WORK}/download/csv-query"' "archive is generated from exact fixtures"

# Only the explicitly selected source tree is copied. Archives are not inputs.
INPUT="${WORK}/input"
mkdir -p "${INPUT}/docs" "${INPUT}/testsuite"
cp -R "${REPO_ROOT}/docs/manual" "${REPO_ROOT}/docs/templates" "${INPUT}/docs/"
cp "${REPO_ROOT}/docs/site.yaml" "${INPUT}/docs/site.yaml"
cp -R "${EXAMPLES_ROOT}" "${INPUT}/testsuite/examples"
mkdir -p "${INPUT}/docs/manual/reference/_generated"
printf '{{ this_must_not_be_discovered /}}\n' > "${INPUT}/docs/manual/reference/_generated/fragment.md"
mkdir -p "${WORK}/rebuild/downloads"
cp "${SITE}/downloads/"*.tar "${WORK}/rebuild/downloads/"

build() {
    "${POND_BIN}" run -d "${INPUT}" host+sitegen:///docs/site.yaml -- build "${WORK}/rebuild" --strict > "${WORK}/build.log" 2>&1
}
PAGE="${INPUT}/docs/manual/reference/sitegen/shortcodes.md"
ORIGINAL="${REPO_ROOT}/docs/manual/reference/sitegen/shortcodes.md"
check 'build' "cached binary rebuild succeeds without generated fragments becoming pages"
printf '\n{{ source path="/testsuite/examples/missing.txt" region="query" /}}\n' >> "${PAGE}"
check '! build' "strict build rejects missing include"
check_contains "${WORK}/build.log" "include diagnostic names referring page" '/docs/manual/reference/sitegen/shortcodes.md'
check_contains "${WORK}/build.log" "include diagnostic names requested source" '/testsuite/examples/missing.txt'
check_contains "${WORK}/build.log" "include diagnostic names requested region" "region 'query'"
cp "${SHORTCODES}" "${WORK}/previous.html"
check '! DOCS_SOURCE_ROOT="${INPUT}" DOCS_OUT="${SITE}" bash "${REPO_ROOT}/scripts/docs-build.sh" > "${WORK}/wrapper.log" 2>&1' "failed staged build is rejected"
check 'cmp "${SHORTCODES}" "${WORK}/previous.html"' "failed staged build preserves the previous preview"
cp "${ORIGINAL}" "${PAGE}"
printf '\n{{ source path="/testsuite/examples/csv-query/run.sh" region="missing" /}}\n' >> "${PAGE}"
check '! build' "strict build rejects missing region"
check_contains "${WORK}/build.log" "region diagnostic names requested region" "missing region 'missing'"
cp "${ORIGINAL}" "${PAGE}"
printf '\n[Broken](../../missing.md#absent)\n' >> "${PAGE}"
check '! build' "strict build rejects broken source link"
check_contains "${WORK}/build.log" "source-link diagnostic identifies target" 'missing.md#absent'
cp "${ORIGINAL}" "${PAGE}"
printf '\n[Broken](../../guides/timeseries.md#absent)\n' >> "${PAGE}"
check '! build' "strict build rejects broken fragment"
check_contains "${WORK}/build.log" "fragment diagnostic identifies target" 'timeseries.html#absent'
cp "${ORIGINAL}" "${PAGE}"
sed 's|/guides/timeseries.html|/missing-navigation.html|' "${REPO_ROOT}/docs/site.yaml" > "${INPUT}/docs/site.yaml"
check '! build' "strict build rejects invalid navigation target"
check_contains "${WORK}/build.log" "navigation diagnostic identifies target" 'missing-navigation.html'
cp "${REPO_ROOT}/docs/site.yaml" "${INPUT}/docs/site.yaml"
sed 's|doc_id: reference:shortcodes|doc_id: reference:shortcodes\nslug: reference/formats/csv|' "${ORIGINAL}" > "${PAGE}"
check '! build' "strict build rejects output collision"
check_contains "${WORK}/build.log" "collision diagnostic names output" "output collision 'reference/formats/csv.html'"
cp "${ORIGINAL}" "${PAGE}"
printf '\n{{ source path="/docs/site.yaml" /}}\n' >> "${PAGE}"
check '! build' "include cannot read outside approved roots"
check_contains "${WORK}/build.log" "include confinement diagnostic" 'outside documentation include_roots'
cp "${ORIGINAL}" "${PAGE}"
printf '\nA cached-binary prose edit.\n' >> "${PAGE}"
mkdir "${WORK}/tools"
printf '#!/bin/bash\necho "Unexpected Cargo invocation" >&2\nexit 99\n' > "${WORK}/tools/cargo"
chmod +x "${WORK}/tools/cargo"
check 'PATH="${WORK}/tools:${PATH}" DOCS_SOURCE_ROOT="${INPUT}" DOCS_OUT="${SITE}" bash "${REPO_ROOT}/scripts/docs-build.sh" > "${WORK}/wrapper.log" 2>&1' "ordinary prose edits rebuild with Cargo unavailable"
check_contains "${SHORTCODES}" "prose edit reaches output" 'A cached-binary prose edit.'
check '[[ ! -e "${WORK}/input/pond" && ! -e "${WORK}/input/.pond" ]]' "manual requires no pond"
check_finish
