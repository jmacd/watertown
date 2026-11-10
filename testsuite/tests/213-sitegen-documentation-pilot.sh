#!/bin/bash
# EXPERIMENT: Documentation readiness gate, using cached pond only
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
GIT_FACTORY="${SITE}/reference/factories/git-ingest.html"
CSV="${SITE}/reference/formats/csv.html"
GUIDE="${SITE}/guides/timeseries.html"
SHORTCODES="${SITE}/reference/sitegen/shortcodes.html"
for page in "${SITE}/index.html" "${FACTORY}" "${GIT_FACTORY}" "${CSV}" "${GUIDE}" "${SHORTCODES}" "${SITE}/guides/architecture.html"; do
    check '[[ -f "${page}" ]]' "pilot page $(basename "${page}") generated"
done
check '[[ "$(find "${SITE}" -name "*.html" -type f | wc -l | tr -d " ")" = 49 ]]' "complete manual renders 49 pages"
for format in csv excelhtml hledger jsonlogs oteljson weblog xls; do
    check '[[ -f "${SITE}/reference/formats/${format}.html" ]]' "individual ${format} format page generated"
    check_contains "${SITE}/index.html" "${format} has a direct sidebar link" "href=\"/watertown/reference/formats/${format}.html\""
done
check_contains "${GIT_FACTORY}" "each public factory has an individual page" '<h1 id="git-ingestion">Git ingestion'
check_contains "${GIT_FACTORY}" "factory page includes generated full configuration" 'id="full-configuration"'
check_contains "${GIT_FACTORY}" "page contents nests schema heading" 'href="#full-configuration">Full configuration</a><ul>'
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
check_not_contains "${GIT_FACTORY}" "example is not collapsible" '<details'
check_contains "${GIT_FACTORY}" "example is displayed inline" '<pre><code class="language-yaml">'
check_not_contains "${GIT_FACTORY}" "example has no download link" 'Download default.yaml'
check_contains "${GIT_FACTORY}" "annotated YAML comments are included verbatim" 'Required: example Git remote.'
mkdir "${WORK}/download"
tar -xf "${SITE}/downloads/csv-query.tar" -C "${WORK}/download"
check 'diff -r "${EXAMPLES_ROOT}/csv-query" "${WORK}/download/csv-query"' "archive is generated from exact fixtures"

# Only the explicitly selected source tree is copied. Archives are not inputs.
INPUT="${WORK}/input"
mkdir -p "${INPUT}/docs" "${INPUT}/testsuite"
cp -R "${REPO_ROOT}/docs/manual" "${REPO_ROOT}/docs/templates" "${INPUT}/docs/"
cp "${REPO_ROOT}/docs/site.yaml" "${REPO_ROOT}/docs/documentation-inventory.json" "${INPUT}/docs/"
cp -R "${EXAMPLES_ROOT}" "${INPUT}/testsuite/examples"
python3 - "${REPO_ROOT}" "${INPUT}" <<'PY'
import json
from pathlib import Path
import shutil
import sys
root, target = map(Path, sys.argv[1:])
catalog = json.loads((root / "docs/documentation-inventory.json").read_text())
for page in catalog["pages"].values():
    if "output" not in page:
        continue
    source = Path(page["path"])
    destination = target / source
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(root / source, destination)
    example = source.with_name("default.yaml")
    if (root / example).is_file():
        shutil.copyfile(root / example, target / example)
PY
mkdir -p "${INPUT}/docs/manual/reference/_generated"
printf '{{ this_must_not_be_discovered /}}\n' > "${INPUT}/docs/manual/reference/_generated/fragment.md"

build() {
    DOCS_SOURCE_ROOT="${INPUT}" DOCS_OUT="${WORK}/rebuild" \
        bash "${REPO_ROOT}/scripts/docs-build.sh" > "${WORK}/build.log" 2>&1
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
DEFAULTS="${INPUT}/crates/gitpond/src/factory/default.yaml"
mv "${DEFAULTS}" "${WORK}/missing-default.yaml"
check '! DOCS_SOURCE_ROOT="${INPUT}" DOCS_OUT="${SITE}" bash "${REPO_ROOT}/scripts/docs-build.sh" > "${WORK}/wrapper.log" 2>&1' "missing colocated YAML rejects assembly"
check_contains "${WORK}/wrapper.log" "missing YAML diagnostic identifies its component" 'crates/gitpond/src/factory/default.yaml'
check 'cmp "${SHORTCODES}" "${WORK}/previous.html"' "failed assembly preserves the previous preview"
mv "${WORK}/missing-default.yaml" "${DEFAULTS}"
DUPLICATE="${INPUT}/docs/manual/reference/factories/git-ingest.md"
cp "${INPUT}/crates/gitpond/src/factory/documentation.md" "${DUPLICATE}"
check '! build' "assembly rejects duplicate authored component pages"
check_contains "${WORK}/build.log" "duplicate diagnostic identifies its output" 'Generated component output collides with authored input: docs/manual/reference/factories/git-ingest.md'
rm -- "${DUPLICATE}"
printf '\nA cached-binary prose edit.\n' >> "${PAGE}"
COMPONENT="${INPUT}/crates/gitpond/src/factory/documentation.md"
printf '\nA colocated component prose edit.\n' >> "${COMPONENT}"
printf '\n# A colocated YAML edit.\n' >> "${DEFAULTS}"
cp "${COMPONENT}" "${WORK}/authored-component.md"
cp "${DEFAULTS}" "${WORK}/authored-default.yaml"
mkdir "${WORK}/tools"
printf '#!/bin/bash\necho "Unexpected Cargo invocation" >&2\nexit 99\n' > "${WORK}/tools/cargo"
chmod +x "${WORK}/tools/cargo"
check 'PATH="${WORK}/tools:${PATH}" DOCS_SOURCE_ROOT="${INPUT}" DOCS_OUT="${SITE}" bash "${REPO_ROOT}/scripts/docs-build.sh" > "${WORK}/wrapper.log" 2>&1' "ordinary prose edits rebuild with Cargo unavailable"
check_contains "${SHORTCODES}" "prose edit reaches output" 'A cached-binary prose edit.'
check_contains "${GIT_FACTORY}" "colocated component prose reaches output without Cargo" 'A colocated component prose edit.'
check_contains "${GIT_FACTORY}" "colocated YAML reaches output without Cargo" '# A colocated YAML edit.'
check 'cmp "${COMPONENT}" "${WORK}/authored-component.md" && cmp "${DEFAULTS}" "${WORK}/authored-default.yaml"' "build leaves colocated inputs unchanged"
check '[[ ! -e "${INPUT}/docs/manual/reference/factories/git-ingest.md" ]]' "component pages exist only in the generated staging tree"
check '[[ ! -e "${WORK}/input/pond" && ! -e "${WORK}/input/.pond" ]]' "manual requires no pond"
check_finish
