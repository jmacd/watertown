#!/bin/bash
# EXPERIMENT: Shared documentation site, RSS, and recovery packages
# EXPECTED: Assert exported data, published paths/feed semantics, and restored bytes
set -euo pipefail
source check.sh
EXAMPLES_ROOT="${EXAMPLES_ROOT:-/examples}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/watertown-test.XXXXXX")"
trap 'rm -r -- "${WORK}"' EXIT

EXAMPLE_OUTPUT="${WORK}/timeseries" bash "${EXAMPLES_ROOT}/timeseries-site/run.sh"
SITE="${WORK}/timeseries/site"
check '[[ -f "${SITE}/index.html" && -f "${SITE}/temperature.html" ]]' "timeseries routes generated"
check_contains "${SITE}/temperature.html" "chart has exported data manifest" 'chart-data'
check_contains "${SITE}/temperature.html" "chart asset has project prefix" '/sensor-example/chart.js'
check '[[ $(wc -l < "${WORK}/timeseries/parquet-files.txt") -eq 1 ]]' "one small source export"
check_contains "${WORK}/timeseries/exported-values.txt" "export preserves four source rows" '| 4 '
check_contains "${WORK}/timeseries/exported-values.txt" "export preserves calculated temperature sum" '101.6'
check '[[ -f "${SITE}/chart.js" && -d "${SITE}/vendor" ]]' "data page assets included"

EXAMPLE_OUTPUT="${WORK}/blog" bash "${EXAMPLES_ROOT}/blog-rss/run.sh"
SITE="${WORK}/blog/site"
check '[[ -f "${SITE}/first.html" && -f "${SITE}/second.html" && -f "${SITE}/hidden.html" ]]' "blog routes include hidden page"
check_contains "${SITE}/feed.xml" "RSS links are absolute with prefix" 'https://example.invalid/sample-blog/second.html'
check_not_contains "${SITE}/feed.xml" "hidden draft excluded from RSS" 'Hidden draft'
check_not_contains "${SITE}/feed.xml" "undated content excluded from RSS" 'Undated page'
check '[[ $(grep -o "<item>" "${SITE}/feed.xml" | wc -l) -eq 2 ]]' "RSS has exactly two eligible posts"
check "awk 'BEGIN {RS=\"</item>\"} /Second post/ {newer=NR} /First post/ {older=NR} END {exit !(newer > 0 && older > newer)}' \"${SITE}/feed.xml\"" "RSS is newest first"
check_contains "${SITE}/blog.html" "blog card uses prefixed URL" '/sample-blog/first.html'
check_not_contains "${SITE}/blog.html" "hidden draft excluded from cards" 'Hidden draft'

EXAMPLE_OUTPUT="${WORK}/recovery" bash "${EXAMPLES_ROOT}/pond-recovery/run.sh"
check 'cmp "${EXAMPLES_ROOT}/csv-query/readings.csv" "${WORK}/recovery/restored.csv"' "restore preserves authoritative CSV bytes"
check 'cmp "${EXAMPLES_ROOT}/csv-query/semicolon.csv" "${WORK}/recovery/update.csv"' "restored replica receives subsequent update"
check 'grep -Eq "^[0-9a-f]{64}$" "${WORK}/recovery/restored-fsck.txt"' "restored local integrity check succeeds"
SOURCE_TIP="$(awk '/last pushed:/ {print $3}' "${WORK}/recovery/source-status.txt")"
RESTORED_TIP="$(awk '/last pulled:/ {print $3}' "${WORK}/recovery/restored-status.txt")"
check '[[ ${#SOURCE_TIP} -eq 64 && "${SOURCE_TIP}" == "${RESTORED_TIP}" ]]' "restored replica converges on pushed content tip"
check_contains "${WORK}/recovery/pull-again.txt" "unchanged pull is idempotent" 'already up to date'
check_contains "${WORK}/recovery/refused.txt" "restore cannot clobber existing pond" 'refusing to restore over an existing pond'
check_finish
