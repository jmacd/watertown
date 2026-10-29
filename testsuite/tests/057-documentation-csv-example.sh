#!/bin/bash
# EXPERIMENT: Published CSV fixture and URL parameters
# EXPECTED: Parsed schema, filtered rows, aggregates, and delimiter agree with the manual
set -euo pipefail
source check.sh
WORK="$(mktemp -d "${TMPDIR:-/tmp}/watertown-test.XXXXXX")"
trap 'rm -r -- "${WORK}"' EXIT
export EXAMPLE_OUTPUT="${WORK}/csv"
bash "${EXAMPLES_ROOT:-/examples}/csv-query/run.sh"
check_contains "${EXAMPLE_OUTPUT}/selected.txt" "parsed schema" 'timestamp'
check_contains "${EXAMPLE_OUTPUT}/selected.txt" "temperature column" 'temperature'
check_contains "${EXAMPLE_OUTPUT}/selected.txt" "humidity column" 'humidity'
check_contains "${EXAMPLE_OUTPUT}/selected.txt" "first selected row" '2024-01-01T01:00:00'
check_contains "${EXAMPLE_OUTPUT}/selected.txt" "last selected row" '2024-01-01T02:00:00'
check_not_contains "${EXAMPLE_OUTPUT}/selected.txt" "filter excludes first input" '2024-01-01T00:00:00'
check 'grep -Eq "\| 25(\.0)? +\| 44 +\|" "${EXAMPLE_OUTPUT}/selected.txt"' "selected values 25 and 44"
check 'grep -Eq "\| 30(\.0)? +\| 46 +\|" "${EXAMPLE_OUTPUT}/selected.txt"' "selected values 30 and 46"
check 'grep -Eq "\| 3 +\| 75(\.0)? +\| 45(\.0)? +\|" "${EXAMPLE_OUTPUT}/summary.txt"' "three rows, total 75, mean humidity 45"
check 'grep -Eq "\| 75(\.0)? +\|" "${EXAMPLE_OUTPUT}/parameter.txt"' "semicolon URL parameter total"
check '! bash "${EXAMPLES_ROOT:-/examples}/csv-query/run.sh" > "${WORK}/refused.txt" 2>&1' "refuses an existing output directory"
check_finish
