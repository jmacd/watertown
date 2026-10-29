#!/bin/bash
# EXPERIMENT: Synthetic timeseries factory — create and inspect
# EXPECTED: The published example asserts inclusive boundaries and deterministic values
set -euo pipefail
source check.sh
WORK="$(mktemp -d "${TMPDIR:-/tmp}/watertown-test.XXXXXX")"
trap 'rm -r -- "${WORK}"' EXIT
export EXAMPLE_OUTPUT="${WORK}/synthetic"
bash "${EXAMPLES_ROOT:-/examples}/synthetic-timeseries/run.sh"
check 'grep -Eq "\| 4 +\|" "${EXAMPLE_OUTPUT}/summary.txt"' "four hourly samples including end"
check_contains "${EXAMPLE_OUTPUT}/summary.txt" "inclusive start" '2024-01-01T00:00:00'
check_contains "${EXAMPLE_OUTPUT}/summary.txt" "inclusive end" '2024-01-01T03:00:00'
check_contains "${EXAMPLE_OUTPUT}/values.txt" "timestamp schema" 'timestamp'
check_contains "${EXAMPLE_OUTPUT}/values.txt" "value schema" 'temperature'
for value in 20 23.6 27.2 30.8; do
    check 'grep -Eq "\| ${value}(\.0)? +\|" "${EXAMPLE_OUTPUT}/values.txt"' "calculated value ${value}"
done
check '[[ $(grep -c "2024-01-01T" "${EXAMPLE_OUTPUT}/values.txt") -eq 4 ]]' "exactly four ordered result rows"
check_finish
