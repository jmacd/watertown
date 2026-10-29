#!/bin/bash
set -euo pipefail
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -n "${EXAMPLE_OUTPUT:-}" ]]; then
    mkdir "${EXAMPLE_OUTPUT}"
    WORK="$(cd "${EXAMPLE_OUTPUT}" && pwd)"
else
    WORK="$(mktemp -d "${TMPDIR:-/tmp}/watertown-csv.XXXXXX")"
fi

# docs:start query
pond cat -d "${SOURCE_DIR}" 'host+csv:///readings.csv' --format=table \
    --sql 'SELECT timestamp, temperature, humidity FROM source WHERE temperature >= 25 ORDER BY timestamp' \
    > "${WORK}/selected.txt"
pond cat -d "${SOURCE_DIR}" 'host+csv:///readings.csv' --format=table \
    --sql 'SELECT COUNT(*) AS rows, SUM(temperature) AS total, AVG(humidity) AS mean_humidity FROM source' \
    > "${WORK}/summary.txt"
pond cat -d "${SOURCE_DIR}" 'host+csv:///semicolon.csv?delimiter=;' --format=table \
    --sql 'SELECT SUM(temperature) AS parameter_total FROM source' \
    > "${WORK}/parameter.txt"
# docs:end query
cat "${WORK}/selected.txt" "${WORK}/summary.txt" "${WORK}/parameter.txt"
echo "Example output: ${WORK}"
