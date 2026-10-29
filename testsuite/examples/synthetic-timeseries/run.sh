#!/bin/bash
set -euo pipefail
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -n "${EXAMPLE_OUTPUT:-}" ]]; then
    mkdir "${EXAMPLE_OUTPUT}"
    WORK="$(cd "${EXAMPLE_OUTPUT}" && pwd)"
else
    WORK="$(mktemp -d "${TMPDIR:-/tmp}/watertown-synthetic.XXXXXX")"
fi
export POND="${WORK}/pond"

# docs:start create-and-query
pond init --birthplace documentation-example
pond mkdir /sensors
pond mknod synthetic-timeseries /sensors/synthetic --config-path "${SOURCE_DIR}/config.yaml"
pond cat /sensors/synthetic --format=table --sql \
    'SELECT COUNT(*) AS rows, MIN(timestamp) AS first_ts, MAX(timestamp) AS last_ts FROM source' \
    > "${WORK}/summary.txt"
pond cat /sensors/synthetic --format=table --sql \
    'SELECT timestamp, temperature FROM source ORDER BY timestamp' \
    > "${WORK}/values.txt"
# docs:end create-and-query
cat "${WORK}/summary.txt" "${WORK}/values.txt"
echo "Example output: ${WORK}"
