#!/bin/bash
set -euo pipefail
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -n "${EXAMPLE_OUTPUT:-}" ]]; then
    mkdir "${EXAMPLE_OUTPUT}"
    WORK="$(cd "${EXAMPLE_OUTPUT}" && pwd)"
else
    WORK="$(mktemp -d "${TMPDIR:-/tmp}/watertown-site.XXXXXX")"
fi
export POND="${WORK}/pond"

pond init --birthplace documentation-site
for path in /sensors /pages /etc; do
    pond mkdir "${path}"
done
pond mknod synthetic-timeseries /sensors/temperature \
    --config-path "${SOURCE_DIR}/../synthetic-timeseries/config.yaml"
pond copy "host://${SOURCE_DIR}/index.md" /pages/index.md
pond copy "host://${SOURCE_DIR}/sensor.md" /pages/sensor.md
pond mknod sitegen /etc/site --config-path "${SOURCE_DIR}/site.yaml"
pond run /etc/site -- build "${WORK}/site" --strict
find "${WORK}/site/data" -name '*.parquet' | LC_ALL=C sort > "${WORK}/parquet-files.txt"
while IFS= read -r file; do
    pond cat "host+table://${file}" --format=table --sql \
        'SELECT COUNT(*) AS rows, CAST(SUM(temperature) AS DECIMAL(10,1)) AS total FROM source'
done < "${WORK}/parquet-files.txt" > "${WORK}/exported-values.txt"
echo "Example output: ${WORK}"
