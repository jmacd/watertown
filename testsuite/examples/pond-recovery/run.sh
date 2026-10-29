#!/bin/bash
set -euo pipefail
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -n "${EXAMPLE_OUTPUT:-}" ]]; then
    mkdir "${EXAMPLE_OUTPUT}"
    WORK="$(cd "${EXAMPLE_OUTPUT}" && pwd)"
else
    WORK="$(mktemp -d "${TMPDIR:-/tmp}/watertown-recovery.XXXXXX")"
fi
export POND="${WORK}/source"
pond init --birthplace documentation-recovery
pond mkdir /data
pond copy "host://${SOURCE_DIR}/../csv-query/readings.csv" /data/readings.csv
pond backup add origin "file://${WORK}/backup"
pond push origin
pond status > "${WORK}/source-status.txt"

export POND="${WORK}/restored"
pond restore origin "file://${WORK}/backup"
pond fsck > "${WORK}/restored-fsck.txt"
pond cat /data/readings.csv > "${WORK}/restored.csv"
pond status > "${WORK}/restored-status.txt"
pond pull origin > "${WORK}/pull-again.txt" 2>&1

POND="${WORK}/source" pond copy \
    "host://${SOURCE_DIR}/../csv-query/semicolon.csv" /data/update.csv
POND="${WORK}/source" pond push origin
pond pull origin
pond cat /data/update.csv > "${WORK}/update.csv"

if pond restore origin "file://${WORK}/backup" > "${WORK}/refused.txt" 2>&1; then
    echo "Restore unexpectedly replaced an existing pond" >&2
    exit 1
fi
echo "Example output: ${WORK}"
