#!/bin/bash
# EXPERIMENT: monitor-report over metrics with gaps in their readings
# DESCRIPTION: During the watershop pumphouse outage (2026-11-18) the system
#   pressure sensor stopped reporting while other station metrics continued,
#   and the water monitor failed with:
#
#     monitor query failed: Schema error: No field named system_pressure_value
#
#   which stopped every check from publishing. Each check reads its source
#   bounded to the check's window, and oteljson infers its columns from the
#   records it holds, so the windowed table can lack a column. Two gaps:
#
#   1. A metric that reported in the past but not inside the window (the
#      outage). The pressure readings are in an archived log; the active log
#      holds only recent well-depth readings.
#   2. A metric this pond has never received (a new or reset pond, or a
#      sensor that is not yet reporting): no version has the column at all.
#
# EXPECTED: The report publishes. Both gaps read as no data (state unknown,
#   no samples) and the well-depth check is evaluated normally.
set -e
source check.sh

echo "=== Experiment: monitor-report data gaps ==="

pond init --birthplace test-host

# ---- Setup: pressure readings stop two hours ago; tank level never reports ----

NOW_S=$(date +%s)
line() {
    # line SECONDS_AGO NAME=VALUE...
    local ns="$(( NOW_S - $1 ))000000000"
    shift
    local metrics=""
    for metric in "$@"; do
        local name=${metric%%=*}
        local value=${metric#*=}
        [ -n "$metrics" ] && metrics+=","
        metrics+="{\"name\":\"${name}\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"${ns}\",\"asDouble\":${value}}]}}"
    done
    echo "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[${metrics}]}]}]}"
}

mkdir -p /var/log/station
{
    line 7200 well_depth_value=41.0 system_pressure_value=1.5
    line 7140 well_depth_value=41.1 system_pressure_value=1.6
} > /var/log/station/station-1.json
{
    line 90 well_depth_value=41.5
    line 60 well_depth_value=41.6
} > /var/log/station/station.json

cat > /tmp/ingest.yaml << 'EOF'
archived_pattern: /var/log/station/station-*.json
active_pattern: /var/log/station/station.json
pond_path: /ingest
timestamp_field: timeUnixNano
timestamp_unit: nanoseconds
EOF

pond mkdir -p /system/etc
pond mkdir -p /ingest
pond mknod logfile-ingest /system/etc/10-station --config-path /tmp/ingest.yaml

echo ""
echo "--- Ingest: archived log with pressure, active log without ---"
pond run /system/etc/10-station
pond list '/ingest/*'

PRESSURE_ROWS=$(pond cat oteljson:///ingest/station-1.json --format=table \
  --sql "SELECT COUNT(system_pressure_value) AS c FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

# ---- Monitor: a 3h well-depth window, a 5m pressure window (all gap), and
# ---- a tank-level check on a metric never received ----

mkdir -p /tmp/monitor
cat > /tmp/monitor.yaml << 'YAML'
pond: test
title: "Station status"
output_dir: /tmp/monitor
checks:
  - id: well-depth-healthy
    label: "Well depth"
    description: "The well has recovered recently."
    href: "/data/well-depth.html"
    type: stale-or-all-below
    source: "oteljson:///ingest/station*.json"
    timestamp_column: timestamp
    value_column: well_depth_value
    unit: m
    threshold: 40
    window: 3h
    stale_window: 24h
  - id: tank-level-low
    label: "Tank level"
    description: "The tank stays full."
    href: "/data/tank-level.html"
    type: any-observed-below
    source: "oteljson:///ingest/station*.json"
    timestamp_column: timestamp
    value_column: concrete_tank_level_value
    unit: m
    threshold: 3
    window: 3h
  - id: system-pressure-range
    label: "System pressure"
    description: "System pressure indicates demand."
    href: "/data/system-pressure.html"
    type: average-range
    source: "oteljson:///ingest/station*.json"
    timestamp_column: timestamp
    value_column: system_pressure_value
    unit: psi
    lower_bound: 0.5
    upper_bound: 2.2
    window: 5m
YAML

pond mknod monitor-report /system/etc/00-monitor --config-path /tmp/monitor.yaml

echo ""
echo "--- Run monitor ---"
RC=0
pond run /system/etc/00-monitor > /tmp/monitor.log 2>&1 || RC=$?
cat /tmp/monitor.log
cat /tmp/monitor/status.json || true

check_state() {
    jq -r ".checks[] | select(.id == \"$1\") | .$2" /tmp/monitor/status.json
}

# ---- Verify ----

echo ""
echo "--- Verification ---"

check '[ "${PRESSURE_ROWS}" = "2" ]' \
  "the pond holds pressure readings, just none recent (got ${PRESSURE_ROWS})"

check '[ "$RC" = "0" ]' "monitor run succeeds despite the gaps (exit ${RC})"

check_not_contains /tmp/monitor.log "no schema error for an absent column" \
  'No field named'

check '[ -f /tmp/monitor/status.json ]' "status.json is published"

check '[ "$(check_state system-pressure-range state)" = "unknown" ]' \
  "pressure check reads the outage as no data (unknown)"

check '[ "$(check_state system-pressure-range sample_count)" = "0" ]' \
  "pressure check has no samples in its window"

check '[ "$(check_state tank-level-low state)" = "unknown" ]' \
  "tank-level check reads a never-reported metric as no data (unknown)"

check '[ "$(check_state tank-level-low sample_count)" = "0" ]' \
  "tank-level check has no samples"

check_contains /tmp/monitor.log "the never-reported metric is logged as a gap" \
  '["concrete_tank_level_value"] column; treating it as no data'

check '[ "$(check_state well-depth-healthy state)" = "healthy" ]' \
  "well-depth check is still evaluated (healthy)"

check '[ "$(check_state well-depth-healthy sample_count)" = "4" ]' \
  "well-depth check sees all four readings in its 3h window"

check_finish
