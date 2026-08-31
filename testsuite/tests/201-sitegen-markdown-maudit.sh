#!/bin/bash
# EXPERIMENT: Sitegen factory — Markdown + Maud site generation
# EXPECTED: Full pipeline from synthetic waveforms through join/pivot/reduce
#           then sitegen factory renders Markdown pages to HTML using Maud layouts.
#           Produces index.html + per-param + per-site pages.
#
# Pipeline: synthetic-timeseries → timeseries-join → timeseries-pivot
#           → temporal-reduce → sitegen (pond run /site.yaml build)
#
# Data: 2 sites (NorthDock, SouthDock) × 2 params (Temperature, DO)
#       1 year ending at test execution, at 1h resolution
set -e
source check.sh

echo "=== Experiment: Sitegen Factory — Markdown + Maud ==="

pond init --birthplace test-host

OUTDIR=/tmp/sitegen-dist

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Create synthetic sensor data (same as test 200)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create synthetic sensors ---"

# Keep the generated dashboard current so chart.js's live-monitoring default
# window always contains test data.
START_DATE=$(date -u -d "365 days ago" +%Y-%m-%dT00:00:00Z 2>/dev/null \
          || date -u -v-365d +%Y-%m-%dT00:00:00Z)
END_DATE=$(date -u +%Y-%m-%dT00:00:00Z)

cat > /tmp/sensors.yaml << YAML
entries:
  - name: "north_temperature"
    factory: "synthetic-timeseries"
    config:
      start: "${START_DATE}"
      end:   "${END_DATE}"
      interval: "1h"
      points:
        - name: "temperature.C"
          components:
            - type: sine
              amplitude: 4.0
              period: "24h"
              offset: 14.0
            - type: sine
              amplitude: 6.0
              period: "8760h"
              phase: -1.5708

  - name: "north_do"
    factory: "synthetic-timeseries"
    config:
      start: "${START_DATE}"
      end:   "${END_DATE}"
      interval: "1h"
      points:
        - name: "do.mgL"
          components:
            - type: sine
              amplitude: 1.5
              period: "24h"
              offset: 8.0
              phase: 3.1416
            - type: sine
              amplitude: 2.0
              period: "8760h"
              phase: 1.5708

  - name: "south_temperature"
    factory: "synthetic-timeseries"
    config:
      start: "${START_DATE}"
      end:   "${END_DATE}"
      interval: "1h"
      points:
        - name: "temperature.C"
          components:
            - type: triangle
              amplitude: 3.5
              period: "24h"
              offset: 15.0
            - type: sine
              amplitude: 5.5
              period: "8760h"
              phase: -1.5708

  - name: "south_do"
    factory: "synthetic-timeseries"
    config:
      start: "${START_DATE}"
      end:   "${END_DATE}"
      interval: "1h"
      points:
        - name: "do.mgL"
          components:
            - type: sine
              amplitude: 1.2
              period: "24h"
              offset: 7.5
              phase: 3.1416
            - type: square
              amplitude: 0.5
              period: "4380h"
              offset: 0.0
            - type: sine
              amplitude: 1.8
              period: "8760h"
              phase: 1.5708
YAML

pond mknod dynamic-dir /sensors --config-path /tmp/sensors.yaml
echo "✓ /sensors created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Create per-site joins at /combined
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Create per-site joins ---"

cat > /tmp/combined.yaml << 'YAML'
entries:
  - name: "NorthDock"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "series:///sensors/north_temperature"
          scope: TempProbe
        - pattern: "series:///sensors/north_do"
          scope: DOProbe

  - name: "SouthDock"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "series:///sensors/south_temperature"
          scope: TempProbe
        - pattern: "series:///sensors/south_do"
          scope: DOProbe
YAML

pond mknod dynamic-dir /combined --config-path /tmp/combined.yaml
echo "✓ /combined created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Create per-param pivots at /singled
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3: Create per-param pivots ---"

cat > /tmp/singled.yaml << 'YAML'
entries:
  - name: "Temperature"
    factory: "timeseries-pivot"
    config:
      pattern: "series:///combined/*"
      columns:
        - "TempProbe.temperature.C"

  - name: "DO"
    factory: "timeseries-pivot"
    config:
      pattern: "series:///combined/*"
      columns:
        - "DOProbe.do.mgL"
YAML

pond mknod dynamic-dir /singled --config-path /tmp/singled.yaml
echo "✓ /singled created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Create temporal reduce at /reduced
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 4: Create temporal reduce (1h avg/min/max) ---"

cat > /tmp/reduce.yaml << 'YAML'
entries:
  - name: "single_site"
    factory: "temporal-reduce"
    config:
      in_pattern: "/combined/*"
      out_pattern: "$0"
      time_column: "timestamp"
      resolutions: ["1h", "2h", "4h", "12h", "24h"]
      aggregations:
        - type: "avg"
          columns: ["*"]
        - type: "min"
          columns: ["*"]
        - type: "max"
          columns: ["*"]

  - name: "single_param"
    factory: "temporal-reduce"
    config:
      in_pattern: "/singled/*"
      out_pattern: "$0"
      time_column: "timestamp"
      resolutions: ["1h", "2h", "4h", "12h", "24h"]
      aggregations:
        - type: "avg"
          columns: ["*"]
        - type: "min"
          columns: ["*"]
        - type: "max"
          columns: ["*"]
YAML

pond mknod dynamic-dir /reduced --config-path /tmp/reduce.yaml
echo "✓ /reduced created"

echo ""
echo "Reduced directory tree:"
pond list /reduced/**

# ══════════════════════════════════════════════════════════════════════════════
# Step 5: Load markdown pages + site.yaml into pond
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 5: Load sitegen pages into pond ---"

pond mkdir /site

# Write markdown pages into pond
cat > /tmp/index.md << 'MD'
---
title: "Synthetic Example — Watertown"
layout: default
---

# Synthetic Example

Watertown End-to-End Pipeline Demo

## About This Example

This dashboard is generated entirely from **synthetic waveform data**
using the full Watertown pipeline:

- **synthetic-timeseries** — generates sine/triangle/square waveforms
- **timeseries-join** — combines parameters per site
- **timeseries-pivot** — extracts one parameter across all sites
- **temporal-reduce** — downsamples to hourly avg/min/max
- **sitegen** — renders Markdown pages to HTML with Maud layouts
- **pond export** — writes Parquet + HTML to disk

## Navigation

Use the sidebar to explore data **by parameter** or **by site**.
MD

cat > /tmp/data.md << 'MD'
---
title: "{{ $0 }}"
layout: data
---

# {{ $0 }}

{{ breadcrumb /}}

{{ chart /}}
MD

cat > /tmp/sidebar.md << 'MD'
## 🌡️ Synthetic Example

### Overview

- [Home](/)

### By Parameter

{{ nav_list collection="params" base="/params" /}}

### By Site

{{ nav_list collection="sites" base="/sites" /}}
MD

pond copy host:///tmp/index.md   /site/index.md
pond copy host:///tmp/data.md    /site/data.md
pond copy host:///tmp/sidebar.md /site/sidebar.md
echo "✓ markdown pages loaded"

# Create a minimal logo for the CSS background-image reference
cat > /tmp/logo.svg << 'SVG'
<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100"><rect width="100" height="100" fill="none"/></svg>
SVG
pond mkdir /img
pond copy host:///tmp/logo.svg /img/logo.svg
echo "✓ logo.svg loaded"

# Write site.yaml config
cat > /tmp/site.yaml << 'YAML'

site:
  title: "Synthetic Example"
  base_url: "/"

exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"
    target_points: 1500
  - name: "sites"
    pattern: "/reduced/single_site/*/*.series"
    target_points: 1500

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
  - name: "params"
    type: static
    slug: "params"
    routes:
      - name: "param-detail"
        type: template
        slug: "$0"
        page: "/site/data.md"
        export: "params"
  - name: "sites"
    type: static
    slug: "sites"
    routes:
      - name: "site-detail"
        type: template
        slug: "$0"
        page: "/site/data.md"
        export: "sites"

partials:
  sidebar: "/site/sidebar.md"

static:
  - pattern: "/img/*"
YAML

pond mknod sitegen /site.yaml --config-path /tmp/site.yaml
echo "✓ site.yaml loaded (factory=sitegen)"

echo ""
echo "Pond /etc listing:"
pond list '/site/**'

# ══════════════════════════════════════════════════════════════════════════════
# Step 6: Run sitegen factory
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 6: Run sitegen factory ---"
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"

pond run /site.yaml build "${OUTDIR}"
echo "✓ sitegen complete"

# ══════════════════════════════════════════════════════════════════════════════
# Step 7: Verification
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "=== VERIFICATION ==="

echo ""
echo "Output directory structure:"
find "${OUTDIR}" -type f | sort

echo ""
echo "File counts:"
HTML_COUNT=$(find "${OUTDIR}" -name '*.html' | wc -l | tr -d ' ')
echo "  HTML files: ${HTML_COUNT}"

# Check expected files

check_has_parquet_in() {
  local dir="$1"
  local label="$2"
  local count
  count=$(find "$dir" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
  if [ "$count" -gt 0 ]; then
    echo "  ✓ ${label} (${count} parquet files)"
    _CHECK_PASS=$((_CHECK_PASS + 1))
  else
    echo "  ✗ MISSING parquet in: ${label}"
    _CHECK_FAIL=$((_CHECK_FAIL + 1))
  fi
}

echo ""
echo "--- File existence ---"
check '[ -f "${OUTDIR}/index.html" ]'                 "index.html"
check '[ -f "${OUTDIR}/params/Temperature.html" ]'    "params/Temperature.html"
check '[ -f "${OUTDIR}/params/DO.html" ]'             "params/DO.html"
check '[ -f "${OUTDIR}/sites/NorthDock.html" ]'       "sites/NorthDock.html"
check '[ -f "${OUTDIR}/sites/SouthDock.html" ]'       "sites/SouthDock.html"
check '[ -f "${OUTDIR}/style.css" ]'                  "style.css"
check '[ -f "${OUTDIR}/chart.js" ]'                   "chart.js"

echo ""
echo "--- Vendor file checks ---"
check '[ -d "${OUTDIR}/vendor" ]'                            "vendor/ directory"
check '[ -f "${OUTDIR}/vendor/duckdb-eh.wasm" ]'            "vendor/duckdb-eh.wasm"
check '[ -f "${OUTDIR}/vendor/duckdb-browser.mjs" ]'        "vendor/duckdb-browser.mjs"
check '[ -f "${OUTDIR}/vendor/duckdb-browser-eh.worker.js" ]' "vendor/duckdb-browser-eh.worker.js"
check '[ -f "${OUTDIR}/vendor/vega-bundle.mjs" ]'           "vendor/vega-bundle.mjs"
check '[ ! -f "${OUTDIR}/vendor/plot-d3-bundle.mjs" ]'      "no vendor/plot-d3-bundle.mjs (all-in Vega)"

echo ""
echo "--- Content checks ---"
check_contains "${OUTDIR}/index.html" "index.html" "Synthetic Example"
check_contains "${OUTDIR}/index.html" "index.html" "</html>"
check_contains "${OUTDIR}/params/Temperature.html" "params/Temperature.html" "Temperature"
check_contains "${OUTDIR}/params/Temperature.html" "params/Temperature.html" "chart-container"
check_contains "${OUTDIR}/sites/NorthDock.html" "sites/NorthDock.html" "NorthDock"
check_contains "${OUTDIR}/sites/NorthDock.html" "sites/NorthDock.html" 'type="module"'

echo ""
echo "--- Asset checks ---"
check_contains "${OUTDIR}/style.css" "style.css" "sidebar-width"
check_contains "${OUTDIR}/style.css" "style.css" ".chart-container"
check_contains "${OUTDIR}/chart.js" "chart.js" "chart-data"

echo ""
echo "--- Data export checks (Hive-partitioned) ---"

for res in 1h 2h 4h 12h 24h; do
  check_has_parquet_in "${OUTDIR}/data/single_param/Temperature/res=${res}" "data/Temperature res=${res}"
  check_has_parquet_in "${OUTDIR}/data/single_param/DO/res=${res}" "data/DO res=${res}"
  check_has_parquet_in "${OUTDIR}/data/single_site/NorthDock/res=${res}" "data/NorthDock res=${res}"
  check_has_parquet_in "${OUTDIR}/data/single_site/SouthDock/res=${res}" "data/SouthDock res=${res}"
done

# Manifest URLs should reference partitioned paths (contain res= paths)
check_contains "${OUTDIR}/params/Temperature.html" "Temperature manifest has partitioned 1h" 'single_param/Temperature/res=1h/'
check_contains "${OUTDIR}/params/Temperature.html" "Temperature manifest has partitioned 24h" 'single_param/Temperature/res=24h/'

echo ""
echo "--- Navigation checks ---"
check_contains "${OUTDIR}/index.html" "index.html sidebar" 'href="/params/Temperature.html"'
check_contains "${OUTDIR}/index.html" "index.html sidebar" 'href="/sites/NorthDock.html"'

echo ""
echo "--- Temporal bounds checks ---"
# start_time/end_time should be non-zero in the chart manifest
# The manifest contains JSON with "start_time": <epoch_seconds>
check 'grep -oP '"'"'"start_time":\s*\d+'"'"' "${OUTDIR}/params/Temperature.html" | grep -v '"'"'"start_time": *0'"'"' | head -1 > /dev/null 2>&1' "Temperature manifest has non-zero start_time"

echo ""
echo "--- Layout checks ---"
check_not_contains "${OUTDIR}/index.html" "index.html (default layout, no chart.js)" 'chart.js'
check_contains "${OUTDIR}/params/Temperature.html" "params/Temperature.html (data layout)" 'chart.js'

# Copy output to /output if mounted (use: ./run-test.sh 201 --output /tmp/sitegen-output)
if [ -d /output ]; then
  cp -r "${OUTDIR}/"* /output/
fi

check_finish
