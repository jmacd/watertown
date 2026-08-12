// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// Watertown sitegen — chart.js
// Reads the chart-data JSON manifest, loads .parquet files via DuckDB-WASM,
// renders time-series line charts with Vega-Lite.
// Supports duration buttons (12h … 7d) and click-drag brush-to-zoom.
//
// LAZY LOADING: Parquet files are fetched on demand — only the files needed
// for the current resolution and visible time window are loaded. Each
// resolution tier is designed so that a typical screen view spans at most
// one partition boundary, meaning at most 2 files are fetched per render.

import { initDuckdb, createFileRegistry, rowsToCsv } from "./duckdb-shared.js";
import { loadVega, buildMetricChartSpec, escapeField } from "./vega-shared.js";

(async function () {
  "use strict";

  const manifestEl = document.querySelector('script.chart-data[type="application/json"]');
  if (!manifestEl) return;

  const container = document.getElementById("chart");
  if (!container) return;

  let manifest;
  try {
    manifest = JSON.parse(manifestEl.textContent);
  } catch (e) {
    return;
  }
  if (!manifest || manifest.length === 0) return;

  // Optional metric instrument-kind registry, emitted by sitegen alongside
  // the chart-data manifest.  Keys are `<param>.<unit>` (chart-grouping
  // suffix); values are "counter" | "updowncounter" | "gauge" | "rate".
  // Counter metrics are plotted as a first-difference rate per scope; "rate"
  // metrics plot as-is but with a zero-anchored, always-rendered y-axis;
  // everything else is plotted as-is.  Missing or empty registry => all
  // metrics treated as gauges (no transforms), matching legacy behaviour.
  const registryEl = document.querySelector('script.chart-registry[type="application/json"]');
  let metricKinds = {};
  if (registryEl) {
    try {
      metricKinds = JSON.parse(registryEl.textContent) || {};
    } catch (e) {
      metricKinds = {};
    }
  }
  function kindFor(chartKey) {
    return metricKinds[chartKey] || "gauge";
  }

  // Optional annotation manifest: a secondary interval export (emitted by
  // sitegen as `script.chart-annotations`) drawn as shaded background bands
  // behind every chart on the page. Rows come from parquet with `start_time`
  // / `end_time` (timestamps) and a `label` categorizing each interval
  // (e.g. "leak" / "no-leak"). Absent manifest => no bands (legacy behaviour).
  const annotationsEl = document.querySelector('script.chart-annotations[type="application/json"]');
  let annotationManifest = [];
  if (annotationsEl) {
    try {
      annotationManifest = JSON.parse(annotationsEl.textContent) || [];
    } catch (e) {
      annotationManifest = [];
    }
  }

  // Fixed colors for known interval labels; unknown labels fall back to gray.
  // Kept literal (identity color scale) so vega-shared stays domain-agnostic.
  // Pump-state and leak labels share this table (both drawn as bands).
  const ANNOTATION_COLORS = {
    leak: "#d64545",
    "no-leak": "#3f9e5a",
    pumping: "#4a72b0",
    recovery: "#b8912f",
  };
  function annotationColor(label) {
    return ANNOTATION_COLORS[label] || "#888888";
  }

  // The merged water-overlay carries two independent overlays (tracks) in its
  // columns: pump state (categorical) and leak risk (score-scaled). Each is
  // toggled by its own checkbox; the choice is persisted in localStorage so it
  // sticks across pages. Legacy label-only annotation files map to the leak
  // track. Defaults: both on.
  const OVERLAY_TRACKS = [
    { id: "pump", label: "Pump state" },
    { id: "leak", label: "Leak risk" },
  ];
  const TRACK_STORAGE_KEY = "sitegen.overlayTracks";
  function loadEnabledTracks() {
    try {
      const raw = localStorage.getItem(TRACK_STORAGE_KEY);
      if (raw) return { pump: true, leak: true, ...JSON.parse(raw) };
    } catch (e) { /* ignore */ }
    return { pump: true, leak: true };
  }
  function saveEnabledTracks() {
    try { localStorage.setItem(TRACK_STORAGE_KEY, JSON.stringify(enabledTracks)); }
    catch (e) { /* ignore */ }
  }
  let enabledTracks = loadEnabledTracks();

  // Optional per-chart caption strings, also emitted by sitegen.  Keys
  // match `metricKinds`; values are plain text rendered beneath each
  // chart.  Missing entries render no caption.
  const captionsEl = document.querySelector('script.chart-captions[type="application/json"]');
  let metricCaptions = {};
  if (captionsEl) {
    try {
      metricCaptions = JSON.parse(captionsEl.textContent) || {};
    } catch (e) {
      metricCaptions = {};
    }
  }

  // IEC byte formatter for `<param>.bytes` y-axes.  Picks the largest
  // unit whose magnitude keeps the displayed value below 1024 and uses
  // up to one fractional digit.  `null`/`NaN` render as the empty
  // string so the hover tooltip doesn't blow up on missing data.
  function formatBytes(v) {
    if (v == null || !isFinite(v)) return "";
    const sign = v < 0 ? "-" : "";
    let n = Math.abs(v);
    const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let i = 0;
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
    const digits = (n >= 100 || i === 0) ? 0 : 1;
    return `${sign}${n.toFixed(digits)} ${units[i]}`;
  }

  // ── State ──────────────────────────────────────────────────────────────────

  // Time range options (label, days).  Short (selfmon-style) and long
  // (water/noyo-style) sets coexist; `visibleRanges` (below) hides
  // buttons that don't fit the actual data span, so each dashboard
  // sees only the ranges that make sense for it.  Default is chosen
  // adaptively based on dataSpanDays.
  const ranges = [
    ["12h", 0.5], ["24h", 1], ["2d", 2], ["7d", 7],
    ["2W", 14], ["1M", 30], ["3M", 90], ["6M", 180],
    ["1Y", 365], ["2Y", 730], ["5Y", 1826], ["10Y", 3652],
  ];
  // Will be reassigned after dataSpanDays is computed below.
  let activeDays = 90;

  // Custom zoom range (set by brush, cleared by Reset or duration button)
  // When non-null, overrides the duration-button window.
  let zoomDomain = null;  // [beginMs, endMs] or null

  // Current effective window, refreshed on every render. Used by the window
  // inputs (start/end readout + editor) and the "Copy link" button.
  let curBegin = null, curEnd = null;

  // Most recently queried rows (the data backing the current view), kept so the
  // "Download CSV" button can export exactly what is shown for the window.
  let lastData = null;

  // A shareable link encodes the window as an absolute, fixed epoch-ms range
  // in the URL hash (`#t=<begin>,<end>`). Parse it on load so the page opens
  // showing exactly that range.
  {
    const m = /[#&]t=(\d+),(\d+)/.exec(location.hash);
    if (m) {
      const a = +m[1], b = +m[2];
      if (isFinite(a) && isFinite(b) && a < b) zoomDomain = [a, b];
    }
  }

  // Compute the data time span from the manifest so we can hide buttons
  // for ranges wider than the available data.
  let dataStartMs = Infinity;
  let dataEndMs = 0;
  for (const m of manifest) {
    if (m.start_time > 0) dataStartMs = Math.min(dataStartMs, m.start_time * 1000);
    if (m.end_time > 0) dataEndMs = Math.max(dataEndMs, m.end_time * 1000);
  }
  if (dataStartMs === Infinity) dataStartMs = 0;
  const dataSpanDays = dataEndMs > 0 ? (Date.now() - dataStartMs) / 86400000 : Infinity;

  // ── Duration button bar ────────────────────────────────────────────────────

  const toolbar = document.createElement("div");
  toolbar.className = "chart-toolbar";
  container.before(toolbar);

  const btnBar = document.createElement("div");
  btnBar.className = "duration-buttons";
  toolbar.appendChild(btnBar);

  // Pick an adaptive default based on the data span so high-frequency
  // dashboards (selfmon, span ~hours-days) default to "24h" while
  // low-frequency ones (water/noyo, span months-years) default to "3M".
  if (dataSpanDays >= 30) {
    activeDays = 90;
  } else if (dataSpanDays >= 7) {
    activeDays = 7;
  } else if (dataSpanDays >= 1) {
    activeDays = 1;
  } else if (isFinite(dataSpanDays)) {
    activeDays = 0.5;
  }

  // A site may pin an explicit default range via `data-default-range` on the
  // chart container (sitegen `site.default_range`, e.g. "1M"). When the label
  // resolves to a known range it overrides the adaptive default; otherwise the
  // adaptive value stands. The clamp to `visibleRanges` below still applies, so
  // a pinned range wider than the data falls back to the largest that fits.
  const defaultRangeLabel = container.dataset.defaultRange;
  if (defaultRangeLabel) {
    const match = ranges.find(([label]) => label === defaultRangeLabel);
    if (match) activeDays = match[1];
  }

  // Show every button whose window fits within the data span, plus exactly
  // one button that exceeds it (the smallest such window). This gives a single
  // "zoom out past all the data" option without a row of redundant oversized
  // buttons — e.g. a 6-year series shows up to 5Y plus 10Y, while a 3-month
  // series shows up to 1M plus 3M and hides everything larger.
  const visibleRanges = [];
  for (const r of ranges) {
    visibleRanges.push(r);
    if (r[1] > dataSpanDays) break; // first (smallest) exceeding button; stop
  }

  // If the current default doesn't appear in the visible set, pick the largest.
  if (!visibleRanges.some(([, days]) => days === activeDays) && visibleRanges.length > 0) {
    activeDays = visibleRanges[visibleRanges.length - 1][1];
  }

  visibleRanges.forEach(([text, days]) => {
    const btn = document.createElement("button");
    btn.textContent = text;
    btn.dataset.days = days;
    if (days === activeDays) btn.classList.add("active");
    btn.onclick = () => {
      zoomDomain = null;
      hideResetBtn();
      btnBar.querySelectorAll("button").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      activeDays = days;
      renderChart();
    };
    btnBar.appendChild(btn);
  });

  // Reset-zoom button (disabled until a brush selection is made)
  const resetBtn = document.createElement("button");
  resetBtn.className = "reset-zoom";
  resetBtn.textContent = "Reset zoom";
  resetBtn.disabled = true;
  resetBtn.onclick = () => {
    zoomDomain = null;
    hideResetBtn();
    // Re-activate the current duration button
    btnBar.querySelectorAll("button").forEach(b => {
      b.classList.toggle("active", parseFloat(b.dataset.days) === activeDays);
    });
    renderChart();
  };
  toolbar.appendChild(resetBtn);

  // Overlay track checkboxes — only when this page has an annotation manifest.
  // Toggling re-renders the chart(s) with the selected tracks and persists the
  // choice. Placed in its own toolbar row so it reads as page-level controls.
  if (annotationManifest.length) {
    const trackBar = document.createElement("div");
    trackBar.className = "overlay-tracks";
    const lbl = document.createElement("span");
    lbl.className = "overlay-tracks-label";
    lbl.textContent = "Overlays:";
    trackBar.appendChild(lbl);
    OVERLAY_TRACKS.forEach(t => {
      const wrap = document.createElement("label");
      wrap.className = "overlay-track";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = enabledTracks[t.id] !== false;
      cb.onchange = () => {
        enabledTracks[t.id] = cb.checked;
        saveEnabledTracks();
        renderChart();
      };
      wrap.appendChild(cb);
      wrap.appendChild(document.createTextNode(" " + t.label));
      trackBar.appendChild(wrap);
    });
    toolbar.appendChild(trackBar);
  }

  function showResetBtn() { resetBtn.disabled = false; }
  function hideResetBtn() { resetBtn.disabled = true; }

  // ── Window readout / editor + shareable link ─────────────────────────────────
  // A second toolbar row shows the effective window as two editable
  // datetime-local fields (start → end). Editing either sets a custom
  // (absolute) window, exactly like a brush selection. "Copy link" writes a
  // URL that reproduces the current absolute range.
  const windowBar = document.createElement("div");
  windowBar.className = "window-bar";

  const winLabel = document.createElement("span");
  winLabel.className = "window-label";
  winLabel.textContent = "Window:";

  const startInput = document.createElement("input");
  startInput.type = "datetime-local";
  startInput.className = "window-input";
  startInput.setAttribute("aria-label", "Window start");

  const arrow = document.createElement("span");
  arrow.className = "window-arrow";
  arrow.textContent = "→";

  const endInput = document.createElement("input");
  endInput.type = "datetime-local";
  endInput.className = "window-input";
  endInput.setAttribute("aria-label", "Window end");

  const copyBtn = document.createElement("button");
  copyBtn.type = "button";
  copyBtn.className = "copy-link";
  copyBtn.textContent = "Copy link";

  const downloadBtn = document.createElement("button");
  downloadBtn.type = "button";
  downloadBtn.className = "download-csv";
  downloadBtn.textContent = "Download CSV";
  downloadBtn.disabled = true;

  // "Explore this data" cross-link — only when sitegen emitted an explorer URL
  // (data-explore-url) on the chart container. Hands the current view's
  // overlapping files + time window to the explorer so the user can query
  // exactly what the chart shows, then edit the SQL freely.
  const exploreUrl = container.dataset.exploreUrl || "";
  let exploreBtn = null;
  if (exploreUrl) {
    exploreBtn = document.createElement("button");
    exploreBtn.type = "button";
    exploreBtn.className = "explore-data";
    exploreBtn.textContent = "Explore this data";
    exploreBtn.disabled = true;
  }

  windowBar.append(winLabel, startInput, arrow, endInput, copyBtn, downloadBtn);
  if (exploreBtn) windowBar.append(exploreBtn);
  toolbar.after(windowBar);

  function pad2(n) { return String(n).padStart(2, "0"); }
  function toLocalInput(ms) {
    const d = new Date(ms);
    return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}` +
      `T${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
  }

  // Keep the inputs in step with the rendered window, but never overwrite a
  // field the user is actively editing.
  function syncWindowControls(begin, end) {
    curBegin = begin;
    curEnd = end;
    if (document.activeElement !== startInput) startInput.value = toLocalInput(begin);
    if (document.activeElement !== endInput) endInput.value = toLocalInput(end);
  }

  function applyWindow() {
    const s = new Date(startInput.value).getTime();
    const e = new Date(endInput.value).getTime();
    if (!isFinite(s) || !isFinite(e) || s >= e) {
      // Revert to the last good values on invalid input.
      if (curBegin != null) syncWindowControls(curBegin, curEnd);
      return;
    }
    zoomDomain = [s, e];
    btnBar.querySelectorAll("button").forEach(b => b.classList.remove("active"));
    showResetBtn();
    renderChart();
  }
  startInput.addEventListener("change", applyWindow);
  endInput.addEventListener("change", applyWindow);

  let copyResetTimer = null;
  copyBtn.addEventListener("click", async () => {
    if (curBegin == null) return;
    const url = `${location.origin}${location.pathname}` +
      `#t=${Math.round(curBegin)},${Math.round(curEnd)}`;
    let ok = false;
    try {
      await navigator.clipboard.writeText(url);
      ok = true;
    } catch (e) {
      // Clipboard unavailable (e.g. non-secure context): reflect the range in
      // the address bar so the user can copy it manually.
      history.replaceState(null, "", url);
    }
    copyBtn.textContent = ok ? "Copied!" : "Link in URL";
    copyBtn.classList.add("copied");
    clearTimeout(copyResetTimer);
    copyResetTimer = setTimeout(() => {
      copyBtn.textContent = "Copy link";
      copyBtn.classList.remove("copied");
    }, 1800);
  });

  // Serialize the queried rows to CSV: a `timestamp` column (ISO 8601 / UTC)
  // followed by every non-partition data column, sorted for stable output.
  function buildCsv(rows) {
    return rowsToCsv(rows, {
      timestampCol: "timestamp",
      excludeCols: [...PARTITION_COLS],
      timestampToIso: (v) => toDate(v).toISOString(),
    });
  }

  downloadBtn.addEventListener("click", () => {
    if (!lastData || !lastData.length) return;
    const csv = buildCsv(lastData);
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    const slug = (location.pathname.split("/").pop() || "data").replace(/\.html?$/i, "") || "data";
    const stamp = (ms) => toLocalInput(ms).replace(/[:T]/g, "-");
    a.href = url;
    a.download = `${slug}_${stamp(curBegin)}_${stamp(curEnd)}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  });

  // Build the data-less Vega-Lite spec handed to the explorer when the user
  // clicks "Explore this data": fold the per-metric avg columns into one line
  // series each over a temporal x axis. The explorer injects its own query
  // result as the data, so this mirrors the shape of vega-shared's
  // `buildLineSpec` (which the explorer's "Reset to auto" reproduces).
  function buildExploreSpec(avgCols) {
    const cols = avgCols.map(escapeField);
    const multi = cols.length > 1;
    return {
      $schema: "https://vega.github.io/schema/vega-lite/v5.json",
      width: "container",
      height: 340,
      // Fit the y extent rather than forcing a zero baseline (meaningless for
      // arbitrary-scale sensor readings); matches the gauge spec default.
      config: { scale: { zero: false } },
      transform: multi ? [{ fold: cols, as: ["series", "value"] }] : [],
      mark: { type: "line", clip: true, tooltip: true },
      encoding: {
        x: { field: "timestamp", type: "temporal", title: "timestamp" },
        y: multi
          ? { field: "value", type: "quantitative" }
          : { field: cols[0], type: "quantitative", title: avgCols[0] },
        ...(multi ? { color: { field: "series", type: "nominal", title: null } } : {}),
      },
    };
  }

  if (exploreBtn) {
    exploreBtn.addEventListener("click", () => {
      if (curBegin == null || curEnd == null) return;
      const begin = Math.round(curBegin);
      const end = Math.round(curEnd);
      // The exact files backing the current view, as absolute URLs so the
      // explorer (served from a different path) can fetch them unchanged.
      const urls = overlappingEntries(begin, end)
        .map(e => new URL(e.url, location.href).href)
        .filter(Boolean);
      if (urls.length === 0) return;
      // The explorer registers the handed files as a view named `chart_data`;
      // this query reproduces the chart's window byte-for-byte, then the user
      // can edit it freely.
      const sql =
        `SELECT * FROM chart_data ` +
        `WHERE epoch_ms(timestamp) BETWEEN ${begin} AND ${end} ` +
        `ORDER BY timestamp`;
      const label = (document.title || "Chart data").trim() || "Chart data";
      const params = new URLSearchParams();
      params.set("label", label);
      params.set("files", urls.join(","));
      params.set("sql", sql);
      // Hand the explorer a clean default visualization: one avg line per metric
      // (matching this page's chart, minus the min/max bands) so "Explore this
      // data" opens straight into chart view instead of the raw query grid. The
      // user can still edit the SQL/spec or switch to the table. Field names are
      // dot-escaped because the temporal-reduce columns (`do.avg` etc.) contain
      // dots, which Vega-Lite would otherwise read as nested-object access.
      const avgCols = [];
      for (const series of groupColumns(lastData).values()) {
        for (const s of series) if (s.avg) avgCols.push(s.avg);
      }
      if (avgCols.length) {
        params.set("view", "chart");
        params.set("spec", JSON.stringify(buildExploreSpec(avgCols)));
      }
      location.assign(`${exploreUrl}#${params.toString()}`);
    });
  }

  // If the page opened with a shared (custom) range, reflect that state:
  // no duration button is active and Reset is available.
  if (zoomDomain) {
    btnBar.querySelectorAll("button").forEach(b => b.classList.remove("active"));
    showResetBtn();
  }

  // ── Full-screen toggle ──────────────────────────────────────────────────────
  // Inject a control into the top bar, just right of the "<- Home" link, that
  // expands the chart view to fill the page (same chart, wider/taller). It is
  // JS-injected so it never appears for no-JS visitors, who could not use the
  // interactive view anyway. The same button becomes an "x Close" affordance
  // while active; Escape also exits.
  const topBar = document.querySelector(".top-bar");
  if (topBar) {
    const fsToggle = document.createElement("button");
    fsToggle.type = "button";
    fsToggle.className = "fullscreen-toggle";

    const syncFsLabel = () => {
      const on = document.body.classList.contains("chart-fullscreen");
      fsToggle.innerHTML = on
        ? '<span class="fs-icon">\u00d7</span> Close'
        : 'Full screen <span class="fs-icon">\u2192</span>';
      fsToggle.setAttribute("aria-pressed", on ? "true" : "false");
      fsToggle.title = on ? "Exit full screen (Esc)" : "Expand chart to full screen";
    };

    const setFullscreen = (on) => {
      document.body.classList.toggle("chart-fullscreen", on);
      syncFsLabel();
      // Re-render so the chart recomputes its width/height for the new size.
      renderChart();
    };

    fsToggle.onclick = () =>
      setFullscreen(!document.body.classList.contains("chart-fullscreen"));
    syncFsLabel();

    const backLink = topBar.querySelector(".top-bar-back");
    if (backLink) backLink.insertAdjacentElement("afterend", fsToggle);
    else topBar.insertAdjacentElement("afterbegin", fsToggle);

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && document.body.classList.contains("chart-fullscreen")) {
        setFullscreen(false);
      }
    });
  }

  // Status message while loading
  container.innerHTML = '<div class="empty-state">Loading chart data…</div>';

  // ── DuckDB-WASM init ──────────────────────────────────────────────────────

  let db, conn;
  try {
    ({ db, conn } = await initDuckdb());
  } catch (e) {
    container.innerHTML = `<div class="empty-state">DuckDB-WASM failed to load: ${e.message}</div>`;
    return;
  }

  // ── Lazy file loading ─────────────────────────────────────────────────────
  //
  // Files are NOT loaded eagerly. Instead, we index the manifest by resolution
  // and only fetch + register files when they are needed for a query. A cache
  // ensures each file is fetched at most once.

  // Verify manifest has loadable files
  const fileUrls = manifest.map(m => m.file).filter(Boolean);
  if (fileUrls.length === 0) {
    container.innerHTML = '<div class="empty-state">No exported parquet files in manifest.</div>';
    return;
  }

  // Cache: file URL → DuckDB registered name (populated lazily by ensureFile)
  const { ensureFile } = createFileRegistry(db);

  // ── Resolution detection ────────────────────────────────────────────────
  //
  // Find the resolution from captures — it's the capture that starts with
  // "res=".  The index varies by glob pattern, so we detect it dynamically.

  function extractResolution(captures) {
    if (!captures) return null;
    for (const c of captures) {
      if (typeof c === "string" && c.startsWith("res=")) return c;
    }
    return null;
  }

  // Parse a duration string like "1h", "6h", "1d" to seconds.
  function parseDurationSecs(durStr) {
    const m = durStr.match(/^(\d+)([smhd])$/);
    if (!m) return Infinity;
    const n = parseInt(m[1]);
    switch (m[2]) {
      case "s": return n;
      case "m": return n * 60;
      case "h": return n * 3600;
      case "d": return n * 86400;
      default: return Infinity;
    }
  }

  // Group manifest entries by resolution.
  // Entries keep their file URL — the actual fetch happens in queryData().
  const byResolution = new Map();
  for (const m of manifest) {
    const res = extractResolution(m.captures) || "res=1h";
    if (!byResolution.has(res)) byResolution.set(res, []);
    if (!m.file) continue;
    byResolution.get(res).push({
      url: m.file,
      start_time: m.start_time || 0,
      end_time: m.end_time || 0,
    });
  }

  // Sort resolutions finest-first by parsing the duration value.
  const sortedResolutions = [...byResolution.keys()].sort((a, b) => {
    const da = parseDurationSecs(a.replace("res=", ""));
    const db = parseDurationSecs(b.replace("res=", ""));
    return da - db;
  });

  // ── Data-driven resolution picker ─────────────────────────────────────────
  //
  // For a given time window, pick the finest resolution where at most 2
  // manifest files overlap the window. This guarantees at most 2 fetches
  // per render, matching the tiling design.
  //
  // If NO resolution satisfies ≤ 2 files, fall back to the coarsest and
  // warn — this means the data needs a coarser resolution tier or wider
  // partitions.

  function countOverlapping(entries, beginMs, endMs) {
    let count = 0;
    for (const f of entries) {
      if (f.start_time === 0 || (f.start_time * 1000 <= endMs && f.end_time * 1000 >= beginMs)) {
        count++;
      }
    }
    return count;
  }

  function pickResolution(beginMs, endMs) {
    // Try finest resolution first — pick the finest with ≤ 2 overlapping files.
    for (const res of sortedResolutions) {
      const entries = byResolution.get(res) || [];
      if (countOverlapping(entries, beginMs, endMs) <= 2) return res;
    }
    // No resolution fits in 2 files — use coarsest and warn.
    const coarsest = sortedResolutions[sortedResolutions.length - 1];
    const entries = byResolution.get(coarsest) || [];
    const n = countOverlapping(entries, beginMs, endMs);
    console.warn(
      `chart.js: no resolution fits within 2 files for this view ` +
      `(coarsest ${coarsest} needs ${n} files). ` +
      `Consider adding a coarser resolution tier or wider partitions.`
    );
    return coarsest;
  }

  // ── Data query ─────────────────────────────────────────────────────────────

  // For monitoring: the time axis always extends to now.
  const nowMs = Date.now();

  // Resolve the files overlapping a window at the auto-picked resolution.
  // Shared by queryData (which fetches them) and the "Explore this data"
  // cross-link (which hands their URLs to the explorer) so both see exactly
  // the same file set for a given window.
  function overlappingEntries(beginMs, endMs) {
    const res = pickResolution(beginMs, endMs);
    const entries = byResolution.get(res) || [];
    return entries.filter(f =>
      f.start_time === 0 || (f.start_time * 1000 <= endMs && f.end_time * 1000 >= beginMs)
    );
  }

  // Query data for a time window [beginMs, endMs].
  // Resolution is chosen automatically based on window width.
  // Only the parquet files that overlap the window are fetched (lazy).
  async function queryData(beginMs, endMs) {
    // Filter to files whose time range overlaps the query window.
    const overlapping = overlappingEntries(beginMs, endMs);

    // Lazily fetch only the overlapping files (parallel).
    const loaded = await Promise.all(overlapping.map(f => ensureFile(f.url)));
    const tableNames = loaded.filter(Boolean);
    if (tableNames.length === 0) return [];

    const parts = tableNames.map(t =>
      `SELECT * FROM read_parquet('${t}') WHERE epoch_ms(timestamp) >= ${beginMs} AND epoch_ms(timestamp) <= ${endMs}`
    );
    const sql = parts.join(" UNION ALL BY NAME ") + " ORDER BY timestamp ASC";

    try {
      const result = await conn.query(sql);
      return result.toArray();
    } catch (e) {
      console.error("Query failed:", e);
      return [];
    }
  }

  // Annotation files are pond-wide interval sets (not tiled by resolution), so
  // they are registered once and queried in full for the visible window. Each
  // file is expected to carry the merged water-overlay schema (start_time,
  // end_time, pump_state, leak_score, leak_label, leak_confidence); a legacy
  // label-only file is accepted too (its `label` maps to the leak track). The
  // schema is probed once so the SELECT can substitute NULLs for absent
  // columns without erroring in DuckDB.
  const annotationUrls = annotationManifest.map(m => m.file).filter(Boolean);
  let annotationColumns = null;

  async function getAnnotationColumns(name) {
    if (annotationColumns) return annotationColumns;
    try {
      const r = await conn.query(`SELECT * FROM read_parquet('${name}') LIMIT 0`);
      annotationColumns = r.schema.fields.map(f => f.name);
    } catch (e) {
      annotationColumns = [];
    }
    return annotationColumns;
  }

  async function queryAnnotations(beginMs, endMs) {
    if (annotationUrls.length === 0) return [];
    const loaded = await Promise.all(annotationUrls.map(u => ensureFile(u)));
    const names = loaded.filter(Boolean);
    if (names.length === 0) return [];
    const cols = await getAnnotationColumns(names[0]);
    const has = c => cols.includes(c);
    const sel = [
      "epoch_ms(start_time) AS start",
      'epoch_ms(end_time) AS "end"',
      has("pump_state") ? "pump_state" : "NULL AS pump_state",
      has("leak_score") ? "CAST(leak_score AS DOUBLE) AS leak_score" : "NULL AS leak_score",
      has("leak_label")
        ? "leak_label"
        : (has("label") ? "label AS leak_label" : "NULL AS leak_label"),
      has("leak_confidence")
        ? "CAST(leak_confidence AS DOUBLE) AS leak_confidence"
        : "NULL AS leak_confidence",
    ].join(", ");
    const parts = names.map(t =>
      `SELECT ${sel} FROM read_parquet('${t}') ` +
      `WHERE epoch_ms(end_time) >= ${beginMs} AND epoch_ms(start_time) <= ${endMs}`
    );
    const sql = parts.join(" UNION ALL BY NAME ") + " ORDER BY start ASC";
    try {
      const result = await conn.query(sql);
      return result.toArray().map(r => ({
        start: Number(r.start),
        end: Number(r.end),
        pump_state: r.pump_state == null ? null : String(r.pump_state),
        leak_score: r.leak_score == null ? null : Number(r.leak_score),
        leak_label: r.leak_label == null ? null : String(r.leak_label),
        leak_confidence: r.leak_confidence == null ? null : Number(r.leak_confidence),
      }));
    } catch (e) {
      console.error("Annotation query failed:", e);
      return [];
    }
  }

  // Convert merged-overlay rows into the flat band list vega-shared draws.
  // Two tracks are layered: pump state (behind) then leak risk (on top). Each
  // band carries an identity `color` and per-datum `opacity`. Pump bands use a
  // fixed faint opacity; leak bands scale opacity with the score so confident
  // leaks read strongly while low-risk periods stay faint. Only enabled tracks
  // contribute.
  const PUMP_OPACITY = 0.12;
  const LEAK_OPACITY_FLOOR = 0.05;
  const LEAK_OPACITY_SPAN = 0.4;
  function buildAnnotationBands(rows) {
    const bands = [];
    if (enabledTracks.pump !== false) {
      for (const r of rows) {
        if (!r.pump_state) continue;
        bands.push({
          start: r.start,
          end: r.end,
          color: annotationColor(r.pump_state),
          opacity: PUMP_OPACITY,
          label: r.pump_state,
        });
      }
    }
    if (enabledTracks.leak !== false) {
      for (const r of rows) {
        if (r.leak_score == null && !r.leak_label) continue;
        const score = r.leak_score == null
          ? (r.leak_label === "leak" ? 1 : 0)
          : Math.max(0, Math.min(1, r.leak_score));
        const label = r.leak_label || (score >= 0.5 ? "leak" : "no-leak");
        const conf = r.leak_confidence == null ? 1 : Math.max(0, Math.min(1, r.leak_confidence));
        bands.push({
          start: r.start,
          end: r.end,
          color: annotationColor(label),
          opacity: LEAK_OPACITY_FLOOR + LEAK_OPACITY_SPAN * score * conf,
          label: `${label} (${Math.round(score * 100)}%)`,
        });
      }
    }
    return bands;
  }

  // ── Helpers ────────────────────────────────────────────────────────────────
  // Resolve the page theme's foreground and a faint grid color from CSS custom
  // properties so the Vega charts match the surrounding light/dark styling
  // (Vega cannot read CSS variables itself, so we pass resolved colors in).
  function resolveTheme() {
    const cs = getComputedStyle(document.body);
    const fg = (cs.getPropertyValue("--fg") || "").trim() || "#333333";
    return { fg, grid: "rgba(128,128,128,0.2)" };
  }
  const chartTheme = resolveTheme();

  function toDate(v) {
    return typeof v === "bigint" ? new Date(Number(v / 1000000n)) : new Date(v);
  }

  function legendLabel(base) {
    const parts = base.split(".");
    return parts.length >= 2 ? parts.slice(0, 2).join(" ") : base;
  }

  const PARTITION_COLS = new Set(["year", "month", "day", "hour", "minute"]);

  function groupColumns(data) {
    // Discover numeric columns across ALL rows.  UNION ALL BY NAME fills
    // missing instruments with NULL; typeof null !== "number", so checking
    // only the first row misses series whose data starts later.
    const numericCols = new Set();
    const candidates = new Set();

    for (const col of Object.keys(data[0])) {
      if (col === "timestamp" || PARTITION_COLS.has(col)) continue;
      const v = data[0][col];
      if (typeof v === "number" || typeof v === "bigint") {
        numericCols.add(col);
      } else if (v == null) {
        candidates.add(col);  // null — may be numeric in later rows
      }
    }

    // Resolve remaining candidates by scanning subsequent rows
    for (let i = 1; i < data.length && candidates.size > 0; i++) {
      for (const col of candidates) {
        const v = data[i][col];
        if (v != null) {
          if (typeof v === "number" || typeof v === "bigint") {
            numericCols.add(col);
          }
          candidates.delete(col);
        }
      }
    }

    const stats = new Map();
    for (const col of numericCols) {
      for (const suffix of [".avg", ".min", ".max"]) {
        if (col.endsWith(suffix)) {
          const base = col.slice(0, -suffix.length);
          if (!stats.has(base)) stats.set(base, {});
          stats.get(base)[suffix.slice(1)] = col;
          break;
        }
      }
      if (![".avg", ".min", ".max"].some(s => col.endsWith(s))) {
        stats.set(col, { avg: col });
      }
    }

    const charts = new Map();
    for (const [base, s] of stats) {
      const parts = base.split(".");
      const chartKey = parts.length >= 2 ? parts.slice(-2).join(".") : base;
      if (!charts.has(chartKey)) charts.set(chartKey, []);
      charts.get(chartKey).push({ base, ...s });
    }
    return charts;
  }

  const palette = [
    "#0284c7", "#059669", "#d97706", "#7c3aed",
    "#db2777", "#0891b2", "#ea580c", "#4f46e5",
  ];

  // Compute first-difference rate (units/second) per column for monotonic
  // counters.  Negative deltas are treated as counter resets and emit null.
  //
  // Tracks `prev` PER COLUMN (not per row) because the cross-pond
  // `timeseries-join` produces a FULL OUTER JOIN on `timestamp`: most
  // rows have non-null values for only one pond's columns.  A row-scoped
  // `prev` would treat almost every cell as null.  Per-column tracking
  // forward-looks across null rows so each column's rate is computed
  // from its previous non-null sample.
  //
  // Output rows preserve `data`'s timestamps; cells are null until both
  // a prior and current sample exist for that column.  The first row
  // is therefore all-null and Plot's `defined` predicate elides it.
  function computeCounterRates(data, columns) {
    const out = [];
    const prevByCol = new Map();  // col -> { tMs, value }
    for (const row of data) {
      const t = row.timestamp;
      const tMs = typeof t === "bigint" ? Number(t / 1000000n)
        : (t instanceof Date ? t.getTime() : new Date(t).getTime());
      const newRow = { timestamp: row.timestamp };
      for (const col of columns) {
        const a = row[col];
        if (a == null) {
          newRow[col] = null;
          continue;
        }
        const av = typeof a === "bigint" ? Number(a) : Number(a);
        const prev = prevByCol.get(col);
        if (prev) {
          const dtSec = (tMs - prev.tMs) / 1000;
          if (dtSec > 0) {
            const delta = av - prev.value;
            newRow[col] = delta >= 0 ? delta / dtSec : null;
          } else {
            newRow[col] = null;
          }
        } else {
          newRow[col] = null;
        }
        // Only update prev on non-null current values so null rows
        // don't truncate the lookback window.
        prevByCol.set(col, { tMs, value: av });
      }
      out.push(newRow);
    }
    return out;
  }

  // ── Brush-to-zoom ─────────────────────────────────────────────────────────

  // Attach a brush overlay to a chart wrapper.  The overlay sits on top of
  // the chart SVG and translates pixel drag → time domain → re-render.
  //
  // `domainBegin` / `domainEnd` are epoch-ms values that correspond to the
  // left/right edges of the plot area (the SVG viewBox minus margins).
  function attachBrush(wrapper, plotEl, domainBegin, domainEnd, marginLeft, plotWidth, hover) {
    const overlay = document.createElement("div");
    overlay.className = "brush-overlay";
    // Position over just the plot area (inside margins)
    overlay.style.left = marginLeft + "px";
    overlay.style.width = plotWidth + "px";
    wrapper.style.position = "relative";
    wrapper.appendChild(overlay);

    const rect = document.createElement("div");
    rect.className = "brush-rect";
    overlay.appendChild(rect);

    // ── Hover crosshair + value tooltip ──────────────────────────────────────
    // A thin vertical line tracks the cursor (snapped to the nearest sample)
    // and a small box lists each series' value (with its colour and units) at
    // that instant. Built on the same overlay so it shares the plot geometry
    // and yields to brushing while a drag is in progress.
    let crosshair = null, tooltip = null, sampleMs = null;
    if (hover && hover.rows.length) {
      crosshair = document.createElement("div");
      crosshair.className = "crosshair-line";
      crosshair.style.display = "none";
      overlay.appendChild(crosshair);

      tooltip = document.createElement("div");
      tooltip.className = "chart-tooltip";
      tooltip.style.display = "none";
      overlay.appendChild(tooltip);

      sampleMs = hover.rows.map(hover.toMs);
    }

    function nearestIdx(t) {
      let lo = 0, hi = sampleMs.length - 1;
      if (t <= sampleMs[0]) return 0;
      if (t >= sampleMs[hi]) return hi;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (sampleMs[mid] < t) lo = mid + 1; else hi = mid - 1;
      }
      // `lo` is the first sample >= t; pick whichever neighbour is closer.
      return (Math.abs(sampleMs[lo] - t) < Math.abs(t - sampleMs[lo - 1])) ? lo : lo - 1;
    }

    function showHover(offsetX, offsetY) {
      if (!crosshair) return;
      const clampedX = Math.max(0, Math.min(offsetX, plotWidth));
      const t = domainBegin + (clampedX / plotWidth) * (domainEnd - domainBegin);
      const idx = nearestIdx(t);
      const row = hover.rows[idx];
      const px = ((sampleMs[idx] - domainBegin) / (domainEnd - domainBegin)) * plotWidth;

      crosshair.style.left = px + "px";
      crosshair.style.display = "block";

      const timeEl = document.createElement("div");
      timeEl.className = "tt-time";
      timeEl.textContent = new Date(sampleMs[idx]).toLocaleString();
      tooltip.replaceChildren(timeEl);
      for (const s of hover.series) {
        const r = document.createElement("div");
        r.className = "tt-row";
        const dot = document.createElement("span");
        dot.className = "tt-dot";
        dot.style.background = s.color;
        const lab = document.createElement("span");
        lab.className = "tt-label";
        lab.textContent = s.label;
        const val = document.createElement("span");
        val.className = "tt-val";
        val.textContent = hover.fmt(row[s.col]);
        r.append(dot, lab, val);
        tooltip.appendChild(r);
      }
      tooltip.style.display = "block";

      // Prefer the right of the line; flip left if it would overflow.
      let tl = px + 14;
      if (tl + tooltip.offsetWidth > plotWidth) tl = px - tooltip.offsetWidth - 14;
      if (tl < 0) tl = 4;
      tooltip.style.left = tl + "px";
      tooltip.style.top = Math.max(0, offsetY - tooltip.offsetHeight / 2) + "px";
    }

    function hideHover() {
      if (!crosshair) return;
      crosshair.style.display = "none";
      tooltip.style.display = "none";
    }

    let startX = null;

    overlay.addEventListener("mousedown", e => {
      e.preventDefault();
      startX = e.offsetX;
      rect.style.left = startX + "px";
      rect.style.width = "0";
      rect.style.display = "block";
      hideHover();
    });

    overlay.addEventListener("mousemove", e => {
      if (startX === null) { showHover(e.offsetX, e.offsetY); return; }
      const curX = Math.max(0, Math.min(e.offsetX, plotWidth));
      const left = Math.min(startX, curX);
      const width = Math.abs(curX - startX);
      rect.style.left = left + "px";
      rect.style.width = width + "px";
    });

    const finish = e => {
      if (startX === null) return;
      const endX = Math.max(0, Math.min(e.offsetX, plotWidth));
      const left = Math.min(startX, endX);
      const right = Math.max(startX, endX);
      startX = null;
      rect.style.display = "none";

      // Ignore tiny drags (< 5 px)
      if (right - left < 5) return;

      // Map pixel range → time domain
      const t0 = domainBegin + (left / plotWidth) * (domainEnd - domainBegin);
      const t1 = domainBegin + (right / plotWidth) * (domainEnd - domainBegin);

      zoomDomain = [t0, t1];
      // Deactivate duration buttons, show reset
      btnBar.querySelectorAll("button").forEach(b => b.classList.remove("active"));
      showResetBtn();
      renderChart();
    };

    overlay.addEventListener("mouseup", finish);
    overlay.addEventListener("mouseleave", e => {
      if (startX !== null) finish(e);
      hideHover();
    });
  }

  // ── Render ─────────────────────────────────────────────────────────────────

  async function renderChart() {
    // Determine the visible x-domain first — this drives both the query
    // (resolution + time filter) and the chart axis.
    const domainEnd = zoomDomain ? zoomDomain[1] : nowMs;
    const domainBegin = zoomDomain ? zoomDomain[0] : (nowMs - activeDays * 86400000);

    // Reflect the effective window in the start/end inputs (and Copy-link
    // state) before querying, so the readout is correct even if no data
    // falls in range.
    syncWindowControls(domainBegin, domainEnd);

    const data = await queryData(domainBegin, domainEnd);

    // Interval annotations (pump state / leak risk) for the visible window,
    // shared by every chart on the page as shaded background band layers. Rows
    // are queried once, then flattened into per-track bands honoring the
    // current checkbox selection.
    const annotationRows = await queryAnnotations(domainBegin, domainEnd);
    const annotationBands = buildAnnotationBands(annotationRows);

    // Keep the raw rows for CSV export; disable the button when empty.
    lastData = data;
    downloadBtn.disabled = data.length === 0;
    if (exploreBtn) exploreBtn.disabled = data.length === 0;

    container.innerHTML = "";

    if (data.length === 0) {
      container.innerHTML = '<div class="empty-state">No data for selected time range</div>';
      return;
    }

    const charts = groupColumns(data);

    // In full-screen mode the chart fills the viewport: split the available
    // height across however many charts the page renders. Otherwise the chart
    // keeps its fixed 300px height.
    const fullscreen = document.body.classList.contains("chart-fullscreen");
    const chartHeight = fullscreen
      ? Math.max(300, Math.floor((window.innerHeight - 260) / Math.max(1, charts.size)) - 40)
      : 300;

    for (const [chartKey, series] of charts) {
      // Counter metrics (e.g. committed.txn_ids) are monotonic counts;
      // graphing the raw value yields a meaningless cumulative staircase.
      // Apply a per-scope first-difference rate transform so the y-axis
      // reflects current activity (events/second).  Updowncounters and
      // gauges plot as-is.
      const isCounter = kindFor(chartKey) === "counter";
      const keyParts = chartKey.split(".");
      const unit = keyParts[keyParts.length - 1] || "";
      // Rate/flow gauges (e.g. usage.gpm): plotted as-is like any gauge, but
      // zero is a meaningful baseline, so anchor the y-axis at 0 and keep it
      // rendered even when only one line has data in the window. Percentages
      // have the same zero-baseline requirement: a 2% budget position must not
      // look large merely because every series sits between 1.9% and 2.1%.
      const yZero = kindFor(chartKey) === "rate" || unit === "percent";
      const allCols = [];
      for (const s of series) {
        if (s.avg) allCols.push(s.avg);
        if (s.min) allCols.push(s.min);
        if (s.max) allCols.push(s.max);
      }
      const plotData = isCounter ? computeCounterRates(data, allCols) : data;

      const title = keyParts.length >= 2
        ? `${keyParts[0].charAt(0).toUpperCase() + keyParts[0].slice(1)} (${keyParts[1]})`
        : chartKey;

      const legendItems = series.map((s, i) => ({
        label: legendLabel(s.base),
        color: palette[i % palette.length],
      }));

      const header = document.createElement("div");
      header.style.cssText = "display:flex; align-items:baseline; gap:1rem; margin-bottom:0.5rem;";
      header.innerHTML =
        `<h3 style="margin:0; font-size:1.1rem; color:var(--fg)">${title}</h3>` +
        `<span style="font-size:0.8rem; color:var(--fg-muted)">${legendItems.map(l =>
          `<span style="color:${l.color}">&#9679;</span> ${l.label}`
        ).join("&emsp;")}</span>`;

      const isBytes = unit === "bytes";
      const yLabel = isCounter ? `${unit}/s` : unit;

      // Value formatter for the hover tooltip: humanize bytes, otherwise show a
      // sensible number of significant digits followed by the unit label.
      const fmtVal = (v) => {
        if (v == null) return "—";
        const n = Number(v);
        if (Number.isNaN(n)) return "—";
        if (isBytes) return isCounter ? `${formatBytes(n)}/s` : formatBytes(n);
        const a = Math.abs(n);
        let s;
        if (a !== 0 && (a < 0.001 || a >= 1e7)) {
          s = n.toExponential(2);
        } else {
          const digits = a < 10 ? 3 : (a < 1000 ? 2 : 0);
          s = n.toLocaleString(undefined, { maximumFractionDigits: digits });
        }
        return yLabel ? `${s} ${yLabel}` : s;
      };

      // Build the Vega-Lite spec for this chart and project the rows to plain
      // Vega values: timestamp -> epoch-ms number, each series column -> Number
      // or null (null cells become path breaks via the spec's `invalid: null`).
      const colorFor = (i) => palette[i % palette.length];
      const spec = buildMetricChartSpec({
        series: series.map((s, i) => ({
          avg: s.avg, min: s.min, max: s.max, color: colorFor(i),
        })),
        xDomain: [domainBegin, domainEnd],
        yLabel,
        height: chartHeight,
        byteAxis: isBytes,
        yZero,
        theme: chartTheme,
        annotations: annotationBands,
      });

      const values = plotData.map((d) => {
        const o = { timestamp: +toDate(d.timestamp) };
        for (const c of allCols) {
          const v = d[c];
          o[c] = v == null ? null : Number(v);
        }
        return o;
      });

      const plotDiv = document.createElement("div");
      plotDiv.className = "chart-plot";
      plotDiv.style.width = "100%";

      const wrapper = document.createElement("div");
      wrapper.className = "chart";
      wrapper.appendChild(header);
      wrapper.appendChild(plotDiv);

      // Caption (textContent only -- never innerHTML -- to avoid
      // injection via config-supplied strings).
      const captionText = metricCaptions[chartKey];
      if (captionText) {
        const cap = document.createElement("p");
        cap.className = "chart-caption";
        cap.textContent = captionText;
        wrapper.appendChild(cap);
      }

      container.appendChild(wrapper);

      const embed = await loadVega();
      let view = null;
      try {
        const res = await embed(
          plotDiv,
          { ...spec, data: { values } },
          { actions: false, renderer: "svg" }
        );
        view = res.view;
      } catch (e) {
        plotDiv.innerHTML =
          '<div class="empty-state">Chart render failed: ' +
          String((e && e.message) || e) + "</div>";
        continue;
      }

      // Vega's `width: "container"` reads plotDiv.clientWidth at embed time. If
      // the element is laid out at zero width (fonts/layout not yet settled),
      // every datum collapses to x=0 and the plot looks empty. Re-fit to the
      // real width once and on every resize so the line spans the plot.
      const refit = () => {
        if (!view) return;
        const w = plotDiv.clientWidth || (plotDiv.parentElement && plotDiv.parentElement.clientWidth) || 700;
        view.width(w).resize().runAsync();
      };
      requestAnimationFrame(refit);
      new ResizeObserver(refit).observe(plotDiv);

      // Attach brush-to-zoom on the rendered SVG, plus a hover crosshair that
      // reports each series' value at the pointed-to sample. The brush/hover
      // overlay is library-agnostic DOM positioned from the Vega view geometry:
      // `origin()[0]` is the data-rect left (y-axis gutter), `width()` the inner
      // plot width.
      const hoverSeries = series
        .map((s, i) => ({ col: s.avg, color: colorFor(i), label: legendLabel(s.base) }))
        .filter(h => h.col);
      const hover = {
        rows: plotData,
        toMs: (r) => +toDate(r.timestamp),
        series: hoverSeries,
        fmt: fmtVal,
      };
      attachBrush(wrapper, plotDiv, domainBegin, domainEnd, view.origin()[0], view.width(), hover);
    }
  }

  await renderChart();
})();
