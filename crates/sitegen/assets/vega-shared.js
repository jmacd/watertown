// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// Watertown sitegen -- vega-shared.js
//
// Shared Vega-Lite rendering helpers used by the explorer (explore.js) and,
// in a later stage, the chart pages (chart.js). Centralizing the lazy vendor
// import, encoding inference, spec construction, and row sanitization keeps the
// two render paths from diverging.

let vegaEmbedFn = null;

// Escape a data column name for use as a Vega-Lite field reference. Vega-Lite
// interprets an unescaped `.` as nested-object access, so a flat column whose
// name contains a dot -- e.g. the temporal-reduce `do.avg` / `committed.txn_ids.max`
// columns -- must have its dots backslash-escaped or the encoding silently
// resolves to undefined and the mark renders nothing. Brackets are escaped for
// the same reason. Always pass column names through this before placing them in
// a spec's `field`/`fold`; keep the raw name for human-facing `title` text.
export function escapeField(name) {
  return String(name).replace(/([.[\]])/g, "\\$1");
}

// Lazily import the vendored vega-embed bundle, resolved relative to this
// module so any asset that imports it picks up the same vendor copy. The bundle
// is large, so callers should invoke this only when a chart is first shown.
export async function loadVega() {
  if (vegaEmbedFn) return vegaEmbedFn;
  const mod = await import(
    /* @vite-ignore */ new URL("./vendor/vega-bundle.mjs", import.meta.url).href
  );
  vegaEmbedFn = mod.vegaEmbed;
  return vegaEmbedFn;
}

// Detect whether a column is temporal / quantitative / nominal by sampling its
// first non-null value across the materialized rows.
export function detectKind(field, rows) {
  for (const r of rows) {
    const v = r[field];
    if (v == null) continue;
    if (v instanceof Date) return "temporal";
    if (typeof v === "number" || typeof v === "bigint") return "quantitative";
    return "nominal";
  }
  return "nominal";
}

// Choose an x column (first temporal, else first column) and the numeric y
// columns (all quantitative columns except x).
export function inferEncoding(fields, rows) {
  const kinds = {};
  for (const f of fields) kinds[f] = detectKind(f, rows);
  let xCol = fields.find((f) => kinds[f] === "temporal");
  if (!xCol) xCol = fields[0];
  const yCols = fields.filter((f) => f !== xCol && kinds[f] === "quantitative");
  return { xCol, xKind: kinds[xCol], yCols };
}

// Build a data-less Vega-Lite line-chart spec from result fields/rows. The data
// is injected by the caller at render time so the same spec can be reused as the
// query result changes and edited independently of the data. Returns
// `{ spec }` on success or `{ error }` when there is nothing chartable.
export function buildLineSpec(fields, rows, opts = {}) {
  if (!rows || !rows.length || !fields || !fields.length) {
    return { error: "Run a query to chart its result." };
  }
  const { xCol, xKind, yCols } = inferEncoding(fields, rows);
  if (!yCols.length) {
    return {
      error:
        "No numeric column to plot. The chart view needs at least one numeric column.",
    };
  }
  const multi = yCols.length > 1;
  const spec = {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    width: "container",
    height: opts.height || 340,
    // Match the gauge default: fit the y extent rather than forcing a zero
    // baseline, which is meaningless for arbitrary-scale sensor readings.
    config: { scale: { zero: false } },
    transform: multi ? [{ fold: yCols.map(escapeField), as: ["series", "value"] }] : [],
    mark: { type: "line", clip: true, tooltip: true },
    encoding: {
      x: {
        field: escapeField(xCol),
        type: xKind === "temporal" ? "temporal" : "quantitative",
        title: xCol,
      },
      y: multi
        ? { field: "value", type: "quantitative" }
        : { field: escapeField(yCols[0]), type: "quantitative", title: yCols[0] },
      ...(multi ? { color: { field: "series", type: "nominal", title: null } } : {}),
    },
    // Interval selection bound to scales gives pan + zoom out of the box --
    // brush-zoom parity with the Observable Plot chart path.
    params: [{ name: "grid", select: "interval", bind: "scales" }],
  };
  return { spec };
}

// Build a data-less multi-series metric spec for the chart pages: each series
// draws an optional avg/min/max area band plus an avg line in a fixed palette
// color, sharing one y scale. `xDomain` pins the temporal axis to the queried
// window (epoch-ms numbers); `invalid: null` keeps null cells as path breaks so
// gaps in the data show as gaps (matching the Observable Plot `defined` path).
// Byte axes use SI-suffixed tick labels (`~s`); the exact KiB/MiB formatting is
// applied by the DOM hover tooltip, not the axis.
export function buildMetricChartSpec(opts) {
  const {
    series,
    xField = "timestamp",
    xDomain,
    height = 300,
    byteAxis = false,
    yZero = false,
    theme,
    annotations = [],
  } = opts;
  const yAxis = { grid: true, title: null };
  const xAxis = { grid: true, title: "Date" };
  if (byteAxis) yAxis.format = "~s";
  // For rate/flow metrics (`yZero`), zero is a meaningful baseline, so anchor
  // the y scale at 0 for every series mapped to it.  Other charts plot
  // arbitrary-scale readings and keep the fitted-extent default (zero: false).
  const yScale = yZero ? { zero: true } : null;
  const layers = [];

  // For rate charts, pin 0 into the shared domain even when no series contains
  // a zero-valued point. All layers declare the same axis so Vega-Lite merges
  // them into one conventional numeric axis.
  if (yZero) {
    layers.push({
      data: { values: [{ __axis0: 0 }] },
      mark: { type: "rule", opacity: 0, clip: true },
      encoding: {
        y: { field: "__axis0", type: "quantitative", scale: yScale, axis: yAxis },
      },
    });
  }

  // Optional annotation bands: a secondary interval dataset (e.g. pump state /
  // leak risk periods) drawn as full-height shaded rects behind the series.
  // Rows carry epoch-ms `start`/`end`, a literal `color` (identity scale), a
  // per-datum `opacity` (identity, so pump categorical bands and leak
  // score-scaled bands can coexist), and a `label` for the tooltip. Pushed
  // first so the series draw on top.
  if (annotations && annotations.length) {
    layers.push({
      data: { values: annotations },
      mark: { type: "rect", clip: true, tooltip: true },
      encoding: {
        x: { field: "start", type: "temporal", scale: { domain: xDomain, nice: false }, axis: xAxis },
        x2: { field: "end" },
        color: { field: "color", type: "nominal", scale: null, legend: null },
        opacity: { field: "opacity", type: "quantitative", scale: null, legend: null },
        tooltip: [{ field: "label", type: "nominal", title: "Period" }],
      },
    });
  }

  for (const s of series) {
    if (s.min && s.max) {
      layers.push({
        mark: { type: "area", color: s.color, opacity: 0.15, clip: true, invalid: null },
        encoding: {
          y: { field: escapeField(s.min), type: "quantitative", axis: yAxis, ...(yScale ? { scale: yScale } : {}) },
          y2: { field: escapeField(s.max) },
        },
      });
    }
    if (s.avg) {
      layers.push({
        mark: { type: "line", color: s.color, strokeWidth: 1.5, clip: true, invalid: null },
        encoding: {
          y: { field: escapeField(s.avg), type: "quantitative", axis: yAxis, ...(yScale ? { scale: yScale } : {}) },
        },
      });
    }
  }
  return {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    width: "container",
    height,
    config: themeConfig(theme),
    encoding: {
      x: {
        field: xField,
        type: "temporal",
        title: "Date",
        // `nice: false` keeps the rendered scale pinned to the exact
        // `xDomain` edges. Vega's default temporal "nice" rounds the domain
        // out to a tidy time unit (e.g. the nearest hour), which would leave
        // the axis (and anything computed from the queried domain) subtly
        // offset from what's actually plotted.
        scale: { domain: xDomain, nice: false },
        axis: xAxis,
      },
    },
    layer: layers,
  };
}

// Build a data-less overview-timeline spec carrying an `interval` selection on
// the x (time) axis, used by the overlay analysis page to pick a date range.
// Each row draws a vertical stem (`yTopField` -> `yField`, identity color) plus
// a filled dot at `yTopField`; the selection's extent drives the analysis
// charts. `yDomain` pins the depth axis. Callers read the `brush` signal off the
// returned view on pointer release (the selection is otherwise continuous).
export function buildBrushOverviewSpec(opts) {
  const {
    xField = "timestamp",
    yField = "min_depth",
    yTopField = "y_top",
    colorField = "color",
    yTitle = "Depth (m)",
    yDomain,
    height = 160,
    theme,
  } = opts;
  return {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    width: "container",
    height,
    config: themeConfig(theme),
    encoding: {
      x: { field: xField, type: "temporal", title: null, axis: { grid: false } },
    },
    layer: [
      {
        // The interval selection lives on a single layer unit; placing it at the
        // top of a layered spec makes Vega-Lite emit duplicate `brush_x` signals.
        params: [{ name: "brush", select: { type: "interval", encodings: ["x"] } }],
        mark: { type: "rule", strokeWidth: 1, opacity: 0.4 },
        encoding: {
          y: {
            field: yTopField,
            type: "quantitative",
            title: yTitle,
            axis: { grid: true },
            ...domainScale(yDomain),
          },
          y2: { field: yField },
          color: { field: colorField, type: "nominal", scale: null, legend: null },
        },
      },
      {
        transform: [{ filter: "isValid(datum.static_depth)" }],
        mark: { type: "point", filled: true, size: 8, opacity: 0.7 },
        encoding: {
          y: { field: "static_depth", type: "quantitative" },
          color: { field: colorField, type: "nominal", scale: null, legend: null },
        },
      },
    ],
  };
}

// Coerce rows to Vega-friendly plain objects: BigInt -> Number, keep Date. All
// fields are included so a hand-edited spec can reference any result column.
export function sanitizeRows(fields, rows) {
  return rows.map((r) => {
    const o = {};
    for (const c of fields) {
      const v = r[c];
      o[c] = typeof v === "bigint" ? Number(v) : v;
    }
    return o;
  });
}

// Common chart config that keeps the SVG transparent and pulls axis/grid colors
// from the surrounding theme, so a migrated chart matches the page's light/dark
// styling (the Observable Plot path read `var(--fg)`; Vega cannot resolve CSS
// variables itself, so callers resolve them and pass the colors in via `theme`).
function themeConfig(theme) {
  const fg = (theme && theme.fg) || "#333333";
  const grid = (theme && theme.grid) || "rgba(128,128,128,0.2)";
  return {
    background: "transparent",
    view: { stroke: null },
    // These charts plot arbitrary-scale sensor readings (depths, levels,
    // pressures), not ratios, so a zero baseline is meaningless and just
    // squashes the variation. Default every scale to fit its data extent; an
    // individual encoding can still opt back in with `scale: { zero: true }`.
    scale: { zero: false },
    axis: {
      labelColor: fg,
      titleColor: fg,
      domainColor: fg,
      tickColor: fg,
      gridColor: grid,
    },
  };
}

// Optional scale clause from an explicit [min, max] domain; returns `{}` when no
// domain is given so the spec relies on Vega's automatic extent.
function domainScale(domain) {
  return domain ? { scale: { domain } } : {};
}

// Build a data-less multi-line spec where each line is one group (`detailField`,
// e.g. a pump-event id) drawn in a literal per-row color (`colorField` holds CSS
// color strings; `scale: null` makes Vega use them verbatim, matching Observable
// Plot's `color: { type: "identity" }`). Used by the overlay analysis charts.
export function buildMultiLineSpec(opts) {
  const {
    xField,
    yField,
    detailField = "event",
    colorField = "color",
    xTitle,
    yTitle,
    xDomain,
    yDomain,
    height = 360,
    strokeWidth = 0.8,
    strokeOpacity = 0.4,
    theme,
  } = opts;
  return {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    width: "container",
    height,
    config: themeConfig(theme),
    mark: { type: "line", strokeWidth, opacity: strokeOpacity, clip: true },
    encoding: {
      x: {
        field: xField,
        type: "quantitative",
        title: xTitle,
        axis: { grid: true },
        ...domainScale(xDomain),
      },
      y: {
        field: yField,
        type: "quantitative",
        title: yTitle,
        axis: { grid: true },
        ...domainScale(yDomain),
      },
      detail: { field: detailField, type: "nominal" },
      color: { field: colorField, type: "nominal", scale: null, legend: null },
    },
  };
}

// Build a data-less layered spec: a faint connecting line plus filled points
// colored per-row (`colorField`, identity colors as above) over a temporal x
// axis. Mirrors the overlay summary charts (duty cycle, max drawdown, duration).
export function buildDotLineSpec(opts) {
  const {
    xField = "timestamp",
    yField,
    xTitle = "Date",
    yTitle,
    yDomain,
    height = 300,
    lineColor = "#6b7280",
    colorField = "color",
    pointSize = 50,
    theme,
  } = opts;
  const yEnc = {
    field: yField,
    type: "quantitative",
    title: yTitle,
    axis: { grid: true },
    ...domainScale(yDomain),
  };
  return {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    width: "container",
    height,
    config: themeConfig(theme),
    encoding: {
      x: { field: xField, type: "temporal", title: xTitle, axis: { grid: true } },
    },
    layer: [
      {
        mark: { type: "line", color: lineColor, strokeWidth: 1, opacity: 0.5 },
        encoding: { y: yEnc },
      },
      {
        mark: { type: "point", filled: true, size: pointSize },
        encoding: {
          y: { field: yField, type: "quantitative" },
          color: { field: colorField, type: "nominal", scale: null, legend: null },
        },
      },
    ],
  };
}

// -- Perceptual month colors (OKLCH) ----------------------------------------
// Colors are generated in OKLCH (perceptually uniform) rather than hand-picked:
// each month gets an evenly-spaced hue at fixed chroma, and a month's years map
// to a uniform LIGHTNESS ramp (darker = older, lighter = newer). Everything is
// gamut-mapped to an sRGB #rrggbb string so Vega/d3-color render it reliably
// (they don't parse oklch()).

function oklchToLinearRgb(L, C, H) {
  const h = (H * Math.PI) / 180;
  const a = C * Math.cos(h);
  const b = C * Math.sin(h);
  const l_ = L + 0.3963377774 * a + 0.2158037573 * b;
  const m_ = L - 0.1055613458 * a - 0.0638541728 * b;
  const s_ = L - 0.0894841775 * a - 1.291485548 * b;
  const l = l_ * l_ * l_;
  const m = m_ * m_ * m_;
  const s = s_ * s_ * s_;
  return [
    4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s,
    -1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s,
    -0.0041960863 * l - 0.7034186147 * m + 1.707614701 * s,
  ];
}

function linearToHex(rgb) {
  const gamma = function (c) {
    const v = c <= 0.0031308 ? 12.92 * c : 1.055 * Math.pow(c, 1 / 2.4) - 0.055;
    return Math.round(Math.min(1, Math.max(0, v)) * 255);
  };
  const hx = function (n) { return n.toString(16).padStart(2, "0"); };
  return "#" + hx(gamma(rgb[0])) + hx(gamma(rgb[1])) + hx(gamma(rgb[2]));
}

// OKLCH -> sRGB hex, reducing chroma until the color is inside the sRGB gamut so
// the requested hue/lightness is preserved (rather than hard-clipping channels).
export function oklchHex(L, C, H) {
  let c = C;
  let rgb = oklchToLinearRgb(L, c, H);
  const inGamut = function (v) { return v.every(function (x) { return x >= -1e-4 && x <= 1 + 1e-4; }); };
  while (!inGamut(rgb) && c > 0) {
    c -= 0.005;
    rgb = oklchToLinearRgb(L, c, H);
  }
  return linearToHex(rgb);
}

const MONTH_HUE0 = 25;   // Jan hue; months step by 30 deg around the wheel
const MONTH_CHROMA = 0.13;

// Color for month `monthIndex` (0 = Jan) at perceptual lightness `L`.
export function monthColorAt(monthIndex, L) {
  return oklchHex(L, MONTH_CHROMA, (monthIndex * 30 + MONTH_HUE0) % 360);
}

// Per-year shade of a month hue. `t` in [0,1]: 0 = oldest (darker), 1 = newest
// (lighter). Range kept off the extremes so old lines aren't muddy and new lines
// don't wash out on a light background.
export function monthShade(monthIndex, t) {
  return monthColorAt(monthIndex, 0.45 + t * 0.33);
}

// Representative color per month-of-year (index 0 = Jan) for the legend + the
// all-months overview, at a mid lightness.
export const MONTH_COLORS = Array.from({ length: 12 }, function (_, i) {
  return monthColorAt(i, 0.62);
});

// Build a data-less "median + P10-P90 band" spec, one line per calendar month.
// Lines are colored by MONTH-OF-YEAR (12 distinct colors reused across years) so
// the same month in different years shares a hue and seasonality reads at a
// glance; each individual year-month is still its own line (detail = 'YYYY-MM').
// Selecting a month-of-year in the legend fades the other months right down and,
// for the selected month, redraws each year in a light->dark shade of the month
// hue (`shadeField`, precomputed per row) with a year label at each line's end,
// so the individual years separate out. Hovering snaps to the nearest (selected)
// point and shows a tooltip with its exact year-month.
export function buildBandSpec(opts) {
  const {
    xField,
    xTitle,
    xScaleType = "linear",
    loField = "s_p10",
    midField = "s_p50",
    hiField = "s_p90",
    monthField = "month",
    shadeField = "shade",
    yearField = "year",
    yTitle,
    yDomain,
    height = 380,
    theme,
    initMonth = null,
  } = opts;
  const sel = "monthSel";
  const moy = "moy";
  // Legend-bound point selection; optionally pre-selected from a shared link so
  // "Copy link" round-trips the chosen month-of-year.
  const selParam = { name: sel, select: { type: "point", fields: [moy] }, bind: "legend" };
  if (initMonth) selParam.value = [{ [moy]: initMonth }];
  const monthNames =
    "['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']" +
    "[toNumber(datum.value) - 1]";
  const tooltip = [
    { field: monthField, title: "Year-month" },
    { field: xField, type: "quantitative", title: xTitle, format: ".3~f" },
    { field: midField, type: "quantitative", title: yTitle, format: ".3~f" },
  ];
  return {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    width: "container",
    height,
    config: themeConfig(theme),
    // Derive month-of-year ('01'..'12') from the 'YYYY-MM' label for coloring.
    transform: [{ calculate: "substring(datum." + monthField + ", 5, 7)", as: moy }],
    encoding: {
      x: {
        field: xField,
        type: "quantitative",
        title: xTitle,
        scale: { type: xScaleType, zero: false },
        axis: { grid: true },
      },
      color: {
        field: moy,
        type: "ordinal",
        title: "Month",
        // Explicit month-number -> color so each month has ONE stable, distinct
        // color across years and across both charts. Domain is pinned so a
        // missing month never shifts the mapping.
        scale: {
          domain: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
          range: MONTH_COLORS,
        },
        legend: {
          labelExpr: monthNames,
          symbolLimit: 12,
          clipHeight: 22,
          symbolType: "square",
          symbolSize: 320,
          symbolStrokeWidth: 0,
          labelFontSize: 15,
          titleFontSize: 16,
          rowPadding: 5,
        },
      },
    },
    layer: [
      {
        // P10-P90 band: hidden until a month-of-year is selected in the legend.
        // The legend-bound selection param is declared here (in a single unit
        // layer) rather than at the top level: a top-level param in a layered
        // spec is pushed into every layer and produces "Duplicate signal name".
        params: [selParam],
        mark: { type: "area", clip: true },
        encoding: {
          y: {
            field: loField,
            type: "quantitative",
            title: yTitle,
            axis: { grid: true },
            ...domainScale(yDomain),
          },
          y2: { field: hiField },
          detail: { field: monthField, type: "ordinal" },
          opacity: {
            condition: { param: sel, empty: false, value: 0.2 },
            value: 0,
          },
        },
      },
      {
        // Overview median line per year-month: colored by month hue, all bright
        // when nothing is selected; a selection fades every line far down (the
        // selected month is redrawn crisply by the emphasis layer below).
        mark: { type: "line", clip: true, strokeWidth: 1.3 },
        // Overview median line per year-month: colored by month hue and all
        // bright when nothing is selected. Once ANY month is selected every
        // overview line is hidden (test on the selection store, not a per-datum
        // match) so only the emphasis layer's selected-month years remain -- a
        // per-datum condition would leave the other months at full opacity.
        mark: { type: "line", clip: true, strokeWidth: 1.3 },
        encoding: {
          y: { field: midField, type: "quantitative", ...domainScale(yDomain) },
          detail: { field: monthField, type: "ordinal" },
          opacity: {
            condition: { test: "length(data('" + sel + "_store')) === 0", value: 1 },
            value: 0,
          },
        },
      },
      {
        // Emphasis: only the SELECTED month's lines, one per year in a light->
        // dark shade of the month hue (precomputed `shadeField`) so the years
        // separate out. Empty selection matches nothing, so this is invisible in
        // the overview.
        transform: [{ filter: { param: sel, empty: false } }],
        mark: { type: "line", clip: true, strokeWidth: 2.5 },
        encoding: {
          y: { field: midField, type: "quantitative", ...domainScale(yDomain) },
          detail: { field: monthField, type: "ordinal" },
          color: { field: shadeField, type: "nominal", scale: null, legend: null },
        },
      },
      {
        // Year label at the end (max x) of each selected line.
        transform: [
          { filter: { param: sel, empty: false } },
          { joinaggregate: [{ op: "max", field: xField, as: "_maxx" }], groupby: [monthField] },
          { filter: "datum." + xField + " === datum._maxx" },
        ],
        mark: { type: "text", align: "left", dx: 5, dy: 0, fontSize: 10 },
        encoding: {
          y: { field: midField, type: "quantitative", ...domainScale(yDomain) },
          text: { field: yearField, type: "nominal" },
          color: { field: shadeField, type: "nominal", scale: null, legend: null },
        },
      },
      {
        // Invisible points that back a nearest-point hover: snaps to the closest
        // year-month/x and shows its exact label + values in a tooltip. Filtered
        // by the legend selection so a selected month hides other months from the
        // tooltip too (empty selection includes all, so hover works with none
        // selected).
        transform: [{ filter: { param: sel } }],
        params: [
          {
            name: "hover",
            select: {
              type: "point",
              on: "pointerover",
              nearest: true,
              clear: "pointerout",
            },
          },
        ],
        mark: { type: "point", filled: true, size: 55 },
        encoding: {
          y: { field: midField, type: "quantitative", ...domainScale(yDomain) },
          opacity: {
            condition: { param: "hover", empty: false, value: 1 },
            value: 0,
          },
          tooltip,
        },
      },
    ],
  };
}
