/**
 * Headless Vega-Lite render test for the shared `vega-shared.js` module.
 *
 * This is the de-risking step the data-explorer design (Stage 3 S3.3) calls for
 * before migrating chart.js/overlay.js to Vega-Lite: it proves the in-browser
 * Vega path actually works -- the vendored vega-embed bundle imports, a spec
 * built by `buildLineSpec` compiles, and `vegaEmbed` renders an SVG -- without
 * needing DuckDB, parquet, or a full explore site.
 *
 * The test writes a self-contained fixture page into the served site root (which
 * ships `vega-shared.js` and `vendor/vega-bundle.mjs` in every site), loads it
 * in Puppeteer, and verifies both the single-series and multi-series (folded)
 * spec paths render SVG with no JavaScript errors.
 *
 * Usage:
 *   SITE_ROOT=/tmp/test-output node tests/213-browser-vega-render.mjs
 */
import puppeteer from "puppeteer";
import { writeFileSync, rmSync, existsSync, realpathSync } from "node:fs";
import { join } from "node:path";

const BASE_URL = process.env.BASE_URL || "http://localhost:4174";
const BASE_PATH = process.env.BASE_PATH || "/";
const SITE_ROOT = realpathSync(process.env.SITE_ROOT || "/tmp/test-output");

// The fixture relies on shipped assets being present at the site root.
for (const asset of ["vega-shared.js", "vendor/vega-bundle.mjs"]) {
  if (!existsSync(join(SITE_ROOT, asset))) {
    console.error(`Missing required asset for render test: ${asset} in ${SITE_ROOT}`);
    process.exit(1);
  }
}

const FIXTURE_NAME = "__vega_render_test.html";
const FIXTURE_PATH = join(SITE_ROOT, FIXTURE_NAME);

// A self-contained page that drives the real shipped vega-shared.js through
// both the single-series and the multi-series (fold) spec paths, rendering each
// with the vendored vega-embed bundle. Results are reported on `window` so the
// Puppeteer side can assert without scraping logs.
const FIXTURE_HTML = `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>vega render test</title></head>
<body>
  <div id="single" style="width:600px"></div>
  <div id="multi" style="width:600px"></div>
  <div id="dotted" style="width:600px"></div>
  <div id="daily" style="width:600px"></div>
  <script type="module">
    import { loadVega, buildLineSpec, sanitizeRows, buildDailyStackedAreaSpec } from "${BASE_PATH}vega-shared.js";

    async function render(targetId, fields, rows) {
      const built = buildLineSpec(fields, rows, { height: 240 });
      if (built.error) throw new Error("buildLineSpec(" + targetId + "): " + built.error);
      const embed = await loadVega();
      const spec = {
        ...built.spec,
        width: "container",
        data: { values: sanitizeRows(fields, rows) },
      };
      await embed(document.getElementById(targetId), spec, {
        actions: false,
        renderer: "svg",
      });
    }

    (async () => {
      const t0 = Date.parse("2026-01-01T00:00:00Z");
      const hour = 3600 * 1000;

      // Single numeric series over a temporal x axis.
      const singleFields = ["timestamp", "value"];
      const singleRows = Array.from({ length: 6 }, (_, i) => ({
        timestamp: new Date(t0 + i * hour),
        value: Math.sin(i) + 2,
      }));
      await render("single", singleFields, singleRows);

      // Two numeric series -> exercises the fold transform + color encoding.
      const multiFields = ["timestamp", "avg", "max"];
      const multiRows = Array.from({ length: 6 }, (_, i) => ({
        timestamp: new Date(t0 + i * hour),
        avg: Math.sin(i) + 2,
        max: Math.sin(i) + 3,
      }));
      await render("multi", multiFields, multiRows);

      // Dotted column names such as the temporal-reduce do.avg / do.max
      // columns must be dot-escaped in field references, or Vega-Lite reads
      // the dot as nested-object access and the line silently renders no data.
      const dottedFields = ["timestamp", "do.avg", "do.max"];
      const dottedRows = Array.from({ length: 6 }, (_, i) => ({
        timestamp: new Date(t0 + i * hour),
        "do.avg": Math.sin(i) + 2,
        "do.max": Math.sin(i) + 3,
      }));
      await render("dotted", dottedFields, dottedRows);

      const day = 86400000;
      const daily = buildDailyStackedAreaSpec([
        { timestamp: t0, series: "Blog", value: 2 },
        { timestamp: t0, series: "Monitoring", value: 3 },
        { timestamp: t0 + day, series: "Blog", value: 4 },
        { timestamp: t0 + 3 * day, series: "Monitoring", value: 7 },
      ], { days: 4, now: t0 + 3 * day + hour, order: ["Blog", "Monitoring"] });
      const embed = await loadVega();
      const { view } = await embed(document.getElementById("daily"), daily.spec, {
        actions: false, renderer: "svg",
      });
      window.__dailyView = view;
      window.__vegaReady = true;
    })().catch((e) => {
      window.__vegaError = String((e && e.message) || e);
    });
  </script>
</body>
</html>
`;

let pass = 0;
let fail = 0;
function check(cond, msg) {
  if (cond) {
    console.log(`  [PASS] ${msg}`);
    pass++;
  } else {
    console.log(`  [FAIL] ${msg}`);
    fail++;
  }
}

writeFileSync(FIXTURE_PATH, FIXTURE_HTML);

const browser = await puppeteer.launch({
  headless: true,
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});

try {
  const url = `${BASE_URL}${BASE_PATH}${FIXTURE_NAME}`;
  console.log(`\n========== vega-shared render ==========`);
  console.log(`URL: ${url}`);

  const tab = await browser.newPage();
  const jsErrors = [];
  tab.on("pageerror", (err) => jsErrors.push(err.message));

  await tab.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

  // Wait until the module reports success or failure on `window`.
  const result = await tab
    .waitForFunction(
      () => window.__vegaReady === true || typeof window.__vegaError === "string",
      { timeout: 60000 }
    )
    .then(() => tab.evaluate(() => ({ ready: window.__vegaReady === true, error: window.__vegaError || null })))
    .catch(() => ({ ready: false, error: "timeout waiting for render" }));

  check(jsErrors.length === 0, `no JavaScript errors (got ${jsErrors.length})`);
  jsErrors.forEach((e) => console.log(`    JS error: ${e}`));

  check(!result.error, `render reported no error${result.error ? `: ${result.error}` : ""}`);
  check(result.ready, `both specs rendered to completion`);

  const singleSvg = await tab.evaluate(
    () => document.querySelectorAll("#single svg").length
  );
  check(singleSvg > 0, `single-series chart rendered SVG (${singleSvg})`);

  const multiSvg = await tab.evaluate(
    () => document.querySelectorAll("#multi svg").length
  );
  check(multiSvg > 0, `multi-series chart rendered SVG (${multiSvg})`);

  // The fold path should produce one line mark group per series; assert there
  // are stroked paths so we know data actually drew, not just an empty axis.
  const singlePaths = await tab.evaluate(
    () => document.querySelectorAll("#single svg path[stroke]").length
  );
  check(singlePaths > 0, `single-series drew a line path (${singlePaths})`);

  // The dotted-column spec must draw real line geometry: count stroked line
  // paths whose `d` carries line-to (`L`) commands. Before the dot-escape fix
  // these resolved to undefined and only the axes drew.
  const dottedPaths = await tab.evaluate(() => {
    const paths = document.querySelectorAll(
      "#dotted svg g.mark-line path, #dotted svg path[stroke]"
    );
    let withGeometry = 0;
    paths.forEach((p) => {
      if (/L/.test(p.getAttribute("d") || "")) withGeometry += 1;
    });
    return withGeometry;
  });
  check(dottedPaths > 0, `dotted-column spec drew line geometry (${dottedPaths})`);

  const daily = await tab.evaluate(() => {
    const paths = [...document.querySelectorAll("#daily svg g.mark-area path")];
    const width = window.__dailyView.width();
    const boxes = paths.map((path) => {
      const box = path.getBBox();
      return { x: box.x, end: box.x + box.width, height: box.height };
    });
    const stacked = [];
    function visit(item) {
      if (item.datum?.series && Number.isFinite(item.datum.upper)) stacked.push(item.datum);
      for (const child of item.items || []) visit(child);
    }
    visit(window.__dailyView.scenegraph().root);
    return {
      boxes, width,
      axis: document.querySelector("#daily svg").textContent.includes("UTC day"),
      totals: ["2026-01-01", "2026-01-02", "2026-01-04"].map((day) =>
        Math.max(...stacked.filter((row) => row.day === day).map((row) => row.upper))),
    };
  });
  check(daily.axis, "stacked area labels the UTC axis");
  check(JSON.stringify(daily.totals) === "[5,4,7]", `Vega stacks actual daily totals (${daily.totals})`);
  check(daily.boxes.filter((box) => box.height > 0).length >= 3, "stacked area draws both runs, including an isolated day");
  check(daily.boxes.every((box) => box.end <= daily.width / 2 + 0.1 || box.x >= daily.width * 3 / 4 - 0.1),
    "no area polygon bridges the missing UTC day");

  await tab.close();
} finally {
  await browser.close();
  try {
    rmSync(FIXTURE_PATH, { force: true });
  } catch {
    // Best-effort cleanup of the fixture file.
  }
}

console.log(`\n========== Summary ==========`);
console.log(`${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
