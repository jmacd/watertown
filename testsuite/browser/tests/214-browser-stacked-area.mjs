// Real Parquet -> DuckDB-WASM -> stacked-area.js -> Vega-Lite integration.
import assert from "node:assert/strict";
import puppeteer from "puppeteer";
import { writeFileSync, rmSync, realpathSync } from "node:fs";
import { join } from "node:path";

const root = realpathSync(process.env.SITE_ROOT || "/tmp/test-output");
const base = process.env.BASE_URL || "http://localhost:4174";
const prefix = process.env.BASE_PATH || "/";
const name = "__stacked_area_test.html";
const path = join(root, name);
writeFileSync(path, `<!doctype html>
<html><head><meta charset="utf-8"><title>Daily counts</title></head>
<body>
<div id="stacked-area" style="width:680px">
  <script class="stacked-area-data" type="application/json">[]</script>
</div>
<script class="stacked-area-options" type="application/json">
{"x":"utc.day","y":"reported.views","color":"site.section","days":"4","order":"Blog,Monitoring"}
</script>
<script type="module">
import { initDuckdb } from "${prefix}duckdb-shared.js";
try {
  const { db, conn } = await initDuckdb();
  const today = Math.floor(Date.now() / 86400000) * 86400000;
  try {
    await conn.query(\`COPY (
      SELECT epoch_ms(\${today - 2 * 86400000}) AS "utc.day",
        'Blog' AS "site.section", 3::BIGINT AS "reported.views"
      UNION ALL SELECT epoch_ms(\${today}), 'Blog', 2
      UNION ALL SELECT epoch_ms(\${today}), 'Monitoring', 5
    ) TO 'daily.parquet' (FORMAT PARQUET)\`);
    const bytes = await db.copyFileToBuffer("daily.parquet");
    const url = URL.createObjectURL(new Blob([bytes]));
    document.querySelector(".stacked-area-data").textContent = JSON.stringify([{file: url}]);
    window.addEventListener("pagehide", () => URL.revokeObjectURL(url), {once: true});
  } finally {
    await conn.close();
    await db.terminate();
  }
  await import("${prefix}stacked-area.js");
} catch (error) {
  window.__fixtureError = String(error);
}
</script></body></html>`);

let browser;
try {
  browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  const page = await browser.newPage();
  await page.emulateTimezone("America/Los_Angeles");
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(`${base}${prefix}${name}`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() =>
    window.__fixtureError || document.querySelector("#stacked-area svg") ||
    document.getElementById("stacked-area").textContent.includes("Chart unavailable"),
  { timeout: 90000 });
  const result = await page.evaluate(() => ({
    error: window.__fixtureError,
    text: document.getElementById("stacked-area").textContent,
    areas: [...document.querySelectorAll("#stacked-area svg g.mark-area path")]
      .filter((path) => path.getBBox().height > 0).length,
  }));
  assert.equal(result.error, undefined);
  assert.deepEqual(errors, []);
  assert.ok(result.areas >= 3, JSON.stringify(result));
  assert.match(result.text, /today is partial/);
  assert.match(result.text, /UTC day/);
  assert.match(result.text, /Blog/);
  assert.match(result.text, /Monitoring/);
  console.log("PASS: real Parquet renders daily stacked areas with mapped columns in a non-UTC browser.");
} finally {
  if (browser) await browser.close();
  rmSync(path, { force: true });
}
