/**
 * Sitegen browser validation — Puppeteer tests for rendered site.
 *
 * Checks that every page loads, styles are applied, sidebar links work,
 * charts are present on data pages, and no JS errors occur.
 *
 * Supports both root and subdir deployments via BASE_PATH:
 *   BASE_PATH="/"       → links are /params/Temperature.html
 *   BASE_PATH="/myapp/" → links are /myapp/params/Temperature.html
 *
 * Usage:
 *   BASE_URL=http://localhost:4174 BASE_PATH="/" SITE_ROOT=/tmp/test-output node validate.mjs
 *   BASE_URL=http://localhost:4174 BASE_PATH="/myapp/" SITE_ROOT=/tmp/test-output-subdir node validate.mjs
 */
import puppeteer from "puppeteer";
import { readdirSync, statSync, existsSync, realpathSync } from "node:fs";
import { join, relative } from "node:path";

const BASE_URL = process.env.BASE_URL || "http://localhost:4174";
const BASE_PATH = process.env.BASE_PATH || "/";
const SITE_ROOT = realpathSync(process.env.SITE_ROOT || "/tmp/test-output");

/**
 * Discover pages from the generated site directory.
 * index.html is the home page, everything else is a data page (has chart.js).
 */
function discoverPages(siteRoot) {
  const pages = [];
  function walk(dir) {
    for (const entry of readdirSync(dir)) {
      const full = join(dir, entry);
      if (statSync(full).isDirectory()) {
        // Skip Vite build artifacts and data directories
        if (entry === "node_modules" || entry === ".vite" || entry === "data" || entry === "assets") continue;
        walk(full);
      } else if (entry.endsWith(".html")) {
        const rel = relative(siteRoot, full);
        const isIndex = rel === "index.html";
        pages.push({
          path: rel,
          title: isIndex ? "" : entry.replace(".html", ""),
          isIndex,
        });
      }
    }
  }
  walk(siteRoot);
  return pages;
}

const PAGES = discoverPages(SITE_ROOT);
console.log(`Discovered ${PAGES.length} pages in ${SITE_ROOT}:`);
PAGES.forEach(p => console.log(`  ${p.path} (${p.layout})`));

let pass = 0;
let fail = 0;

function ok(msg) {
  console.log(`  ✅ ${msg}`);
  pass++;
}
function nok(msg) {
  console.log(`  ❌ ${msg}`);
  fail++;
}
function check(cond, msg) {
  cond ? ok(msg) : nok(msg);
}

// ─── Per-page checks ────────────────────────────────────────

async function testPage(browser, page) {
  const url = `${BASE_URL}${BASE_PATH}${page.path}`;
  const errors = [];
  const diagnostics = [];
  const tab = await browser.newPage();

  // Collect JS errors and non-trivial HTTP failures
  tab.on("pageerror", (err) => errors.push(err.message));
  tab.on("console", (message) => {
    if (["error", "warning"].includes(message.type())) {
      diagnostics.push(`${message.type()}: ${message.text()}`);
    }
  });
  tab.on("requestfailed", (request) => {
    diagnostics.push(
      `request failed: ${request.failure()?.errorText || "unknown error"}: ${request.url()}`
    );
  });
  tab.on("response", (res) => {
    const u = res.url();
    if (
      res.status() >= 400 &&
      !u.includes("favicon") &&
      !u.includes("cdn.jsdelivr")
    ) {
      errors.push(`HTTP ${res.status()}: ${u}`);
    }
  });

  try {
    await tab.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });
    await new Promise((r) => setTimeout(r, 1000));

    console.log(`\n=== ${page.path} ===`);

    // Non-data pages: check JS errors immediately (data pages check after chart loads)
    // Detect data pages by presence of chart.js script tag
    const isDataPage = await tab.evaluate(
      () => document.querySelector('script[src*="chart.js"]') !== null
    );
    if (!isDataPage) {
      check(errors.length === 0, `no JS errors (got ${errors.length})`);
      errors.forEach((e) => console.log(`    ERROR: ${e}`));
    }

    // Version marker (meta generator tag)
    const generator = await tab.evaluate(
      () => document.querySelector('meta[name="generator"]')?.content || ""
    );
    check(
      generator.startsWith("Watertown v"),
      `version marker present ("${generator}")`
    );

    // Title present
    const title = await tab.title();
    check(
      title.includes(page.title),
      `title contains "${page.title}" (got "${title}")`
    );

    // Layout check: data pages include chart.js, default pages do not
    check(
      page.isIndex ? !isDataPage : isDataPage,
      page.isIndex ? "default layout (no chart.js)" : "data layout (has chart.js)"
    );

    // Sidebar exists
    const hasSidebar = await tab.evaluate(
      () => document.querySelector("nav.sidebar") !== null
    );
    check(hasSidebar, "sidebar present");

    // Sidebar has navigation links
    const navLinks = await tab.evaluate(
      () => document.querySelectorAll("nav.sidebar .nav-list a").length
    );
    check(navLinks >= 2, `sidebar has nav links (${navLinks})`);

    // Navigation links have correct base path prefix
    const linkHrefs = await tab.evaluate(
      () => [...document.querySelectorAll("nav.sidebar .nav-list a")].map(a => a.getAttribute("href"))
    );
    const allPrefixed = linkHrefs.every(h => h.startsWith(BASE_PATH));
    check(allPrefixed, `all nav links start with "${BASE_PATH}" (${linkHrefs.length} links)`);
    if (!allPrefixed) {
      linkHrefs.filter(h => !h.startsWith(BASE_PATH)).forEach(h => console.log(`    BAD LINK: ${h}`));
    }

    // Styles loaded (sidebar has non-zero width)
    const sidebarWidth = await tab.evaluate(() => {
      const el = document.querySelector("nav.sidebar");
      return el ? el.getBoundingClientRect().width : 0;
    });
    check(
      sidebarWidth > 100,
      `styles applied — sidebar width ${sidebarWidth}px`
    );

    // Main content not empty
    const mainText = await tab.evaluate(() => {
      const el = document.querySelector("main");
      return el ? el.textContent.trim().length : 0;
    });
    check(mainText > 10, `main content not empty (${mainText} chars)`);

    // Data pages: chart container and manifest
    if (isDataPage) {
      const hasChart = await tab.evaluate(
        () => document.querySelector(".chart-container") !== null
      );
      check(hasChart, "chart container present");

      // Note: chart.js reads the <script class="chart-data"> manifest then
      // replaces container.innerHTML, destroying the script tag. So we can't
      // query for it after page load. Instead we verify chart.js successfully
      // consumed the manifest by checking its rendered output below.

      // Verify no parquet file fetch failures (HTTP 4xx/5xx on /data/ URLs)
      const dataErrors = errors.filter(e => e.includes("/data/") && e.startsWith("HTTP"));
      check(dataErrors.length === 0, `no parquet fetch errors (got ${dataErrors.length})`);
      dataErrors.forEach(e => console.log(`    DATA ERROR: ${e}`));

      // Duration buttons rendered by chart.js
      const btnCount = await tab.evaluate(
        () => document.querySelectorAll(".duration-buttons button").length
      );
      check(btnCount >= 5, `duration buttons present (${btnCount})`);

      // Wait for DuckDB-WASM to load and chart to render (up to 30s)
      const chartRendered = await tab.evaluate(() => {
        return new Promise((resolve) => {
          let tries = 0;
          const check = () => {
            // Vega-Lite renders SVG elements inside the chart container
            const svgs = document.querySelectorAll(".chart-container svg");
            if (svgs.length > 0) return resolve(true);
            if (++tries > 60) return resolve(false);
            setTimeout(check, 500);
          };
          check();
        });
      });
      check(chartRendered, "chart rendered SVG (DuckDB-WASM + Vega-Lite)");

      if (!chartRendered) {
        const chartState = await tab.evaluate(() => {
          const container = document.querySelector(".chart-container");
          const emptyState = container?.querySelector(".empty-state");
          return {
            text: container?.textContent?.trim() || "",
            emptyState: emptyState?.textContent?.trim() || "",
          };
        });
        console.log(`    chart state: ${chartState.text || "(empty)"}`);
        if (chartState.emptyState) {
          console.log(`    empty state: ${chartState.emptyState}`);
        }
        diagnostics.forEach((message) => console.log(`    DIAGNOSTIC: ${message}`));
      }

      if (chartRendered) {
        // Check SVG has actual plot content (paths/lines, not just empty axes)
        const plotMarks = await tab.evaluate(() => {
          const paths = document.querySelectorAll(
            ".chart-container svg path, .chart-container svg line, .chart-container svg circle"
          );
          return paths.length;
        });
        check(plotMarks > 0, `chart has plot marks (${plotMarks} path/line/circle elements)`);

        // Vega's SVG renderer roots the chart at <svg class="marks">; assert it
        // to lock in the all-in Vega-Lite migration (no Observable Plot).
        const vegaRendered = await tab.evaluate(
          () => document.querySelectorAll(".chart-container svg.marks").length
        );
        check(vegaRendered > 0, `chart rendered via Vega (${vegaRendered} svg.marks)`);

        // Assert the avg line marks carry real geometry, not just empty axes.
        // Vega groups data marks under `g.mark-line`/`g.mark-area` (axes use
        // `role-axis`), and a multi-point line path's `d` contains line-to (`L`)
        // commands. A regression where field references were not dot-escaped (so
        // dotted columns like `do.avg` resolved to undefined) rendered axes but
        // empty data paths -- this assertion catches that.
        const dataMarks = await tab.evaluate(() => {
          const paths = document.querySelectorAll(
            ".chart-container svg g.mark-line path, .chart-container svg g.mark-area path"
          );
          let withGeometry = 0;
          paths.forEach((p) => {
            const d = p.getAttribute("d") || "";
            if (/L/.test(d)) withGeometry += 1;
          });
          return { total: paths.length, withGeometry };
        });
        check(
          dataMarks.withGeometry > 0,
          `chart has data line/area marks with geometry ` +
            `(${dataMarks.withGeometry}/${dataMarks.total} mark-line/mark-area paths)`
        );

        // Check for multiple chart sections (min/max/avg grouping)
        const chartDivs = await tab.evaluate(
          () => document.querySelectorAll(".chart-container .chart").length
        );
        check(chartDivs >= 1, `chart sections rendered (${chartDivs})`);
      }

      // Verify no errors accumulated during chart loading
      check(
        errors.length === 0,
        `no errors after chart load (got ${errors.length})`
      );
      errors.forEach((e) => console.log(`    ERROR: ${e}`));
    }
  } catch (err) {
    console.log(`\n=== ${page.path} ===`);
    nok(`page load failed: ${err.message}`);
  }

  await tab.close();
}

// ─── Navigation test ────────────────────────────────────────

async function testNavigation(browser) {
  console.log("\n=== Navigation ===");
  const tab = await browser.newPage();

  await tab.goto(`${BASE_URL}${BASE_PATH}index.html`, {
    waitUntil: "domcontentloaded",
    timeout: 10000,
  });
  await new Promise((r) => setTimeout(r, 500));

  // Click a sidebar link — Temperature exists in both root and subdir sites
  const link = await tab.evaluate(() => {
    const a = document.querySelector(
      'nav.sidebar .nav-list a[href*="Temperature"]'
    );
    if (a) {
      a.click();
      return a.href;
    }
    return null;
  });
  check(link !== null, "found Temperature link in sidebar");

  await new Promise((r) => setTimeout(r, 1000));
  const url = tab.url();
  check(url.includes("Temperature"), `navigated to Temperature page`);
  check(url.includes(BASE_PATH), `URL includes base path "${BASE_PATH}"`);

  const title = await tab.title();
  check(title.includes("Temperature"), "Temperature page title correct");

  await tab.close();
}

// ─── Main ───────────────────────────────────────────────────

async function main() {
  console.log(`Validating site at: ${BASE_URL}${BASE_PATH} (${PAGES.length} pages)\n`);

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--disable-dev-shm-usage",  // Use /tmp instead of /dev/shm (prevents OOM)
      "--disable-gpu",            // Reduce memory pressure from WASM-heavy pages
      "--no-sandbox",
    ],
  });

  try {
    for (const page of PAGES) {
      await testPage(browser, page);
    }
    await testNavigation(browser);
  } finally {
    await browser.close();
  }

  console.log(`\n=== Results: ${pass} passed, ${fail} failed ===`);

  if (fail > 0) {
    console.log("\nFAILED");
    process.exit(1);
  }

  console.log("\n=== Browser validation PASSED ===");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
