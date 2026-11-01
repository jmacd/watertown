import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import puppeteer from "puppeteer";

const css = readFileSync(
  new URL("../../../crates/sitegen/assets/style.css", import.meta.url),
  "utf8",
);

test("manual tables keep field names intact and scroll on narrow screens", async () => {
  const browser = await puppeteer.launch({
    headless: "shell",
    args: ["--disable-dev-shm-usage", "--no-sandbox"],
  });
  try {
    const page = await browser.newPage();
    await page.setContent(`
      <style>${css}</style>
      <body class="documentation">
        <main class="content-page">
        <article class="manual-article">
          <table>
            <thead><tr>
              <th>Field</th><th>Accepted type</th><th>Required</th>
              <th>Default</th><th>Description</th>
            </tr></thead>
            <tbody>
              <tr>
                <td><code>prefix</code></td><td>string | null</td>
                <td>no</td><td>not specified</td>
                <td>A relative path limiting which files are visible inside the directory.</td>
              </tr>
              <tr>
                <td><code>schema_infer_max_records</code></td><td>integer</td>
                <td>no</td><td>100</td>
                <td>The maximum number of records used to discover column types before reading the remaining data.</td>
              </tr>
            </tbody>
          </table>
        </article>
        </main>
      </body>
    `);
    for (const width of [1200, 375]) {
      await page.setViewport({ width, height: 900 });
      const layout = await page.evaluate(() => {
        function lines(element) {
          const range = document.createRange();
          range.selectNodeContents(element);
          return range.getClientRects().length;
        }
        const table = document.querySelector("table");
        return {
          fieldLines: [...table.querySelectorAll("td:first-child code")].map(lines),
          headingLines: [...table.querySelectorAll("th")].map(lines),
          descriptionLines: lines(table.querySelector("td:last-child")),
          scrollWidth: table.scrollWidth,
          width: table.clientWidth,
          pageWidth: document.documentElement.scrollWidth,
        };
      });
      assert.ok(layout.fieldLines.every((count) => count === 1), `split field at ${width}px`);
      assert.ok(layout.headingLines.every((count) => count === 1), `split heading at ${width}px`);
      assert.ok(layout.descriptionLines > 1, `description should wrap at ${width}px`);
      assert.ok(layout.pageWidth <= width, `table widened the page at ${width}px`);
      if (width === 375) {
        assert.ok(layout.scrollWidth > layout.width, "narrow tables should scroll");
      }
    }
  } finally {
    await browser.close();
  }
});
