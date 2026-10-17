import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import vm from "node:vm";

const source = readFileSync(
  new URL("../../../crates/sitegen/assets/chart.js", import.meta.url),
  "utf8",
).replace(/^import .+;\n/gm, "");
const NOW = 1792200000000;
const DAY = 86400000;

async function openChart({ search = "", hash = "", defaultRange, spanDays = 120, now = NOW } = {}) {
  const elements = [];
  function element(tag) {
    const classes = new Set();
    const el = {
      tag, dataset: {}, children: [],
      classList: {
        add: (name) => classes.add(name),
        remove: (name) => classes.delete(name),
        contains: (name) => classes.has(name),
        toggle: (name, on) => on ? classes.add(name) : classes.delete(name),
      },
      append(...children) { this.children.push(...children); },
      appendChild(child) { this.children.push(child); },
      before() {},
      after() {},
      setAttribute() {},
      addEventListener(type, callback) { this[`on${type}`] = callback; },
      querySelectorAll(selector) {
        assert.equal(selector, "button");
        return this.children.filter((child) => child.tag === "button");
      },
    };
    elements.push(el);
    return el;
  }
  const container = element("div");
  container.dataset.defaultRange = defaultRange;
  const manifest = {
    textContent: JSON.stringify([{
      file: "data.parquet",
      start_time: (now - spanDays * DAY) / 1000,
      end_time: now / 1000,
      captures: ["res=1h"],
    }]),
  };
  const queries = [];
  const warnings = [];
  const copied = [];
  const location = new URL(`https://example.com/data/pressure.html${search}${hash}`);
  const reloads = [];
  location.reload = () => reloads.push(location.href);
  await vm.runInNewContext(source, {
    document: {
      querySelector: (selector) =>
        selector === 'script.chart-data[type="application/json"]' ? manifest : null,
      getElementById: () => container,
      createElement: element,
    },
    location,
    history: { replaceState: (_, __, url) => { location.href = url; } },
    Date: class extends Date { static now() { return now; } },
    URL,
    URLSearchParams,
    getComputedStyle: () => ({ getPropertyValue: () => "" }),
    initDuckdb: async () => ({
      db: {},
      conn: { query: async (sql) => { queries.push(sql); return { toArray: () => [] }; } },
    }),
    createFileRegistry: () => ({ ensureFile: async () => "data.parquet" }),
    console: { warn: (message) => warnings.push(message), error: assert.fail },
    navigator: { clipboard: { writeText: async (url) => copied.push(url) } },
    clearTimeout() {},
    setTimeout() {},
  });
  return {
    elements, queries, warnings, copied, location, reloads,
    activeButtons: () => elements.filter((el) => el.tag === "button" && el.classList.contains("active")),
  };
}

function assertWindow(chart, begin, end) {
  assert.ok(chart.queries.at(-1)?.includes(`epoch_ms(timestamp) >= ${begin} AND epoch_ms(timestamp) <= ${end}`));
}

for (const [range, days] of [["24h", 1], ["7d", 7]]) {
  test(`?range=${range} overrides the site default and queries the exact trailing window`, async () => {
    const chart = await openChart({ search: `?range=${range}`, defaultRange: "1M" });
    assertWindow(chart, NOW - days * DAY, NOW);
    assert.deepEqual(chart.activeButtons().map((el) => el.textContent), [range]);
    const copyBtn = chart.elements.find((el) => el.className === "copy-link");
    await copyBtn.onclick();
    assert.deepEqual(chart.copied, [
      `https://example.com/data/pressure.html#t=${NOW - days * DAY},${NOW}`,
    ]);
  });
}

test("an explicit duration is not clamped to a young dataset's available buttons", async () => {
  const chart = await openChart({ search: "?range=7d", spanDays: 0.25 });
  assertWindow(chart, NOW - 7 * DAY, NOW);
  assert.deepEqual(chart.activeButtons().map((el) => el.textContent), ["7d"]);
});

test("an absolute shared window takes precedence over a navigation duration", async () => {
  const begin = NOW - 2 * DAY;
  const end = NOW - DAY;
  const chart = await openChart({ search: "?range=7d", hash: `#t=${begin},${end}` });
  assertWindow(chart, begin, end);
  assert.equal(chart.activeButtons().length, 0);
  assert.equal(chart.elements.find((el) => el.className === "reset-zoom").disabled, false);
});

test("missing and unknown ranges preserve existing defaults; unknown ranges warn", async () => {
  for (const search of ["", "?range=invalid", "?range="]) {
    const chart = await openChart({ search, defaultRange: "1M" });
    assertWindow(chart, NOW - 30 * DAY, NOW);
    assert.deepEqual(chart.activeButtons().map((el) => el.textContent), ["1M"]);
    assert.equal(chart.warnings.length, search ? 1 : 0);
  }
  const adaptive = await openChart();
  assertWindow(adaptive, NOW - 90 * DAY, NOW);
});

test("duration selection survives browser reload and Reload data, advancing to now", async () => {
  const chart = await openChart({ defaultRange: "1M", search: "?other=keep", hash: "#notes" });
  await chart.elements.find((el) => el.textContent === "7d").onclick();
  assertWindow(chart, NOW - 7 * DAY, NOW);
  assert.equal(chart.location.searchParams.get("range"), "7d");
  assert.equal(chart.location.searchParams.get("other"), "keep");
  assert.equal(chart.location.hash, "#notes");

  const reloaded = await openChart({
    search: chart.location.search, hash: chart.location.hash, now: NOW + DAY,
  });
  assertWindow(reloaded, NOW - 6 * DAY, NOW + DAY);
  assert.deepEqual(reloaded.activeButtons().map((el) => el.textContent), ["7d"]);

  reloaded.elements.find((el) => el.className === "reload-data").onclick();
  assert.deepEqual(reloaded.reloads, [reloaded.location.href]);
  const refreshed = await openChart({
    search: new URL(reloaded.reloads[0]).search, now: NOW + 2 * DAY,
  });
  assertWindow(refreshed, NOW - 5 * DAY, NOW + 2 * DAY);
});

test("edited absolute windows survive reload and Reset zoom restores the selected duration", async () => {
  const chart = await openChart({ search: "?range=7d&other=keep" });
  const inputs = chart.elements.filter((el) => el.className === "window-input");
  // datetime-local values are interpreted in the user's local timezone.
  inputs[0].value = "2026-10-14T10:00";
  inputs[1].value = "2026-10-14T11:00";
  const begin = new Date(inputs[0].value).getTime();
  const end = new Date(inputs[1].value).getTime();
  await inputs[0].onchange();
  assertWindow(chart, begin, end);
  assert.equal(new URLSearchParams(chart.location.hash.slice(1)).get("t"), `${begin},${end}`);

  chart.elements.find((el) => el.className === "reload-data").onclick();
  const reloadUrl = new URL(chart.reloads[0]);
  const reloaded = await openChart({ search: reloadUrl.search, hash: reloadUrl.hash, now: NOW + DAY });
  assertWindow(reloaded, begin, end);
  assert.equal(reloaded.activeButtons().length, 0);
  await reloaded.elements.find((el) => el.className === "reset-zoom").onclick();
  assertWindow(reloaded, NOW - 6 * DAY, NOW + DAY);
  assert.equal(reloaded.location.hash, "");
  assert.equal(reloaded.location.searchParams.get("range"), "7d");
  assert.equal(reloaded.location.searchParams.get("other"), "keep");
  assert.deepEqual(reloaded.activeButtons().map((el) => el.textContent), ["7d"]);
  assert.equal(reloaded.elements.find((el) => el.className === "reset-zoom").disabled, true);
});

test("choosing a duration clears an old absolute window from the reload URL", async () => {
  const chart = await openChart({
    search: "?range=7d", hash: `#t=${NOW - 2 * DAY},${NOW - DAY}`,
  });
  await chart.elements.find((el) => el.textContent === "24h").onclick();
  assert.equal(chart.location.hash, "");
  assert.equal(chart.location.searchParams.get("range"), "24h");
  assertWindow(chart, NOW - DAY, NOW);
});
