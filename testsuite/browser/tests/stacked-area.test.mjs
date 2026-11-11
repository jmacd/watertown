import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import vm from "node:vm";

const assets = new URL("../../../crates/sitegen/assets/", import.meta.url);
const source = readFileSync(new URL("vega-shared.js", assets), "utf8");
const { buildDailyStackedAreaSpec } = await import(
  `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`
);
const DAY = 86400000;
const today = Date.parse("2026-11-08T00:00:00Z");
const options = { now: today + 3600000, days: 4, order: ["Blog", "Monitoring"] };
const row = (day, series, value) => ({ timestamp: today + day * DAY, series, value });

test("daily stacks preserve totals and leave whole missing days as gaps", () => {
  const { spec } = buildDailyStackedAreaSpec([
    row(-3, "Blog", 2), row(-3, "Monitoring", 3),
    row(-2, "Blog", 4), row(0, "Monitoring", 7),
  ], options);
  const area = spec.layer[1];
  const data = area.data.values;
  assert.equal(area.encoding.x.scale.type, "utc");
  assert.deepEqual(area.encoding.x.scale.domain, [today - 3 * DAY, today + DAY]);
  assert.equal(area.encoding.y.scale.zero, true);
  assert.equal(area.mark.interpolate, "step-after");
  assert.equal(area.encoding.detail.field, "segment");
  assert.equal(area.transform[0].stack, "value");
  const observed = data.filter((r) => r.timestamp === Date.parse(`${r.day}T00:00:00Z`));
  assert.deepEqual(observed.map((r) => r.value), [2, 3, 4, 0, 0, 7]);
  assert.deepEqual(observed.map((r) => r.total), [5, 5, 4, 4, 7, 7]);
  assert.deepEqual([...new Set(observed.map((r) => r.segment))], [0, 1]);
  assert.ok(!observed.some((r) => r.timestamp === today - DAY));
  assert.equal(Math.max(...data.filter((r) => r.segment === 0).map((r) => r.timestamp)), today - DAY);
  assert.equal(Math.min(...data.filter((r) => r.segment === 1).map((r) => r.timestamp)), today);
  assert.ok(observed.filter((r) => r.timestamp === today).every((r) => r.period === "Partial day"));
});

test("the UTC window excludes old/future data and sums split daily categories", () => {
  const { spec } = buildDailyStackedAreaSpec([
    row(-4, "Old", 99), row(-3, "Blog", 2), row(-3, "Blog", 3),
    row(0, "Other", 1), row(1, "Future", 99),
  ], options);
  const area = spec.layer[1];
  assert.deepEqual(area.encoding.color.scale.domain, ["Blog", "Monitoring", "Other"]);
  const first = area.data.values.find((r) => r.series === "Blog");
  assert.equal(first.value, 5);
  assert.equal(first.total, 5);
  assert.ok(!area.data.values.some((r) => ["Old", "Future"].includes(r.series)));
  assert.match(buildDailyStackedAreaSpec([], options).error, /No reported data/);
});

test("invalid inputs do not become apparent zero counts", () => {
  for (const bad of [
    { ...row(0, "Blog", 1), timestamp: today + 1000 },
    row(0, "", 1), row(0, null, 1), row(0, "Blog", null),
    row(0, "Blog", -1), row(0, "Blog", NaN),
    row(0, "Blog", Number.MAX_SAFE_INTEGER + 1),
  ]) {
    assert.throws(() => buildDailyStackedAreaSpec([bad], options));
  }
  assert.throws(() => buildDailyStackedAreaSpec([row(0, "Blog", 1)], { ...options, days: 0 }));
});

const loader = readFileSync(new URL("stacked-area.js", assets), "utf8").replace(/^import .+;\n/gm, "");

async function openChart({ missing = false } = {}) {
  const manifest = { textContent: JSON.stringify([{ file: "counts.parquet" }, { file: "counts.parquet" }]) };
  const plotOptions = { textContent: JSON.stringify({
    x: 'utc."day', y: "reported.views", color: "site.section", days: "30",
  }) };
  const container = {
    querySelector: () => manifest,
    replaceChildren(...children) { this.children = children; },
  };
  const fetched = [], queries = [], rendered = [], errors = [], closed = [];
  await vm.runInNewContext(loader, {
    document: {
      getElementById: () => container,
      querySelector: () => plotOptions,
      createElement: () => ({ style: {} }),
      body: {},
    },
    location: { href: "https://example.com/noyo-harbor/analytics/index.html" },
    URL,
    window: { addEventListener() {} },
    getComputedStyle: () => ({ getPropertyValue: () => "" }),
    console: { error: (...args) => errors.push(args) },
    initDuckdb: async () => ({
      db: { terminate: async () => closed.push("db") },
      conn: {
        close: async () => closed.push("conn"),
        query: async (sql) => {
          queries.push(sql);
          return { toArray: () => [{ timestamp: BigInt(today), series: "Blog", value: 3n }] };
        },
      },
    }),
    createFileRegistry: () => ({
      ensureFile: async (url) => { fetched.push(url); return missing ? null : "f0.parquet"; },
    }),
    buildDailyStackedAreaSpec: (rows, opts) => buildDailyStackedAreaSpec(rows, { ...opts, now: today }),
    loadVega: async () => async (_, spec) => {
      rendered.push(spec);
      return { view: { finalize() {} } };
    },
  });
  return { container, fetched, queries, rendered, errors, closed };
}

test("the renderer loads Parquet once, quotes mapped fields, and converts Arrow integers", async () => {
  const result = await openChart();
  assert.deepEqual(result.fetched, ["https://example.com/noyo-harbor/analytics/counts.parquet"]);
  assert.ok(result.queries[0].includes('epoch_ms("utc.""day")'));
  assert.ok(result.queries[0].includes('"reported.views" AS value'));
  assert.ok(result.queries[0].includes('"site.section" AS VARCHAR'));
  assert.equal(result.rendered[0].layer[1].data.values[0].value, 3);
  assert.match(result.container.children[0].textContent, /today is partial/);
  assert.deepEqual(result.closed, ["conn", "db"]);
  assert.equal(result.errors.length, 0);
});

test("a missing Parquet file fails the chart rather than showing partial totals", async () => {
  const result = await openChart({ missing: true });
  assert.equal(result.rendered.length, 0);
  assert.equal(result.queries.length, 0);
  assert.match(result.container.textContent, /Chart unavailable/);
  assert.deepEqual(result.closed, ["conn", "db"]);
});
