import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import vm from "node:vm";

const source = readFileSync(
  new URL("../../../crates/sitegen/assets/manual-navigation.js", import.meta.url),
  "utf8",
);

function visit(storage, { baseUrl = "/watertown/", activeTop = 900 } = {}) {
  const warnings = [];
  let pagehide;
  const sidebar = {
    dataset: { baseUrl },
    scrollTop: 0,
    getBoundingClientRect: () => ({ top: 0, bottom: 600 }),
    querySelector: () => ({
      getBoundingClientRect: () => ({
        top: activeTop - sidebar.scrollTop,
        bottom: activeTop + 30 - sidebar.scrollTop,
      }),
    }),
  };
  vm.runInNewContext(source, {
    document: { currentScript: { previousElementSibling: sidebar } },
    sessionStorage: storage,
    window: { addEventListener: (event, callback) => {
      assert.equal(event, "pagehide");
      pagehide = callback;
    } },
    console: { warn: (...args) => warnings.push(args) },
  });
  return { sidebar, warnings, leave: () => pagehide() };
}

function storage() {
  const values = new Map();
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
  };
}

test("manual navigation retains scroll across pages and isolates site prefixes", () => {
  const saved = storage();
  const first = visit(saved);
  first.sidebar.scrollTop = 700;
  first.leave();
  const next = visit(saved);
  assert.equal(next.sidebar.scrollTop, 700);
  const otherSite = visit(saved, { baseUrl: "/other/", activeTop: 100 });
  assert.equal(otherSite.sidebar.scrollTop, 0);
});

test("direct links and offscreen selections reveal the active page without scrolling the document", () => {
  const saved = storage();
  const direct = visit(saved);
  assert.equal(direct.sidebar.scrollTop, 342);
  direct.sidebar.scrollTop = 700;
  direct.leave();
  const earlier = visit(saved, { activeTop: 500 });
  assert.equal(earlier.sidebar.scrollTop, 488);
});

test("blocked storage reports errors while keeping the current page visible", () => {
  const blocked = () => { throw new Error("Storage blocked"); };
  const page = visit({ getItem: blocked, setItem: blocked });
  assert.equal(page.sidebar.scrollTop, 342);
  assert.equal(page.warnings.length, 1);
  page.leave();
  assert.equal(page.warnings.length, 2);
});
