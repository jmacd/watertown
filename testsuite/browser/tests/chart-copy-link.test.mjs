import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import vm from "node:vm";

const source = readFileSync(
  new URL("../../../crates/sitegen/assets/chart.js", import.meta.url),
  "utf8",
);
const handler = source.slice(
  source.indexOf("  let copyResetTimer = null;"),
  source.indexOf("  // Serialize the queried rows to CSV:"),
);

function setup({ clipboard, secure = true }) {
  let click;
  let timer;
  const classes = new Set();
  const copyBtn = {
    textContent: "Copy link",
    classList: {
      add: (name) => classes.add(name),
      remove: (name) => classes.delete(name),
    },
    addEventListener: (_, callback) => { click = callback; },
  };
  const copyStatus = { textContent: "" };
  const errors = [];
  const urls = [];
  vm.runInNewContext(handler, {
    copyBtn,
    copyStatus,
    curBegin: 1234,
    curEnd: 5678,
    location: { origin: "http://watershop.example", pathname: "/data/pressure.html" },
    navigator: { clipboard },
    window: { isSecureContext: secure },
    history: { replaceState: (_, __, url) => urls.push(url) },
    console: { error: (...args) => errors.push(args) },
    clearTimeout: () => { timer = null; },
    setTimeout: (callback) => { timer = callback; return 1; },
  });
  return {
    click: () => click(),
    copyBtn, copyStatus, classes, errors, urls,
    timer: () => timer,
  };
}

test("HTTP clipboard failure persists and preserves a manually copyable range", async () => {
  const state = setup({ clipboard: undefined, secure: false });
  await state.click();
  assert.equal(state.copyBtn.textContent, "Copy failed");
  assert.match(state.copyStatus.textContent, /requires HTTPS.*address bar/);
  assert.deepEqual(state.urls, ["http://watershop.example/data/pressure.html#t=1234,5678"]);
  assert.equal(state.errors.length, 1);
  assert.equal(state.classes.has("copied"), false);
  assert.equal(state.timer(), null);
});

test("clipboard rejection cancels previous success feedback and a retry clears the error", async () => {
  let reject = false;
  const copied = [];
  const state = setup({
    clipboard: {
      writeText: async (url) => {
        if (reject) throw new Error("Permission denied");
        copied.push(url);
      },
    },
  });
  await state.click();
  assert.equal(state.copyBtn.textContent, "Copied!");
  assert.equal(state.classes.has("copied"), true);
  assert.equal(typeof state.timer(), "function");
  assert.deepEqual(copied, ["http://watershop.example/data/pressure.html#t=1234,5678"]);

  reject = true;
  await state.click();
  assert.equal(state.copyBtn.textContent, "Copy failed");
  assert.match(state.copyStatus.textContent, /Could not copy.*address bar/);
  assert.doesNotMatch(state.copyStatus.textContent, /HTTPS/);
  assert.equal(state.classes.has("copied"), false);
  assert.equal(state.timer(), null);
  assert.equal(state.errors.length, 1);

  reject = false;
  await state.click();
  assert.equal(state.copyStatus.textContent, "");
  assert.equal(state.copyBtn.textContent, "Copied!");
  state.timer()();
  assert.equal(state.copyBtn.textContent, "Copy link");
  assert.equal(state.classes.has("copied"), false);
});
