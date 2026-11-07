// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import puppeteer from "puppeteer";

const script = await readFile(new URL("../../../crates/sitegen/assets/analytics.js", import.meta.url), "utf8");
let browser;
let server;
let origin;
let records = [];
let status = 204;

before(async () => {
  server = createServer(async (req, res) => {
    const url = new URL(req.url, origin);
    if (url.pathname === "/analytics.js") {
      res.writeHead(200, { "Content-Type": "text/javascript" }).end(script);
    } else if (url.pathname === "/_analytics/view") {
      let body = "";
      for await (const chunk of req) body += chunk;
      records.push({
        payload: Object.fromEntries(url.searchParams),
        method: req.method,
        body,
        headers: req.headers,
      });
      res.writeHead(status).end();
    } else {
      const state = url.searchParams.get("state") || "visible";
      res.writeHead(200, { "Content-Type": "text/html" }).end(`<!doctype html>
        <title>Analytics fixture</title>
        <script>
          window.restored = false;
          window.addEventListener("pageshow", e => { window.restored = e.persisted; });
          // Deterministic prerender/visibility signals in an actual headless browser.
          window.fixtureVisibility = ${JSON.stringify(state === "hidden" ? "hidden" : "visible")};
          window.fixturePrerendering = ${state === "prerender"};
          Object.defineProperty(document, "visibilityState", {get: () => window.fixtureVisibility});
          Object.defineProperty(document, "prerendering", {get: () => window.fixturePrerendering});
        </script>
        ${url.pathname === "/other" ? "" : `<script defer src="/analytics.js"
          data-page-uri="/noyo-harbor/sites/BDock" data-catalog-id="fixture-catalog"
          data-endpoint="/_analytics/view"></script>`}
        <body>Fixture</body>`);
    }
  });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  origin = `http://127.0.0.1:${server.address().port}`;
  browser = await puppeteer.launch({ headless: true });
});

after(async () => {
  await browser?.close();
  if (server) await new Promise(resolve => server.close(resolve));
});

async function settle(page) {
  await page.waitForNetworkIdle({ idleTime: 100 });
}

async function withPage(state, run) {
  records = [];
  status = 204;
  const page = await browser.newPage();
  try {
    await page.goto(`${origin}/page?state=${state}`, { waitUntil: "networkidle0" });
    await run(page);
  } finally {
    await page.close();
  }
}

async function signal(page, visibility, prerendering = false) {
  await page.evaluate((visibility, prerendering) => {
    window.fixtureVisibility = visibility;
    window.fixturePrerendering = prerendering;
    document.dispatchEvent(new Event("visibilitychange"));
    document.dispatchEvent(new Event("prerenderingchange"));
    window.dispatchEvent(new PageTransitionEvent("pageshow", { persisted: true }));
  }, visibility, prerendering);
  await settle(page);
}

test("first visible view omits credentials, referrer, URL state and identifiers", async () => {
  await withPage("hidden", async page => {
    assert.equal(records.length, 0);
    await page.evaluate(() => {
      document.cookie = "fixture=not-analytics; SameSite=Lax; path=/";
      history.replaceState(null, "", "/page?private=query#fragment");
    });
    await signal(page, "visible");
    assert.equal(records.length, 1);
    assert.deepEqual(records[0].payload, {
      catalog_id: "fixture-catalog",
      page_uri: "/noyo-harbor/sites/BDock",
    });
    assert.equal(records[0].method, "POST");
    assert.equal(records[0].body, "");
    assert.equal(records[0].headers.cookie, undefined);
    assert.equal(records[0].headers.referer, undefined);
    await signal(page, "hidden");
    await signal(page, "visible");
    assert.equal(records.length, 1, "tab returns and persisted pageshow must not send again");
  });
});

test("already visible documents report immediately; new documents report again", async () => {
  await withPage("visible", async page => {
    assert.equal(records.length, 1);
    await page.reload({ waitUntil: "networkidle0" });
    assert.equal(records.length, 2);
  });
});

test("prerender waits for activation and visibility", async () => {
  await withPage("prerender", async page => {
    assert.equal(records.length, 0);
    await signal(page, "visible", true);
    assert.equal(records.length, 0);
    await signal(page, "hidden", false);
    assert.equal(records.length, 0);
    await signal(page, "visible", false);
    assert.equal(records.length, 1);
  });
});

test("real bfcache restoration retains the in-memory dispatch guard", async () => {
  await withPage("visible", async page => {
    assert.equal(records.length, 1);
    await page.goto(`${origin}/other`, { waitUntil: "networkidle0" });
    await page.goBack({ waitUntil: "networkidle0" });
    assert.equal(await page.evaluate(() => window.restored), true, "fixture must actually use bfcache");
    await signal(page, "visible");
    assert.equal(records.length, 1);
  });
});

test("embedded documents never report", async () => {
  records = [];
  const page = await browser.newPage();
  try {
    await page.goto(`${origin}/other`);
    await page.evaluate(origin => {
      const frame = document.createElement("iframe");
      frame.src = `${origin}/page`;
      document.body.append(frame);
    }, origin);
    await settle(page);
    assert.equal(records.length, 0);
  } finally {
    await page.close();
  }
});

test("rejections and network failures warn once, do not retry or affect content", async () => {
  for (const networkFailure of [false, true]) {
    await withPage("hidden", async page => {
      const warnings = [];
      page.on("console", message => {
        if (message.type() === "warn") warnings.push(message.text());
      });
      status = 503;
      if (networkFailure) {
        await page.setRequestInterception(true);
        page.on("request", request => {
          if (new URL(request.url()).pathname === "/_analytics/view") void request.abort();
          else void request.continue();
        });
      }
      await signal(page, "visible");
      await signal(page, "hidden");
      await signal(page, "visible");
      assert.equal(records.length, networkFailure ? 0 : 1);
      assert.deepEqual(warnings, [networkFailure
        ? "Site analytics: view request failed"
        : "Site analytics: view was not accepted"]);
      assert.equal(await page.title(), "Analytics fixture");
    });
  }
});
