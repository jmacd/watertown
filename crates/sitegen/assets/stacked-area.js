// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

import { initDuckdb, createFileRegistry } from "./duckdb-shared.js";
import { loadVega, buildDailyStackedAreaSpec } from "./vega-shared.js";

(async function () {
  const container = document.getElementById("stacked-area");
  const manifestEl = container?.querySelector('script.stacked-area-data[type="application/json"]');
  if (!manifestEl) return;
  try {
    const manifest = JSON.parse(manifestEl.textContent);
    const optionsEl = document.querySelector('script.stacked-area-options[type="application/json"]');
    const options = optionsEl ? JSON.parse(optionsEl.textContent) : {};
    if (!manifest.length) return;
    container.textContent = "Loading chart data...";
    const { db, conn } = await initDuckdb();
    let rows;
    try {
      const { ensureFile } = createFileRegistry(db);
      const names = [];
      for (const file of new Set(manifest.map((entry) => entry.file))) {
        const name = await ensureFile(new URL(file, location.href).href);
        if (!name) throw new Error("A chart data file could not be loaded.");
        names.push(`'${name.replace(/'/g, "''")}'`);
      }
      const quote = (name) => `"${String(name).replace(/"/g, '""')}"`;
      const result = await conn.query(
        `SELECT epoch_ms(${quote(options.x || "timestamp")}) AS timestamp,
         CAST(${quote(options.color || "series")} AS VARCHAR) AS series,
         ${quote(options.y || "value")} AS value
         FROM read_parquet([${names.join(",")}], union_by_name=true)`
      );
      rows = result.toArray().map((row) => ({
        timestamp: row.timestamp == null ? null : Number(row.timestamp),
        series: row.series,
        value: row.value == null ? null : Number(row.value),
      }));
    } finally {
      await conn.close();
      await db.terminate();
    }
    const { spec, error } = buildDailyStackedAreaSpec(rows, {
      days: Number(options.days || 30),
      order: (options.order || "").split(",").map((s) => s.trim()).filter(Boolean),
      yTitle: options.y_title || options.y || "Count",
      theme: { fg: getComputedStyle(document.body).getPropertyValue("--fg").trim() || "#333333" },
    });
    if (error) {
      container.textContent = error;
      return;
    }
    const note = document.createElement("p");
    note.textContent = "UTC days; today is partial. Gaps mean no reported data, not confirmed zero.";
    const plot = document.createElement("div");
    plot.style.width = "100%";
    container.replaceChildren(note, plot);
    const embed = await loadVega();
    const { view } = await embed(plot, spec, { actions: false, renderer: "svg" });
    window.addEventListener("pagehide", () => view.finalize(), { once: true });
  } catch (error) {
    console.error("stacked-area: render failed", error);
    container.textContent = "Chart unavailable: data could not be loaded or rendered.";
  }
})();
