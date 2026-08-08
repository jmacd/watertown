// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

import { createFileRegistry, initDuckdb } from "./duckdb-shared.js";

function number(value) {
  return value == null ? null : Number(value);
}

function formatValue(value, unit) {
  const n = number(value);
  if (n == null || !Number.isFinite(n)) return "unknown";
  if (unit !== "bytes") return Math.round(n).toLocaleString();

  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let scaled = n;
  let index = 0;
  while (Math.abs(scaled) >= 1024 && index < units.length - 1) {
    scaled /= 1024;
    index++;
  }
  const digits = scaled >= 100 || index === 0 ? 0 : scaled >= 10 ? 1 : 2;
  return `${scaled.toFixed(digits)} ${units[index]}`;
}

function formatDuration(value) {
  const seconds = number(value);
  if (seconds == null || !Number.isFinite(seconds)) return "unknown";
  if (seconds < 60) return `${Math.max(0, Math.round(seconds))}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`;
  return `${(seconds / 86400).toFixed(1)}d`;
}

function labelFor(row) {
  const leaf = String(row.limiter || "limiter").split("/").pop();
  return leaf
    .replace(/-ops$/, " operations")
    .replace(/-/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function appendText(parent, tag, className, text) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  element.textContent = text;
  parent.appendChild(element);
  return element;
}

function utilizationClass(ratio) {
  if (ratio >= 0.9) return "limit-red";
  if (ratio >= 0.7) return "limit-yellow";
  return "limit-green";
}

function renderMeter(parent, title, used, limit, unit, resetSeconds) {
  const usedNumber = number(used);
  const limitNumber = number(limit);
  const ratio =
    usedNumber != null && limitNumber != null && limitNumber > 0
      ? usedNumber / limitNumber
      : 0;

  const heading = document.createElement("div");
  heading.className = "limiter-meter-heading";
  appendText(heading, "span", "limiter-meter-name", title);
  appendText(
    heading,
    "span",
    "limiter-meter-value",
    `${formatValue(used, unit)} / ${formatValue(limit, unit)} (${(
      ratio * 100
    ).toFixed(1)}%)`
  );
  parent.appendChild(heading);

  const track = document.createElement("div");
  track.className = "limiter-meter-track";
  track.setAttribute("role", "meter");
  track.setAttribute("aria-valuemin", "0");
  track.setAttribute("aria-valuemax", String(limitNumber || 0));
  track.setAttribute("aria-valuenow", String(usedNumber || 0));
  track.setAttribute("aria-label", `${title} utilization`);
  const fill = document.createElement("div");
  fill.className = `limiter-meter-fill ${utilizationClass(ratio)}`;
  fill.style.width = `${Math.min(100, Math.max(0, ratio * 100))}%`;
  track.appendChild(fill);
  parent.appendChild(track);

  appendText(
    parent,
    "div",
    "limiter-meter-reset",
    `Resets in ${formatDuration(resetSeconds)}`
  );
}

function renderLimiter(row) {
  const card = document.createElement("section");
  card.className = "limiter-card";
  appendText(card, "h3", "limiter-name", labelFor(row));
  appendText(
    card,
    "div",
    "limiter-identity",
    `${row.remotes || "no remote"} \u00b7 ${row.unit || "unknown unit"}`
  );

  if (row.error) {
    appendText(card, "p", "limiter-error", String(row.error));
    return card;
  }

  renderMeter(
    card,
    "Sliding window",
    row.charged,
    row.limit,
    row.unit,
    row.reset_secs
  );
  if (number(row.burst) > 0 || number(row.burst_charged) > 0) {
    renderMeter(
      card,
      "Burst",
      row.burst_charged,
      row.burst,
      row.unit,
      row.burst_reset_secs
    );
  }

  const observed = formatValue(row.observed, row.unit);
  const suffix =
    row.observed_window_complete === true
      ? ""
      : row.observed_window_complete === false
        ? " \u00b7 observation window warming up"
        : "";
  appendText(
    card,
    "div",
    "limiter-observed",
    `Observed ${observed}${suffix}`
  );
  return card;
}

function renderDashboard(container, rows) {
  container.innerHTML = "";
  if (!rows.length) {
    appendText(container, "p", "limiter-empty", "No limiter state available.");
    return;
  }

  const grouped = new Map();
  for (const row of rows) {
    const pond = String(row.pond || "unknown pond");
    if (!grouped.has(pond)) grouped.set(pond, []);
    grouped.get(pond).push(row);
  }

  for (const [pond, pondRows] of grouped) {
    const section = document.createElement("section");
    section.className = "limiter-pond";
    appendText(section, "h2", "limiter-pond-name", pond);

    const grid = document.createElement("div");
    grid.className = "limiter-grid";
    for (const row of pondRows) grid.appendChild(renderLimiter(row));
    section.appendChild(grid);
    container.appendChild(section);
  }
}

async function main() {
  const container = document.getElementById("limiters");
  if (!container) return;
  const manifestElement = container.querySelector(
    'script.limiter-data[type="application/json"]'
  );
  if (!manifestElement) return;

  const manifest = JSON.parse(manifestElement.textContent);
  if (!manifest.length) {
    renderDashboard(container, []);
    return;
  }
  container.innerHTML = '<p class="limiter-loading">Loading limiter state...</p>';

  const { db, conn } = await initDuckdb();
  const { ensureFile } = createFileRegistry(db);
  const views = [];
  for (let i = 0; i < manifest.length; i++) {
    const file = await ensureFile(manifest[i].file);
    if (!file) continue;
    const view = `limiter_${i}`;
    await conn.query(
      `CREATE VIEW ${view} AS SELECT * FROM read_parquet('${file}')`
    );
    views.push(view);
  }
  if (!views.length) {
    renderDashboard(container, []);
    return;
  }

  const result = await conn.query(
    views
      .map((view) => `SELECT * FROM ${view}`)
      .join(" UNION ALL BY NAME ") + " ORDER BY pond, limiter, unit"
  );
  renderDashboard(container, result.toArray());
}

main().catch((error) => {
  console.error("limiter dashboard failed:", error);
  const container = document.getElementById("limiters");
  if (container) {
    container.innerHTML = "";
    appendText(
      container,
      "p",
      "limiter-error",
      `Unable to load limiter state: ${String(error)}`
    );
  }
});
