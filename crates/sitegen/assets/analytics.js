// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

(() => {
  const metadata = document.currentScript?.dataset;
  if (!metadata || !metadata.endpoint || !metadata.catalogId || !metadata.pageUri) {
    console.warn("Site analytics: missing page metadata");
    return;
  }
  if (window.self !== window.top) return;

  let attempted = false;
  async function report() {
    if (attempted || document.prerendering || document.visibilityState !== "visible") return;
    attempted = true;
    document.removeEventListener("visibilitychange", report);
    document.removeEventListener("prerenderingchange", report);
    window.removeEventListener("pageshow", report);
    try {
      const endpoint = new URL(metadata.endpoint, window.location.origin);
      endpoint.search = new URLSearchParams({
        page_uri: metadata.pageUri,
        catalog_id: metadata.catalogId,
      }).toString();
      const response = await fetch(endpoint, {
        method: "POST",
        credentials: "omit",
        referrerPolicy: "no-referrer",
        keepalive: true,
        cache: "no-store",
      });
      if (!response.ok) console.warn("Site analytics: view was not accepted");
    } catch {
      console.warn("Site analytics: view request failed");
    }
  }

  document.addEventListener("visibilitychange", report);
  document.addEventListener("prerenderingchange", report);
  window.addEventListener("pageshow", report);
  void report();
})();
