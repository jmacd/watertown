// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

(() => {
  const sidebar = document.currentScript.previousElementSibling;
  const key = `watertown:manual-scroll:${sidebar.dataset.baseUrl}`;
  try {
    const saved = sessionStorage.getItem(key);
    if (saved !== null && Number.isFinite(Number(saved))) {
      sidebar.scrollTop = Number(saved);
    }
  } catch (error) {
    console.warn("Cannot restore manual sidebar position:", error);
  }

  // Direct links and changed viewport sizes may put the current page offscreen.
  const active = sidebar.querySelector('[aria-current="page"]');
  if (active) {
    const bounds = sidebar.getBoundingClientRect();
    const link = active.getBoundingClientRect();
    if (bounds.top > link.top) sidebar.scrollTop += link.top - bounds.top - 12;
    else if (link.bottom > bounds.bottom) sidebar.scrollTop += link.bottom - bounds.bottom + 12;
  }

  window.addEventListener("pagehide", () => {
    try {
      sessionStorage.setItem(key, String(sidebar.scrollTop));
    } catch (error) {
      console.warn("Cannot save manual sidebar position:", error);
    }
  });
})();
