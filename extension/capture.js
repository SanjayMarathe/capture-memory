// capture.js — runs in every page/frame at document_start.
// Job: turn raw browser signals into a small, structured event stream and
// hand it off to the background worker, which owns the socket to the backend.
//
// Privacy note: keystroke capture NEVER records typed characters. We only
// record event metadata (target element type/name, key category, timing).
// This is enough to reconstruct "user typed into the email field, then
// clicked submit" sequences without ever touching what was typed.

(() => {
  const SESSION_ID_KEY = "__capture_memory_session_id";
  const sessionId =
    sessionStorage.getItem(SESSION_ID_KEY) ||
    (() => {
      const id = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
      sessionStorage.setItem(SESSION_ID_KEY, id);
      return id;
    })();

  let buffer = [];
  const FLUSH_INTERVAL_MS = 1500;
  const MAX_BUFFER = 50;

  function emit(event) {
    buffer.push({
      ...event,
      t: Date.now(),
      url: location.href,
      sessionId,
    });
    if (buffer.length >= MAX_BUFFER) flush();
  }

  function flush() {
    if (buffer.length === 0) return;
    const batch = buffer;
    buffer = [];
    try {
      chrome.runtime.sendMessage({ type: "capture-batch", sessionId, events: batch });
    } catch (e) {
      // Extension context can be invalidated on reload; just drop the batch.
    }
  }
  setInterval(flush, FLUSH_INTERVAL_MS);
  window.addEventListener("beforeunload", flush);

  // Fetch/console interception must run in the page's MAIN world. page-hook.js
  // sends bounded error metadata across this relay; only this isolated script
  // can access chrome.runtime and forward it to the background worker.
  const PAGE_EVENT_SOURCE = "capture-memory-page-hook-v1";
  const PAGE_EVENT_KINDS = new Set(["console_error", "runtime_error", "unhandled_rejection", "network_failure"]);
  window.addEventListener("message", (message) => {
    if (message.source !== window || message.data?.source !== PAGE_EVENT_SOURCE) return;
    const event = message.data?.event;
    if (!event || typeof event !== "object" || !PAGE_EVENT_KINDS.has(event.kind)) return;
    emit(event);
  });

  // ---------- Interaction sequence (click / keystroke, privacy-safe) ----------
  function describeTarget(el) {
    if (!el || !el.tagName) return null;
    return {
      tag: el.tagName.toLowerCase(),
      id: el.id || null,
      name: el.getAttribute && el.getAttribute("name"),
      type: el.getAttribute && el.getAttribute("type"),
      role: el.getAttribute && el.getAttribute("role"),
      testId: el.getAttribute && el.getAttribute("data-testid"),
      // A short, non-sensitive label — never full innerText of arbitrary content.
      label: (el.getAttribute && (el.getAttribute("aria-label") || el.name)) || null,
    };
  }

  document.addEventListener(
    "click",
    (e) => {
      emit({ kind: "click", target: describeTarget(e.target) });
    },
    true
  );

  const NAV_KEYS = new Set([
    "Enter",
    "Tab",
    "Escape",
    "Backspace",
    "ArrowUp",
    "ArrowDown",
    "ArrowLeft",
    "ArrowRight",
  ]);
  document.addEventListener(
    "keydown",
    (e) => {
      const isPrintable = e.key.length === 1;
      emit({
        kind: "keystroke",
        target: describeTarget(e.target),
        // Never store the actual character. Store category only.
        keyCategory: isPrintable ? "char" : NAV_KEYS.has(e.key) ? e.key : "other",
        modifiers: { ctrl: e.ctrlKey, meta: e.metaKey, alt: e.altKey, shift: e.shiftKey },
      });
    },
    true
  );
})();
