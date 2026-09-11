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

  // ---------- Console errors ----------
  const origConsoleError = console.error.bind(console);
  console.error = (...args) => {
    emit({
      kind: "console_error",
      message: safeStringify(args),
    });
    origConsoleError(...args);
  };

  window.addEventListener("error", (e) => {
    emit({
      kind: "runtime_error",
      message: e.message,
      source: e.filename,
      line: e.lineno,
      col: e.colno,
      stack: e.error && e.error.stack ? String(e.error.stack).slice(0, 2000) : null,
    });
  });

  window.addEventListener("unhandledrejection", (e) => {
    emit({
      kind: "unhandled_rejection",
      message: safeStringify([e.reason]),
    });
  });

  // ---------- Network failures ----------
  const origFetch = window.fetch;
  window.fetch = async (...args) => {
    const started = Date.now();
    const req = args[0];
    const url = typeof req === "string" ? req : req && req.url;
    try {
      const res = await origFetch(...args);
      if (!res.ok) {
        emit({
          kind: "network_failure",
          method: (args[1] && args[1].method) || "GET",
          url,
          status: res.status,
          statusText: res.statusText,
          durationMs: Date.now() - started,
        });
      }
      return res;
    } catch (err) {
      emit({
        kind: "network_failure",
        method: (args[1] && args[1].method) || "GET",
        url,
        status: 0,
        statusText: String(err && err.message),
        durationMs: Date.now() - started,
      });
      throw err;
    }
  };

  const origXhrOpen = XMLHttpRequest.prototype.open;
  const origXhrSend = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function (method, url, ...rest) {
    this.__cm_method = method;
    this.__cm_url = url;
    this.__cm_start = Date.now();
    return origXhrOpen.call(this, method, url, ...rest);
  };
  XMLHttpRequest.prototype.send = function (...args) {
    this.addEventListener("loadend", () => {
      if (this.status === 0 || this.status >= 400) {
        emit({
          kind: "network_failure",
          method: this.__cm_method,
          url: this.__cm_url,
          status: this.status,
          statusText: this.statusText,
          durationMs: Date.now() - this.__cm_start,
        });
      }
    });
    return origXhrSend.apply(this, args);
  };

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

  function safeStringify(args) {
    try {
      return args
        .map((a) => (typeof a === "string" ? a : JSON.stringify(a)))
        .join(" ")
        .slice(0, 2000);
    } catch {
      return "[unserializable]";
    }
  }
})();
