// Runs in the target page's MAIN world so it can observe the app's own
// console, fetch, and XHR activity. It cannot access extension APIs; events
// cross a narrow postMessage bridge to the isolated capture.js relay.
(() => {
  if (window.__captureMemoryPageHookInstalled) return;
  window.__captureMemoryPageHookInstalled = true;

  const PAGE_EVENT_SOURCE = "capture-memory-page-hook-v1";
  const emit = (event) => window.postMessage({ source: PAGE_EVENT_SOURCE, event }, "*");
  const safeStringify = (values) => {
    try {
      return values
        .map((value) => typeof value === "string" ? value : JSON.stringify(value))
        .join(" ")
        .slice(0, 2000);
    } catch {
      return "[unserializable error]";
    }
  };

  const originalConsoleError = console.error.bind(console);
  console.error = (...args) => {
    emit({ kind: "console_error", message: safeStringify(args) });
    originalConsoleError(...args);
  };

  window.addEventListener("error", (event) => emit({
    kind: "runtime_error",
    message: String(event.message || "Page runtime error").slice(0, 2000),
    source: String(event.filename || "").slice(0, 500),
    line: event.lineno || 0,
    col: event.colno || 0,
  }));

  window.addEventListener("unhandledrejection", (event) => emit({
    kind: "unhandled_rejection",
    message: safeStringify([event.reason]),
  }));

  const originalFetch = window.fetch.bind(window);
  window.fetch = async (...args) => {
    const started = Date.now();
    const request = args[0];
    const init = args[1];
    const url = typeof request === "string" || request instanceof URL ? String(request) : request?.url;
    const method = init?.method || (request instanceof Request ? request.method : "GET");
    try {
      const response = await originalFetch(...args);
      if (!response.ok) emit({
        kind: "network_failure",
        method,
        requestUrl: url,
        status: response.status,
        statusText: response.statusText,
        durationMs: Date.now() - started,
      });
      return response;
    } catch (error) {
      emit({
        kind: "network_failure",
        method,
        requestUrl: url,
        status: 0,
        statusText: String(error?.message || "Network request failed").slice(0, 500),
        durationMs: Date.now() - started,
      });
      throw error;
    }
  };

  const xhrMetadata = new WeakMap();
  const originalXhrOpen = XMLHttpRequest.prototype.open;
  const originalXhrSend = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function (method, url, ...rest) {
    xhrMetadata.set(this, { method, url: String(url), started: 0 });
    return originalXhrOpen.call(this, method, url, ...rest);
  };
  XMLHttpRequest.prototype.send = function (...args) {
    const metadata = xhrMetadata.get(this) || { method: "GET", url: "", started: 0 };
    metadata.started = Date.now();
    xhrMetadata.set(this, metadata);
    this.addEventListener("loadend", () => {
      if (this.status === 0 || this.status >= 400) emit({
        kind: "network_failure",
        method: metadata.method,
        requestUrl: metadata.url,
        status: this.status,
        statusText: this.statusText,
        durationMs: Date.now() - metadata.started,
      });
    }, { once: true });
    return originalXhrSend.apply(this, args);
  };
})();
