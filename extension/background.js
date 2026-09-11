// background.js — MV3 service worker.
// Owns the WebSocket connection to the capture backend and forwards batches
// coming from content scripts across all tabs. Service workers can be killed
// and restarted by Chrome at any time, so the socket is reconnected lazily
// and queued events survive a restart via chrome.storage.session.

const BACKEND_WS_URL = "ws://localhost:8000/ws/ingest"; // override for prod
const QUEUE_KEY = "cm_pending_events";

let socket = null;
let connecting = false;

function connect() {
  if (socket && socket.readyState === WebSocket.OPEN) return;
  if (connecting) return;
  connecting = true;

  socket = new WebSocket(BACKEND_WS_URL);

  socket.addEventListener("open", async () => {
    connecting = false;
    await drainQueue();
  });

  socket.addEventListener("close", () => {
    connecting = false;
    socket = null;
    setTimeout(connect, 2000); // simple backoff; fine for a hackathon demo
  });

  socket.addEventListener("error", () => {
    try {
      socket.close();
    } catch {}
  });
}

async function drainQueue() {
  const { [QUEUE_KEY]: queue = [] } = await chrome.storage.session.get(QUEUE_KEY);
  if (queue.length === 0) return;
  for (const batch of queue) {
    sendOrQueue(batch, /*alreadyQueued*/ true);
  }
  await chrome.storage.session.set({ [QUEUE_KEY]: [] });
}

async function sendOrQueue(batch, alreadyQueued = false) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(batch));
    return;
  }
  if (!alreadyQueued) {
    const { [QUEUE_KEY]: queue = [] } = await chrome.storage.session.get(QUEUE_KEY);
    queue.push(batch);
    await chrome.storage.session.set({ [QUEUE_KEY]: queue });
  }
  connect();
}

chrome.runtime.onMessage.addListener((message, sender) => {
  if (message?.type !== "capture-batch") return;
  sendOrQueue({
    sessionId: message.sessionId,
    tabId: sender.tab?.id ?? null,
    events: message.events,
  });
});

chrome.runtime.onStartup.addListener(connect);
chrome.runtime.onInstalled.addListener(connect);
connect();
