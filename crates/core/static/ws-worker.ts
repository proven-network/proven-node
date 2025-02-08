/// <reference lib="webworker" />

const INACTIVITY_TIMEOUT = 60000; // 60 seconds in milliseconds
const CHECK_INTERVAL = 5000; // 5 seconds in milliseconds

class WebSocketWorker {
  ws: WebSocket | null;
  ports: Set<MessagePort>;
  lastActivity: number;
  timeoutChecker: number | null;
  intentionallyClosed: boolean;

  constructor() {
    this.ws = null;
    this.ports = new Set();
    this.lastActivity = Date.now();
    this.timeoutChecker = null;
    this.intentionallyClosed = false;

    this.init();
    this.startTimeoutChecker();
  }

  init() {
    if (this.ws?.readyState === WebSocket.OPEN || this.intentionallyClosed)
      return;

    this.ws = new WebSocket("ws://localhost:3200/app/abc/ws");
    this.updateLastActivity();
    this.intentionallyClosed = false;

    this.ws.onmessage = (event) => {
      this.updateLastActivity();
      this.ports.forEach((port) => {
        port.postMessage({ type: "ws-message", data: event.data });
      });
    };

    this.ws.onclose = () => {
      this.ws = null;
      // Only reconnect if closure wasn't intentional
      if (!this.intentionallyClosed) {
        setTimeout(() => this.init(), 1000);
      }
    };
  }

  updateLastActivity() {
    this.lastActivity = Date.now();
  }

  startTimeoutChecker() {
    this.timeoutChecker = setInterval(() => {
      if (Date.now() - this.lastActivity > INACTIVITY_TIMEOUT) {
        this.closeConnection();
      }
    }, CHECK_INTERVAL) as unknown as number;
  }

  closeConnection() {
    if (this.ws) {
      this.intentionallyClosed = true;
      this.ws.close();
      this.ws = null;
    }
  }

  send(message: string) {
    this.intentionallyClosed = false; // Reset the flag when attempting to send
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      this.init();
    }
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.updateLastActivity();
      this.ws.send(message);
    }
  }
}

function main() {
  const worker = new WebSocketWorker();
  const { addEventListener } = self as unknown as SharedWorkerGlobalScope;

  addEventListener("connect", (e) => {
    const port = e.ports[0];
    worker.ports.add(port);
    worker.updateLastActivity();

    port.onmessage = (e) => {
      if (e.data.type === "ws-send") {
        worker.send(e.data.message);
      }
    };

    port.start();
  });
}

main();
