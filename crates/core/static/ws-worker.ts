/// <reference lib="webworker" />

const INACTIVITY_TIMEOUT = 60000; // 60 seconds in milliseconds
const CHECK_INTERVAL = 5000; // 5 seconds in milliseconds

type WorkerMessage = {
  type: "send";
  nonce: number;
  data: Uint8Array;
};

class WebSocketWorker {
  ws: WebSocket | null;
  ports: Set<MessagePort>;
  lastActivity: number;
  timeoutChecker: number | null;
  intentionallyClosed: boolean;
  pendingRequests: Map<number, MessagePort>;

  constructor() {
    this.ws = null;
    this.ports = new Set();
    this.lastActivity = Date.now();
    this.timeoutChecker = null;
    this.intentionallyClosed = false;
    this.pendingRequests = new Map();

    this.init();
    this.startTimeoutChecker();
  }

  init() {
    if (this.ws?.readyState === WebSocket.OPEN || this.intentionallyClosed)
      return;

    const query = globalThis.location.search;
    this.ws = new WebSocket(
      `ws://localhost:3200/app/application_id/ws${query}`
    );
    this.ws.binaryType = "arraybuffer";
    this.updateLastActivity();
    this.intentionallyClosed = false;

    this.ws.onmessage = (event) => {
      this.updateLastActivity();
      this.handleWebSocketMessage(event.data);
    };

    this.ws.onclose = () => {
      console.log("WebSocket connection closed");
      this.ws = null;
      // Only reconnect if closure wasn't intentional
      if (!this.intentionallyClosed) {
        setTimeout(() => this.init(), 1000);
      }
    };

    this.ws.onerror = (error) => {
      console.error("WebSocket error:", error);
    };
  }

  handleWebSocketMessage(data: ArrayBuffer) {
    // Forward the binary message to all connected ports
    this.ports.forEach((port) => {
      port.postMessage({
        type: "ws-message",
        data: data,
      });
    });
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

  send(data: Uint8Array) {
    this.intentionallyClosed = false; // Reset the flag when attempting to send
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      this.init();
    }

    // Wait for connection to be ready
    const sendWhenReady = () => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.updateLastActivity();
        this.ws.send(data);
      } else if (!this.intentionallyClosed) {
        // Retry after a short delay
        setTimeout(sendWhenReady, 100);
      }
    };

    sendWhenReady();
  }

  handlePortMessage(port: MessagePort, data: any) {
    if (data.type === "send") {
      const message = data as WorkerMessage;

      // Store which port sent this request for response routing
      this.pendingRequests.set(message.nonce, port);

      // Send the binary data over WebSocket
      this.send(message.data);
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
      worker.handlePortMessage(port, e.data);
    };

    port.onmessageerror = (e) => {
      console.error("Port message error:", e);
    };

    // Clean up when port is closed
    port.addEventListener("close", () => {
      worker.ports.delete(port);
    });

    port.start();
  });
}

main();
