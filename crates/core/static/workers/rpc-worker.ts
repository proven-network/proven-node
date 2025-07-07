/// <reference lib="webworker" />

const INACTIVITY_TIMEOUT = 60000; // 60 seconds in milliseconds
const CHECK_INTERVAL = 5000; // 5 seconds in milliseconds

type WorkerMessage = {
  type: 'send';
  nonce: number;
  data: Uint8Array;
};

class RpcWorker {
  ws: WebSocket | null;
  ports: Set<MessagePort>;
  lastActivity: number;
  timeoutChecker: number | null;
  intentionallyClosed: boolean;
  pendingRequests: Map<number, MessagePort>;
  httpEndpoint: string;
  wsEndpoint: string;

  constructor() {
    this.ws = null;
    this.ports = new Set();
    this.lastActivity = Date.now();
    this.timeoutChecker = null;
    this.intentionallyClosed = false;
    this.pendingRequests = new Map();

    const query = globalThis.location.search;
    const host = globalThis.location.host;
    const path = globalThis.location.pathname;
    const applicationId = path.split('/')[2];

    this.httpEndpoint = `http://${host}/app/${applicationId}/rpc/http${query}`;
    this.wsEndpoint = `ws://${host}/app/${applicationId}/rpc/ws${query}`;

    this.startTimeoutChecker();
  }

  initWebSocket() {
    if (this.ws?.readyState === WebSocket.OPEN || this.intentionallyClosed) return;

    this.ws = new WebSocket(this.wsEndpoint);
    this.ws.binaryType = 'arraybuffer';
    this.updateLastActivity();
    this.intentionallyClosed = false;

    this.ws.onmessage = (event) => {
      this.updateLastActivity();
      this.handleWebSocketMessage(event.data);
    };

    this.ws.onclose = () => {
      this.ws = null;
      // Only reconnect if closure wasn't intentional
      if (!this.intentionallyClosed) {
        setTimeout(() => this.initWebSocket(), 1000);
      }
    };

    this.ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
  }

  handleWebSocketMessage(data: ArrayBuffer) {
    // Forward the binary message to all connected ports
    this.ports.forEach((port) => {
      port.postMessage({
        type: 'ws-message',
        data: data,
      });
    });
  }

  async sendViaHttp(data: Uint8Array, nonce: number): Promise<void> {
    try {
      const response = await fetch(this.httpEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/octet-stream',
        },
        body: data,
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const responseData = await response.arrayBuffer();

      // Send response back to the requesting port
      const port = this.pendingRequests.get(nonce);
      if (port) {
        port.postMessage({
          type: 'http-response',
          nonce: nonce,
          data: responseData,
        });
        this.pendingRequests.delete(nonce);
      }
    } catch (error) {
      console.error('HTTP request failed:', error);

      // Send error back to the requesting port
      const port = this.pendingRequests.get(nonce);
      if (port) {
        port.postMessage({
          type: 'http-error',
          nonce: nonce,
          error: error instanceof Error ? error.message : 'Unknown error',
        });
        this.pendingRequests.delete(nonce);
      }
    }
  }

  sendViaWebSocket(data: Uint8Array) {
    this.intentionallyClosed = false; // Reset the flag when attempting to send
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      this.initWebSocket();
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

  updateLastActivity() {
    this.lastActivity = Date.now();
  }

  startTimeoutChecker() {
    this.timeoutChecker = setInterval(() => {
      if (Date.now() - this.lastActivity > INACTIVITY_TIMEOUT) {
        this.closeWebSocketConnection();
      }
    }, CHECK_INTERVAL) as unknown as number;
  }

  closeWebSocketConnection() {
    if (this.ws) {
      this.intentionallyClosed = true;
      this.ws.close();
      this.ws = null;
    }
  }

  async handlePortMessage(port: MessagePort, data: any) {
    if (data.type === 'send') {
      const message = data as WorkerMessage;

      // Store which port sent this request for response routing
      this.pendingRequests.set(message.nonce, port);

      // Decide transport: use WebSocket if already open, otherwise use HTTP
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.sendViaWebSocket(message.data);
      } else {
        await this.sendViaHttp(message.data, message.nonce);
      }
    }
  }
}

const worker = new RpcWorker();

self.addEventListener('connect', (event: Event) => {
  const connectEvent = event as MessageEvent;
  const port = connectEvent.ports[0];

  if (!port) {
    console.error('RPC Worker: No port found');
    return;
  }

  worker.ports.add(port);
  worker.updateLastActivity();

  port.onmessage = (e) => {
    worker.handlePortMessage(port, e.data);
  };

  port.onmessageerror = (e) => {
    console.error('Port message error:', e);
  };

  // Clean up when port is closed
  port.addEventListener('close', () => {
    worker.ports.delete(port);
  });

  port.start();
});
