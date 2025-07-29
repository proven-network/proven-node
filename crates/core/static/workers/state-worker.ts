/// <reference lib="webworker" />

// Simple state storage worker - no reactivity, just key-value storage
// Reactivity happens in the SDK via Preact signals

interface WorkerMessage {
  type: 'init' | 'get' | 'set' | 'ping';
  tabId?: string;
  key?: string;
  value?: any;
  timestamp?: number;
}

interface WorkerResponse {
  type: 'value' | 'set_complete' | 'error' | 'pong' | 'init_confirmed';
  key?: string;
  value?: any;
  exists?: boolean;
  tabId?: string;
  error?: string;
  timestamp?: number;
}

// Global state storage for all tabs
class StateWorker {
  private ports: Set<MessagePort> = new Set();
  private storage: Map<string, any> = new Map();

  constructor() {}

  addPort(port: MessagePort) {
    this.ports.add(port);

    // Set up message handling
    port.onmessage = (event) => {
      this.handleMessage(event.data, port);
    };

    // Clean up when port closes
    port.onmessageerror = () => {
      this.removePort(port);
    };
  }

  removePort(port: MessagePort) {
    if (this.ports.has(port)) {
      this.ports.delete(port);
    }
  }

  private handleMessage(message: WorkerMessage, fromPort: MessagePort) {
    switch (message.type) {
      case 'init':
        // Send confirmation that worker is ready
        if (message.tabId) {
          const initResponse: WorkerResponse = {
            type: 'init_confirmed',
            tabId: message.tabId,
          };
          this.sendToPort(fromPort, initResponse);
        }
        break;

      case 'get':
        if (message.key) {
          this.handleGetValue(message.key, fromPort, message.tabId);
        } else {
          this.sendError(fromPort, 'Missing key for get operation');
        }
        break;

      case 'set':
        if (message.key !== undefined) {
          this.handleSetValue(message.key, message.value, fromPort, message.tabId);
        } else {
          this.sendError(fromPort, 'Missing key for set operation');
        }
        break;

      case 'ping': {
        const response: WorkerResponse = {
          type: 'pong',
        };
        if (message.timestamp !== undefined) {
          response.timestamp = message.timestamp;
        }
        this.sendToPort(fromPort, response);
        break;
      }

      default:
        console.warn('State Worker: Unknown message type:', message.type);
        this.sendError(fromPort, `Unknown message type: ${message.type}`);
    }
  }

  private handleGetValue(key: string, fromPort: MessagePort, tabId?: string) {
    const exists = this.storage.has(key);
    const value = exists ? this.storage.get(key) : undefined;

    const response: WorkerResponse = {
      type: 'value',
      key,
      value,
      exists,
    };
    if (tabId !== undefined) {
      response.tabId = tabId;
    }
    this.sendToPort(fromPort, response);
  }

  private handleSetValue(key: string, value: any, fromPort: MessagePort, tabId?: string) {
    // Store the value
    this.storage.set(key, value);

    // Confirm to the sender
    const response: WorkerResponse = {
      type: 'set_complete',
      key,
      value,
    };
    if (tabId !== undefined) {
      response.tabId = tabId;
    }
    this.sendToPort(fromPort, response);

    // Broadcast the update to all other ports (cross-tab sync)
    this.broadcastUpdate(key, value, fromPort);
  }

  private broadcastUpdate(key: string, value: any, excludePort: MessagePort) {
    this.ports.forEach((port) => {
      if (port !== excludePort) {
        try {
          port.postMessage({
            type: 'value',
            key,
            value,
            exists: true,
          });
        } catch (error) {
          console.error('State Worker: Failed to broadcast to port:', error);
          this.removePort(port);
        }
      }
    });
  }

  private sendToPort(port: MessagePort, response: WorkerResponse) {
    try {
      port.postMessage(response);
    } catch (error) {
      console.error('State Worker: Failed to send to specific port:', error);
      this.removePort(port);
    }
  }

  private sendError(port: MessagePort, error: string) {
    this.sendToPort(port, {
      type: 'error',
      error,
    });
  }

  cleanup() {
    // Close all ports
    this.ports.forEach((port) => {
      try {
        port.close();
      } catch (error) {
        console.error('State Worker: Error closing port:', error);
      }
    });

    this.ports.clear();
    this.storage.clear();
  }
}

const stateWorker = new StateWorker();

// Handle new connections
self.addEventListener('connect', (event: Event) => {
  const connectEvent = event as MessageEvent;
  const port = connectEvent.ports[0];

  if (!port) {
    console.error('State Worker: No port found');
    return;
  }

  // Wait for initial message with tab ID
  port.onmessage = (initEvent) => {
    const { type, tabId } = initEvent.data;

    if (type === 'init' && tabId) {
      console.log('State Worker: Received init message from tab:', tabId);
      stateWorker.addPort(port);

      // Send init confirmation immediately
      const initResponse: WorkerResponse = {
        type: 'init_confirmed',
        tabId: tabId as string, // We already checked it exists above
      };
      port.postMessage(initResponse);
      console.log('State Worker: Sent init_confirmed to tab:', tabId);
    } else {
      console.error('State Worker: Invalid init message:', initEvent.data);
      port.close();
    }
  };

  port.start();
});

// Cleanup on worker termination
self.addEventListener('beforeunload', () => {
  stateWorker.cleanup();
});

// Export types for TypeScript consumers
export type { WorkerMessage, WorkerResponse };
