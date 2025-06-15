// Helper to communicate with the SharedWorker message broker
// Provides a clean API for iframe-to-iframe communication

interface BrokerMessage {
  type: string;
  fromIframe: string;
  toIframe?: string;
  data: any;
  messageId?: string; // For request-response correlation
  isResponse?: boolean; // To distinguish responses from regular messages
}

interface MessageHandler {
  (message: BrokerMessage, respond?: (responseData: any) => void): void;
}

interface PendingRequest {
  resolve: (value: any) => void;
  reject: (reason?: any) => void;
  timeout: number;
}

export class MessageBroker {
  private worker: SharedWorker | null = null;
  private port: MessagePort | null = null;
  private windowId: string;
  private iframeType: string;
  private messageHandlers: Map<string, MessageHandler[]> = new Map();
  private pendingRequests: Map<string, PendingRequest> = new Map();
  private isConnected: boolean = false;
  private readonly REQUEST_TIMEOUT = 30000; // 30 second timeout

  constructor(windowId: string, iframeType: string) {
    this.windowId = windowId;
    this.iframeType = iframeType;
  }

  async connect(): Promise<void> {
    if (this.isConnected) {
      return;
    }

    try {
      // Create SharedWorker connection with windowId in query string
      this.worker = new SharedWorker(
        `../workers/broker-worker.js?window=${this.windowId}`
      );
      this.port = this.worker.port;

      // Set up message handling
      this.port.onmessage = (event) => {
        this.handleIncomingMessage(event.data);
      };

      this.port.onmessageerror = (error) => {
        console.error(
          `Broker: Connection error for ${this.iframeType}:`,
          error
        );
        this.isConnected = false;
      };

      // Start the port
      this.port.start();

      // Send init message
      this.port.postMessage({
        type: "init",
        iframeType: this.iframeType,
      });

      this.isConnected = true;
      console.debug(`Broker: Connected ${this.iframeType}`);
    } catch (error) {
      console.error(`Broker: Failed to connect ${this.iframeType}:`, error);
      this.isConnected = false;
      throw error;
    }
  }

  private handleIncomingMessage(message: BrokerMessage) {
    console.debug(`Broker: ${this.iframeType} received message:`, message);

    // Handle responses to pending requests
    if (message.isResponse && message.messageId) {
      const pendingRequest = this.pendingRequests.get(message.messageId);
      if (pendingRequest) {
        clearTimeout(pendingRequest.timeout);
        this.pendingRequests.delete(message.messageId);
        pendingRequest.resolve(message.data);
        return;
      } else {
        console.warn(
          `Broker: Received response for unknown request ${message.messageId}`
        );
        return;
      }
    }

    // Handle regular messages
    const handlers = this.messageHandlers.get(message.type);
    if (handlers) {
      handlers.forEach((handler) => {
        try {
          // Create respond function if this message has a messageId (indicating it's a request)
          const respond = message.messageId
            ? (responseData: any) => {
                this.sendResponse(message.messageId!, responseData);
              }
            : undefined;

          handler(message, respond);
        } catch (error) {
          console.error(
            `Broker: Error in message handler for ${message.type}:`,
            error
          );
          // If this was a request and there was an error, send error response
          if (message.messageId) {
            this.sendResponse(message.messageId, null, error.message);
          }
        }
      });
    } else {
      console.warn(
        `Broker: No handlers registered for message type: ${message.type}`
      );
      // If this was a request with no handler, send error response
      if (message.messageId) {
        this.sendResponse(
          message.messageId,
          null,
          `No handler for message type: ${message.type}`
        );
      }
    }
  }

  private async sendResponse(
    messageId: string,
    responseData: any,
    error?: string
  ): Promise<void> {
    if (!this.isConnected) {
      await this.connect();
    }

    if (!this.port) {
      throw new Error("Broker: Not connected");
    }

    const response: BrokerMessage = {
      type: "response",
      fromIframe: this.iframeType,
      data: error ? { error } : responseData,
      messageId,
      isResponse: true,
    };

    console.debug(`Broker: ${this.iframeType} sending response:`, response);
    this.port.postMessage(response);
  }

  private generateMessageId(): string {
    return (
      "msg_" +
      Date.now().toString(36) +
      "_" +
      Math.random().toString(36).substring(2, 15)
    );
  }

  async send(type: string, data: any, toIframe?: string): Promise<void> {
    if (!this.isConnected) {
      await this.connect();
    }

    if (!this.port) {
      throw new Error("Broker: Not connected");
    }

    const message: BrokerMessage = {
      type,
      fromIframe: this.iframeType,
      toIframe,
      data,
    };

    console.debug(`Broker: ${this.iframeType} sending message:`, message);
    this.port.postMessage(message);
  }

  async request<T = any>(
    type: string,
    data: any,
    toIframe: string
  ): Promise<T> {
    if (!toIframe) {
      throw new Error("Broker: request() requires a target iframe type");
    }

    if (!this.isConnected) {
      await this.connect();
    }

    if (!this.port) {
      throw new Error("Broker: Not connected");
    }

    const messageId = this.generateMessageId();

    // Create promise for the response
    const promise = new Promise<T>((resolve, reject) => {
      // Set up timeout
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(messageId);
        reject(new Error(`Request timeout after ${this.REQUEST_TIMEOUT}ms`));
      }, this.REQUEST_TIMEOUT);

      // Store the pending request
      this.pendingRequests.set(messageId, {
        resolve: (value: any) => {
          if (value && value.error) {
            reject(new Error(value.error));
          } else {
            resolve(value);
          }
        },
        reject,
        timeout,
      });
    });

    const message: BrokerMessage = {
      type,
      fromIframe: this.iframeType,
      toIframe,
      data,
      messageId,
    };

    console.debug(`Broker: ${this.iframeType} sending request:`, message);
    this.port.postMessage(message);

    return promise;
  }

  on(messageType: string, handler: MessageHandler): void {
    if (!this.messageHandlers.has(messageType)) {
      this.messageHandlers.set(messageType, []);
    }
    this.messageHandlers.get(messageType)!.push(handler);
    console.debug(
      `Broker: ${this.iframeType} registered handler for ${messageType}`
    );
  }

  off(messageType: string, handler: MessageHandler): void {
    const handlers = this.messageHandlers.get(messageType);
    if (handlers) {
      const index = handlers.indexOf(handler);
      if (index > -1) {
        handlers.splice(index, 1);
        console.debug(
          `Broker: ${this.iframeType} removed handler for ${messageType}`
        );
      }
    }
  }

  disconnect(): void {
    // Clear all pending requests
    this.pendingRequests.forEach((request) => {
      clearTimeout(request.timeout);
      request.reject(new Error("Broker disconnected"));
    });
    this.pendingRequests.clear();

    if (this.port) {
      this.port.close();
      this.port = null;
    }
    this.worker = null;
    this.isConnected = false;
    this.messageHandlers.clear();
    console.debug(`Broker: ${this.iframeType} disconnected`);
  }

  isReady(): boolean {
    return this.isConnected;
  }
}

// Utility function to extract window ID from URL fragment
export function getWindowIdFromUrl(): string | null {
  const fragment = window.location.hash.substring(1); // Remove the #
  if (fragment.startsWith("window=")) {
    return fragment.substring(7); // Remove 'window='
  }
  return null;
}

// Utility function to generate a random window ID
export function generateWindowId(): string {
  return (
    "win_" +
    Math.random().toString(36).substring(2, 15) +
    Math.random().toString(36).substring(2, 15)
  );
}
