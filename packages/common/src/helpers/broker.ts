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
  timeout: ReturnType<typeof setTimeout>;
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
  private messageQueue: BrokerMessage[] = [];
  private connectionPromise: Promise<void> | null = null;
  private retryCount: number = 0;
  private readonly MAX_RETRIES = 3;
  private readonly INITIAL_RETRY_DELAY = 1000; // 1 second
  private healthCheckInterval: ReturnType<typeof setInterval> | null = null;
  private readonly HEALTH_CHECK_INTERVAL = 30000; // 30 seconds
  private lastActivityTime: number = Date.now();

  constructor(windowId: string, iframeType: string) {
    this.windowId = windowId;
    this.iframeType = iframeType;
  }

  async connect(): Promise<void> {
    if (this.isConnected) {
      return;
    }

    // Return existing connection promise if one is in progress
    if (this.connectionPromise) {
      return this.connectionPromise;
    }

    this.connectionPromise = this.doConnect();
    return this.connectionPromise;
  }

  private async doConnect(): Promise<void> {
    let lastError: Error | null = null;

    for (this.retryCount = 0; this.retryCount <= this.MAX_RETRIES; this.retryCount++) {
      try {
        if (this.retryCount > 0) {
          const delay = this.INITIAL_RETRY_DELAY * Math.pow(2, this.retryCount - 1);
          console.debug(
            `Broker: Retrying connection for ${this.iframeType} (attempt ${this.retryCount}/${this.MAX_RETRIES}) after ${delay}ms`
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
        }

        // Create SharedWorker connection with windowId in query string
        this.worker = new SharedWorker(`../workers/broker-worker.js?window=${this.windowId}`);
        this.port = this.worker.port;

        // Set up message handling
        this.port.onmessage = (event) => {
          this.handleIncomingMessage(event.data);
        };

        this.port.onmessageerror = (error) => {
          console.error(`Broker: Connection error for ${this.iframeType}:`, error);
          this.isConnected = false;
          this.connectionPromise = null; // Allow reconnection attempts
        };

        // Start the port
        this.port.start();

        // Send init message and wait for confirmation
        await this.sendInitMessage();

        this.isConnected = true;
        this.retryCount = 0; // Reset retry count on successful connection
        this.lastActivityTime = Date.now();
        console.debug(`Broker: Connected ${this.iframeType}`);

        // Start health checks
        this.startHealthChecks();

        // Flush any queued messages
        this.flushMessageQueue();
        return; // Success, exit retry loop
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        console.error(
          `Broker: Connection attempt ${this.retryCount + 1} failed for ${this.iframeType}:`,
          lastError
        );

        // Clean up on failure
        this.isConnected = false;
        if (this.port) {
          this.port.close();
          this.port = null;
        }
        this.worker = null;

        // Don't retry if this is the last attempt
        if (this.retryCount === this.MAX_RETRIES) {
          break;
        }
      }
    }

    // All retries failed
    this.connectionPromise = null;
    throw new Error(
      `Failed to connect ${this.iframeType} after ${this.MAX_RETRIES + 1} attempts. Last error: ${lastError?.message}`
    );
  }

  private async sendInitMessage(): Promise<void> {
    if (!this.port) {
      throw new Error('Port not available');
    }

    return new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error(`Init message timeout for ${this.iframeType}`));
      }, 5000); // 5 second timeout for init

      // Listen for init confirmation
      const handleInitConfirmation = (event: MessageEvent) => {
        if (event.data.type === 'init_confirmed') {
          clearTimeout(timeout);
          this.port!.removeEventListener('message', handleInitConfirmation);
          resolve();
        }
      };

      this.port!.addEventListener('message', handleInitConfirmation);

      // Send init message
      this.port!.postMessage({
        type: 'init',
        iframeType: this.iframeType,
      });
    });
  }

  private flushMessageQueue(): void {
    if (this.messageQueue.length === 0) {
      return;
    }

    console.debug(
      `Broker: Flushing ${this.messageQueue.length} queued messages for ${this.iframeType}`
    );

    const queuedMessages = [...this.messageQueue];
    this.messageQueue = [];

    queuedMessages.forEach((message) => {
      if (this.port) {
        this.port.postMessage(message);
      }
    });
  }

  private handleIncomingMessage(message: BrokerMessage) {
    console.debug(`Broker: ${this.iframeType} received message:`, message);
    this.lastActivityTime = Date.now();

    // Handle responses to pending requests
    if (message.isResponse && message.messageId) {
      // Responses should always be directed to a particular iframe
      if (!message.toIframe) {
        return;
      }

      // Ignore responses to messages that are not directed to this iframe
      if (message.toIframe !== this.iframeType) {
        return;
      }

      const pendingRequest = this.pendingRequests.get(message.messageId);
      if (pendingRequest) {
        clearTimeout(pendingRequest.timeout);
        this.pendingRequests.delete(message.messageId);
        pendingRequest.resolve(message.data);
        return;
      } else {
        console.warn(`Broker: Received response for unknown request ${message.messageId}`);
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
                this.sendResponse(message.messageId!, message.fromIframe, responseData);
              }
            : undefined;

          handler(message, respond);
        } catch (error) {
          console.error(`Broker: Error in message handler for ${message.type}:`, error);
          // If this was a request and there was an error, send error response
          if (message.messageId) {
            this.sendResponse(
              message.messageId,
              message.fromIframe,
              null,
              error instanceof Error ? error.message : String(error)
            );
          }
        }
      });
    } else {
      console.warn(`Broker: No handlers registered for message type: ${message.type}`);
      // If this was a request with no handler, send error response
      if (message.messageId) {
        this.sendResponse(
          message.messageId,
          message.fromIframe,
          null,
          `No handler for message type: ${message.type}`
        );
      }
    }
  }

  private async sendResponse(
    messageId: string,
    respondingTo: string,
    responseData: any,
    error?: string
  ): Promise<void> {
    if (!this.isConnected) {
      await this.connect();
    }

    if (!this.port) {
      throw new Error('Broker: Not connected');
    }

    const response: BrokerMessage = {
      type: 'response',
      fromIframe: this.iframeType,
      toIframe: respondingTo,
      data: error ? { error } : responseData,
      messageId,
      isResponse: true,
    };

    console.debug(`Broker: ${this.iframeType} sending response:`, response);
    this.port.postMessage(response);
  }

  async send(type: string, data: any, toIframe?: string): Promise<void> {
    const message: BrokerMessage = {
      type,
      fromIframe: this.iframeType,
      data,
      ...(toIframe !== undefined && { toIframe }),
    };

    if (!this.isConnected) {
      // Queue the message if not connected
      console.debug(`Broker: ${this.iframeType} queuing message (not connected):`, message);
      this.messageQueue.push(message);

      // Try to connect in the background
      this.connect().catch((error) => {
        console.error(`Broker: Failed to connect while sending message:`, error);
      });

      return;
    }

    if (!this.port) {
      throw new Error('Broker: Not connected');
    }

    console.debug(`Broker: ${this.iframeType} sending message:`, message);
    this.port.postMessage(message);
  }

  async request<T = any>(type: string, data: any, toIframe: string): Promise<T> {
    if (!toIframe) {
      throw new Error('Broker: request() requires a target iframe type');
    }

    // Always ensure we're connected before making a request
    if (!this.isConnected) {
      await this.connect();
    }

    if (!this.port) {
      throw new Error('Broker: Not connected');
    }

    const messageId = crypto.randomUUID();

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
    console.debug(`Broker: ${this.iframeType} registered handler for ${messageType}`);
  }

  off(messageType: string, handler: MessageHandler): void {
    const handlers = this.messageHandlers.get(messageType);
    if (handlers) {
      const index = handlers.indexOf(handler);
      if (index > -1) {
        handlers.splice(index, 1);
        console.debug(`Broker: ${this.iframeType} removed handler for ${messageType}`);
      }
    }
  }

  disconnect(): void {
    // Stop health checks
    this.stopHealthChecks();

    // Clear all pending requests
    this.pendingRequests.forEach((request) => {
      clearTimeout(request.timeout);
      request.reject(new Error('Broker disconnected'));
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

  private startHealthChecks(): void {
    this.stopHealthChecks(); // Ensure no duplicate intervals

    this.healthCheckInterval = setInterval(() => {
      this.performHealthCheck();
    }, this.HEALTH_CHECK_INTERVAL);
  }

  private stopHealthChecks(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
    }
  }

  private async performHealthCheck(): Promise<void> {
    if (!this.isConnected) {
      return;
    }

    const timeSinceLastActivity = Date.now() - this.lastActivityTime;

    // If no activity for twice the health check interval, consider connection stale
    if (timeSinceLastActivity > this.HEALTH_CHECK_INTERVAL * 2) {
      console.warn(
        `Broker: No activity for ${timeSinceLastActivity}ms, testing connection for ${this.iframeType}`
      );

      try {
        // Send a ping message to test the connection
        await this.send('ping', { timestamp: Date.now() });
        console.debug(`Broker: Health check passed for ${this.iframeType}`);
      } catch (error) {
        console.error(`Broker: Health check failed for ${this.iframeType}:`, error);

        // Mark as disconnected and attempt reconnection
        this.isConnected = false;
        this.connectionPromise = null;

        // Try to reconnect
        this.connect().catch((reconnectError) => {
          console.error(`Broker: Reconnection failed for ${this.iframeType}:`, reconnectError);
        });
      }
    }
  }

  isReady(): boolean {
    return this.isConnected;
  }
}

// Utility function to extract window ID from URL fragment
export function getWindowIdFromUrl(): string | null {
  const fragment = window.location.hash.substring(1); // Remove the #
  if (fragment.startsWith('window=')) {
    return fragment.substring(7); // Remove 'window='
  }
  return null;
}

// Utility function to generate a random window ID
export function generateWindowId(): string {
  return (
    'win_' +
    Math.random().toString(36).substring(2, 15) +
    Math.random().toString(36).substring(2, 15)
  );
}
