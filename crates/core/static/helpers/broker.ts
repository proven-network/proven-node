// Helper to communicate with the SharedWorker message broker
// Provides a clean API for iframe-to-iframe communication

interface BrokerMessage {
  type: string;
  fromIframe: string;
  toIframe?: string;
  data: any;
}

interface MessageHandler {
  (message: BrokerMessage): void;
}

export class MessageBroker {
  private worker: SharedWorker | null = null;
  private port: MessagePort | null = null;
  private windowId: string;
  private iframeType: string;
  private messageHandlers: Map<string, MessageHandler[]> = new Map();
  private isConnected: boolean = false;

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

    const handlers = this.messageHandlers.get(message.type);
    if (handlers) {
      handlers.forEach((handler) => {
        try {
          handler(message);
        } catch (error) {
          console.error(
            `Broker: Error in message handler for ${message.type}:`,
            error
          );
        }
      });
    } else {
      console.warn(
        `Broker: No handlers registered for message type: ${message.type}`
      );
    }
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
