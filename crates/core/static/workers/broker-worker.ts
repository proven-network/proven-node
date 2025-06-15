/// <reference lib="webworker" />

// SharedWorker to broker messages between iframes within the same tab
// Each tab gets its own worker instance via unique query string

interface BrokerMessage {
  type: string;
  fromIframe: string;
  toIframe?: string; // Optional - if not specified, broadcast to all iframes
  data: any;
  messageId?: string; // For request-response correlation
  isResponse?: boolean; // To distinguish responses from regular messages
}

interface IframeConnection {
  port: MessagePort;
  iframeType: string; // 'button', 'register', etc.
}

class MessageBroker {
  private connections: IframeConnection[] = [];

  constructor() {
    console.log("SharedWorker: Message broker initialized");
  }

  addConnection(port: MessagePort, iframeType: string) {
    const connection: IframeConnection = {
      port,
      iframeType,
    };

    this.connections.push(connection);

    console.log(`SharedWorker: Added ${iframeType} connection`);
    console.log(`SharedWorker: Now has ${this.connections.length} connections`);

    // Set up message handling for this connection
    port.onmessage = (event) => {
      this.handleMessage(event.data, connection);
    };

    // Clean up when connection closes
    port.onmessageerror = () => {
      this.removeConnection(connection);
    };
  }

  removeConnection(connection: IframeConnection) {
    const index = this.connections.indexOf(connection);
    if (index > -1) {
      this.connections.splice(index, 1);
      console.log(`SharedWorker: Removed ${connection.iframeType} connection`);
    }
  }

  handleMessage(message: BrokerMessage, fromConnection: IframeConnection) {
    console.log(
      `SharedWorker: Message from ${fromConnection.iframeType}:`,
      message
    );

    // Handle responses - these should be routed back to the original requester
    if (message.isResponse && message.messageId) {
      // For responses, we need to route back to the original sender
      // The original sender is determined by looking at the message history
      // Since we don't track request origins, we'll broadcast responses to all other connections
      // and let the client-side broker handle the correlation
      this.routeMessage(message, fromConnection, true);
      return;
    }

    // Handle regular messages and requests
    this.routeMessage(message, fromConnection, false);
  }

  private routeMessage(
    message: BrokerMessage,
    fromConnection: IframeConnection,
    isResponse: boolean
  ) {
    // Route message to appropriate iframe(s)
    for (const connection of this.connections) {
      // Don't send message back to sender (unless it's a response being routed)
      if (connection === fromConnection && !isResponse) {
        continue;
      }

      // For responses, broadcast to all other connections and let client handle correlation
      if (isResponse) {
        if (connection === fromConnection) {
          continue; // Don't send response back to the responder
        }
      } else {
        // For regular messages/requests, apply normal routing rules
        // If toIframe is specified, only send to that iframe type
        if (message.toIframe && connection.iframeType !== message.toIframe) {
          continue;
        }
      }

      try {
        connection.port.postMessage({
          ...message,
          fromIframe: fromConnection.iframeType,
        });
        console.log(
          `SharedWorker: Forwarded ${isResponse ? "response" : "message"} to ${connection.iframeType}`
        );
      } catch (error) {
        console.error(
          `SharedWorker: Failed to send message to ${connection.iframeType}:`,
          error
        );
        // Remove dead connection
        this.removeConnection(connection);
      }
    }
  }
}

const broker = new MessageBroker();

// Handle new connections
self.addEventListener("connect", (event: Event) => {
  const connectEvent = event as MessageEvent;
  const port = connectEvent.ports[0];

  // Wait for initial message with iframe type
  port.onmessage = (initEvent) => {
    const { type, iframeType } = initEvent.data;

    if (type === "init" && iframeType) {
      broker.addConnection(port, iframeType);
    } else {
      console.error("SharedWorker: Invalid init message:", initEvent.data);
      port.close();
    }
  };

  port.start();
});

// Export types for TypeScript consumers
export type { BrokerMessage, IframeConnection };
