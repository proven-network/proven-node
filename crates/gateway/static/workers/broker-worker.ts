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

  constructor() {}

  addConnection(port: MessagePort, iframeType: string) {
    const connection: IframeConnection = {
      port,
      iframeType,
    };

    this.connections.push(connection);

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
    }
  }

  handleMessage(message: BrokerMessage, fromConnection: IframeConnection) {
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
      // Always exclude sender from receiving their own messages/broadcasts
      if (connection === fromConnection) {
        continue;
      }

      // For responses, broadcast to all other connections and let client handle correlation
      if (isResponse) {
        // Already excluded sender above, so continue to all other connections
      } else {
        // For regular messages/requests, apply routing rules
        // If toIframe is specified, only send to that iframe type
        if (message.toIframe && connection.iframeType !== message.toIframe) {
          continue;
        }
        // If no toIframe (broadcast), send to all other connections (sender already excluded)
      }

      try {
        connection.port.postMessage({
          ...message,
          fromIframe: fromConnection.iframeType,
        });
      } catch (error) {
        console.error(`Broker Worker: Failed to send message to ${connection.iframeType}:`, error);
        // Remove dead connection
        this.removeConnection(connection);
      }
    }
  }
}

const broker = new MessageBroker();

// Handle new connections
self.addEventListener('connect', (event: Event) => {
  const connectEvent = event as MessageEvent;
  const port = connectEvent.ports[0];

  if (!port) {
    console.error('Broker Worker: No port found');
    return;
  }

  // Wait for initial message with iframe type
  port.onmessage = (initEvent) => {
    const { type, iframeType } = initEvent.data;

    if (type === 'init' && iframeType) {
      broker.addConnection(port, iframeType);

      // Send confirmation back to the iframe
      port.postMessage({
        type: 'init_confirmed',
        iframeType: iframeType,
      });
    } else {
      console.error('Broker Worker: Invalid init message:', initEvent.data);
      port.close();
    }
  };

  port.start();
});

// Export types for TypeScript consumers
export type { BrokerMessage, IframeConnection };
