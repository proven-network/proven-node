/// <reference lib="webworker" />

// SharedWorker to broker messages between iframes within the same tab
// Each tab gets its own worker instance via unique query string

interface BrokerMessage {
  type: string;
  fromIframe: string;
  toIframe?: string; // Optional - if not specified, broadcast to all iframes
  data: any;
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

    // Route message to appropriate iframe(s)
    for (const connection of this.connections) {
      // Don't send message back to sender
      if (connection === fromConnection) {
        continue;
      }

      // If toIframe is specified, only send to that iframe type
      if (message.toIframe && connection.iframeType !== message.toIframe) {
        continue;
      }

      try {
        connection.port.postMessage({
          ...message,
          fromIframe: fromConnection.iframeType,
        });
        console.log(
          `SharedWorker: Forwarded message to ${connection.iframeType}`
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
