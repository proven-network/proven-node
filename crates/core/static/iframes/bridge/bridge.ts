import { MessageBroker, getWindowIdFromUrl } from "../../helpers/broker";

// Message types for parent ↔ bridge communication
type ParentToBridgeMessage = {
  type: "execute" | "whoAmI";
  nonce: number;
  data?: {
    script?: string;
    handler?: string;
    args?: any[];
  };
};

type BridgeToParentMessage = {
  type: "response" | "open_registration_modal" | "close_registration_modal";
  nonce?: number;
  success?: boolean;
  data?: any;
  error?: string;
};

function isParentMessage(data: unknown): data is ParentToBridgeMessage {
  return (
    typeof data === "object" &&
    data !== null &&
    "type" in data &&
    "nonce" in data &&
    typeof (data as ParentToBridgeMessage).nonce === "number" &&
    ["execute", "whoAmI"].includes((data as ParentToBridgeMessage).type)
  );
}

class BridgeClient {
  broker: MessageBroker;
  windowId: string;

  constructor() {
    // Extract window ID from URL fragment
    this.windowId = getWindowIdFromUrl() || "unknown";

    // Initialize broker synchronously - will throw if it fails
    this.broker = new MessageBroker(this.windowId, "sdk");

    this.initializeBroker();
    this.setupParentListener();
  }

  async initializeBroker() {
    try {
      await this.broker.connect();

      // Set up message handlers - just forward everything to parent
      this.broker.on("response", (message) => {
        this.forwardToParent(message.data);
      });

      this.broker.on("open_registration_modal", (message) => {
        this.forwardToParent({
          type: "open_registration_modal",
        });
      });

      this.broker.on("close_registration_modal", (message) => {
        this.forwardToParent({
          type: "close_registration_modal",
        });
      });

      console.log("Bridge: Broker initialized successfully");
    } catch (error) {
      console.error("Bridge: Failed to initialize broker:", error);
      throw new Error(
        `Bridge: Failed to initialize broker: ${error instanceof Error ? error.message : "Unknown error"}`
      );
    }
  }

  setupParentListener() {
    // Listen for messages from parent SDK
    window.addEventListener("message", (event) => {
      // Only handle messages from parent window
      if (event.source === parent && isParentMessage(event.data)) {
        this.handleParentMessage(event.data);
      }
    });
  }

  async handleParentMessage(message: ParentToBridgeMessage) {
    try {
      console.log("Bridge: Received message from parent:", message);

      // Forward SDK requests directly to RPC iframe via broker
      await this.broker.send(message.type, message, "rpc");
    } catch (error) {
      console.error("Bridge: Error handling parent message:", error);
      this.forwardToParent({
        type: "response",
        nonce: message.nonce,
        success: false,
        error: error instanceof Error ? error.message : "Unknown error",
      });
    }
  }

  forwardToParent(message: BridgeToParentMessage) {
    console.log("Bridge: Forwarding to parent:", message);
    parent.postMessage(message, "*");
  }

  // Initialize the client when the page loads
  static init() {
    const client = new BridgeClient();

    // Make client available globally for debugging
    (globalThis as any).bridgeClient = client;
  }
}

// Initialize when the page loads
if (globalThis.addEventListener) {
  globalThis.addEventListener("DOMContentLoaded", BridgeClient.init);
} else {
  // Fallback for cases where DOMContentLoaded has already fired
  BridgeClient.init();
}
