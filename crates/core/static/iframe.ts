import { Proof } from "@radixdlt/radix-dapp-toolkit";

type RequestAttestation = {
  type: "requestAttestation";
  nonce: string;
};

type RequestRolaChallenge = {
  type: "requestRolaChallenge";
};

type VerifyRolaProofs = {
  type: "verifyRolaProofs";
  proofs: Proof[];
};

type Request = RequestAttestation | RequestRolaChallenge | VerifyRolaProofs;
type ParentMessage = { nonce: number; request: Request };

function isParentMessage(data: unknown): data is ParentMessage {
  return (
    typeof data === "object" &&
    data !== null &&
    "nonce" in data &&
    typeof (data as ParentMessage).nonce === "number" &&
    "request" in data
  );
}

class IframeClient {
  worker: SharedWorker;

  constructor() {
    this.worker = new SharedWorker("./ws-worker.js");
    this.setupWorkerCommunication();
    this.setupParentCommunication();
  }

  setupWorkerCommunication() {
    this.worker.port.start();
    this.worker.port.onmessage = (e) => {
      if (e.data.type === "ws-message") {
        // Handle WebSocket messages
        this.handleWebSocketMessage(e.data.data);
      }
    };
  }

  setupParentCommunication() {
    window.addEventListener("message", (event: MessageEvent<unknown>) => {
      // Check if origin is allowed
      if (event.origin !== "https://example.com") {
        console.error("Unauthorized origin:", event.origin);
        return;
      }

      // Type guard check
      if (!isParentMessage(event.data)) {
        console.error("Invalid message format:", event.data);
        return;
      }

      // Handle messages from parent
      this.handleParentMessage(event.data);
    });
  }

  handleWebSocketMessage(data) {
    // Forward WebSocket messages to parent
    window.parent.postMessage(
      {
        type: "ws-message",
        data: data,
      },
      "https://example.com"
    );
  }

  handleParentMessage(data: ParentMessage) {
    // Forward parent messages to WebSocket
    if (data.type === "ws-send") {
      this.worker.port.postMessage({
        type: "ws-send",
        message: data.message,
      });
    }
  }

  // Initialize the client when the page loads
  static init() {
    globalThis.iframeClient = new IframeClient();
  }
}

// Initialize when the page loads
globalThis.addEventListener("DOMContentLoaded", IframeClient.init);
