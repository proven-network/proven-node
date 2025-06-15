import { createSession, getSession } from "../../helpers/sessions";
import { CoseSign1Decoder, CoseSign1Encoder } from "../../helpers/cose";
import { bytesToHex, hexToBytes } from "@noble/curves/abstract/utils";
import { MessageBroker, getWindowIdFromUrl } from "../../helpers/broker";

// Generic message types for broker communication
type RpcRequest = {
  type: "rpc_request";
  data: any; // The raw RPC call data to be signed
};

type RpcResponse = {
  success: boolean;
  data?: any;
  error?: string;
};

class RpcClient {
  worker: SharedWorker | null = null;
  session: any;
  coseEncoder: any;
  coseDecoder: any;
  broker: MessageBroker;
  windowId: string;
  pendingRequests = new Map<
    number,
    { resolve: (data: any) => void; reject: (error: Error) => void }
  >();
  private requestCounter = 0;

  constructor() {
    // Extract window ID from URL fragment
    this.windowId = getWindowIdFromUrl() || "unknown";

    // Initialize broker synchronously - will throw if it fails
    this.broker = new MessageBroker(this.windowId, "rpc");

    this.initializeSession();
  }

  async initializeSession() {
    const urlParams = new URLSearchParams(window.location.search);
    const applicationId = urlParams.get("app") || "application_id";

    try {
      let session = await getSession(applicationId);

      if (!session) {
        console.log("RPC: Creating new session...");
        session = await createSession(applicationId);
        console.log("RPC: Session created!", session);
      }

      this.session = session;
      await this.setupCose();
      await this.setupWorkerCommunication();
      await this.initializeBroker();

      console.log("RPC: Client initialized successfully");
    } catch (error) {
      console.error("RPC: Failed to initialize session:", error);
    }
  }

  async setupCose() {
    const externalAad = hexToBytes(this.session.sessionId.replace(/-/g, ""));

    this.coseEncoder = CoseSign1Encoder(this.session.signingKey, externalAad);
    this.coseDecoder = CoseSign1Decoder(this.session.verifyingKey, externalAad);
  }

  async setupWorkerCommunication() {
    this.worker = new SharedWorker(
      `../workers/rpc-worker.js?session=${this.session.sessionId}`
    );

    this.worker.port.start();
    this.worker.port.onmessage = (e) => {
      this.handleWorkerMessage(e.data);
    };
  }

  async initializeBroker() {
    try {
      await this.broker.connect();

      // Set up generic message handler for rpc_request requests
      this.broker.on("rpc_request", async (message, respond) => {
        if (respond) {
          try {
            const result = await this.handleRpcRequest(message.data);
            respond(result);
          } catch (error) {
            respond({
              success: false,
              error: error instanceof Error ? error.message : "Unknown error",
            });
          }
        }
      });

      console.log("RPC: Broker initialized successfully");
    } catch (error) {
      console.error("RPC: Failed to initialize broker:", error);
      throw new Error(
        `RPC: Failed to initialize broker: ${error instanceof Error ? error.message : "Unknown error"}`
      );
    }
  }

  private getNextRequestId(): number {
    return ++this.requestCounter;
  }

  async handleRpcRequest(rpcCallData: any): Promise<RpcResponse> {
    try {
      const requestId = this.getNextRequestId();

      // Sign the RPC call data with COSE
      const encodedData = await this.coseEncoder.encode(rpcCallData, {
        seq: requestId,
      });

      // Create promise for the response
      const responsePromise = new Promise<any>((resolve, reject) => {
        this.pendingRequests.set(requestId, { resolve, reject });

        // Set timeout
        setTimeout(() => {
          if (this.pendingRequests.has(requestId)) {
            this.pendingRequests.delete(requestId);
            reject(new Error("Request timeout"));
          }
        }, 30000); // 30 second timeout
      });

      // Send to worker
      if (this.worker) {
        this.worker.port.postMessage({
          type: "send",
          nonce: requestId,
          data: encodedData,
        });
      } else {
        throw new Error("Worker not initialized");
      }

      // Wait for response
      const responseData = await responsePromise;

      return {
        success: true,
        data: responseData,
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : "Unknown error",
      };
    }
  }

  async handleWorkerMessage(message: any) {
    if (message.type === "ws-message" || message.type === "http-response") {
      try {
        // Decode COSE message
        const data = new Uint8Array(message.data);
        const decodedResult = await this.coseDecoder.decodeAndVerify(data);

        if (decodedResult.isOk()) {
          const { seq } = decodedResult.value.headers;
          if (typeof seq === "number") {
            const pendingRequest = this.pendingRequests.get(seq);
            if (pendingRequest) {
              this.pendingRequests.delete(seq);
              pendingRequest.resolve(decodedResult.value.payload);
            }
          }
        } else {
          console.error(
            "RPC: Failed to decode COSE message:",
            decodedResult.error
          );
        }
      } catch (error) {
        console.error("RPC: Error handling response message:", error);
      }
    } else if (message.type === "http-error") {
      // Handle HTTP errors
      const pendingRequest = this.pendingRequests.get(message.nonce);
      if (pendingRequest) {
        this.pendingRequests.delete(message.nonce);
        pendingRequest.reject(new Error(message.error));
      }
    }
  }

  // Initialize the client when the page loads
  static init() {
    const client = new RpcClient();

    // Make client available globally for debugging
    (globalThis as any).rpcClient = client;
  }
}

// Initialize when the page loads
if (globalThis.addEventListener) {
  globalThis.addEventListener("DOMContentLoaded", RpcClient.init);
} else {
  // Fallback for cases where DOMContentLoaded has already fired
  RpcClient.init();
}
