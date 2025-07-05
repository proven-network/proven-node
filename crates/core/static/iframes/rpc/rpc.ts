/// <reference lib="DOM" />
import { createSession, getSession } from "@proven-network/common";
import { CoseSign1Decoder, CoseSign1Encoder } from "@proven-network/common";
import { bytesToHex, hexToBytes } from "@noble/curves/abstract/utils";
import { MessageBroker, getWindowIdFromUrl } from "@proven-network/common";

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
  private initializationPromise: Promise<void> | null = null;
  private isInitialized = false;
  private queuedRequests: Array<{
    rpcCallData: any;
    resolve: (response: RpcResponse) => void;
    reject: (error: Error) => void;
  }> = [];

  constructor() {
    // Extract window ID from URL fragment
    this.windowId = getWindowIdFromUrl() || "unknown";

    // Initialize broker synchronously - will throw if it fails
    this.broker = new MessageBroker(this.windowId, "rpc");

    // Initialize broker immediately but defer session initialization
    this.initializeBroker();
  }

  private async ensureInitialized(): Promise<void> {
    if (this.isInitialized) {
      return;
    }

    if (this.initializationPromise) {
      return this.initializationPromise;
    }

    this.initializationPromise = this.initializeSession();
    return this.initializationPromise;
  }

  private async initializeSession(): Promise<void> {
    const urlParams = new URLSearchParams(window.location.search);
    const applicationId = urlParams.get("app") || "application_id";

    try {
      console.debug("RPC: Initializing session...");

      let session = await getSession(applicationId);

      if (!session) {
        console.debug("RPC: Creating new session...");
        session = await createSession(applicationId);
        console.debug("RPC: Session created!", session);
      }

      this.session = session;
      await this.setupCose();
      await this.setupWorkerCommunication();

      this.isInitialized = true;
      console.debug("RPC: Client initialized successfully");

      // Process any queued requests
      await this.processQueuedRequests();
    } catch (error) {
      console.error("RPC: Failed to initialize session:", error);
      this.initializationPromise = null; // Allow retry
      throw error;
    }
  }

  private async processQueuedRequests(): Promise<void> {
    console.debug(
      `RPC: Processing ${this.queuedRequests.length} queued requests`
    );

    const requests = [...this.queuedRequests];
    this.queuedRequests = [];

    for (const request of requests) {
      try {
        const response = await this.executeRpcRequest(request.rpcCallData);
        request.resolve(response);
      } catch (error) {
        request.reject(
          error instanceof Error ? error : new Error("Unknown error")
        );
      }
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

  private async initializeBroker() {
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

      console.debug("RPC: Broker initialized successfully");
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
    // Check if we're initialized
    if (!this.isInitialized) {
      // If not initialized, either queue the request or start initialization
      return new Promise((resolve, reject) => {
        // Queue this request
        this.queuedRequests.push({
          rpcCallData,
          resolve,
          reject,
        });

        // Start initialization if not already started
        this.ensureInitialized().catch((error) => {
          // If initialization fails, reject all queued requests
          const requests = [...this.queuedRequests];
          this.queuedRequests = [];

          for (const request of requests) {
            request.reject(error);
          }
        });
      });
    }

    // If we're initialized, execute the request directly
    return this.executeRpcRequest(rpcCallData);
  }

  private async executeRpcRequest(rpcCallData: any): Promise<RpcResponse> {
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
