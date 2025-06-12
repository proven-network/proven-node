import { createSession, getSession } from "../../helpers/sessions";
import { CoseSign1Decoder, CoseSign1Encoder } from "../../helpers/cose";
import { bytesToHex, hexToBytes } from "@noble/curves/abstract/utils";
import { MessageBroker, getWindowIdFromUrl } from "../../helpers/broker";

type WhoAmI = "WhoAmI";
type WhoAmIResponse = { identity_address: string; account_addresses: string[] };

type ExecuteHash = { ExecuteHash: [string, string, any[]] };
type Execute = {
  Execute: [string, string, any[]];
};
type ExecuteOutput = string | number | boolean | null | undefined;
type ExecuteLog = {
  level: string;
  args: ExecuteOutput[];
};
type ExecuteSuccess = {
  output: ExecuteOutput;
  duration: {
    secs: number;
    nanos: number;
  };
  logs: ExecuteLog[];
};

type RpcCall = WhoAmI | ExecuteHash | Execute;

type SdkMessage = {
  type: "execute" | "whoAmI";
  nonce: number;
  data?: {
    script?: string;
    handler?: string;
    args?: any[];
  };
};

type IframeResponse = {
  type: "response";
  nonce: number;
  success: boolean;
  data?: any;
  error?: string;
};

function isSdkMessage(data: unknown): data is SdkMessage {
  return (
    typeof data === "object" &&
    data !== null &&
    "type" in data &&
    "nonce" in data &&
    typeof (data as SdkMessage).nonce === "number" &&
    ["execute", "whoAmI"].includes((data as SdkMessage).type)
  );
}

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

      // Set up message handlers for direct SDK requests from bridge
      this.broker.on("execute", (message) => {
        if (isSdkMessage(message.data)) {
          this.handleSdkMessage(message.data);
        }
      });

      this.broker.on("whoAmI", (message) => {
        if (isSdkMessage(message.data)) {
          this.handleSdkMessage(message.data);
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

  async handleSdkMessage(message: SdkMessage) {
    try {
      let rpcCall: RpcCall;

      if (message.type === "whoAmI") {
        rpcCall = "WhoAmI";
      } else if (message.type === "execute") {
        const { script, handler, args } = message.data!;

        // Hash the script and handler for ExecuteHash first
        const moduleHash = await this.hashScript(script!);
        rpcCall = { ExecuteHash: [moduleHash, handler!, args || []] };
      } else {
        throw new Error(`Unknown message type: ${message.type}`);
      }

      await this.sendRpcCall(message.nonce, rpcCall, message);
    } catch (error) {
      this.sendResponse({
        type: "response",
        nonce: message.nonce,
        success: false,
        error: error instanceof Error ? error.message : "Unknown error",
      });
    }
  }

  async hashScript(script: string): Promise<string> {
    const rawHash = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(`${script}`)
    );
    return bytesToHex(new Uint8Array(rawHash));
  }

  async sendRpcCall(
    nonce: number,
    rpcCall: RpcCall,
    originalMessage?: SdkMessage
  ) {
    try {
      // Encode the RPC call with COSE
      const encodedData = await this.coseEncoder.encode(rpcCall, {
        seq: nonce,
      });

      // Store the pending request
      this.pendingRequests.set(nonce, {
        resolve: (data) => {
          this.handleRpcResponse(nonce, data, originalMessage);
        },
        reject: (error) => {
          this.sendResponse({
            type: "response",
            nonce,
            success: false,
            error: error.message,
          });
        },
      });

      // Send to worker
      if (this.worker) {
        this.worker.port.postMessage({
          type: "send",
          nonce,
          data: encodedData,
        });
      }
    } catch (error) {
      this.sendResponse({
        type: "response",
        nonce,
        success: false,
        error:
          error instanceof Error ? error.message : "Failed to encode request",
      });
    }
  }

  async handleWorkerMessage(message: any) {
    if (message.type === "ws-message") {
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
        console.error("RPC: Error handling WebSocket message:", error);
      }
    }
  }

  async handleRpcResponse(
    nonce: number,
    data: any,
    originalMessage?: SdkMessage
  ) {
    try {
      if (originalMessage?.type === "execute") {
        // Handle execute responses
        if (data.ExecuteSuccess) {
          const result = data.ExecuteSuccess.Ok as ExecuteSuccess;
          this.processExecuteLogs(result.logs);

          this.sendResponse({
            type: "response",
            nonce,
            success: true,
            data: result.output,
          });
        } else if (data === "ExecuteHashUnknown") {
          // Retry with full script
          const { script, handler, args } = originalMessage.data!;
          const fullRpcCall: Execute = {
            Execute: [script!, handler!, args || []],
          };

          await this.sendRpcCall(nonce, fullRpcCall, originalMessage);
        } else if (data.ExecuteFailure) {
          this.sendResponse({
            type: "response",
            nonce,
            success: false,
            error: data.ExecuteFailure,
          });
        } else {
          this.sendResponse({
            type: "response",
            nonce,
            success: false,
            error: "Unexpected response from execute",
          });
        }
      } else if (originalMessage?.type === "whoAmI") {
        // Handle whoAmI responses
        if (data.WhoAmI) {
          this.sendResponse({
            type: "response",
            nonce,
            success: true,
            data: data.WhoAmI as WhoAmIResponse,
          });
        } else {
          this.sendResponse({
            type: "response",
            nonce,
            success: false,
            error: "WhoAmI response is missing",
          });
        }
      }
    } catch (error) {
      this.sendResponse({
        type: "response",
        nonce,
        success: false,
        error: error instanceof Error ? error.message : "Unknown error",
      });
    }
  }

  processExecuteLogs(logs: ExecuteLog[]) {
    logs.forEach((log) => {
      if (log.level === "log") {
        console.log(...log.args);
      } else if (log.level === "error") {
        console.error(...log.args);
      } else if (log.level === "warn") {
        console.warn(...log.args);
      } else if (log.level === "debug") {
        console.debug(...log.args);
      } else if (log.level === "info") {
        console.info(...log.args);
      }
    });
  }

  async sendResponse(response: IframeResponse) {
    await this.broker.send("response", response, "sdk");
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
