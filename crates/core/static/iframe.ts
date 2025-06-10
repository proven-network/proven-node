import { createSession, getSession } from "./sessions";
import { CoseSign1Decoder, CoseSign1Encoder } from "./helpers/cose";
import { bytesToHex, hexToBytes } from "@noble/curves/abstract/utils";
import { WebAuthnClient } from "./webauthn";

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

type WorkerRequest = {
  nonce: number;
  data: Uint8Array;
};

type WorkerResponse = {
  nonce: number;
  data: any;
  error?: string;
};

const iframeId = Math.floor(Math.random() * 1000000);

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

class IframeClient {
  worker: SharedWorker;
  session: any;
  coseEncoder: any;
  coseDecoder: any;
  webAuthnClient: WebAuthnClient;
  pendingRequests = new Map<
    number,
    { resolve: (data: any) => void; reject: (error: Error) => void }
  >();

  constructor() {
    this.webAuthnClient = new WebAuthnClient();
    this.initializeSession();
  }

  async initializeSession() {
    const urlParams = new URLSearchParams(window.location.search);
    const applicationId = urlParams.get("app") || "application_id";

    try {
      let session = await getSession(applicationId);

      if (!session) {
        console.log("Creating new session...");
        session = await createSession(applicationId);
        console.log("Session created!", session);
      }

      this.session = session;
      await this.setupCose();
      await this.setupWorkerCommunication();
      this.setupParentCommunication();
    } catch (error) {
      console.error("Failed to initialize session:", error);
    }
  }

  async setupCose() {
    const externalAad = hexToBytes(this.session.sessionId.replace(/-/g, ""));

    this.coseEncoder = CoseSign1Encoder(this.session.signingKey, externalAad);
    this.coseDecoder = CoseSign1Decoder(this.session.verifyingKey, externalAad);
  }

  async setupWorkerCommunication() {
    this.worker = new SharedWorker(
      `./ws-worker.js?session=${this.session.sessionId}`
    );

    this.worker.port.start();
    this.worker.port.onmessage = (e) => {
      this.handleWorkerMessage(e.data);
    };
  }

  setupParentCommunication() {
    window.addEventListener("message", (event: MessageEvent<unknown>) => {
      // Only handle messages from parent window
      if (event.source !== window.parent) {
        return;
      }

      if (!isSdkMessage(event.data)) {
        console.error("Invalid message format:", event.data);
        return;
      }

      this.handleSdkMessage(event.data);
    });
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
      this.sendResponseToParent({
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
          this.sendResponseToParent({
            type: "response",
            nonce,
            success: false,
            error: error.message,
          });
        },
      });

      // Send to worker
      this.worker.port.postMessage({
        type: "send",
        nonce,
        data: encodedData,
      });
    } catch (error) {
      this.sendResponseToParent({
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
          console.error("Failed to decode COSE message:", decodedResult.error);
        }
      } catch (error) {
        console.error("Error handling WebSocket message:", error);
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

          this.sendResponseToParent({
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
          this.sendResponseToParent({
            type: "response",
            nonce,
            success: false,
            error: data.ExecuteFailure,
          });
        } else {
          this.sendResponseToParent({
            type: "response",
            nonce,
            success: false,
            error: "Unexpected response from execute",
          });
        }
      } else if (originalMessage?.type === "whoAmI") {
        // Handle whoAmI responses
        if (data.WhoAmI) {
          this.sendResponseToParent({
            type: "response",
            nonce,
            success: true,
            data: data.WhoAmI as WhoAmIResponse,
          });
        } else {
          this.sendResponseToParent({
            type: "response",
            nonce,
            success: false,
            error: "WhoAmI response is missing",
          });
        }
      }
    } catch (error) {
      this.sendResponseToParent({
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

  sendResponseToParent(response: IframeResponse) {
    window.parent.postMessage(response, "*");
  }

  // Initialize the client when the page loads
  static init() {
    globalThis.iframeClient = new IframeClient();
  }
}

// Initialize when the page loads
if (globalThis.addEventListener) {
  globalThis.addEventListener("DOMContentLoaded", IframeClient.init);
} else {
  // Fallback for cases where DOMContentLoaded has already fired
  IframeClient.init();
}
