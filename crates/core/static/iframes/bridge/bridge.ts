import { MessageBroker, getWindowIdFromUrl } from "../../helpers/broker";
import { bytesToHex } from "@noble/curves/abstract/utils";
import type {
  WhoAmI,
  WhoAmIResponse,
  ExecuteHash,
  Execute,
  ExecuteLog,
  ExecutionResult,
} from "../../common";

// Message types for parent ↔ bridge communication
export type WhoAmIMessage = {
  type: "whoAmI";
  nonce: number;
};

export type ExecuteMessage = {
  type: "execute";
  nonce: number;
  data: {
    script: string;
    handler: string;
    args?: any[];
  };
};

export type ParentToBridgeMessage = WhoAmIMessage | ExecuteMessage;

export type ResponseMessage = {
  type: "response";
  nonce: number;
  success: boolean;
  data?: any;
  error?: string;
};

export type OpenModalMessage = {
  type: "open_registration_modal";
};

export type CloseModalMessage = {
  type: "close_registration_modal";
};

export type BridgeToParentMessage =
  | ResponseMessage
  | OpenModalMessage
  | CloseModalMessage;

function isParentMessage(data: unknown): data is ParentToBridgeMessage {
  if (
    typeof data !== "object" ||
    data === null ||
    !("type" in data) ||
    !("nonce" in data) ||
    typeof (data as any).nonce !== "number"
  ) {
    return false;
  }

  const message = data as any;

  if (message.type === "whoAmI") {
    return true;
  }

  if (message.type === "execute") {
    return (
      "data" in message &&
      typeof message.data === "object" &&
      message.data !== null &&
      typeof message.data.script === "string" &&
      typeof message.data.handler === "string"
    );
  }

  return false;
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

      // Set up message handlers for modal events
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

      console.debug("Bridge: Broker initialized successfully");
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
      console.debug("Bridge: Received message from parent:", message);

      if (message.type === "whoAmI") {
        await this.handleWhoAmI(message);
      } else if (message.type === "execute") {
        await this.handleExecute(message);
      } else {
        // This should never happen due to our type guard, but TypeScript requires it
        const exhaustiveCheck: never = message;
        throw new Error(
          `Unknown message type: ${(exhaustiveCheck as any).type}`
        );
      }
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

  async handleWhoAmI(message: WhoAmIMessage) {
    try {
      const rpcCall: WhoAmI = "WhoAmI";

      const response = await this.broker.request<{
        success: boolean;
        data?: any;
        error?: string;
      }>("rpc_request", rpcCall, "rpc");

      if (response.success && response.data?.WhoAmI) {
        this.forwardToParent({
          type: "response",
          nonce: message.nonce,
          success: true,
          data: response.data.WhoAmI as WhoAmIResponse,
        });
      } else {
        this.forwardToParent({
          type: "response",
          nonce: message.nonce,
          success: false,
          error: response.error || "WhoAmI response is missing",
        });
      }
    } catch (error) {
      this.forwardToParent({
        type: "response",
        nonce: message.nonce,
        success: false,
        error:
          error instanceof Error ? error.message : "Failed to execute WhoAmI",
      });
    }
  }

  async handleExecute(message: ExecuteMessage) {
    try {
      const { script, handler, args } = message.data;

      if (!script || !handler) {
        throw new Error("Script and handler are required for execute");
      }

      // Start with ExecuteHash (optimized path)
      const moduleHash = await this.hashScript(script);
      let rpcCall: ExecuteHash = {
        ExecuteHash: [moduleHash, handler, args || []],
      };

      const response = await this.broker.request<{
        success: boolean;
        data?: any;
        error?: string;
      }>("rpc_request", rpcCall, "rpc");

      if (!response.success) {
        throw new Error(response.error || "Failed to execute RPC call");
      }

      // Handle the response
      if (response.data?.ExecuteSuccess) {
        const result = response.data.ExecuteSuccess as ExecutionResult;
        this.handleExecutionResult(result, message.nonce);
      } else if (response.data === "ExecuteHashUnknown") {
        // Retry with full script
        console.debug("Bridge: ExecuteHash unknown, retrying with full script");
        await this.retryWithFullScript(message, script, handler, args || []);
      } else if (response.data?.ExecuteFailure) {
        this.forwardToParent({
          type: "response",
          nonce: message.nonce,
          success: false,
          error: response.data.ExecuteFailure,
        });
      } else {
        this.forwardToParent({
          type: "response",
          nonce: message.nonce,
          success: false,
          error: "Unexpected response from execute",
        });
      }
    } catch (error) {
      this.forwardToParent({
        type: "response",
        nonce: message.nonce,
        success: false,
        error:
          error instanceof Error ? error.message : "Failed to execute script",
      });
    }
  }

  async retryWithFullScript(
    originalMessage: ParentToBridgeMessage,
    script: string,
    handler: string,
    args: any[]
  ) {
    try {
      const fullRpcCall: Execute = {
        Execute: [script, handler, args],
      };

      const response = await this.broker.request<{
        success: boolean;
        data?: any;
        error?: string;
      }>("rpc_request", fullRpcCall, "rpc");

      if (!response.success) {
        throw new Error(response.error || "Failed to execute full script");
      }

      if (response.data?.ExecuteSuccess) {
        const result = response.data.ExecuteSuccess as ExecutionResult;
        this.handleExecutionResult(result, originalMessage.nonce);
      } else if (response.data?.ExecuteFailure) {
        this.forwardToParent({
          type: "response",
          nonce: originalMessage.nonce,
          success: false,
          error: response.data.ExecuteFailure,
        });
      } else {
        this.forwardToParent({
          type: "response",
          nonce: originalMessage.nonce,
          success: false,
          error: "Unexpected response from full script execute",
        });
      }
    } catch (error) {
      this.forwardToParent({
        type: "response",
        nonce: originalMessage.nonce,
        success: false,
        error:
          error instanceof Error
            ? error.message
            : "Failed to retry with full script",
      });
    }
  }

  handleExecutionResult(result: ExecutionResult, nonce: number) {
    if ("Ok" in result) {
      // Handle successful execution
      const successResult = result.Ok;
      this.processExecuteLogs(successResult.logs);

      this.forwardToParent({
        type: "response",
        nonce: nonce,
        success: true,
        data: successResult.output,
      });
    } else if ("Error" in result) {
      // Handle runtime error
      const errorResult = result.Error;
      this.processExecuteLogs(errorResult.logs);

      this.forwardToParent({
        type: "response",
        nonce: nonce,
        success: false,
        error: `Runtime error: ${errorResult.error.message}`,
      });
    } else {
      // This should never happen, but handle it gracefully
      this.forwardToParent({
        type: "response",
        nonce: nonce,
        success: false,
        error: "Invalid execution result format",
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

  forwardToParent(message: BridgeToParentMessage) {
    console.debug("Bridge: Forwarding to parent:", message);
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
