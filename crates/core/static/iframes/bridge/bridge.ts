import { MessageBroker, getWindowIdFromUrl } from "../../helpers/broker";
import { bytesToHex } from "@noble/curves/abstract/utils";
import type {
  WhoAmI,
  WhoAmIResult,
  ExecuteHash,
  Execute,
  ExecuteLog,
  ExecutionResult,
  ExecuteError,
  ExecuteResult,
  ExecuteHashResult,
  RpcResponse,
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
      const rpcCall: WhoAmI = { type: "WhoAmI", data: null };

      const response = await this.broker.request<{
        success: boolean;
        data?: RpcResponse<WhoAmIResult>;
        error?: string;
      }>("rpc_request", rpcCall, "rpc");

      if (response.success && response.data?.type === "WhoAmI") {
        this.forwardToParent({
          type: "response",
          nonce: message.nonce,
          success: true,
          data: response.data.data, // Extract the WhoAmIResult from the tagged response
        });
      } else {
        this.forwardToParent({
          type: "response",
          nonce: message.nonce,
          success: false,
          error: response.error || "WhoAmI response is missing or malformed",
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
        type: "ExecuteHash",
        data: {
          args: args || [],
          handler_specifier: handler,
          module_hash: moduleHash,
        },
      };

      const response = await this.broker.request<{
        success: boolean;
        data?: RpcResponse<ExecuteHashResult>;
        error?: string;
      }>("rpc_request", rpcCall, "rpc");

      if (!response.success) {
        throw new Error(response.error || "Failed to execute RPC call");
      }

      if (!response.data || response.data.type !== "ExecuteHash") {
        throw new Error("Invalid response format from ExecuteHash");
      }

      const executeHashResult = response.data.data;

      // Handle the different result types
      if (executeHashResult.result === "success") {
        const executionResult = executeHashResult.data as ExecutionResult;
        this.handleExecutionResult(executionResult, message.nonce);
      } else if (executeHashResult.result === "error") {
        // This could be HashUnknown - retry with full script
        console.debug("Bridge: ExecuteHash unknown, retrying with full script");
        await this.retryWithFullScript(message, script, handler, args || []);
      } else if (executeHashResult.result === "failure") {
        this.forwardToParent({
          type: "response",
          nonce: message.nonce,
          success: false,
          error: executeHashResult.data as string,
        });
      } else {
        this.forwardToParent({
          type: "response",
          nonce: message.nonce,
          success: false,
          error: "Unexpected response format from ExecuteHash",
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
        type: "Execute",
        data: {
          module: script,
          handler_specifier: handler,
          args: args,
        },
      };

      const response = await this.broker.request<{
        success: boolean;
        data?: RpcResponse<ExecuteResult>;
        error?: string;
      }>("rpc_request", fullRpcCall, "rpc");

      if (!response.success) {
        throw new Error(response.error || "Failed to execute full script");
      }

      if (!response.data || response.data.type !== "Execute") {
        throw new Error("Invalid response format from Execute");
      }

      const executeResult = response.data.data;

      if (executeResult.result === "success") {
        const executionResult = executeResult.data as ExecutionResult;
        this.handleExecutionResult(executionResult, originalMessage.nonce);
      } else if (
        executeResult.result === "failure" ||
        executeResult.result === "error"
      ) {
        this.forwardToParent({
          type: "response",
          nonce: originalMessage.nonce,
          success: false,
          error: executeResult.data as string,
        });
      } else {
        this.forwardToParent({
          type: "response",
          nonce: originalMessage.nonce,
          success: false,
          error: "Unexpected response format from Execute",
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
    // Send the full ExecutionResult back to the SDK to handle
    this.forwardToParent({
      type: "response",
      nonce: nonce,
      success: true,
      data: result,
    });
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
