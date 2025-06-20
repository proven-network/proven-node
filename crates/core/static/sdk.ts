import { generateWindowId } from "./helpers/broker";
import type {
  ExecuteOutput,
  WhoAmIResponse,
  ExecutionResult,
  ExecuteError,
} from "./common";
import type {
  ParentToBridgeMessage,
  BridgeToParentMessage,
  ResponseMessage,
  OpenModalMessage,
  CloseModalMessage,
} from "./iframes/bridge/bridge";

export type ProvenSDK = {
  execute: (
    script: string,
    handler: string,
    args?: any[]
  ) => Promise<ExecuteOutput>;
  whoAmI: () => Promise<WhoAmIResponse>;
  initConnectButton: (targetElement?: HTMLElement | string) => Promise<void>;
};

export type Logger = {
  debug: (message: string, data?: any) => void;
  log: (message: string, data?: any) => void;
  error: (message: string, data?: any) => void;
  info: (message: string, data?: any) => void;
  warn: (message: string, data?: any) => void;
};

export const ProvenSDK = (options: {
  logger?: Logger;
  authGatewayOrigin: string;
  applicationId: string;
}): ProvenSDK => {
  const { logger, authGatewayOrigin, applicationId } = options;

  // Build iframe URLs from well-known paths
  const bridgeIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/bridge.html`;
  const connectIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/connect.html`;
  const registerIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/register.html`;
  const rpcIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/rpc.html`;

  // Generate unique window ID for this SDK instance
  const windowId = generateWindowId();
  logger?.debug("SDK: Generated window ID:", windowId);

  let bridgeIframe: HTMLIFrameElement | null = null;
  let connectIframe: HTMLIFrameElement | null = null;
  let rpcIframe: HTMLIFrameElement | null = null;
  let modalIframe: HTMLIFrameElement | null = null;
  let modalOverlay: HTMLDivElement | null = null;
  let bridgeIframeReady = false;
  let rpcIframeReady = false;
  let connectIframeReady = false;
  let nonce = 0;
  const pendingCallbacks = new Map<
    number,
    { resolve: (data: any) => void; reject: (error: Error) => void }
  >();

  const createModalOverlay = (): HTMLDivElement => {
    const overlay = document.createElement("div");
    overlay.style.cssText = `
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      z-index: 10000;
    `;

    return overlay;
  };

  const openRegistrationModal = (): void => {
    if (modalIframe && modalOverlay) {
      // Modal already open
      return;
    }

    logger?.debug("SDK: Opening registration modal");

    // Create overlay
    modalOverlay = createModalOverlay();

    // Create modal iframe
    modalIframe = document.createElement("iframe");
    modalIframe.src = `${registerIframeUrl}?app=${applicationId}#window=${windowId}`;
    modalIframe.setAttribute(
      "sandbox",
      "allow-scripts allow-same-origin allow-forms"
    );
    modalIframe.setAttribute("allow", "publickey-credentials-create *;");

    // Style the modal iframe to fill the screen
    modalIframe.style.cssText = `
      width: 100%;
      height: 100%;
      border: none;
      background: transparent;
    `;

    modalOverlay.appendChild(modalIframe);
    document.body.appendChild(modalOverlay);

    // Send init message to modal after it loads
    modalIframe.onload = () => {
      setTimeout(() => {
        if (modalIframe && modalIframe.contentWindow) {
          modalIframe.contentWindow.postMessage(
            {
              type: "init_registration",
            },
            "*"
          );
        }
      }, 100);
    };
  };

  const closeRegistrationModal = (): void => {
    logger?.debug("SDK: Closing registration modal");

    if (modalOverlay && modalOverlay.parentNode) {
      modalOverlay.parentNode.removeChild(modalOverlay);
    }

    modalIframe = null;
    modalOverlay = null;
  };

  const createBridgeIframe = (): Promise<void> => {
    return new Promise((resolve, reject) => {
      if (bridgeIframe && bridgeIframeReady) {
        resolve();
        return;
      }

      bridgeIframe = document.createElement("iframe");
      bridgeIframe.src = `${bridgeIframeUrl}?app=${applicationId}#window=${windowId}`;
      bridgeIframe.setAttribute("sandbox", "allow-scripts allow-same-origin");
      bridgeIframe.setAttribute(
        "allow",
        "publickey-credentials-create *; publickey-credentials-get *"
      );

      // Hide the bridge iframe as it's only for communication
      bridgeIframe.style.cssText = `
        position: absolute;
        width: 1px;
        height: 1px;
        top: -1000px;
        left: -1000px;
        border: none;
        visibility: hidden;
      `;

      bridgeIframe.onload = () => {
        // Wait a bit for iframe to initialize
        setTimeout(() => {
          bridgeIframeReady = true;
          resolve();
        }, 100);
      };

      bridgeIframe.onerror = () => {
        reject(new Error("Failed to load bridge iframe"));
      };

      // Append bridge iframe to document body (hidden)
      document.body.appendChild(bridgeIframe);
    });
  };

  const createRpcIframe = (): Promise<void> => {
    return new Promise((resolve, reject) => {
      if (rpcIframe && rpcIframeReady) {
        resolve();
        return;
      }

      rpcIframe = document.createElement("iframe");
      rpcIframe.src = `${rpcIframeUrl}?app=${applicationId}#window=${windowId}`;
      rpcIframe.setAttribute("sandbox", "allow-scripts allow-same-origin");

      // Hide the RPC iframe as it's only for communication
      rpcIframe.style.cssText = `
        position: absolute;
        width: 1px;
        height: 1px;
        top: -1000px;
        left: -1000px;
        border: none;
        visibility: hidden;
      `;

      rpcIframe.onload = () => {
        // Wait a bit for iframe to initialize
        setTimeout(() => {
          rpcIframeReady = true;
          resolve();
        }, 100);
      };

      rpcIframe.onerror = () => {
        reject(new Error("Failed to load RPC iframe"));
      };

      // Append RPC iframe to document body (hidden)
      document.body.appendChild(rpcIframe);
    });
  };

  const createConnectIframe = (
    targetElement?: HTMLElement | string
  ): Promise<void> => {
    return new Promise((resolve, reject) => {
      if (connectIframe && connectIframeReady) {
        resolve();
        return;
      }

      connectIframe = document.createElement("iframe");
      connectIframe.src = `${connectIframeUrl}?app=${applicationId}#window=${windowId}`;
      connectIframe.setAttribute("sandbox", "allow-scripts allow-same-origin");
      connectIframe.setAttribute("allow", "publickey-credentials-get *");

      // Set iframe dimensions for the smart auth button
      connectIframe.style.width = "180px";
      connectIframe.style.height = "65px";
      connectIframe.style.border = "none";
      connectIframe.style.background = "transparent";

      connectIframe.onload = () => {
        // Wait a bit for iframe to initialize
        setTimeout(() => {
          connectIframeReady = true;
          resolve();
        }, 100);
      };

      connectIframe.onerror = () => {
        reject(new Error("Failed to load connect iframe"));
      };

      // Append iframe to target element or document body
      let target = document.body;
      if (targetElement) {
        if (typeof targetElement === "string") {
          const element = document.querySelector(targetElement);
          if (element) {
            target = element as HTMLElement;
          }
        } else {
          target = targetElement;
        }
      }
      target.appendChild(connectIframe);
    });
  };

  const handleIframeMessage = (event: MessageEvent) => {
    // Handle messages from bridge iframe
    if (bridgeIframe && event.source === bridgeIframe.contentWindow) {
      const message = event.data;

      if (message.type === "response") {
        // Handle API responses
        const callback = pendingCallbacks.get(message.nonce);
        if (callback) {
          pendingCallbacks.delete(message.nonce);

          if (message.success) {
            callback.resolve(message.data);
          } else {
            callback.reject(new Error(message.error || "Unknown error"));
          }
        }
      } else if (message.type === "open_registration_modal") {
        // Handle modal open requests from button iframe (via bridge)
        openRegistrationModal();
      } else if (message.type === "close_registration_modal") {
        // Handle modal close requests from registration iframe (via bridge)
        closeRegistrationModal();
      }
      return;
    }

    // Handle messages from button iframe (for backwards compatibility during transition)
    if (connectIframe && event.source === connectIframe.contentWindow) {
      const message = event.data as OpenModalMessage;

      if (message.type === "open_registration_modal") {
        openRegistrationModal();
      }
      return;
    }

    // Handle messages from modal iframe (for backwards compatibility during transition)
    if (modalIframe && event.source === modalIframe.contentWindow) {
      const message = event.data as CloseModalMessage;

      if (message.type === "close_registration_modal") {
        closeRegistrationModal();
      }
      return;
    }
  };

  const sendMessage = async (message: ParentToBridgeMessage): Promise<any> => {
    // Wait for bridge iframe to be ready if it's not already
    if (!bridgeIframeReady) {
      await createBridgeIframe();
    }

    return new Promise((resolve, reject) => {
      const currentNonce = nonce++;
      message.nonce = currentNonce;

      pendingCallbacks.set(currentNonce, { resolve, reject });

      // Set timeout for the request
      setTimeout(() => {
        if (pendingCallbacks.has(currentNonce)) {
          pendingCallbacks.delete(currentNonce);
          reject(new Error("Request timeout"));
        }
      }, 30000); // 30 second timeout

      bridgeIframe!.contentWindow!.postMessage(message, "*");
    });
  };

  /**
   * Creates a proper JavaScript Error object from ExecuteError details.
   * This error can be thrown and will behave like a normal browser error.
   */
  const createErrorFromExecuteError = (executeError: ExecuteError): Error => {
    const error = new Error(executeError.error.message);
    error.name = executeError.error.name;

    // Set the stack trace if available
    if (executeError.error.stack) {
      error.stack = executeError.error.stack;
    }

    // Add duration as a custom property for debugging
    (error as any).executionDuration = executeError.duration;

    return error;
  };

  /**
   * Processes execution logs and outputs them to the console
   */
  const processExecuteLogs = (logs: any[]) => {
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
  };

  const execute = async (
    script: string,
    handler: string,
    args: any[] = []
  ): Promise<ExecuteOutput> => {
    logger?.debug("SDK: Executing script", { handler, args });

    const response = await sendMessage({
      type: "execute",
      nonce: 0, // Will be set by sendMessage
      data: {
        script,
        handler,
        args,
      },
    });

    // The response now contains the full ExecutionResult
    const result = response as ExecutionResult;

    if ("Ok" in result) {
      // Handle successful execution
      const successResult = result.Ok;
      processExecuteLogs(successResult.logs);
      return successResult.output as ExecuteOutput;
    } else if ("Error" in result) {
      // Handle runtime error - process logs then throw error
      const errorResult = result.Error;
      processExecuteLogs(errorResult.logs);

      // Create and throw a proper JavaScript Error
      const jsError = createErrorFromExecuteError(errorResult);
      throw jsError;
    } else {
      // This should never happen, but handle it gracefully
      throw new Error("Invalid execution result format");
    }
  };

  const whoAmI = async (): Promise<WhoAmIResponse> => {
    logger?.debug("SDK: Getting identity");

    const response = await sendMessage({
      type: "whoAmI",
      nonce: 0, // Will be set by sendMessage
    });

    return response;
  };

  const initConnectButton = async (
    targetElement?: HTMLElement | string
  ): Promise<void> => {
    logger?.debug("SDK: Initializing button iframe");
    await createConnectIframe(targetElement);
  };

  // Initialize bridge and RPC iframes immediately
  Promise.all([
    createBridgeIframe().catch((error) => {
      logger?.error("Failed to initialize bridge iframe:", error);
    }),
    createRpcIframe().catch((error) => {
      logger?.error("Failed to initialize RPC iframe:", error);
    }),
  ]);

  // Listen for messages from iframes
  window.addEventListener("message", handleIframeMessage);

  // Handle ESC key to close modal
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && modalOverlay) {
      closeRegistrationModal();
    }
  });

  return {
    execute,
    initConnectButton,
    whoAmI,
  };
};

// Make ProvenSDK available globally when bundled as IIFE
if (typeof window !== "undefined") {
  (window as any).ProvenSDK = ProvenSDK;
}
