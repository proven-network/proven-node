import { generateWindowId } from "./helpers/broker";
import type { ExecuteOutput, WhoAmIResponse } from "./common";
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
  initButton: (targetElement?: HTMLElement | string) => Promise<void>;
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
  const buttonIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/button.html`;
  const registerIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/register.html`;
  const rpcIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/rpc.html`;

  // Generate unique window ID for this SDK instance
  const windowId = generateWindowId();
  logger?.debug("SDK: Generated window ID:", windowId);

  let bridgeIframe: HTMLIFrameElement | null = null;
  let buttonIframe: HTMLIFrameElement | null = null;
  let rpcIframe: HTMLIFrameElement | null = null;
  let modalIframe: HTMLIFrameElement | null = null;
  let modalOverlay: HTMLDivElement | null = null;
  let bridgeIframeReady = false;
  let rpcIframeReady = false;
  let buttonIframeReady = false;
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

  const createButtonIframe = (
    targetElement?: HTMLElement | string
  ): Promise<void> => {
    return new Promise((resolve, reject) => {
      if (buttonIframe && buttonIframeReady) {
        resolve();
        return;
      }

      buttonIframe = document.createElement("iframe");
      buttonIframe.src = `${buttonIframeUrl}?app=${applicationId}#window=${windowId}`;
      buttonIframe.setAttribute("sandbox", "allow-scripts allow-same-origin");
      buttonIframe.setAttribute("allow", "publickey-credentials-get *");

      // Set iframe dimensions for the smart auth button
      buttonIframe.style.width = "180px";
      buttonIframe.style.height = "65px";
      buttonIframe.style.border = "none";
      buttonIframe.style.background = "transparent";

      buttonIframe.onload = () => {
        // Wait a bit for iframe to initialize
        setTimeout(() => {
          buttonIframeReady = true;
          resolve();
        }, 100);
      };

      buttonIframe.onerror = () => {
        reject(new Error("Failed to load button iframe"));
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
      target.appendChild(buttonIframe);
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
    if (buttonIframe && event.source === buttonIframe.contentWindow) {
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

    return response;
  };

  const whoAmI = async (): Promise<WhoAmIResponse> => {
    logger?.debug("SDK: Getting identity");

    const response = await sendMessage({
      type: "whoAmI",
      nonce: 0, // Will be set by sendMessage
    });

    return response;
  };

  const initButton = async (
    targetElement?: HTMLElement | string
  ): Promise<void> => {
    logger?.debug("SDK: Initializing button iframe");
    await createButtonIframe(targetElement);
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
    whoAmI,
    initButton,
  };
};

// Make ProvenSDK available globally when bundled as IIFE
if (typeof window !== "undefined") {
  (window as any).ProvenSDK = ProvenSDK;
}
