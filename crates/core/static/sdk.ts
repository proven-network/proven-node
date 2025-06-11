import { type RadixDappToolkitOptions } from "@radixdlt/radix-dapp-toolkit";

type ExecuteOutput = string | number | boolean | null | undefined;
type WhoAmIResponse = { identity_address: string; account_addresses: string[] };

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

// Additional message types for modal communication
type ModalMessage = {
  type:
    | "open_registration_modal"
    | "close_registration_modal"
    | "registration_complete";
  success?: boolean;
  username?: string;
};

export type ProvenSDK = {
  execute: (
    script: string,
    handler: string,
    args?: any[]
  ) => Promise<ExecuteOutput>;
  whoAmI: () => Promise<WhoAmIResponse>;
};

export const ProvenSDK = (options: {
  logger?: RadixDappToolkitOptions["logger"];
  iframeUrl: string;
  applicationId: string;
  targetElement?: HTMLElement | string;
}): ProvenSDK => {
  const { logger, iframeUrl, applicationId, targetElement } = options;

  let iframe: HTMLIFrameElement | null = null;
  let modalIframe: HTMLIFrameElement | null = null;
  let modalOverlay: HTMLDivElement | null = null;
  let iframeReady = false;
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
    const baseUrl =
      iframeUrl.replace("/button.html", "") ||
      iframeUrl.replace(/\/[^\/]*$/, "");
    modalIframe.src = `${baseUrl}/register.html?app=${applicationId}`;
    modalIframe.setAttribute(
      "sandbox",
      "allow-scripts allow-same-origin allow-forms"
    );
    modalIframe.setAttribute(
      "allow",
      "publickey-credentials-create *; publickey-credentials-get *"
    );

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

  const createIframe = (): Promise<void> => {
    return new Promise((resolve, reject) => {
      if (iframe && iframeReady) {
        resolve();
        return;
      }

      iframe = document.createElement("iframe");
      iframe.src = `${iframeUrl}?app=${applicationId}`;
      iframe.setAttribute("sandbox", "allow-scripts allow-same-origin");
      iframe.setAttribute(
        "allow",
        "publickey-credentials-create *; publickey-credentials-get *"
      );

      // Set iframe dimensions for the smart auth button
      iframe.style.width = "180px";
      iframe.style.height = "65px";
      iframe.style.border = "none";
      iframe.style.background = "transparent";

      iframe.onload = () => {
        // Wait a bit for iframe to initialize
        setTimeout(() => {
          iframeReady = true;
          resolve();
        }, 100);
      };

      iframe.onerror = () => {
        reject(new Error("Failed to load iframe"));
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
      target.appendChild(iframe);

      // Listen for messages from iframe
      window.addEventListener("message", handleIframeMessage);
    });
  };

  const handleIframeMessage = (event: MessageEvent) => {
    // Handle messages from button iframe
    if (iframe && event.source === iframe.contentWindow) {
      const message = event.data as IframeResponse | ModalMessage;

      if (message.type === "response") {
        const response = message as IframeResponse;
        const callback = pendingCallbacks.get(response.nonce);
        if (callback) {
          pendingCallbacks.delete(response.nonce);

          if (response.success) {
            callback.resolve(response.data);
          } else {
            callback.reject(new Error(response.error || "Unknown error"));
          }
        }
      } else if (message.type === "open_registration_modal") {
        openRegistrationModal();
      }
      return;
    }

    // Handle messages from modal iframe
    if (modalIframe && event.source === modalIframe.contentWindow) {
      const message = event.data as ModalMessage;

      if (message.type === "close_registration_modal") {
        closeRegistrationModal();
      } else if (message.type === "registration_complete") {
        logger?.debug("SDK: Registration completed", {
          success: message.success,
          username: message.username,
        });

        // Notify the button iframe about registration completion
        if (iframe && iframe.contentWindow) {
          iframe.contentWindow.postMessage(
            {
              type: "registration_complete",
              success: message.success,
              username: message.username,
            },
            "*"
          );
        }

        // Close modal after successful registration
        if (message.success) {
          closeRegistrationModal();
        }
      }
      return;
    }
  };

  const sendMessage = async (message: SdkMessage): Promise<any> => {
    // Wait for iframe to be ready if it's not already
    if (!iframeReady) {
      await createIframe();
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

      iframe!.contentWindow!.postMessage(message, "*");
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

  // Initialize iframe immediately
  createIframe().catch((error) => {
    logger?.error("Failed to initialize iframe:", error);
  });

  // Handle ESC key to close modal
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && modalOverlay) {
      closeRegistrationModal();
    }
  });

  return {
    execute,
    whoAmI,
  };
};

// Make ProvenSDK available globally when bundled as IIFE
if (typeof window !== "undefined") {
  (window as any).ProvenSDK = ProvenSDK;
}
