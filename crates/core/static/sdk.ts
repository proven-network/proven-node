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
  let iframeReady = false;
  let nonce = 0;
  const pendingCallbacks = new Map<
    number,
    { resolve: (data: any) => void; reject: (error: Error) => void }
  >();

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

      // Set iframe dimensions for the auth button
      iframe.style.width = "140px";
      iframe.style.height = "50px";
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
    if (!iframe || event.source !== iframe.contentWindow) {
      return;
    }

    const response = event.data as IframeResponse;

    if (response.type === "response") {
      const callback = pendingCallbacks.get(response.nonce);
      if (callback) {
        pendingCallbacks.delete(response.nonce);

        if (response.success) {
          callback.resolve(response.data);
        } else {
          callback.reject(new Error(response.error || "Unknown error"));
        }
      }
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

  return {
    execute,
    whoAmI,
  };
};

// Make ProvenSDK available globally when bundled as IIFE
if (typeof window !== "undefined") {
  (window as any).ProvenSDK = ProvenSDK;
}
