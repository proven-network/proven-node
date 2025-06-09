import { type RadixDappToolkitOptions } from "@radixdlt/radix-dapp-toolkit";

type ExecuteOutput = string | number | boolean | null | undefined;
type WhoAmIResponse = { identity_address: string; account_addresses: string[] };
type AuthState = "signed_in" | "not_signed_in";
type AuthResponse = { state: AuthState };

type SdkMessage = {
  type: "execute" | "whoAmI" | "login" | "logout" | "getAuthState";
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
  login: () => Promise<AuthResponse>;
  logout: () => Promise<AuthResponse>;
  getAuthState: () => Promise<AuthResponse>;
};

export const ProvenSDK = (options: {
  logger?: RadixDappToolkitOptions["logger"];
  iframeUrl: string;
  applicationId: string;
}): ProvenSDK => {
  const { logger, iframeUrl, applicationId } = options;

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
      iframe.style.display = "none";
      iframe.setAttribute("sandbox", "allow-scripts allow-same-origin");
      iframe.setAttribute(
        "allow",
        "publickey-credentials-create *; publickey-credentials-get *"
      );

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

      document.body.appendChild(iframe);

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
    if (!iframe || !iframeReady) {
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

  const login = async (): Promise<AuthResponse> => {
    logger?.debug("SDK: Logging in");

    const response = await sendMessage({
      type: "login",
      nonce: 0, // Will be set by sendMessage
    });

    return response;
  };

  const logout = async (): Promise<AuthResponse> => {
    logger?.debug("SDK: Logging out");

    const response = await sendMessage({
      type: "logout",
      nonce: 0, // Will be set by sendMessage
    });

    return response;
  };

  const getAuthState = async (): Promise<AuthResponse> => {
    logger?.debug("SDK: Getting auth state");

    const response = await sendMessage({
      type: "getAuthState",
      nonce: 0, // Will be set by sendMessage
    });

    return response;
  };

  return {
    execute,
    whoAmI,
    login,
    logout,
    getAuthState,
  };
};

// Make ProvenSDK available globally when bundled as IIFE
if (typeof window !== "undefined") {
  (window as any).ProvenSDK = ProvenSDK;
}
