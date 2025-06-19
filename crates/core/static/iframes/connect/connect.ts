import { authenticate } from "../../helpers/webauthn";
import { MessageBroker, getWindowIdFromUrl } from "../../helpers/broker";
import { hexToBytes } from "@noble/curves/abstract/utils";
import { Anonymize, Identify, WhoAmI, WhoAmIResponse } from "../../common";
import { signAsync, getPublicKeyAsync } from "@noble/ed25519";
import { getSession } from "../../helpers/sessions";

class ConnectClient {
  applicationId: string;
  broker: MessageBroker;
  windowId: string;

  constructor() {
    // Extract application ID from URL path
    this.applicationId =
      globalThis.location.pathname.split("/")[2] || "unknown";

    // Extract window ID from URL fragment
    this.windowId = getWindowIdFromUrl() || "unknown";

    // Initialize broker synchronously - will throw if it fails
    this.broker = new MessageBroker(this.windowId, "connect");

    this.initializeBroker();
  }

  async initializeBroker() {
    try {
      await this.broker.connect();

      // Set up message handlers
      this.broker.on("registration_complete", async (message) => {
        console.debug("Connect: Registration completed", message.data);

        // Handle successful registration
        if (message.data.prfResult) {
          await this.handleSuccessfulAuth(message.data.prfResult);
        }
      });
    } catch (error) {
      console.error("Connect: Failed to initialize broker:", error);
      throw new Error(
        `Connect: Failed to initialize broker: ${error instanceof Error ? error.message : "Unknown error"}`
      );
    }
  }

  async handleSuccessfulAuth(prfResult: Uint8Array) {
    console.debug("Connect: Authentication successful, sending identify RPC");

    // Send identify RPC request
    try {
      const session = await getSession(this.applicationId);

      if (!session) {
        throw new Error("Session not found");
      }

      const sessionId = hexToBytes(session.sessionId.replace(/-/g, ""));

      // Get the public key from the PRF result
      const publicKey = await getPublicKeyAsync(prfResult);

      // Sign the session ID with the PRF result
      const sessionIdSignature = await signAsync(sessionId, prfResult);

      const identifyRequest: Identify = {
        Identify: [publicKey, sessionIdSignature],
      };

      const response = await this.broker.request(
        "rpc_request",
        identifyRequest,
        "rpc"
      );
      console.debug("Connect: Identify RPC response:", response);

      // Only update UI after successful identify RPC
      console.debug("Connect: Identify successful, updating UI");
      await this.updateAuthUI();
    } catch (error) {
      console.error("Connect: Failed to send identify RPC request:", error);
    }
  }

  // Auth state methods
  async isSignedIn(): Promise<boolean> {
    try {
      const whoAmIRequest: WhoAmI = "WhoAmI";
      const response = await this.broker.request(
        "rpc_request",
        whoAmIRequest,
        "rpc"
      );

      const whoAmIResponse = response.data.WhoAmI as WhoAmIResponse;

      console.debug("Connect: WhoAmI response:", whoAmIResponse);

      // If the response has an "Identified" variant, user is signed in
      return "Identified" in whoAmIResponse;
    } catch (error) {
      console.error("Connect: Failed to check auth status:", error);
      return false;
    }
  }

  async signOut(): Promise<void> {
    try {
      const anonymizeRequest: Anonymize = "Anonymize";
      const response = await this.broker.request(
        "rpc_request",
        anonymizeRequest,
        "rpc"
      );
      console.debug("Connect: Anonymize RPC response:", response);
      await this.updateAuthUI();
    } catch (error) {
      console.error("Connect: Failed to sign out:", error);
    }
  }

  async updateAuthUI() {
    const userSignedIn = await this.isSignedIn();
    const signedOutView = document.getElementById("signed-out-view");
    const signedInView = document.getElementById("signed-in-view");
    const container = document.getElementById("auth-container");

    if (userSignedIn) {
      signedOutView!.style.display = "none";
      signedInView!.style.display = "block";
    } else {
      signedOutView!.style.display = "block";
      signedInView!.style.display = "none";
      this.resetSignInButton();
    }

    // Show the container now that we've set the correct state
    container!.classList.add("ready");
  }

  resetSignInButton() {
    const button = document.getElementById("auth-button") as HTMLButtonElement;
    if (button) {
      button.textContent = "Sign In";
      button.disabled = false;
    }
  }

  async handleSignIn() {
    const button = document.getElementById("auth-button") as HTMLButtonElement;
    button.disabled = true;
    button.textContent = "Signing in...";

    try {
      const prfResult = await authenticate();
      console.debug("Authentication successful with PRF result");
      await this.handleSuccessfulAuth(prfResult);
    } catch (error) {
      console.error("Authentication error:", error);

      // Check if this is a "no credentials" error
      const errorMessage = (error as Error).message?.toLowerCase() || "";
      const isNoCredentialsError =
        (error as Error).name === "NotAllowedError" &&
        errorMessage.includes("immediate");

      if (isNoCredentialsError) {
        console.debug("No credentials found, opening registration modal");
        // Send message directly to sdk via broker
        await this.broker.send("open_registration_modal", null, "sdk");
        this.resetSignInButton();
      } else {
        // Other error (user cancelled, PRF not available, etc.)
        let errorMessage = "Sign-in failed";
        if ((error as Error).name === "NotAllowedError") {
          errorMessage = "Sign-in was cancelled";
        } else if ((error as Error).message) {
          errorMessage = (error as Error).message;
        }

        this.resetSignInButton();
      }
    }
  }

  // Initialize the client when the page loads
  static init() {
    const client = new ConnectClient();

    // Set up event listeners
    window.addEventListener("load", () => {
      const authButton = document.getElementById("auth-button");
      const signoutButton = document.getElementById("signout-button");

      if (authButton) {
        authButton.addEventListener("click", () => client.handleSignIn());
      }

      if (signoutButton) {
        signoutButton.addEventListener("click", () => client.signOut());
      }

      // Initialize UI state
      client.updateAuthUI().catch(console.error);
    });

    // Make client available globally for debugging
    (globalThis as any).buttonClient = client;
  }
}

// Initialize when the page loads
if (globalThis.addEventListener) {
  globalThis.addEventListener("DOMContentLoaded", ConnectClient.init);
} else {
  // Fallback for cases where DOMContentLoaded has already fired
  ConnectClient.init();
}
