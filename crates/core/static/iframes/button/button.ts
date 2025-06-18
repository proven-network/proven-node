import { isSignedIn, authenticate, signOut } from "../../helpers/webauthn";
import { MessageBroker, getWindowIdFromUrl } from "../../helpers/broker";
import { bytesToHex } from "@noble/curves/abstract/utils";

// Constants
const MASTER_SECRET_KEY = "webauthn_master_secret";

// Master secret functions
function storeMasterSecret(masterSecretBytes: Uint8Array): void {
  const masterSecretHex = bytesToHex(masterSecretBytes);
  sessionStorage.setItem(MASTER_SECRET_KEY, masterSecretHex);
}

export function getMasterSecret(): Uint8Array | null {
  const masterSecretHex = sessionStorage.getItem(MASTER_SECRET_KEY);
  if (masterSecretHex) {
    return new Uint8Array(
      masterSecretHex.match(/.{1,2}/g)!.map((byte) => parseInt(byte, 16))
    );
  }
  return null;
}

class ButtonClient {
  broker: MessageBroker;
  windowId: string;

  constructor() {
    // Extract window ID from URL fragment
    this.windowId = getWindowIdFromUrl() || "unknown";

    // Initialize broker synchronously - will throw if it fails
    this.broker = new MessageBroker(this.windowId, "button");

    this.initializeBroker();
  }

  async initializeBroker() {
    try {
      await this.broker.connect();

      // Set up message handlers
      this.broker.on("registration_complete", (message) => {
        console.log("Button: Registration completed", message.data);

        // Store the PRF result if provided
        if (message.data.prfResult) {
          console.log("Button: Registration successful with PRF result");
          this.handleSuccessfulAuth(message.data.prfResult);
        }
      });

      console.log("Button: Broker initialized successfully");
    } catch (error) {
      console.error("Button: Failed to initialize broker:", error);
      throw new Error(
        `Button: Failed to initialize broker: ${error instanceof Error ? error.message : "Unknown error"}`
      );
    }
  }

  handleSuccessfulAuth(prfResult: Uint8Array) {
    console.log("Button: Storing master secret and updating UI");
    storeMasterSecret(prfResult);
    this.updateAuthUI();
  }

  updateAuthUI() {
    const userSignedIn = isSignedIn();
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
      console.log("Authentication successful with PRF result");
      this.handleSuccessfulAuth(prfResult);
    } catch (error) {
      console.error("Authentication error:", error);

      // Check if this is a "no credentials" error
      const errorMessage = (error as Error).message?.toLowerCase() || "";
      const isNoCredentialsError =
        (error as Error).name === "NotAllowedError" &&
        errorMessage.includes("immediate");

      if (isNoCredentialsError) {
        console.log("No credentials found, opening registration modal");
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

  handleSignOut() {
    signOut();
    this.updateAuthUI();
  }

  // Initialize the client when the page loads
  static init() {
    const client = new ButtonClient();

    // Set up event listeners
    window.addEventListener("load", () => {
      const authButton = document.getElementById("auth-button");
      const signoutButton = document.getElementById("signout-button");

      if (authButton) {
        authButton.addEventListener("click", () => client.handleSignIn());
      }

      if (signoutButton) {
        signoutButton.addEventListener("click", () => client.handleSignOut());
      }

      // Initialize UI state
      client.updateAuthUI();
    });

    // Make client available globally for debugging
    (globalThis as any).buttonClient = client;
  }
}

// Initialize when the page loads
if (globalThis.addEventListener) {
  globalThis.addEventListener("DOMContentLoaded", ButtonClient.init);
} else {
  // Fallback for cases where DOMContentLoaded has already fired
  ButtonClient.init();
}
