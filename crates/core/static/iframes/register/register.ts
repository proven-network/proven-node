import { register } from "../../helpers/webauthn";
import { MessageBroker, getWindowIdFromUrl } from "../../helpers/broker";

class RegisterClient {
  broker: MessageBroker;
  windowId: string;
  username: string = "";

  constructor() {
    // Extract window ID from URL fragment
    this.windowId = getWindowIdFromUrl() || "unknown";

    // Initialize broker synchronously - will throw if it fails
    this.broker = new MessageBroker(this.windowId, "register");

    this.initializeBroker();
    this.setupEventListeners();
  }

  async initializeBroker() {
    try {
      await this.broker.connect();

      console.log("Register: Broker initialized successfully");
    } catch (error) {
      console.error("Register: Failed to initialize broker:", error);
      throw new Error(
        `Register: Failed to initialize broker: ${error instanceof Error ? error.message : "Unknown error"}`
      );
    }
  }

  setupEventListeners() {
    // Form submission
    const form = document.getElementById("registration-form");
    const cancelBtn = document.getElementById("cancel-btn");
    const closeBtn = document.getElementById("close-modal");
    const usernameInput = document.getElementById(
      "username"
    ) as HTMLInputElement;

    if (form) {
      form.addEventListener("submit", (e) => this.handleRegistration(e));
    }

    if (cancelBtn) {
      cancelBtn.addEventListener("click", () => this.closeModal());
    }

    if (closeBtn) {
      closeBtn.addEventListener("click", () => this.closeModal());
    }

    // Auto-focus username input
    if (usernameInput) {
      usernameInput.focus();
    }

    // Close modal on Escape key
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        this.closeModal();
      }
    });

    // Close modal when clicking on background
    document.body.addEventListener("click", (event) => {
      // Only close if clicking directly on the body (background), not on the modal
      if (event.target === document.body) {
        this.closeModal();
      }
    });
  }

  showError(message: string) {
    const errorEl = document.getElementById("error-message");
    if (errorEl) {
      errorEl.textContent = message;
      errorEl.style.display = "block";
    }
  }

  hideError() {
    const errorEl = document.getElementById("error-message");
    if (errorEl) {
      errorEl.style.display = "none";
    }
  }

  setLoading(loading: boolean) {
    const createBtn = document.getElementById(
      "create-btn"
    ) as HTMLButtonElement;
    const cancelBtn = document.getElementById(
      "cancel-btn"
    ) as HTMLButtonElement;

    if (createBtn && cancelBtn) {
      if (loading) {
        createBtn.innerHTML =
          '<span class="loading-spinner"></span>Creating Account...';
        createBtn.disabled = true;
        cancelBtn.disabled = true;
      } else {
        createBtn.innerHTML = "Create Account";
        createBtn.disabled = false;
        cancelBtn.disabled = false;
      }
    }
  }

  closeModal() {
    // Send message directly to sdk via broker
    this.broker.send("close_registration_modal", null, "sdk");
  }

  async handleRegistration(event: Event) {
    event.preventDefault();
    this.hideError();

    const usernameInput = document.getElementById(
      "username"
    ) as HTMLInputElement;
    this.username = usernameInput.value.trim();

    if (!this.username) {
      this.showError("Please enter a username");
      usernameInput.focus();
      return;
    }

    if (this.username.length < 3) {
      this.showError("Username must be at least 3 characters");
      usernameInput.focus();
      return;
    }

    if (!/^[a-zA-Z0-9_-]+$/.test(this.username)) {
      this.showError(
        "Username can only contain letters, numbers, underscore, and dash"
      );
      usernameInput.focus();
      return;
    }

    this.setLoading(true);

    try {
      // Call WebAuthn registration with the username
      const response = await register(this.username);

      if (response.ok) {
        console.log("Registration successful for username:", this.username);

        // Send success message directly to button iframe via broker
        await this.broker.send(
          "registration_complete",
          {
            success: true,
            username: this.username,
          },
          "button"
        );

        // Close modal after short delay
        setTimeout(() => this.closeModal(), 500);
      } else {
        const errorText = await response.text();
        this.showError(`Registration failed: ${errorText}`);
      }
    } catch (error) {
      console.error("Registration error:", error);

      let errorMessage = "Registration failed";
      if ((error as Error).name === "NotAllowedError") {
        errorMessage = "Registration was cancelled";
      } else if ((error as Error).message) {
        errorMessage = (error as Error).message;
      }

      this.showError(errorMessage);
    } finally {
      this.setLoading(false);
    }
  }

  // Initialize the client when the page loads
  static init() {
    const client = new RegisterClient();

    // Make client available globally for debugging
    (globalThis as any).registerClient = client;
  }
}

// Initialize when the page loads
if (globalThis.addEventListener) {
  globalThis.addEventListener("DOMContentLoaded", RegisterClient.init);
} else {
  // Fallback for cases where DOMContentLoaded has already fired
  RegisterClient.init();
}
