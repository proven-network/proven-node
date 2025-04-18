import {
  startAuthentication,
  startRegistration,
  type PublicKeyCredentialCreationOptionsJSON,
} from "@simplewebauthn/browser";

class WebAuthnClient {
  async register(): Promise<Response> {
    try {
      const resp = await fetch("./start");

      if (!resp.ok) {
        throw new Error(
          `Failed to get registration options: ${resp.statusText}`
        );
      }

      const { publicKey: optionsJSON } = (await resp.json()) as {
        publicKey: PublicKeyCredentialCreationOptionsJSON;
      };

      const result = await startRegistration({ optionsJSON });

      return fetch("./finish", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(result),
      });
    } catch (err) {
      console.error("WebAuthn registration failed:", err);
      throw err;
    }
  }
}

globalThis.WebAuthnClient = WebAuthnClient;
