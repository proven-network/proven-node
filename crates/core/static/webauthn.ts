import {
  base64UrlToUint8Array,
  uint8ArrayToBase64Url,
} from "./helpers/uint8array";
import { bytesToHex } from "@noble/curves/abstract/utils";
import * as ed from "@noble/ed25519";
import { hkdf } from "@noble/hashes/hkdf";
import { sha256 } from "@noble/hashes/sha2";

// Fields that should be converted from Base64URL to ArrayBuffer
const BASE64URL_FIELDS = new Set([
  "publicKey.challenge",
  "publicKey.user.id",
  "publicKey.extensions.prf.eval.first",
]);

// Special handling for array fields that contain Base64URL data
const ARRAY_BASE64URL_FIELDS = new Set([
  "publicKey.allowCredentials",
  "publicKey.excludeCredentials",
]);

// Deeply converts objects with Base64URL strings to ArrayBuffers
function convertOptionsToBuffer(obj: any, path: string[] = []): any {
  if (typeof obj === "string") {
    // Get the full path
    const fullPath = path.join(".");
    console.debug(`Checking field at path: ${fullPath} with value: ${obj}`);

    // Only convert if the full path matches one of our specified paths
    if (BASE64URL_FIELDS.has(fullPath)) {
      try {
        console.debug(`Converting ${fullPath} from Base64URL to ArrayBuffer`);
        return base64UrlToUint8Array(obj);
      } catch (e) {
        console.warn(`Failed to convert ${fullPath} to ArrayBuffer:`, e);
        return obj;
      }
    }
    return obj;
  } else if (Array.isArray(obj)) {
    // Special handling for credential arrays
    const fullPath = path.join(".");
    if (ARRAY_BASE64URL_FIELDS.has(fullPath)) {
      // This is an array of credential descriptors
      return obj.map((item) => {
        if (item && typeof item === "object" && item.id) {
          try {
            console.debug(
              `Converting credential ID from Base64URL to ArrayBuffer`
            );
            return {
              ...item,
              id: base64UrlToUint8Array(item.id),
            };
          } catch (e) {
            console.warn(`Failed to convert credential ID to ArrayBuffer:`, e);
            return item;
          }
        }
        return item;
      });
    } else {
      // Regular array processing
      return obj.map((item, index) =>
        convertOptionsToBuffer(item, [...path, index.toString()])
      );
    }
  } else if (obj !== null && typeof obj === "object") {
    const newObj: any = {};
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        newObj[key] = convertOptionsToBuffer(obj[key], [...path, key]);
      }
    }
    return newObj;
  }
  return obj;
}

// Deeply converts objects with ArrayBuffers to Base64URL strings
function convertResultToBase64Url(obj: any): any {
  if (obj instanceof ArrayBuffer || obj instanceof Uint8Array) {
    return uint8ArrayToBase64Url(obj);
  } else if (Array.isArray(obj)) {
    return obj.map(convertResultToBase64Url);
  } else if (obj !== null && typeof obj === "object") {
    const newObj: any = {};
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        newObj[key] = convertResultToBase64Url(obj[key]);
      }
    }
    return newObj;
  }
  return obj;
}

export class WebAuthnClient {
  private readonly MASTER_SECRET_KEY = "webauthn_master_secret";

  // Check if user is currently signed in (has master secret in sessionStorage)
  isSignedIn(): boolean {
    return sessionStorage.getItem(this.MASTER_SECRET_KEY) !== null;
  }

  // Get current auth state - simplified to just signed in or not
  getAuthState(): "signed_in" | "not_signed_in" {
    return this.isSignedIn() ? "signed_in" : "not_signed_in";
  }

  // Store master secret in sessionStorage
  private storeMasterSecret(masterSecretBytes: Uint8Array): void {
    const masterSecretHex = bytesToHex(masterSecretBytes);
    sessionStorage.setItem(this.MASTER_SECRET_KEY, masterSecretHex);
    console.log("Master secret stored in sessionStorage");
  }

  // Get master secret from sessionStorage
  getMasterSecret(): Uint8Array | null {
    const masterSecretHex = sessionStorage.getItem(this.MASTER_SECRET_KEY);
    if (!masterSecretHex) {
      return null;
    }

    try {
      // Convert hex string back to Uint8Array
      const bytes = new Uint8Array(
        masterSecretHex.match(/.{1,2}/g)!.map((byte) => parseInt(byte, 16))
      );
      return bytes;
    } catch (e) {
      console.error("Failed to parse master secret from sessionStorage:", e);
      return null;
    }
  }

  // Sign out (clear sessionStorage)
  signOut(): void {
    sessionStorage.removeItem(this.MASTER_SECRET_KEY);
    console.log("User signed out");
  }

  // Process PRF results and derive keys
  private async processPRFResults(prfResults: any): Promise<Uint8Array | null> {
    if (!prfResults || !prfResults.results || !prfResults.results.first) {
      console.log("No PRF results available");
      return null;
    }

    console.log("PRF Results:", prfResults);
    console.log("Full PRF Extension results:", prfResults);

    // Use the PRF output directly as the master secret for HKDF
    const masterSecretBytes = new Uint8Array(prfResults.results.first);
    console.log(
      "Using Master Secret (from PRF - hex):",
      bytesToHex(masterSecretBytes)
    );

    // Define the context for the key derivation
    const contextString = "app/1234/main_store";
    const derivedKeyLengthBytes = 32; // For Ed25519 seed

    console.log(`Deriving key for context: "${contextString}"`);

    // 1. Derive the 32-byte seed using HKDF
    const derivedSeed = hkdf(
      sha256, // Underlying hash function
      masterSecretBytes, // Master secret (from PRF)
      undefined, // Salt (optional)
      contextString, // Context-specific information
      derivedKeyLengthBytes // Output length (32 for Ed25519 seed)
    );
    console.log(` Derived Seed via HKDF (hex): ${bytesToHex(derivedSeed)}`);

    // 2. Use the HKDF-derived seed to generate the Ed25519 public key
    const derivedPublicKeyBytes = await ed.getPublicKeyAsync(derivedSeed);
    console.log(
      ` Derived Public Key (hex): ${bytesToHex(derivedPublicKeyBytes)}`
    );

    return masterSecretBytes;
  }

  async register(): Promise<Response> {
    try {
      const resp = await fetch("./start");

      if (!resp.ok) {
        throw new Error(
          `Failed to get registration options: ${resp.statusText}`
        );
      }

      const responseData = await resp.json();
      console.log("Raw server response:", responseData);

      // Convert the options, only converting specific fields
      const options = convertOptionsToBuffer(responseData);
      console.log("Converted options:", options);

      const result = await navigator.credentials.create(options);
      console.log("Credential result:", result);

      if (!result) {
        throw new Error("Failed to create credential - null result");
      }

      // Log PRF results if available
      const credential = result as PublicKeyCredential;
      if ("getClientExtensionResults" in credential) {
        const clientExtensionResults =
          credential.getClientExtensionResults() as { prf?: any };
        if (clientExtensionResults.prf) {
          const masterSecretBytes = await this.processPRFResults(
            clientExtensionResults.prf
          );
          if (masterSecretBytes) {
            this.storeMasterSecret(masterSecretBytes);
          }
        } else {
          console.log("PRF extension not present in authenticator response.");
        }
      } else {
        console.log("getClientExtensionResults not available on credential.");
      }

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

  async authenticate(): Promise<Response> {
    try {
      const resp = await fetch("./auth/start", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });

      // If server returns 404 or similar, it means no credentials are registered
      if (resp.status === 404) {
        throw new Error("No credentials found - registration required");
      }

      if (!resp.ok) {
        throw new Error(
          `Failed to get authentication options: ${resp.statusText}`
        );
      }

      const responseData = await resp.json();
      console.log("Raw auth server response:", responseData);

      // Convert the options for authentication
      const options = convertOptionsToBuffer(responseData);
      console.log("Converted auth options:", options);

      const result = await navigator.credentials.get(options);
      console.log("Authentication result:", result);

      if (!result) {
        throw new Error("Failed to authenticate - null result");
      }

      // Process PRF results for authentication
      const credential = result as PublicKeyCredential;
      if ("getClientExtensionResults" in credential) {
        const clientExtensionResults =
          credential.getClientExtensionResults() as { prf?: any };
        if (clientExtensionResults.prf) {
          const masterSecretBytes = await this.processPRFResults(
            clientExtensionResults.prf
          );
          if (masterSecretBytes) {
            this.storeMasterSecret(masterSecretBytes);
          }
        } else {
          console.log("PRF extension not present in authentication response.");
        }
      } else {
        console.log("getClientExtensionResults not available on credential.");
      }

      return fetch("./auth/finish", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(result),
      });
    } catch (err) {
      console.error("WebAuthn authentication failed:", err);
      throw err;
    }
  }
}
