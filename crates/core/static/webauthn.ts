import {
  base64UrlToUint8Array,
  uint8ArrayToBase64Url,
  uint8ArrayToHex,
} from "./helpers/uint8array";
// import { HDKey } from "micro-key-producer/slip10.js"; // Removed
import * as ed from "@noble/ed25519";
import { hkdf } from "@noble/hashes/hkdf";
import { sha256 } from "@noble/hashes/sha2";

// Fields that should be converted from Base64URL to ArrayBuffer
const BASE64URL_FIELDS = new Set([
  "publicKey.challenge",
  "publicKey.user.id",
  "publicKey.extensions.prf.eval.first",
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
    return obj.map((item, index) =>
      convertOptionsToBuffer(item, [...path, index.toString()])
    );
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

class WebAuthnClient {
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
          console.log("PRF Results:", clientExtensionResults.prf);
          // The actual PRF output might be under clientExtensionResults.prf.results.first or .second
          console.log(
            "Full PRF Extension results:",
            clientExtensionResults.prf
          );

          if (clientExtensionResults.prf.results.first) {
            // Use the PRF output directly as the master secret for HKDF
            const masterSecretBytes = new Uint8Array(
              clientExtensionResults.prf.results.first
            );
            console.log(
              "Using Master Secret (from PRF - hex):",
              uint8ArrayToHex(masterSecretBytes)
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
            console.log(
              ` Derived Seed via HKDF (hex): ${uint8ArrayToHex(derivedSeed)}`
            );

            // 2. Use the HKDF-derived seed to generate the Ed25519 public key
            const derivedPublicKeyBytes =
              await ed.getPublicKeyAsync(derivedSeed);
            console.log(
              ` Derived Public Key (hex): ${uint8ArrayToHex(derivedPublicKeyBytes)}`
            );
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
}

globalThis.WebAuthnClient = WebAuthnClient;
