import { eddsa } from "elliptic";
import { decode as cborDecode } from "cbor-x";
import { X509Certificate, X509ChainBuilder } from "@peculiar/x509";
import { Sign1 } from "@auth0/cose";
import { areEqualUint8Array, uint8ArrayToHex } from "./helpers/uint8array";
import { usEast2Certificate } from "./pems/us-east-2";

type PcrIndex = 0 | 1 | 2 | 3 | 4 | 8;
export type Pcrs = Record<PcrIndex, string>;

export type ExpectedPcrs = Partial<Pcrs>;

export type Session = {
  sessionId: string;
  pcrs: Pcrs;
  signingKey: eddsa.KeyPair;
  verifyingKey: eddsa.KeyPair;
};

export type SerializableSession = {
  sessionId: string;
  pcrs: Pcrs;
  signingKey: string; // hex
  verifyingKey: string; // hex
};

const keys = new eddsa("ed25519");
let session: Session | null = null;

export const createSession = async (applicationId: string) => {
  const newSecretHex = uint8ArrayToHex(
    crypto.getRandomValues(new Uint8Array(32))
  );
  const signingKey = keys.keyFromSecret(newSecretHex);

  // get bytes from private key
  const publicKeyInput = new Uint8Array(signingKey.getPublic());

  // generate nonce to verify in response
  const nonceInput = new Uint8Array(32);
  crypto.getRandomValues(nonceInput);

  const body = new FormData();
  body.append(
    "public_key",
    new Blob([publicKeyInput], { type: "application/octet-stream" })
  );
  body.append(
    "nonce",
    new Blob([nonceInput], { type: "application/octet-stream" })
  );
  body.append("application_id", applicationId);

  // send attestation request
  const response = await fetch("/auth/create_session", {
    method: "POST",
    body,
  });

  if (!response.ok) {
    throw new Error("Failed to fetch attestation document.");
  }

  const data = new Uint8Array(await response.arrayBuffer());

  // decode COSE elements
  const coseElements = (await cborDecode(data)) as Uint8Array[];
  const {
    cabundle,
    certificate,
    nonce,
    pcrs: rawPcrs,
    public_key: verifyingKey,
    user_data: sessionId,
  } = (await cborDecode(coseElements[2]!)) as {
    cabundle: Uint8Array[];
    certificate: Uint8Array;
    nonce: Uint8Array;
    pcrs: { [index: number]: Uint8Array };
    public_key: Uint8Array;
    user_data: Uint8Array;
  };

  // Skip checks in local development mode
  if (!globalThis.location.hostname.includes("localhost")) {
    const leaf = new X509Certificate(certificate);

    // verify nonce or throw error
    if (!areEqualUint8Array(nonceInput, nonce)) {
      throw new Error("Attestation nonce does not match expected value.");
    }

    // verify leaf still valid or throw error
    if (leaf.notAfter < new Date()) {
      throw new Error("Attestation document certificate has expired.");
    }

    // verify cose sign1 or throw error
    const publicKey = await crypto.subtle.importKey(
      "spki",
      new Uint8Array(leaf.publicKey.rawData),
      { name: "ECDSA", namedCurve: "P-384" },
      true,
      ["verify"]
    );
    await Sign1.decode(data).verify(publicKey);

    // verify certificate chain or throw error
    const knownCa = new X509Certificate(usEast2Certificate);
    const chain = await new X509ChainBuilder({
      certificates: cabundle.map((cert) => new X509Certificate(cert)),
    }).build(leaf);
    if (!chain[chain.length - 1]?.equal(knownCa)) {
      throw new Error(
        "x509 certificate chain does not have expected certificate authority."
      );
    }
  }

  const pcrs: Pcrs = {
    0: uint8ArrayToHex(rawPcrs[0]!),
    1: uint8ArrayToHex(rawPcrs[1]!),
    2: uint8ArrayToHex(rawPcrs[2]!),
    3: uint8ArrayToHex(rawPcrs[3]!),
    4: uint8ArrayToHex(rawPcrs[4]!),
    8: uint8ArrayToHex(rawPcrs[8]!),
  };

  session = {
    sessionId: uint8ArrayToHex(sessionId),
    pcrs,
    signingKey,
    verifyingKey: keys.keyFromPublic(uint8ArrayToHex(verifyingKey)),
  };

  // save attested details
  const serializableSession: SerializableSession = {
    sessionId: session.sessionId,
    pcrs: session.pcrs,
    signingKey: session.signingKey.getSecret("hex"),
    verifyingKey: session.verifyingKey.getPublic("hex"),
  };

  localStorage.setItem(
    "currentSession:" + applicationId,
    JSON.stringify(serializableSession)
  );
};

export const getSession = async (applicationId: string) => {
  if (session) {
    return session;
  }

  const sessionData = localStorage.getItem("currentSession:" + applicationId);
  if (!sessionData) {
    return null;
  }

  const { sessionId, pcrs, signingKey, verifyingKey } = JSON.parse(
    sessionData
  ) as SerializableSession;

  session = {
    sessionId,
    pcrs,
    signingKey: keys.keyFromSecret(signingKey),
    verifyingKey: keys.keyFromPublic(verifyingKey),
  };

  return session;
};
