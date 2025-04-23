import * as ed25519 from "@noble/ed25519";
import {
  bytesToHex,
  hexToBytes,
  equalBytes,
} from "@noble/curves/abstract/utils";
import { decode as cborDecode } from "cbor-x";
import { X509Certificate, X509ChainBuilder } from "@peculiar/x509";
import { Sign1 } from "@auth0/cose";
import { usEast2Certificate } from "./pems/us-east-2";

type PcrIndex = 0 | 1 | 2 | 3 | 4 | 8;
export type Pcrs = Record<PcrIndex, string>;

export type ExpectedPcrs = Partial<Pcrs>;

export type Session = {
  sessionId: string;
  pcrs: Pcrs;
  signingKey: Uint8Array;
  verifyingKey: Uint8Array;
};

export type SerializableSession = {
  sessionId: string;
  pcrs: Pcrs;
  signingKey: string;
  verifyingKey: string;
};

let session: Session | null = null;

export const createSession = async (applicationId: string) => {
  const signingKey = ed25519.utils.randomPrivateKey();
  const publicKeyInput = ed25519.getPublicKey(signingKey);

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

  const response = await fetch("/auth/create_session", {
    method: "POST",
    body,
  });

  if (!response.ok) {
    throw new Error("Failed to fetch attestation document.");
  }

  const data = new Uint8Array(await response.arrayBuffer());

  const coseElements = (await cborDecode(data)) as Uint8Array[];
  const {
    cabundle,
    certificate,
    nonce,
    pcrs: rawPcrs,
    public_key: verifyingKeyBytes,
    user_data: sessionIdBytes,
  } = (await cborDecode(coseElements[2]!)) as {
    cabundle: Uint8Array[];
    certificate: Uint8Array;
    nonce: Uint8Array;
    pcrs: { [index: number]: Uint8Array };
    public_key: Uint8Array;
    user_data: Uint8Array;
  };

  if (!globalThis.location.hostname.includes("localhost")) {
    const leaf = new X509Certificate(certificate);

    if (!equalBytes(nonceInput, nonce)) {
      throw new Error("Attestation nonce does not match expected value.");
    }

    if (leaf.notAfter < new Date()) {
      throw new Error("Attestation document certificate has expired.");
    }

    const publicKey = await crypto.subtle.importKey(
      "spki",
      new Uint8Array(leaf.publicKey.rawData),
      { name: "ECDSA", namedCurve: "P-384" },
      true,
      ["verify"]
    );
    await Sign1.decode(data).verify(publicKey);

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
    0: bytesToHex(rawPcrs[0]!),
    1: bytesToHex(rawPcrs[1]!),
    2: bytesToHex(rawPcrs[2]!),
    3: bytesToHex(rawPcrs[3]!),
    4: bytesToHex(rawPcrs[4]!),
    8: bytesToHex(rawPcrs[8]!),
  };

  session = {
    sessionId: bytesToHex(sessionIdBytes),
    pcrs,
    signingKey,
    verifyingKey: verifyingKeyBytes,
  };

  const serializableSession: SerializableSession = {
    sessionId: session.sessionId,
    pcrs: session.pcrs,
    signingKey: bytesToHex(session.signingKey),
    verifyingKey: bytesToHex(session.verifyingKey),
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
    signingKey: hexToBytes(signingKey),
    verifyingKey: hexToBytes(verifyingKey),
  };

  return session;
};
