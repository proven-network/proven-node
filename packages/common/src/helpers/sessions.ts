import * as ed25519 from "@noble/ed25519";
import {
  bytesToHex,
  hexToBytes,
  equalBytes,
} from "@noble/curves/abstract/utils";
import { decode as cborDecode } from "cbor-x";
import { X509Certificate, X509ChainBuilder } from "@peculiar/x509";
import { Sign1 } from "@auth0/cose";
import { mockCertificate } from "../pems/mock";
import { usEast2Certificate } from "../pems/us-east-2";

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
  const publicKeyInput = await ed25519.getPublicKeyAsync(signingKey);

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

  const response = await fetch(`/app/${applicationId}/auth/create_session`, {
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

  const hostname = globalThis.location.hostname;
  const knownCa = new X509Certificate(
    hostname === "localhost" || hostname.endsWith(".local")
      ? mockCertificate
      : usEast2Certificate
  );

  const chain = await new X509ChainBuilder({
    certificates: cabundle.map((cert) => new X509Certificate(cert)),
  }).build(leaf);

  if (!chain[chain.length - 1]?.equal(knownCa)) {
    throw new Error(
      "x509 certificate chain does not have expected certificate authority."
    );
  }

  const pcrs: Pcrs = {
    0: bytesToHex(rawPcrs[0]!),
    1: bytesToHex(rawPcrs[1]!),
    2: bytesToHex(rawPcrs[2]!),
    3: bytesToHex(rawPcrs[3]!),
    4: bytesToHex(rawPcrs[4]!),
    8: bytesToHex(rawPcrs[8]!),
  };

  // TODO: get from options or generate from cargo version for testing
  const expectedPcrs: ExpectedPcrs = {
    0: "30f2c9b7736afa4af8e80acf549cb5c9511c2198f0174bf461a5c547ad3bfbd04dd6370968444bfdf64a34e5dda2a684",
    1: "0ab829f12a94d68545fe4a331fb80ccd2cd0e16b5c84f854a4b7f3c3c32a386dd6aed3f1117e3831bc21ac124a5e9ec8",
    2: "ba73cd68537fb4a9e704df621505420055ee049177c50a59643cad88736fcee578139cf76549cdb9d37169a13276578b",
  };

  // verify expected PCRs or throw error
  expectedPcrs &&
    Object.entries(expectedPcrs).forEach(([index, expectedValue]) => {
      if (pcrs[index as unknown as keyof Pcrs] !== expectedValue) {
        throw new Error(
          `PCR${index} does not match expected value. Expected: ${expectedValue} Actual: ${pcrs[index as unknown as keyof Pcrs]}`
        );
      }
    });

  // Restore UUID with dashes from sessionIdBytes
  const sessionId = bytesToHex(sessionIdBytes).replace(
    /^([0-9a-fA-F]{8})([0-9a-fA-F]{4})([0-9a-fA-F]{4})([0-9a-fA-F]{4})([0-9a-fA-F]{12})$/,
    "$1-$2-$3-$4-$5"
  );

  session = {
    sessionId,
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

  return session;
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
