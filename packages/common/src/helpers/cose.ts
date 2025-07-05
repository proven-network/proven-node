import { Encoder, decode } from 'cbor-x';
import { signAsync, verifyAsync } from '@noble/ed25519';
import { Result, ok, err } from 'neverthrow';

// Note: This is a tiny implementation of COSE Sign1, using noble/ed25519 for signing
// and verifying of EdDSA signatures.
//
// Notes from cose Signature1 spec:
// Sig_structure = [
//   context : "Signature" / "Signature1" / "CounterSignature",
//   body_protected : empty_or_serialized_map,
//   ? sign_protected : empty_or_serialized_map,
//   external_aad : bstr,
//   payload : bstr
// ]

type Payload = unknown;
type UnprotectedHeaders = Record<string, unknown>;
type DecodedMessage = {
  headers: UnprotectedHeaders;
  payload: Payload;
};

// Define key types as Uint8Array for Noble
type PrivateKey = Uint8Array;
type PublicKey = Uint8Array;

// Use Uint8Array instead of Buffer for browser compatibility
const ed25519Header = new Uint8Array([0xa1, 0x01, 0x27]); // -7 = EdDSA
const coseEncoder = new Encoder({
  variableMapSize: true,
  tagUint8Array: false,
  useRecords: false,
  mapsAsObjects: false,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  useTag259ForMaps: false,
});

export const decodeAndVerifyCoseSign1 = async (
  coseSign1: Uint8Array,
  verifyingKey: PublicKey,
  externalAad: Uint8Array = new Uint8Array(0)
): Promise<Result<DecodedMessage, string>> => {
  const coseElements = (await decode(coseSign1)) as [
    Uint8Array,
    UnprotectedHeaders,
    Uint8Array,
    Uint8Array,
  ];

  if (coseElements.length !== 4) {
    return err('Invalid COSE Sign1 structure.');
  }

  const [protectedHeaders, unprotectedHeaders, payload, signature] = coseElements;

  const toBeSigned = await coseEncoder.encode([
    'Signature1', // context
    protectedHeaders, // body_protected
    externalAad, // external_aad
    payload, // payload
  ]);

  if (!(await verifyAsync(signature, toBeSigned, verifyingKey))) {
    return err('COSE Sign1 verification failed.');
  }

  const decodedPayload = await decode(payload);
  return ok({ headers: unprotectedHeaders, payload: decodedPayload });
};

export const CoseSign1Decoder = (
  verifyingKey: PublicKey,
  externalAad: Uint8Array = new Uint8Array(0)
) => ({
  decodeAndVerify: (coseSign1: Uint8Array) =>
    decodeAndVerifyCoseSign1(coseSign1, verifyingKey, externalAad),
});

export const encodeSign1 = async (
  payload: unknown,
  signingKey: PrivateKey,
  externalAad: Uint8Array = new Uint8Array(0),
  unprotectedHeaders: Record<string, unknown> = {}
): Promise<Uint8Array> => {
  const payloadCbor = await coseEncoder.encode(payload);

  const toBeSigned = await coseEncoder.encode([
    'Signature1', // context
    ed25519Header, // body_protected
    externalAad, // external_aad
    payloadCbor, // payload
  ]);

  const signature = await signAsync(toBeSigned, signingKey);
  const coseSign1 = await coseEncoder.encode([
    ed25519Header,
    unprotectedHeaders,
    payloadCbor,
    signature,
  ]);

  return coseSign1;
};

export const CoseSign1Encoder = (
  signingKey: PrivateKey,
  externalAad: Uint8Array = new Uint8Array(0)
) => ({
  encode: (payload: unknown, unprotectedHeaders: Record<string, unknown> = {}) =>
    encodeSign1(payload, signingKey, externalAad, unprotectedHeaders),
});
