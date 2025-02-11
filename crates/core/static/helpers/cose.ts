import { Encoder, decode } from 'cbor-x'
import { eddsa } from 'elliptic'
import { uint8ArrayToHex } from '../helpers/uint8array'
import { Result, ok, err } from 'neverthrow'

// Note: This is a tiny implementation of COSE Sign1, using elliptic for signing
// and verifying of EdDSA signatures due to inability to use @auth0/cose due to
// the browser's WebCrypto API not supporting EdDSA.
//
// Notes from cose Signature1 spec:
// Sig_structure = [
//   context : "Signature" / "Signature1" / "CounterSignature",
//   body_protected : empty_or_serialized_map,
//   ? sign_protected : empty_or_serialized_map,
//   external_aad : bstr,
//   payload : bstr
// ]

type Payload = unknown
type UnprotectedHeaders = Record<string, unknown>
type DecodedMessage = {
  headers: UnprotectedHeaders
  payload: Payload
}

const ed25519Header = Buffer.from([0xa1, 0x01, 0x27]) // -7 = EdDSA
const coseEncoder = new Encoder({
  variableMapSize: true,
  tagUint8Array: false,
  useRecords: false,
  mapsAsObjects: false,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  useTag259ForMaps: false,
})

export const decodeAndVerifyCoseSign1 = async (
  coseSign1: Uint8Array,
  verifyingKey: eddsa.KeyPair,
  externalAad: Uint8Array = Buffer.alloc(0),
): Promise<Result<DecodedMessage, string>> => {
  const coseElements = (await decode(coseSign1)) as [
    Uint8Array,
    UnprotectedHeaders,
    Uint8Array,
    Uint8Array,
  ]

  if (coseElements.length !== 4) {
    return err('Invalid COSE Sign1 structure.')
  }

  const [protectedHeaders, unprotectedHeaders, payload, signature] =
    coseElements

  const toBeSigned = await coseEncoder.encode([
    'Signature1', // context
    protectedHeaders, // body_protected
    externalAad, // external_aad
    payload, // payload
  ])

  if (
    !verifyingKey.verify(
      uint8ArrayToHex(toBeSigned),
      uint8ArrayToHex(signature),
    )
  ) {
    return err('COSE Sign1 verification failed.')
  }

  const decodedPayload = await decode(payload)
  return ok({ headers: unprotectedHeaders, payload: decodedPayload })
}

export const CoseSign1Decoder = (
  verifyingKey: eddsa.KeyPair,
  externalAad: Uint8Array = Buffer.alloc(0),
) => ({
  decodeAndVerify: (coseSign1: Uint8Array) =>
    decodeAndVerifyCoseSign1(coseSign1, verifyingKey, externalAad),
})

export const encodeSign1 = async (
  payload: unknown,
  signingKey: eddsa.KeyPair,
  externalAad: Uint8Array = Buffer.alloc(0),
  unprotectedHeaders: Record<string, unknown> = {},
): Promise<Uint8Array> => {
  const payloadCbor = await coseEncoder.encode(payload)

  const toBeSigned = await coseEncoder.encode([
    'Signature1', // context
    ed25519Header, // body_protected
    externalAad, // external_aad
    payloadCbor, // payload
  ])

  const signature = Buffer.from(signingKey.sign(toBeSigned).toBytes())
  const coseSign1 = await coseEncoder.encode([
    ed25519Header,
    unprotectedHeaders,
    payloadCbor,
    signature,
  ])

  return coseSign1
}

export const CoseSign1Encoder = (
  signingKey: eddsa.KeyPair,
  externalAad: Uint8Array = Buffer.alloc(0),
) => ({
  encode: (
    payload: unknown,
    unprotectedHeaders: Record<string, unknown> = {},
  ) => encodeSign1(payload, signingKey, externalAad, unprotectedHeaders),
})
