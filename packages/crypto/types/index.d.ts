import {
  Curve as RadixCurve,
  PublicKey as RadixPublicKey,
  PrivateKey as RadixPrivateKey,
  Signer as RadixSigner,
  SignerResponse as RadixSignerResponse,
  Signature as RadixSignature,
  SignatureWithPublicKey as RadixSignatureWithPublicKey,
} from '@radixdlt/radix-engine-toolkit';
declare class Signature implements RadixSignature {
  bytes: Uint8Array;
  get curve(): RadixCurve;
  get signature(): Uint8Array;
  constructor(bytes: Uint8Array);
  hex(): string;
  hexString(): string;
  rawBytes(): Uint8Array<ArrayBufferLike>;
  toString: () => string;
}
declare class SignatureWithPublicKey implements RadixSignatureWithPublicKey {
  bytes: Uint8Array;
  publicKey: Uint8Array;
  get curve(): RadixCurve;
  get signature(): Uint8Array;
  constructor(bytes: Uint8Array, publicKey: Uint8Array);
  hex(): string;
  hexString(): string;
  rawBytes(): Uint8Array<ArrayBufferLike>;
  toString: () => string;
}
export declare class PrivateKey implements Omit<RadixPrivateKey, 'bytes'>, RadixSigner {
  keyId: number;
  get curve(): RadixCurve;
  constructor(keyId: number);
  produceSignature(messageHash: Uint8Array): RadixSignerResponse;
  publicKey(): RadixPublicKey;
  publicKeyBytes(): Uint8Array;
  publicKeyHex(): string;
  sign(data: string | Uint8Array): Uint8Array;
  signToSignature(data: string | Uint8Array): Signature;
  signToSignatureWithPublicKey(data: string | Uint8Array): SignatureWithPublicKey;
}
export declare function generateEd25519Key(): PrivateKey;
export {};
