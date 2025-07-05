import {
  Curve as RadixCurve,
  PublicKey as RadixPublicKey,
  PrivateKey as RadixPrivateKey,
  Signer as RadixSigner,
  SignerResponse as RadixSignerResponse,
  Signature as RadixSignature,
  SignatureWithPublicKey as RadixSignatureWithPublicKey,
} from '@radixdlt/radix-engine-toolkit';

class PublicKey implements RadixPublicKey {
  bytes: Uint8Array;

  get curve(): RadixCurve {
    return 'Ed25519';
  }

  get publicKey() {
    return this.bytes;
  }

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
  }

  hex = this.hexString;

  hexString() {
    return Array.from(this.bytes)
      .map((byte) => byte.toString(16).padStart(2, '0'))
      .join('');
  }

  rawBytes() {
    return this.bytes;
  }

  toString = this.hexString;
}

class Signature implements RadixSignature {
  bytes: Uint8Array;

  get curve(): RadixCurve {
    return 'Ed25519';
  }

  get signature(): Uint8Array {
    return this.bytes;
  }

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
  }

  hex() {
    return this.hexString();
  }

  hexString() {
    return Array.from(this.bytes)
      .map((byte) => byte.toString(16).padStart(2, '0'))
      .join('');
  }

  rawBytes() {
    return this.bytes;
  }

  toString = this.hexString;
}

class SignatureWithPublicKey implements RadixSignatureWithPublicKey {
  bytes: Uint8Array;
  publicKey: Uint8Array;

  get curve(): RadixCurve {
    return 'Ed25519';
  }

  get signature(): Uint8Array {
    return this.bytes;
  }

  constructor(bytes: Uint8Array, publicKey: Uint8Array) {
    this.bytes = bytes;
    this.publicKey = publicKey;
  }

  hex() {
    return this.hexString();
  }

  hexString() {
    return Array.from(this.bytes)
      .map((byte) => byte.toString(16).padStart(2, '0'))
      .join('');
  }

  rawBytes() {
    return this.bytes;
  }

  toString = this.hexString;
}

export class PrivateKey implements Omit<RadixPrivateKey, 'bytes'>, RadixSigner {
  keyId: number;

  get curve(): RadixCurve {
    return 'Ed25519';
  }

  constructor(keyId: number) {
    this.keyId = keyId;
  }

  produceSignature(messageHash: Uint8Array): RadixSignerResponse {
    return {
      curve: this.curve,
      publicKey: this.publicKeyBytes(),
      signature: this.sign(messageHash),
    };
  }

  publicKey(): RadixPublicKey {
    return new PublicKey(new Uint8Array());
  }

  publicKeyBytes(): Uint8Array {
    return this.publicKey().bytes;
  }

  publicKeyHex(): string {
    return this.publicKey().hexString();
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  sign(data: string | Uint8Array): Uint8Array {
    return new Signature(new Uint8Array()).bytes;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  signToSignature(data: string | Uint8Array): Signature {
    return new Signature(new Uint8Array());
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  signToSignatureWithPublicKey(data: string | Uint8Array): SignatureWithPublicKey {
    return new SignatureWithPublicKey(new Uint8Array(), this.publicKeyBytes());
  }
}

export function generateEd25519Key() {
  return new PrivateKey(0);
}
