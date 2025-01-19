import {
  Curve as RadixCurve,
  PublicKey as RadixPublicKey,
  PrivateKey as RadixPrivateKey,
  Signer as RadixSigner,
  SignerResponse as RadixSignerResponse,
  Signature as RadixSignature,
  SignatureWithPublicKey as RadixSignatureWithPublicKey,
} from "@radixdlt/radix-engine-toolkit";

type Curve = "Ed25519" & RadixCurve;

const {
  op_generate_ed25519,
  op_get_curve_name,
  op_get_public_key,
  op_sign_bytes,
  op_sign_string,
} = Deno.core.ops;

class PublicKey implements RadixPublicKey {
  bytes: Uint8Array;
  curve: Curve;

  get publicKey(): Uint8Array {
    return this.bytes;
  }

  constructor(bytes: Uint8Array, curve: Curve) {
    this.bytes = bytes;
    this.curve = curve;
  }

  hex(): string {
    return this.hexString();
  }

  hexString(): string {
    return Array.from(this.bytes)
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  }

  rawBytes(): Uint8Array {
    return this.bytes;
  }

  toString(): string {
    return this.hexString();
  }
}

class Signature implements RadixSignature {
  bytes: Uint8Array;
  curve: Curve;

  get signature(): Uint8Array {
    return this.bytes;
  }

  constructor(bytes: Uint8Array, curve: Curve) {
    this.bytes = bytes;
    this.curve = curve;
  }

  hex(): string {
    return this.hexString();
  }

  hexString(): string {
    return Array.from(this.bytes)
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  }

  rawBytes(): Uint8Array {
    return this.bytes;
  }

  toString(): string {
    return this.hexString();
  }
}

class SignatureWithPublicKey implements RadixSignatureWithPublicKey {
  bytes: Uint8Array;
  curve: Curve;
  publicKey: Uint8Array;

  get signature(): Uint8Array {
    return this.bytes;
  }

  constructor(bytes: Uint8Array, curve: Curve, publicKey: Uint8Array) {
    this.bytes = bytes;
    this.curve = curve;
    this.publicKey = publicKey;
  }
}

export class PrivateKey implements Omit<RadixPrivateKey, "bytes">, RadixSigner {
  private keyId: number;

  get curve(): Curve {
    return op_get_curve_name(this.keyId);
  }

  constructor(keyId: number) {
    this.keyId = keyId;
  }

  // For Radix Engine Toolkit
  produceSignature(data: any): RadixSignerResponse {
    return {
      curve: this.curve,
      publicKey: this.publicKeyBytes(),
      signature: this.sign(data),
    };
  }

  publicKey(): PublicKey {
    const publicKey = new PublicKey(op_get_public_key(this.keyId), this.curve);

    Object.freeze(publicKey);

    return publicKey;
  }

  publicKeyBytes(): Uint8Array {
    return this.publicKey().bytes;
  }

  publicKeyHex(): string {
    return this.publicKey().hexString();
  }

  sign(data: string | Uint8Array): Uint8Array {
    return this.signToSignature(data).bytes;
  }

  signToSignature(data: string | Uint8Array): Signature {
    let signature;

    if (typeof data === "string") {
      signature = new Signature(op_sign_string(this.keyId, data), this.curve);
    } else if (data instanceof Uint8Array) {
      signature = new Signature(op_sign_bytes(this.keyId, data), this.curve);
    } else {
      throw new Error("Invalid type for signing");
    }

    Object.freeze(signature);

    return signature;
  }

  signToSignatureWithPublicKey(
    data: string | Uint8Array
  ): SignatureWithPublicKey {
    let signatureWithPublicKey;

    if (typeof data === "string") {
      signatureWithPublicKey = new SignatureWithPublicKey(
        op_sign_string(this.keyId, data),
        this.curve,
        this.publicKeyBytes()
      );
    } else if (data instanceof Uint8Array) {
      signatureWithPublicKey = new SignatureWithPublicKey(
        op_sign_bytes(this.keyId, data),
        this.curve,
        this.publicKeyBytes()
      );
    } else {
      throw new Error("Invalid type for signing");
    }

    Object.freeze(signatureWithPublicKey);

    return signatureWithPublicKey;
  }
}

export function generateEd25519Key(): PrivateKey {
  const keyId = op_generate_ed25519();

  const privateKey = new PrivateKey(keyId);
  Object.freeze(privateKey);

  return privateKey;
}
