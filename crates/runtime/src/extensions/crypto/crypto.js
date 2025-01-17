class PublicKey {
  get publicKey() {
    return this.bytes;
  }

  constructor(bytes, curve) {
    this.bytes = bytes; // Uint8Array
    this.curve = curve; // String
  }

  hex = this.hexString;

  hexString() {
    return Array.from(this.bytes)
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  }

  rawBytes() {
    return this.bytes;
  }

  toString = this.hexString;
}

class Signature {
  constructor(bytes, curve) {
    this.bytes = bytes; // Uint8Array
    this.curve = curve; // String
  }

  hex = this.hexString;

  hexString() {
    return Array.from(this.bytes)
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  }

  rawBytes() {
    return this.bytes;
  }

  signature = this.bytes;
  toString = this.hexString;
}

class SignatureWithPublicKey {
  constructor(bytes, curve, publicKey) {
    this.bytes = bytes; // Uint8Array
    this.curve = curve; // String
    this.publicKey = publicKey; // Uint8Array
  }

  hex = this.hexString;

  hexString() {
    return Array.from(this.bytes)
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  }

  rawBytes() {
    return this.bytes;
  }

  signature = this.bytes;
  toString = this.hexString;
}

export class PrivateKey {
  get curve() {
    const { op_get_curve_name } = Deno.core.ops;
    return op_get_curve_name(this.keyId);
  }

  constructor(keyId) {
    this.keyId = keyId;
  }

  // For Radix Engine Toolkit
  produceSignature(data) {
    return {
      curve: this.curve,
      publicKey: this.publicKeyBytes(),
      signature: this.sign(data),
    }
  }

  publicKey() {
    const { op_get_public_key } = Deno.core.ops;
    return new PublicKey(op_get_public_key(this.keyId), this.curve);
  }

  publicKeyBytes() {
    return this.publicKey().bytes;
  }

  publicKeyHex() {
    return this.publicKey().hexString();
  }

  sign(data) {
    return this.signToSignature(data).bytes;
  }

  signToSignature(data) {
    const { op_sign_bytes, op_sign_string } = Deno.core.ops;

    if (typeof data === "string") {
      return new Signature(op_sign_string(this.keyId, data), this.curve);
    } else if (data instanceof Uint8Array) {
      return new Signature(op_sign_bytes(this.keyId, data), this.curve);
    } else {
      throw new Error("Invalid type for signing");
    }
  }

  signToSignatureWithPublicKey(data) {
    const { op_sign_bytes, op_sign_string } = Deno.core.ops;

    if (typeof data === "string") {
      return new SignatureWithPublicKey(op_sign_string(this.keyId, data), this.curve, this.publicKeyBytes());
    } else if (data instanceof Uint8Array) {
      return new SignatureWithPublicKey(op_sign_bytes(this.keyId, data), this.curve, this.publicKeyBytes());
    } else {
      throw new Error("Invalid type for signing");
    }
  }
}

export function generateEd25519Key() {
  const { op_generate_ed25519 } = Deno.core.ops;
  const keyId = op_generate_ed25519();

  const privateKey = new PrivateKey(keyId);
  Object.freeze(privateKey);

  return privateKey;
}
