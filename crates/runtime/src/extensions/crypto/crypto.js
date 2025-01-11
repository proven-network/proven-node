class PublicKey {
  constructor(bytes) {
    this.bytes = bytes; // Uint8Array
  }

  hexString() {
    return Array.from(this.bytes)
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  }

  rawBytes = this.bytes;
  toString = this.hexString;
}

class Signature {
  constructor(bytes) {
    this.bytes = bytes; // Uint8Array
  }

  hexString() {
    return Array.from(this.bytes)
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  }

  rawBytes = this.bytes;
  toString = this.hexString;
}

class SigningKey {
  constructor(keyId) {
    this.keyId = keyId;
  }

  publicKey() {
    const { op_get_public_key } = Deno.core.ops;
    return new PublicKey(op_get_public_key(this.keyId));
  }

  sign(data) {
    const { op_sign_bytes, op_sign_string } = Deno.core.ops;

    if (typeof data === "string") {
      return new Signature(op_sign_string(this.keyId, data));
    } else if (data instanceof Uint8Array) {
      return new Signature(op_sign_bytes(this.keyId, data));
    } else {
      throw new Error("Invalid type for signing");
    }
  }
}

export function generateEd25519Key() {
  const { op_generate_ed25519 } = Deno.core.ops;
  const keyId = op_generate_ed25519();

  return new SigningKey(keyId);
}
