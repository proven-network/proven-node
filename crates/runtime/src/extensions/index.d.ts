declare namespace Deno {
  namespace core {
    const ops: {
      // crypto
      op_generate_ed25519: () => number;
      op_get_curve_name: (keyId: number) => "Ed25519";
      op_get_public_key: (keyId: number) => Uint8Array;
      op_sign_bytes: (keyId: number, data: Uint8Array) => Uint8Array;
      op_sign_string: (keyId: number, data: string) => Uint8Array;

      // session
      op_get_current_accounts: () => string[];
      op_get_current_identity: () => string | undefined;
    };
  }
}
