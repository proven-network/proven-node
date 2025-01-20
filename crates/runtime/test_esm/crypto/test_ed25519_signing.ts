import { generateEd25519Key } from "@proven-network/crypto";

export const test = async () => {
  const key = generateEd25519Key();

  return [key.publicKey().bytes, key.signToSignature("Hello, world!").bytes];
};
