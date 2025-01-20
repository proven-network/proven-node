import { generateEd25519Key } from "@proven-network/crypto";
import { getApplicationKeyStore } from "@proven-network/kv";

const KEY_STORE = getApplicationKeyStore("keys");

export const save = async () => {
  const key = generateEd25519Key();

  await KEY_STORE.set("key", key);

  return key.publicKey().bytes;
};

export const load = async () => {
  const key = await KEY_STORE.get("key");

  if (!key) {
    throw new Error("Key not found");
  }

  return key.signToSignature("Hello, world!").bytes;
};
