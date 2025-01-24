import { generateEd25519Key } from "@proven-network/crypto";
import { getApplicationKeyStore } from "@proven-network/kv";
import { run } from "@proven-network/handler";

const KEY_STORE = getApplicationKeyStore("keys");

export const save = run(async () => {
  const key = generateEd25519Key();

  await KEY_STORE.set("key", key);

  return key.publicKey().bytes;
});

export const load = run(async () => {
  const key = await KEY_STORE.get("key");

  if (!key) {
    throw new Error("Key not found");
  }

  return key.signToSignature("Hello, world!").bytes;
});
