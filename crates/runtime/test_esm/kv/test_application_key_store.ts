import { generateEd25519Key } from "@proven-network/crypto";
import { getApplicationKeyStore } from "@proven-network/kv";

const APP_KEY_STORE = getApplicationKeyStore("myKeyStore");

export const test = async () => {
  const toSave = generateEd25519Key();

  await APP_KEY_STORE.set("key", toSave);

  const restored = await APP_KEY_STORE.get("key");

  if (!restored) {
    throw new Error("Value not found");
  }

  // Just compare public keys since private key bytes are not exposed
  const comparison = toSave.publicKeyBytes();
  if (!restored.publicKeyBytes().every((byte, index) => byte === comparison[index])) {
    throw new Error("Value mismatch");
  }
}
