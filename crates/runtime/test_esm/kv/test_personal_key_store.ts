import { generateEd25519Key } from "@proven-network/crypto";
import { getPersonalKeyStore } from "@proven-network/kv";

const PERSONAL_KEY_STORE = getPersonalKeyStore("myKeyStore");

export const test = async () => {
  const toSave = generateEd25519Key();

  await PERSONAL_KEY_STORE.set("key", toSave);

  const restored = await PERSONAL_KEY_STORE.get("key");

  if (!restored) {
    throw new Error("Value not found");
  }

  // Just compare public keys since private key bytes are not exposed
  const comparison = toSave.publicKeyBytes();
  if (!restored.publicKeyBytes().every((byte, index) => byte === comparison[index])) {
    throw new Error("Value mismatch");
  }
}
