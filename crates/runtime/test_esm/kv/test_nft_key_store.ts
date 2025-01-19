import { generateEd25519Key } from "@proven-network/crypto";
import { getNftKeyStore } from "@proven-network/kv";

const NFT_KEY_STORE = getNftKeyStore("myKeyStore");
const RESOURCE_ADDR = "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi";

export const test = async () => {
  const nftId = 420;
  const toSave = generateEd25519Key();

  await NFT_KEY_STORE.set(RESOURCE_ADDR, nftId, "key", toSave);

  const restored = await NFT_KEY_STORE.get(RESOURCE_ADDR, nftId, "key");

  if (!restored) {
    throw new Error("Value not found");
  }

  // Just compare public keys since private key bytes are not exposed
  const comparison = toSave.publicKeyBytes();
  if (!restored.publicKeyBytes().every((byte, index) => byte === comparison[index])) {
    throw new Error("Value mismatch");
  }

  const keys = await NFT_KEY_STORE.keys(RESOURCE_ADDR, nftId);

  if (keys.length !== 1) {
    throw new Error("Expected one key");
  }

  if (keys[0] !== "key") {
    throw new Error("Unexpected key");
  }
}
