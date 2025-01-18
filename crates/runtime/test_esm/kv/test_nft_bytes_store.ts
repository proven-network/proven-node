import { getNftBytesStore } from "@proven-network/kv";

const NFT_BYTES_STORE = getNftBytesStore("myBytesStore");
const RESOURCE_ADDR = "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi";

export const test = async () => {
  const nftId = 420;
  const toSave = new Uint8Array([1, 2, 3]);

  await NFT_BYTES_STORE.set(RESOURCE_ADDR, nftId, "key", new Uint8Array([1, 2, 3]));

  const restored = await NFT_BYTES_STORE.get(RESOURCE_ADDR, nftId, "key");

  if (!restored) {
    throw new Error("Value not found");
  }

  if (!restored.every((byte, index) => byte === toSave[index])) {
    throw new Error("Value mismatch");
  }
}
