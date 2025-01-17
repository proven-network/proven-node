import { getApplicationBytesStore } from "@proven-network/kv";

const APP_BYTES_STORE = getApplicationBytesStore("myBytesStore");

export const test = async () => {
  const toSave = new Uint8Array([1, 2, 3]);

  await APP_BYTES_STORE.set("key", new Uint8Array([1, 2, 3]));

  const restored = await APP_BYTES_STORE.get("key");

  if (!restored) {
    throw new Error("Value not found");
  }

  if (!restored.every((byte, index) => byte === toSave[index])) {
    throw new Error("Value mismatch");
  }
}
