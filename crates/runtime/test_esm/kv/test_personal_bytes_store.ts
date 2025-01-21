import { runOnHttp } from "@proven-network/handler";
import { getPersonalBytesStore } from "@proven-network/kv";

const PERSONAL_BYTES_STORE = getPersonalBytesStore("myBytesStore");

// Use HTTP so we can test with and without a session
export const test = runOnHttp(
  async () => {
    const toSave = new Uint8Array([1, 2, 3]);

    await PERSONAL_BYTES_STORE.set("key", new Uint8Array([1, 2, 3]));

    const restored = await PERSONAL_BYTES_STORE.get("key");

    if (!restored) {
      throw new Error("Value not found");
    }

    if (!restored.every((byte, index) => byte === toSave[index])) {
      throw new Error("Value mismatch");
    }

    const keys = await PERSONAL_BYTES_STORE.keys();

    if (keys.length !== 1) {
      throw new Error("Expected one key");
    }

    if (keys[0] !== "key") {
      throw new Error("Unexpected key");
    }
  },
  { path: "/test" }
);
