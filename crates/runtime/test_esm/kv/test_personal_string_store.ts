import { runOnHttp } from "@proven-network/handler";
import { getPersonalStore } from "@proven-network/kv";

const PERSONAL_STORE = getPersonalStore("myStore");

// Use HTTP so we can test with and without a session
export const test = runOnHttp(
  async () => {
    await PERSONAL_STORE.set("key", "myValue");

    const restored = await PERSONAL_STORE.get("key");

    if (!restored) {
      throw new Error("Value not found");
    }

    if (restored !== "myValue") {
      throw new Error("Value mismatch");
    }

    const keys = await PERSONAL_STORE.keys();

    if (keys.length !== 1) {
      throw new Error("Expected one key");
    }

    if (keys[0] !== "key") {
      throw new Error("Unexpected key");
    }
  },
  { path: "/test" }
);
