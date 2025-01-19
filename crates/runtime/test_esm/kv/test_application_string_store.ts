import { getApplicationStore } from "@proven-network/kv";

const APP_STORE = getApplicationStore("myStore");

export const test = async () => {
  await APP_STORE.set("key", "myValue");

  const restored = await APP_STORE.get("key");

  if (!restored) {
    throw new Error("Value not found");
  }

  if (restored !== "myValue") {
    throw new Error("Value mismatch");
  }

  const keys = await APP_STORE.keys();

  if (keys.length !== 1) {
    throw new Error("Expected one key");
  }

  if (keys[0] !== "key") {
    throw new Error("Unexpected key");
  }
}
