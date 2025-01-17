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
}
