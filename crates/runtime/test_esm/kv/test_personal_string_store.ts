import { getPersonalStore } from "@proven-network/kv";

const PERSONAL_STORE = getPersonalStore("myStore");

export const test = async () => {
  await PERSONAL_STORE.set("key", "myValue");

  const restored = await PERSONAL_STORE.get("key");

  if (!restored) {
    throw new Error("Value not found");
  }

  if (restored !== "myValue") {
    throw new Error("Value mismatch");
  }
}
