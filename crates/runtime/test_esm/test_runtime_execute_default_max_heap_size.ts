import { runWithOptions } from "@proven-network/handler";

export const test = runWithOptions({ timeout: 30000 }, () => {
  const largeArray = new Array(40 * 1024 * 1024).fill("a");
  return largeArray;
});
