import { runWithOptions } from "@proven-network/handler";

export const test = runWithOptions({ timeout: 10000 }, async () => {
  const response = await fetch("https://example.com/");

  return response.status;
});
