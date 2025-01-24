import { runWithOptions } from "@proven-network/handler";

export const test = runWithOptions({ timeout: 2000 }, async () => {
  await new Promise<void>((resolve) => {
    setTimeout(() => resolve(), 1500);
  });
});
