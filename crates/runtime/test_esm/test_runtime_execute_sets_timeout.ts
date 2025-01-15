import { runWithOptions } from "@proven-network/handler";

export const test = runWithOptions(async () => {
    await new Promise<void>((resolve) => {
        setTimeout(() => resolve(), 1500);
    });
}, { timeout: 2000 });
