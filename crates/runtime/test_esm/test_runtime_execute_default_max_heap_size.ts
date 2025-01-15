import { runWithOptions } from "@proven-network/handler";

export const test = runWithOptions(() => {
    const largeArray = new Array(40 * 1024 * 1024).fill('a');
    return largeArray;
}, { timeout: 30000 });
