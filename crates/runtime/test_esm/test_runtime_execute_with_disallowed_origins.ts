import { runWithOptions } from "@proven-network/handler";

export const test = runWithOptions(async () => {
    const response = await fetch("https://example.com/");

    return response.status;
}, { timeout: 10000 });
