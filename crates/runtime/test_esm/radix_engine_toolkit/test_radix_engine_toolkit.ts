import { run } from "@proven-network/handler";

import { RadixEngineToolkit } from "@radixdlt/radix-engine-toolkit";

export const test = run(async () => {
  return JSON.stringify(await RadixEngineToolkit.Build.information());
});
