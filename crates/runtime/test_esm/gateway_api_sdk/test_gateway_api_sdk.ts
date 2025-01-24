import { run } from "@proven-network/handler";

import { RadixNetwork } from "@radixdlt/babylon-gateway-api-sdk";

export const test = run(() => {
  return RadixNetwork.Mainnet;
});
