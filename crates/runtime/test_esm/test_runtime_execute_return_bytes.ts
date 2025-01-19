import { runWithOptions } from "@proven-network/handler";

// TODO: Test every handler wrapper and no wrapper
export const test = runWithOptions(async () => {
  return [
    new Uint8Array([1, 2, 3]),
    {
      bytes: new Uint8Array([4, 5, 6])
    }
  ];
}, {});
