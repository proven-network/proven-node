import { runOnHttp } from "@proven-network/handler";
import { getCurrentIdentity } from "@proven-network/session";

// Use HTTP so we can test with and without a session
export const test = runOnHttp(
  () => {
    const identity = getCurrentIdentity();
    return identity;
  },
  { path: "/test" }
);
