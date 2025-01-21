import { runOnHttp } from "@proven-network/handler";
import { getCurrentAccounts } from "@proven-network/session";

// Use HTTP so we can test with and without a session
export const test = runOnHttp(
  () => {
    const accounts = getCurrentAccounts();
    return accounts;
  },
  { path: "/test" }
);
