import { getCurrentAccounts } from "@proven-network/session";

export const test = () => {
    const accounts = getCurrentAccounts();
    return accounts;
}
