import { getCurrentIdentity } from "@proven-network/session";

export const test = () => {
    const identity = getCurrentIdentity();
    return identity;
}
