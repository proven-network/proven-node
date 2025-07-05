import { run } from '@proven-network/handler';
import { getCurrentIdentity } from '@proven-network/session';

export const test = run(() => {
  const identity = getCurrentIdentity();
  return identity;
});
