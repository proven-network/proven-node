import { run } from '@proven-network/handler';
import { getCurrentAccounts } from '@proven-network/session';

export const test = run(() => {
  const accounts = getCurrentAccounts();
  return accounts;
});
