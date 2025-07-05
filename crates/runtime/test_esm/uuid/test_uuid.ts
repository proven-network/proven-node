import { run } from '@proven-network/handler';

import { v4 as uuidv4 } from 'uuid';

export const test = run(() => {
  return uuidv4();
});
