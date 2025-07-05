import { run } from '@proven-network/handler';
import { z } from 'zod';

export const test = run(() => {
  const schema = z.object({
    name: z.string(),
    age: z.number(),
  });

  return schema.parse({ name: 'Alice', age: 30 });
});
