import { run } from '@proven-network/handler';

export const test = run(() => {
  const file = 'test.txt';
  const content = 'Hello, world!';

  Deno.writeTextFileSync(file, content);
  const readContent = Deno.readTextFileSync(file);

  return readContent;
});
