import { generateEd25519Key } from '@proven-network/crypto';
import { run } from '@proven-network/handler';

export const test = run(async () => {
  const key = generateEd25519Key();

  return [key.publicKey().bytes, key.signToSignature('Hello, world!').bytes];
});
