import { generateEd25519Key } from '@proven-network/crypto';
import { run } from '@proven-network/handler';
import { getApplicationKeyStore } from '@proven-network/kv';

const APP_KEY_STORE = getApplicationKeyStore('myKeyStore');

export const test = run(async () => {
  const toSave = generateEd25519Key();

  await APP_KEY_STORE.set('key', toSave);

  const restored = await APP_KEY_STORE.get('key');

  if (!restored) {
    throw new Error('Value not found');
  }

  // Just compare public keys since private key bytes are not exposed
  const comparison = toSave.publicKeyBytes();
  if (!restored.publicKeyBytes().every((byte, index) => byte === comparison[index])) {
    throw new Error('Value mismatch');
  }

  const keys = await APP_KEY_STORE.keys();

  if (keys.length !== 1) {
    throw new Error('Expected one key');
  }

  if (keys[0] !== 'key') {
    throw new Error('Unexpected key');
  }
});
