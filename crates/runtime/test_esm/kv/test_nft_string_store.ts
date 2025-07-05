import { run } from '@proven-network/handler';
import { getNftStore } from '@proven-network/kv';

const NFT_STORE = getNftStore('myStore');
const RESOURCE_ADDR = 'resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi';

export const test = run(async () => {
  const nftId = 420;
  await NFT_STORE.set(RESOURCE_ADDR, nftId, 'key', 'myValue');

  const restored = await NFT_STORE.get(RESOURCE_ADDR, nftId, 'key');

  if (!restored) {
    throw new Error('Value not found');
  }

  if (restored !== 'myValue') {
    throw new Error(`Value mismatch: got ${restored}, expected myValue`);
  }

  const keys = await NFT_STORE.keys(RESOURCE_ADDR, nftId);

  if (keys.length !== 1) {
    throw new Error('Expected one key');
  }

  if (keys[0] !== 'key') {
    throw new Error('Unexpected key');
  }
});
