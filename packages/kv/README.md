# Key-Value Store Types for Proven Application Code

This package defines key-value store types for usage in Proven Application code. It includes string, byte and key stores for application, personal, and NFT contexts.

## Installation

Install this package as a dev dependency:

```bash
npm install --save-dev @proven-network/kv
```

or

```bash
yarn add -D @proven-network/kv
```

## Usage

### Application Store

#### String Store

```typescript
import { getApplicationStore } from '@proven-network/kv';

const APP_STORE = getApplicationStore('myAppStore');

export const handler = async () => {
  await APP_STORE.set('myKey', 'myValue');

  const value = await APP_STORE.get('myKey')!;

  console.log(value); // Output: "myValue"
  console.log(await APP_STORE.keys()); // Output: ["myKey"]
};
```

#### Bytes Store

```typescript
import { getApplicationBytesStore } from '@proven-network/kv';

const APP_BYTES_STORE = getApplicationBytesStore('myAppBytesStore');

export const handler = async () => {
  await APP_BYTES_STORE.set('myKey', new Uint8Array([1, 2, 3]));

  const value = await APP_BYTES_STORE.get('myKey');

  console.log(value); // Output: Uint8Array(3) [1, 2, 3]
  console.log(await APP_STORE.keys()); // Output: ["myKey"]
};
```

#### Key Store

```typescript
import { generateEd25519Key } from '@proven-network/crypto';
import { getApplicationKeyStore } from '@proven-network/kv';

const APP_KEY_STORE = getApplicationKeyStore('myAppKeyStore');

export const handler = async () => {
  await APP_KEY_STORE.set('myKey', generateEd25519Key());

  const value = await APP_KEY_STORE.get('myKey');

  console.log(value); // Output: PrivateKey
  console.log(await APP_STORE.keys()); // Output: ["myKey"]
};
```

### Personal Store

#### String Store

```typescript
import { getPersonalStore } from '@proven-network/kv';

const PERSONAL_STORE = getPersonalStore('myPersonalStore');

export const handler = async () => {
  await PERSONAL_STORE.set('myKey', 'myValue');

  const value = await PERSONAL_STORE.get('myKey');

  console.log(value); // Output: "myValue"
  console.log(await PERSONAL_STORE.keys()); // Output: ["myKey"]
};
```

#### Bytes Store

```typescript
import { getPersonalBytesStore } from '@proven-network/kv';

const PERSONAL_BYTES_STORE = getPersonalBytesStore('myPersonalBytesStore');

export const handler = async () => {
  await PERSONAL_BYTES_STORE.set('myKey', new Uint8Array([1, 2, 3]));

  const value = await PERSONAL_BYTES_STORE.get('myKey');

  console.log(value); // Output: Uint8Array(3) [1, 2, 3]
  console.log(await PERSONAL_STORE.keys()); // Output: ["myKey"]
};
```

#### Key Store

```typescript
import { generateEd25519Key } from '@proven-network/crypto';
import { getPersonalKeyStore } from '@proven-network/kv';

const PERSONAL_KEY_STORE = getApplicationKeyStore('myPersonalKeyStore');

export const handler = async () => {
  await PERSONAL_KEY_STORE.set('myKey', generateEd25519Key());

  const value = await PERSONAL_KEY_STORE.get('myKey');

  console.log(value); // Output: PrivateKey
  console.log(await PERSONAL_STORE.keys()); // Output: ["myKey"]
};
```

### NFT Store

#### String Store

```typescript
import { getNftStore } from '@proven-network/kv';

const NFT_STORE = getNftStore('myNftStore');
const RESOURCE_ADDR = 'resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi';

export const handler = async (nftId: string) => {
  await NFT_STORE.set(RESOURCE_ADDR, nftId, 'myKey', 'myValue');

  const value = await NFT_STORE.get(RESOURCE_ADDR, nftId, 'myKey');

  console.log(value); // Output: "myValue"
  console.log(await NFT_STORE.keys(RESOURCE_ADDR, nftId)); // Output: ["myKey"]
};
```

#### Bytes Store

```typescript
import { getNftBytesStore } from '@proven-network/kv';

const NFT_BYTES_STORE = getNftBytesStore('myNftBytesStore');
const RESOURCE_ADDR = 'resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi';

export const handler = async (nftId: string) => {
  await NFT_BYTES_STORE.set(RESOURCE_ADDR, nftId, 'myKey', new Uint8Array([1, 2, 3]));

  const value = await NFT_BYTES_STORE.get(RESOURCE_ADDR, nftId, 'myKey');

  console.log(value); // Output: Uint8Array(3) [1, 2, 3]
  console.log(await NFT_STORE.keys(RESOURCE_ADDR, nftId)); // Output: ["myKey"]
};
```

#### Key Store

```typescript
import { generateEd25519Key } from '@proven-network/crypto';
import { getNftKeyStore } from '@proven-network/kv';

const NFT_KEY_STORE = getNftKeyStore('myNftKeyStore');
const RESOURCE_ADDR = 'resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi';

export const handler = async (nftId: string) => {
  await NFT_KEY_STORE.set(RESOURCE_ADDR, nftId, generateEd25519Key());

  const value = await NFT_KEY_STORE.get(RESOURCE_ADDR, nftId);

  console.log(value); // Output: PrivateKey
  console.log(await NFT_STORE.keys(RESOURCE_ADDR, nftId)); // Output: ["myKey"]
};
```
