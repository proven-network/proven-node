import { PrivateKey } from '@proven-network/crypto';

export type BytesStore = {
  get(key: string): Promise<Uint8Array | undefined>;
  keys(): Promise<string[]>;
  set(key: string, value: Uint8Array): Promise<void>;
};

export type KeyStore = {
  get(key: string): Promise<PrivateKey | undefined>;
  keys(): Promise<string[]>;
  set(key: string, value: PrivateKey): Promise<void>;
};

export type StringStore = {
  get(key: string): Promise<string | undefined>;
  keys(): Promise<string[]>;
  set(key: string, value: string): Promise<void>;
};

export type NftScopedBytesStore = {
  get(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string
  ): Promise<Uint8Array | undefined>;
  keys(nftResourceAddress: string, nftId: string | number | Uint8Array): Promise<string[]>;
  set(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string,
    value: Uint8Array
  ): Promise<void>;
};

export type NftScopedKeyStore = {
  get(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string
  ): Promise<PrivateKey | undefined>;
  keys(nftResourceAddress: string, nftId: string | number | Uint8Array): Promise<string[]>;
  set(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string,
    value: PrivateKey
  ): Promise<void>;
};

export type NftScopedStringStore = {
  get(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string
  ): Promise<string | undefined>;
  keys(nftResourceAddress: string, nftId: string | number | Uint8Array): Promise<string[]>;
  set(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string,
    value: string
  ): Promise<void>;
};

class ApplicationBytesStore implements BytesStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  get(key: string): Promise<Uint8Array | undefined> {
    return Promise.resolve(new Uint8Array());
  }

  keys(): Promise<string[]> {
    return Promise.resolve([]);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  set(key: string, value: Uint8Array): Promise<void> {
    return Promise.resolve();
  }
}

class ApplicationKeyStore implements KeyStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  get(key: string): Promise<PrivateKey | undefined> {
    return Promise.resolve(new PrivateKey(0));
  }

  keys(): Promise<string[]> {
    return Promise.resolve([]);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  set(key: string, value: PrivateKey): Promise<void> {
    return Promise.resolve();
  }
}

class ApplicationStringStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  get(key: string): Promise<string | undefined> {
    return Promise.resolve('');
  }

  keys(): Promise<string[]> {
    return Promise.resolve([]);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  set(key: string, value: string): Promise<void> {
    return Promise.resolve();
  }
}

export function getApplicationBytesStore(storeName: string): BytesStore {
  return new ApplicationBytesStore(storeName);
}

export function getApplicationKeyStore(storeName: string): KeyStore {
  return new ApplicationKeyStore(storeName);
}

export function getApplicationStore(storeName: string): StringStore {
  return new ApplicationStringStore(storeName);
}

class PersonalBytesStore implements BytesStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  get(key: string): Promise<Uint8Array | undefined> {
    return Promise.resolve(new Uint8Array());
  }

  keys(): Promise<string[]> {
    return Promise.resolve([]);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  set(key: string, value: Uint8Array): Promise<void> {
    return Promise.resolve();
  }
}

class PersonalKeyStore implements KeyStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  get(key: string): Promise<PrivateKey | undefined> {
    return Promise.resolve(new PrivateKey(0));
  }

  keys(): Promise<string[]> {
    return Promise.resolve([]);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  set(key: string, value: PrivateKey): Promise<void> {
    return Promise.resolve();
  }
}

class PersonalStringStore implements StringStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  get(key: string): Promise<string | undefined> {
    return Promise.resolve('');
  }

  keys(): Promise<string[]> {
    return Promise.resolve([]);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  set(key: string, value: string): Promise<void> {
    return Promise.resolve();
  }
}

export function getPersonalBytesStore(storeName: string): BytesStore {
  return new PersonalBytesStore(storeName);
}

export function getPersonalKeyStore(storeName: string): KeyStore {
  return new PersonalKeyStore(storeName);
}

export function getPersonalStore(storeName: string): StringStore {
  return new PersonalStringStore(storeName);
}

class NftBytesStore implements NftScopedBytesStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  get(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string
  ): Promise<Uint8Array | undefined> {
    return Promise.resolve(new Uint8Array());
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  keys(nftResourceAddress: string, nftId: string | number | Uint8Array): Promise<string[]> {
    return Promise.resolve([]);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  set(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string,
    value: Uint8Array
  ): Promise<void> {
    return Promise.resolve();
  }
}

class NftKeyStore implements NftScopedKeyStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  get(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string
  ): Promise<PrivateKey | undefined> {
    return Promise.resolve(new PrivateKey(0));
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  keys(nftResourceAddress: string, nftId: string | number | Uint8Array): Promise<string[]> {
    return Promise.resolve([]);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  set(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string,
    value: PrivateKey
  ): Promise<void> {
    return Promise.resolve();
  }
}

class NftStringStore implements NftScopedStringStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  get(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string
  ): Promise<string | undefined> {
    return Promise.resolve('');
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  keys(nftResourceAddress: string, nftId: string | number | Uint8Array): Promise<string[]> {
    return Promise.resolve([]);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  set(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string,
    value: string
  ): Promise<void> {
    return Promise.resolve();
  }
}

export function getNftStore(storeName: string): NftScopedStringStore {
  return new NftStringStore(storeName);
}

export function getNftBytesStore(storeName: string): NftScopedBytesStore {
  return new NftBytesStore(storeName);
}

export function getNftKeyStore(storeName: string): NftScopedKeyStore {
  return new NftKeyStore(storeName);
}
