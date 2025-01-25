import {
  getApplicationStore as _getApplicationStore,
  getApplicationBytesStore as _getApplicationBytesStore,
  getApplicationKeyStore as _getApplicationKeyStore,
  getNftStore as _getNftStore,
  getNftBytesStore as _getNftBytesStore,
  getNftKeyStore as _getNftKeyStore,
  getPersonalStore as _getPersonalStore,
  getPersonalBytesStore as _getPersonalBytesStore,
  getPersonalKeyStore as _getPersonalKeyStore,
} from "@proven-network/kv";

import { PrivateKey } from "@proven-network/crypto";

type IApplicationBytesStore = ReturnType<typeof _getApplicationBytesStore>;
type IApplicationKeyStore = ReturnType<typeof _getApplicationKeyStore>;
type IApplicationStringStore = ReturnType<typeof _getApplicationStore>;
type INftBytesStore = ReturnType<typeof _getNftBytesStore>;
type INftKeyStore = ReturnType<typeof _getNftKeyStore>;
type INftStringStore = ReturnType<typeof _getNftStore>;
type IPersonalBytesStore = ReturnType<typeof _getPersonalBytesStore>;
type IPersonalKeyStore = ReturnType<typeof _getPersonalKeyStore>;
type IPersonalStringStore = ReturnType<typeof _getPersonalStore>;

class ApplicationBytesStore implements IApplicationBytesStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(_key: string): Promise<Uint8Array | undefined> {
    throw new Error("`get` must be run inside a handler function");
  }

  async keys(): Promise<string[]> {
    throw new Error("`keys` must be run inside a handler function");
  }

  async set(_key: string, _value: Uint8Array): Promise<void> {
    throw new Error("`set` must be run inside a handler function");
  }
}

class ApplicationKeyStore implements IApplicationKeyStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(_key: string): Promise<PrivateKey | undefined> {
    throw new Error("`get` must be run inside a handler function");
  }

  async keys(): Promise<string[]> {
    throw new Error("`keys` must be run inside a handler function");
  }

  async set(_key: string, _value: PrivateKey): Promise<void> {
    throw new Error("`set` must be run inside a handler function");
  }
}

class ApplicationStringStore implements IApplicationStringStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(_key: string): Promise<string | undefined> {
    throw new Error("`get` must be run inside a handler function");
  }

  async keys(): Promise<string[]> {
    throw new Error("`keys` must be run inside a handler function");
  }

  async set(_key: string, _value: string): Promise<void> {
    throw new Error("`set` must be run inside a handler function");
  }
}

export const getApplicationStore = (storeName: string) => {
  if (!storeName) {
    throw new Error("storeName is required");
  }

  return new ApplicationStringStore(storeName);
};

export const getApplicationKeyStore = (storeName: string) => {
  if (!storeName) {
    throw new Error("storeName is required");
  }

  return new ApplicationKeyStore(storeName);
};

export const getApplicationBytesStore = (storeName: string) => {
  if (!storeName) {
    throw new Error("storeName is required");
  }

  return new ApplicationBytesStore(storeName);
};

class NftBytesStore implements INftBytesStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(_key: string): Promise<Uint8Array | undefined> {
    throw new Error("`get` must be run inside a handler function");
  }

  async keys(): Promise<string[]> {
    throw new Error("`keys` must be run inside a handler function");
  }

  async set(_key: string, _value: Uint8Array): Promise<void> {
    throw new Error("`set` must be run inside a handler function");
  }
}

class NftKeyStore implements INftKeyStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(
    _resourceAddress: string,
    _nftId: number | string | Uint8Array,
    _key: string
  ): Promise<PrivateKey | undefined> {
    throw new Error("`get` must be run inside a handler function");
  }

  async keys(
    _resourceAddress: string,
    _nftId: number | string | Uint8Array
  ): Promise<string[]> {
    throw new Error("`keys` must be run inside a handler function");
  }

  async set(
    _resourceAddress: string,
    _nftId: number | string | Uint8Array,
    _key: string,
    _value: PrivateKey
  ): Promise<void> {
    throw new Error("`set` must be run inside a handler function");
  }
}

class NftStringStore implements INftStringStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(
    _resourceAddress: string,
    _nftId: number | string | Uint8Array,
    _key: string
  ): Promise<string | undefined> {
    throw new Error("`get` must be run inside a handler function");
  }

  async keys(
    _resourceAddress: string,
    _nftId: number | string | Uint8Array
  ): Promise<string[]> {
    throw new Error("`keys` must be run inside a handler function");
  }

  async set(
    _resourceAddress: string,
    _nftId: number | string | Uint8Array,
    _key: string,
    _value: string
  ): Promise<void> {
    throw new Error("`set` must be run inside a handler function");
  }
}

export const getNftStore = (storeName: string) => {
  if (!storeName) {
    throw new Error("storeName is required");
  }

  return new NftStringStore(storeName);
};

export const getNftKeyStore = (storeName: string) => {
  if (!storeName) {
    throw new Error("storeName is required");
  }

  return new NftKeyStore(storeName);
};

export const getNftBytesStore = (storeName: string) => {
  if (!storeName) {
    throw new Error("storeName is required");
  }

  return new NftBytesStore(storeName);
};

class PersonalBytesStore implements IPersonalBytesStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(_key: string): Promise<Uint8Array | undefined> {
    throw new Error("`get` must be run inside a handler function");
  }

  async keys(): Promise<string[]> {
    throw new Error("`keys` must be run inside a handler function");
  }

  async set(_key: string, _value: Uint8Array): Promise<void> {
    throw new Error("`set` must be run inside a handler function");
  }
}

class PersonalKeyStore implements IPersonalKeyStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(_key: string): Promise<PrivateKey | undefined> {
    throw new Error("`get` must be run inside a handler function");
  }

  async keys(): Promise<string[]> {
    throw new Error("`keys` must be run inside a handler function");
  }

  async set(_key: string, _value: PrivateKey): Promise<void> {
    throw new Error("`set` must be run inside a handler function");
  }
}

class PersonalStringStore implements IPersonalStringStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(_key: string): Promise<string | undefined> {
    throw new Error("`get` must be run inside a handler function");
  }

  async keys(): Promise<string[]> {
    throw new Error("`keys` must be run inside a handler function");
  }

  async set(_key: string, _value: string): Promise<void> {
    throw new Error("`set` must be run inside a handler function");
  }
}

export const getPersonalStore = (storeName: string) => {
  if (!storeName) {
    throw new Error("storeName is required");
  }

  return new PersonalStringStore(storeName);
};

export const getPersonalKeyStore = (storeName: string) => {
  if (!storeName) {
    throw new Error("storeName is required");
  }

  return new PersonalKeyStore(storeName);
};

export const getPersonalBytesStore = (storeName: string) => {
  if (!storeName) {
    throw new Error("storeName is required");
  }

  return new PersonalBytesStore(storeName);
};
