import { PrivateKey } from '@proven-network/crypto';
import {
  BytesStore,
  KeyStore,
  StringStore,
  NftScopedBytesStore,
  NftScopedKeyStore,
  NftScopedStringStore,
} from '@proven-network/kv';

const {
  op_application_keys,
  op_get_application_bytes,
  op_set_application_bytes,
  op_get_application_key,
  op_set_application_key,
  op_get_application_string,
  op_set_application_string,
  op_nft_keys,
  op_get_nft_bytes,
  op_set_nft_bytes,
  op_get_nft_key,
  op_set_nft_key,
  op_get_nft_string,
  op_set_nft_string,
  op_personal_keys,
  op_get_personal_bytes,
  op_set_personal_bytes,
  op_get_personal_key,
  op_set_personal_key,
  op_get_personal_string,
  op_set_personal_string,
} = globalThis.Deno.core.ops;

class ApplicationBytesStore implements BytesStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    return await op_get_application_bytes(this.storeName, key);
  }

  async keys(): Promise<string[]> {
    return await op_application_keys(this.storeName, 'bytes');
  }

  async set(key: string, value: Uint8Array): Promise<void> {
    return await op_set_application_bytes(this.storeName, key, value);
  }
}

class ApplicationKeyStore implements KeyStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(key: string): Promise<PrivateKey | undefined> {
    const keyId = await op_get_application_key(this.storeName, key);

    if (typeof keyId === 'number') {
      const privateKey = new PrivateKey(keyId);
      Object.freeze(privateKey);

      return privateKey;
    } else {
      return;
    }
  }

  async keys(): Promise<string[]> {
    return await op_application_keys(this.storeName, 'key');
  }

  async set(key: string, value: PrivateKey): Promise<void> {
    return await op_set_application_key(this.storeName, key, value.keyId);
  }
}

class ApplicationStringStore implements StringStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(key: string): Promise<string | undefined> {
    return await op_get_application_string(this.storeName, key);
  }

  async keys(): Promise<string[]> {
    return await op_application_keys(this.storeName, 'string');
  }

  async set(key: string, value: string): Promise<void> {
    return await op_set_application_string(this.storeName, key, value);
  }
}

export const getApplicationBytesStore = (storeName: string) => {
  if (!storeName) {
    throw new Error('storeName is required');
  }

  return new ApplicationBytesStore(storeName);
};

export const getApplicationKeyStore = (storeName: string) => {
  if (!storeName) {
    throw new Error('storeName is required');
  }

  return new ApplicationKeyStore(storeName);
};

export const getApplicationStore = (storeName: string) => {
  if (!storeName) {
    throw new Error('storeName is required');
  }

  return new ApplicationStringStore(storeName);
};

const RUID_CHECK = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function prepareNftId(nftId: number | string | Uint8Array) {
  if (typeof nftId === 'number') {
    return `#${nftId}#`;
  } else if (typeof nftId === 'string') {
    if (RUID_CHECK.test(nftId)) {
      return `{${nftId}}`;
    } else {
      return `<${nftId}>`;
    }
  } else if (nftId instanceof Uint8Array) {
    const hex = Array.from(nftId)
      .map((byte) => byte.toString(16).padStart(2, '0'))
      .join('');

    return `[${hex}]`;
  } else {
    throw new Error('nftId must be a number, string, or Uint8Array');
  }
}

class NftBytesStore implements NftScopedBytesStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(
    resourceAddress: string,
    nftId: number | string | Uint8Array,
    key: string
  ): Promise<Uint8Array | undefined> {
    const result = await op_get_nft_bytes(
      this.storeName,
      resourceAddress,
      prepareNftId(nftId),
      key
    );

    if (result === 'NftDoesNotExist') {
      throw new Error('NFT does not exist');
    } else if (result === 'NoAccountsInContext') {
      throw new Error('No accounts in context');
    } else if (typeof result === 'object' && 'OwnershipInvalid' in result) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    } else if (result === 'None') {
      return;
    }

    return result.Some;
  }

  async keys(resourceAddress: string, nftId: number | string | Uint8Array): Promise<string[]> {
    const result = await op_nft_keys(this.storeName, 'bytes', resourceAddress, prepareNftId(nftId));

    if (result === 'NftDoesNotExist') {
      throw new Error('NFT does not exist');
    } else if (result === 'NoAccountsInContext') {
      throw new Error('No accounts in context');
    } else if (typeof result === 'object' && 'OwnershipInvalid' in result) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    } else if (result === 'None') {
      return [];
    }

    return result.Some;
  }

  async set(
    resourceAddress: string,
    nftId: number | string | Uint8Array,
    key: string,
    value: Uint8Array
  ): Promise<void> {
    const result = await op_set_nft_bytes(
      this.storeName,
      resourceAddress,
      prepareNftId(nftId),
      key,
      value
    );

    if (result === 'NftDoesNotExist') {
      throw new Error('NFT does not exist');
    } else if (result === 'NoAccountsInContext') {
      throw new Error('No accounts in context');
    } else if (typeof result === 'object' && 'OwnershipInvalid' in result) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    }
  }
}

class NftKeyStore implements NftScopedKeyStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(
    resourceAddress: string,
    nftId: number | string,
    key: string
  ): Promise<PrivateKey | undefined> {
    let keyId: number;

    const result = await op_get_nft_key(this.storeName, resourceAddress, prepareNftId(nftId), key);

    if (result === 'NftDoesNotExist') {
      throw new Error('NFT does not exist');
    } else if (result === 'NoAccountsInContext') {
      throw new Error('No accounts in context');
    } else if (typeof result === 'object' && 'OwnershipInvalid' in result) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    } else if (result === 'None') {
      return undefined;
    } else {
      keyId = result.Some;
    }

    const privateKey = new PrivateKey(keyId);
    Object.freeze(privateKey);

    return privateKey;
  }

  async keys(resourceAddress: string, nftId: number | string | Uint8Array): Promise<string[]> {
    const result = await op_nft_keys(this.storeName, 'key', resourceAddress, prepareNftId(nftId));

    if (result === 'NftDoesNotExist') {
      throw new Error('NFT does not exist');
    } else if (result === 'NoAccountsInContext') {
      throw new Error('No accounts in context');
    } else if (typeof result === 'object' && 'OwnershipInvalid' in result) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    } else if (result === 'None') {
      return [];
    }

    return result.Some;
  }

  async set(
    resourceAddress: string,
    nftId: number | string | Uint8Array,
    key: string,
    value: PrivateKey
  ): Promise<void> {
    const result = await op_set_nft_key(
      this.storeName,
      resourceAddress,
      prepareNftId(nftId),
      key,
      value.keyId
    );

    if (result === 'NftDoesNotExist') {
      throw new Error('NFT does not exist');
    } else if (result === 'NoAccountsInContext') {
      throw new Error('No accounts in context');
    } else if (typeof result === 'object' && 'OwnershipInvalid' in result) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    }
  }
}

class NftStringStore implements NftScopedStringStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(
    resourceAddress: string,
    nftId: number | string | Uint8Array,
    key: string
  ): Promise<string | undefined> {
    const result = await op_get_nft_string(
      this.storeName,
      resourceAddress,
      prepareNftId(nftId),
      key
    );

    if (result === 'NftDoesNotExist') {
      throw new Error('NFT does not exist');
    } else if (result === 'NoAccountsInContext') {
      throw new Error('No accounts in context');
    } else if (typeof result === 'object' && 'OwnershipInvalid' in result) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    } else if (result === 'None') {
      return undefined;
    }

    return result.Some;
  }

  async keys(resourceAddress: string, nftId: number | string | Uint8Array): Promise<string[]> {
    const result = await op_nft_keys(
      this.storeName,
      'string',
      resourceAddress,
      prepareNftId(nftId)
    );

    if (result === 'NftDoesNotExist') {
      throw new Error('NFT does not exist');
    } else if (result === 'NoAccountsInContext') {
      throw new Error('No accounts in context');
    } else if (typeof result === 'object' && 'OwnershipInvalid' in result) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    } else if (result === 'None') {
      return [];
    }

    return result.Some;
  }

  async set(
    resourceAddress: string,
    nftId: number | string | Uint8Array,
    key: string,
    value: string
  ): Promise<void> {
    const result = await op_set_nft_string(
      this.storeName,
      resourceAddress,
      prepareNftId(nftId),
      key,
      value
    );

    if (result === 'NftDoesNotExist') {
      throw new Error('NFT does not exist');
    } else if (result === 'NoAccountsInContext') {
      throw new Error('No accounts in context');
    } else if (typeof result === 'object' && 'OwnershipInvalid' in result) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    }
  }
}

export const getNftBytesStore = (storeName: string) => {
  if (!storeName) {
    throw new Error('storeName is required');
  }

  return new NftBytesStore(storeName);
};

export const getNftKeyStore = (storeName: string) => {
  if (!storeName) {
    throw new Error('storeName is required');
  }

  return new NftKeyStore(storeName);
};

export const getNftStore = (storeName: string) => {
  if (!storeName) {
    throw new Error('storeName is required');
  }

  return new NftStringStore(storeName);
};

class PersonalBytesStore implements BytesStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    const result = await op_get_personal_bytes(this.storeName, key);

    if (result === 'NoPersonalContext') {
      throw new Error('No personal context');
    } else if (result === 'None') {
      return undefined;
    } else {
      return result.Some;
    }
  }

  async keys(): Promise<string[]> {
    const result = await op_personal_keys(this.storeName, 'bytes');

    if (result === 'NoPersonalContext') {
      throw new Error('No personal context');
    } else if (result === 'None') {
      return [];
    }

    return result.Some;
  }

  async set(key: string, value: Uint8Array): Promise<void> {
    const result = await op_set_personal_bytes(this.storeName, key, value);

    if (result === 'NoPersonalContext') {
      throw new Error('No personal context');
    }
  }
}

class PersonalKeyStore implements KeyStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(key: string): Promise<PrivateKey | undefined> {
    let keyId;

    const result = await op_get_personal_key(this.storeName, key);

    if (result === 'NoPersonalContext') {
      throw new Error('No personal context');
    } else if (result === 'None') {
      return undefined;
    } else {
      keyId = result.Some;
    }

    const privateKey = new PrivateKey(keyId);
    Object.freeze(privateKey);

    return privateKey;
  }

  async keys(): Promise<string[]> {
    const result = await op_personal_keys(this.storeName, 'key');

    if (result === 'NoPersonalContext') {
      throw new Error('No personal context');
    } else if (result === 'None') {
      return [];
    }

    return result.Some;
  }

  async set(key: string, value: PrivateKey): Promise<void> {
    const result = await op_set_personal_key(this.storeName, key, value.keyId);

    if (result === 'NoPersonalContext') {
      throw new Error('No personal context');
    }
  }
}

class PersonalStringStore implements StringStore {
  storeName: string;

  constructor(storeName: string) {
    this.storeName = storeName;
  }

  async get(key: string): Promise<string | undefined> {
    const result = await op_get_personal_string(this.storeName, key);

    if (result === 'NoPersonalContext') {
      throw new Error('No personal context');
    } else if (result === 'None') {
      return undefined;
    } else {
      return result.Some;
    }
  }

  async keys(): Promise<string[]> {
    const result = await op_personal_keys(this.storeName, 'string');

    if (result === 'NoPersonalContext') {
      throw new Error('No personal context');
    } else if (result === 'None') {
      return [];
    }

    return result.Some;
  }

  async set(key: string, value: string): Promise<void> {
    const result = await op_set_personal_string(this.storeName, key, value);

    if (result === 'NoPersonalContext') {
      throw new Error('No personal context');
    }
  }
}

export const getPersonalBytesStore = (storeName: string) => {
  if (!storeName) {
    throw new Error('storeName is required');
  }

  return new PersonalBytesStore(storeName);
};

export const getPersonalKeyStore = (storeName: string) => {
  if (!storeName) {
    throw new Error('storeName is required');
  }

  return new PersonalKeyStore(storeName);
};

export const getPersonalStore = (storeName: string) => {
  if (!storeName) {
    throw new Error('storeName is required');
  }

  return new PersonalStringStore(storeName);
};
