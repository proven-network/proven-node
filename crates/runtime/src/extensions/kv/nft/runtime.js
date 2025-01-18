import { PrivateKey } from "proven:crypto";

const RUID_CHECK = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function prepareNftId (nftId) {
  if (typeof nftId === "number") {
    return `#${nftId}#`
  } else if (typeof nftId === "string") {
    if (RUID_CHECK.test(nftId)) {
      return `{${nftId}}`
    } else {
      return `<${nftId}>`
    }
  } else if (nftId instanceof Uint8Array) {
    const hex = Array.from(nftId)
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");

    return `[${hex}]`
  } else {
    throw new Error("nftId must be a number, string, or Uint8Array");
  }
}

function getNftBytes (storeName, resourceAddress, nftId, key) {
  const { op_get_nft_bytes } = globalThis.Deno.core.ops;
  return op_get_nft_bytes(storeName, resourceAddress, nftId, key)
}

function setNftBytes (storeName, resourceAddress, nftId, key, value) {
  const { op_set_nft_bytes } = globalThis.Deno.core.ops;
  return op_set_nft_bytes(storeName, resourceAddress, nftId, key, value)
}

function getNftKey (storeName, resourceAddress, nftId, key) {
  const { op_get_nft_key } = globalThis.Deno.core.ops;
  return op_get_nft_key(storeName, resourceAddress, nftId, key)
}

function setNftKey (storeName, resourceAddress, nftId, key, keyId) {
  const { op_set_nft_key } = globalThis.Deno.core.ops;
  return op_set_nft_key(storeName, resourceAddress, nftId, key, keyId)
}

function getNftString (storeName, resourceAddress, nftId, key) {
  const { op_get_nft_string } = globalThis.Deno.core.ops;
  return op_get_nft_string(storeName, resourceAddress, nftId, key)
}

function setNftString (storeName, resourceAddress, nftId, key, value) {
  const { op_set_nft_string } = globalThis.Deno.core.ops;
  return op_set_nft_string(storeName, resourceAddress, nftId, key, value)
}

class NftStringStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (resourceAddress, nftId, key) {
    const result = await getNftString(this.storeName, resourceAddress, prepareNftId(nftId), key);

    if (result === "NftDoesNotExist") {
      throw new Error('NFT does not exist');
    } else if (result === "NoAccountsInContext") {
      throw new Error('No accounts in context');
    } else if (result.OwnershipInvalid) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    } else if (result === "None") {
      return undefined;
    } else {
      return result.Some;
    }
  }

  async set (resourceAddress, nftId, key, value) {
    const result = await setNftString(this.storeName, resourceAddress, prepareNftId(nftId), key, value);

    if (result === "NftDoesNotExist") {
      throw new Error('NFT does not exist');
    } else if (result === "NoAccountsInContext") {
      throw new Error('No accounts in context');
    } else if (result.OwnershipInvalid) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    }
  }
}

class NftBytesStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (resourceAddress, nftId, key) {
    const result = await getNftBytes(this.storeName, resourceAddress, prepareNftId(nftId), key);

    if (result === "NftDoesNotExist") {
      throw new Error('NFT does not exist');
    } else if (result === "NoAccountsInContext") {
      throw new Error('No accounts in context');
    } else if (result.OwnershipInvalid) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    } else if (result === "None") {
      return undefined;
    } else {
      return result.Some;
    }
  }

  async set (resourceAddress, nftId, key, value) {
    const result = await setNftBytes(this.storeName, resourceAddress, prepareNftId(nftId), key, value);

    if (result === "NftDoesNotExist") {
      throw new Error('NFT does not exist');
    } else if (result === "NoAccountsInContext") {
      throw new Error('No accounts in context');
    } else if (result.OwnershipInvalid) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    }
  }
}

class NftKeyStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (resourceAddress, nftId, key) {
    let keyId;

    const result = await getNftKey(this.storeName, resourceAddress, prepareNftId(nftId), key);

    if (result === "NftDoesNotExist") {
      throw new Error('NFT does not exist');
    } else if (result === "NoAccountsInContext") {
      throw new Error('No accounts in context');
    } else if (result.OwnershipInvalid) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    } else if (result === "None") {
      return undefined;
    } else {
      keyId = result.Some;
    }

    if (typeof keyId === 'number') {
      const privateKey = new PrivateKey(keyId);
      Object.freeze(privateKey);

      return privateKey;
    }
  }

  async set (resourceAddress, nftId, key, value) {
    const result = await setNftKey(this.storeName, resourceAddress, prepareNftId(nftId), key, value.keyId);

    if (result === "NftDoesNotExist") {
      throw new Error('NFT does not exist');
    } else if (result === "NoAccountsInContext") {
      throw new Error('No accounts in context');
    } else if (result.OwnershipInvalid) {
      throw new Error(`NFT ownership invalid. Owned by: ${result.OwnershipInvalid}`);
    }
  }
}

export const getNftStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new NftStringStore(storeName)
}

export const getNftBytesStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new NftBytesStore(storeName)
}

export const getNftKeyStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new NftKeyStore(storeName)
}
