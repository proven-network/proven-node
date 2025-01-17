import { PrivateKey } from "proven:crypto";

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

function setNftKey (storeName, resourceAddress, nftId, key, value) {
  const { op_set_nft_key } = globalThis.Deno.core.ops;
  return op_set_nft_key(storeName, resourceAddress, nftId, key, value)
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
    return await getNftString(this.storeName, resourceAddress, nftId, key)
  }

  async set (resourceAddress, nftId, key, value) {
    return await setNftString(this.storeName, resourceAddress, nftId, key, value)
  }
}

class NftBytesStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (resourceAddress, nftId, key) {
    return await getNftBytes(this.storeName, resourceAddress, nftId, key)
  }

  async set (resourceAddress, nftId, key, value) {
    return await setNftBytes(this.storeName, resourceAddress, nftId, key, value)
  }
}

class NftKeyStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (resourceAddress, nftId, key) {
    const keyId = await getNftKey(this.storeName, resourceAddress, nftId, key)

    if (typeof keyId === 'number') {
      return new PrivateKey(keyId)
    }
  }

  async set (resourceAddress, nftId, key, value) {
    return await setNftKey(this.storeName, resourceAddress, nftId, key, value)
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
