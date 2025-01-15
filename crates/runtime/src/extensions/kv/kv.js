import { PrivateKey } from "proven:crypto";

function getApplicationBytes (store_name, key) {
  const { op_get_application_bytes } = globalThis.Deno.core.ops;
  return op_get_application_bytes(store_name, key)
}

function setApplicationBytes (store_name, key, value) {
  const { op_set_application_bytes } = globalThis.Deno.core.ops;
  return op_set_application_bytes(store_name, key, value)
}

function getApplicationKey (store_name, key) {
  const { op_get_application_key } = globalThis.Deno.core.ops;
  return op_get_application_key(store_name, key)
}

function setApplicationKey (store_name, key, value) {
  const { op_set_application_key } = globalThis.Deno.core.ops;
  return op_set_application_key(store_name, key, value)
}

function getApplicationString (store_name, key) {
  const { op_get_application_string } = globalThis.Deno.core.ops;
  return op_get_application_string(store_name, key)
}

function setApplicationString (store_name, key, value) {
  const { op_set_application_string } = globalThis.Deno.core.ops;
  return op_set_application_string(store_name, key, value)
}

function getPersonalBytes (store_name, key) {
  const { op_get_personal_bytes } = globalThis.Deno.core.ops;
  return op_get_personal_bytes(store_name, key)
}

function setPersonalBytes (store_name, key, value) {
  const { op_set_personal_bytes } = globalThis.Deno.core.ops;
  return op_set_personal_bytes(store_name, key, value)
}

function getPersonalString (store_name, key) {
  const { op_get_personal_string } = globalThis.Deno.core.ops;
  return op_get_personal_string(store_name, key)
}

function setPersonalString (store_name, key, value) {
  const { op_set_personal_string } = globalThis.Deno.core.ops;
  return op_set_personal_string(store_name, key, value)
}

function getNftBytes (store_name, nft_id, key) {
  const { op_get_nft_bytes } = globalThis.Deno.core.ops;
  return op_get_nft_bytes(store_name, nft_id, key)
}

function setNftBytes (store_name, nft_id, key, value) {
  const { op_set_nft_bytes } = globalThis.Deno.core.ops;
  return op_set_nft_bytes(store_name, nft_id, key, value)
}

function getNftString (store_name, nft_id, key) {
  const { op_get_nft_string } = globalThis.Deno.core.ops;
  return op_get_nft_string(store_name, nft_id, key)
}

function setNftString (store_name, nft_id, key, value) {
  const { op_set_nft_string } = globalThis.Deno.core.ops;
  return op_set_nft_string(store_name, nft_id, key, value)
}

class ApplicationStringStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    return await getApplicationString(this.storeName, key)
  }

  async set (key, value) {
    return await setApplicationString(this.storeName, key, value)
  }
}

class ApplicationKeyStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    return new PrivateKey(await getApplicationKey(this.storeName, key))
  }

  async set (key, value) {
    return await setApplicationKey(this.storeName, key, value.keyId)
  }
}

class ApplicationBytesStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    return await getApplicationBytes(this.storeName, key)
  }

  async set (key, value) {
    return await setApplicationBytes(this.storeName, key, value)
  }
}

export const getApplicationStore = (storeName) => {
  if (storeName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!storeName) {
    storeName = 'DEFAULT'
  }

  return new ApplicationStringStore(storeName)
}

export const getApplicationKeyStore = (storeName) => {
  if (storeName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!storeName) {
    storeName = 'DEFAULT'
  }

  return new ApplicationKeyStore(storeName)
}

export const getApplicationBytesStore = (storeName) => {
  if (storeName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!storeName) {
    storeName = 'DEFAULT'
  }

  return new ApplicationBytesStore(storeName)
}

class PersonalStringStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    return await getPersonalString(this.storeName, key)
  }

  async set (key, value) {
    return await setPersonalString(this.storeName, key, value)
  }
}

class PersonalBytesStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    return await getPersonalBytes(this.storeName, key)
  }

  async set (key, value) {
    return await setPersonalBytes(this.storeName, key, value)
  }
}

export const getPersonalStore = (storeName) => {
  if (storeName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!storeName) {
    storeName = 'DEFAULT'
  }

  return new PersonalStringStore(storeName)
}

export const getPersonalBytesStore = (storeName) => {
  if (storeName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!storeName) {
    storeName = 'DEFAULT'
  }

  return new PersonalBytesStore(storeName)
}

class NftStringStore {
  constructor (storeName, nftId) {
    this.storeName = storeName
    this.nftId = nftId
  }

  async get (key) {
    return await getNftString(this.storeName, this.nftId, key)
  }

  async set (key, value) {
    return await setNftString(this.storeName, this.nftId, key, value)
  }
}

class NftBytesStore {
  constructor (storeName, nftId) {
    this.storeName = storeName
    this.nftId = nftId
  }

  async get (key) {
    return await getNftBytes(this.storeName, this.nftId, key)
  }

  async set (key, value) {
    return await setNftBytes(this.storeName, this.nftId, key, value)
  }
}

export const getNftStore = (storeName, nftId) => {
  if (storeName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!storeName) {
    storeName = 'DEFAULT'
  }

  return new NftStringStore(storeName, nftId)
}

export const getNftBytesStore = (storeName, nftId) => {
  if (storeName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!storeName) {
    storeName = 'DEFAULT'
  }

  return new NftBytesStore(storeName, nftId)
}
