function getApplicationBytes (store_name, key) {
  const { op_get_application_bytes } = globalThis.Deno.core.ops;
  return op_get_application_bytes(store_name, key)
}

function setApplicationBytes (store_name, key, value) {
  const { op_set_application_bytes } = globalThis.Deno.core.ops;
  return op_set_application_bytes(store_name, key, value)
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

class ApplicationStingStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  get (key) {
    return getApplicationString(this.storeName, key)
  }

  set (key, value) {
    return setApplicationString(this.storeName, key, value)
  }
}

class ApplicationBytesStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  get (key) {
    return getApplicationBytes(this.storeName, key)
  }

  set (key, value) {
    return setApplicationBytes(this.storeName, key, value)
  }
}

export const getApplicationStore = (storeName) => {
  if (storeName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!storeName) {
    storeName = 'DEFAULT'
  }

  return new ApplicationStingStore(storeName)
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

class PersonalStingStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  get (key) {
    return getPersonalString(this.storeName, key)
  }

  set (key, value) {
    return setPersonalString(this.storeName, key, value)
  }
}

class PersonalBytesStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  get (key) {
    return getPersonalBytes(this.storeName, key)
  }

  set (key, value) {
    return setPersonalBytes(this.storeName, key, value)
  }
}

export const getPersonalStore = (storeName) => {
  if (storeName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!storeName) {
    storeName = 'DEFAULT'
  }

  return new PersonalStingStore(storeName)
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

class NftStingStore {
  constructor (storeName, nftId) {
    this.storeName = storeName
    this.nftId = nftId
  }

  get (key) {
    return getNftString(this.storeName, this.nftId, key)
  }

  set (key, value) {
    return setNftString(this.storeName, this.nftId, key, value)
  }
}

class NftBytesStore {
  constructor (storeName, nftId) {
    this.storeName = storeName
    this.nftId = nftId
  }

  get (key) {
    return getNftBytes(this.storeName, this.nftId, key)
  }

  set (key, value) {
    return setNftBytes(this.storeName, this.nftId, key, value)
  }
}

export const getNftStore = (storeName, nftId) => {
  if (storeName === 'DEFAULT') {
    throw new Error('DEFAULT store name is reserved for system use')
  }

  if (!storeName) {
    storeName = 'DEFAULT'
  }

  return new NftStingStore(storeName, nftId)
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
