class NftStringStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    throw new Error('`get` must be run inside a handler function')
  }

  async set (key, value) {
    throw new Error('`set` must be run inside a handler function')
  }
}

class NftKeyStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    throw new Error('`get` must be run inside a handler function')
  }

  async set (key, value) {
    throw new Error('`set` must be run inside a handler function')
  }
}

class NftBytesStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    throw new Error('`get` must be run inside a handler function')
  }

  async set (key, value) {
    return await setNftBytes(this.storeName, key, value)
  }
}

export const getNftStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new NftStringStore(storeName)
}

export const getNftKeyStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new NftKeyStore(storeName)
}

export const getNftBytesStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new NftBytesStore(storeName)
}
