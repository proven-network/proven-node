class ApplicationStringStore {
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

class ApplicationKeyStore {
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

class ApplicationBytesStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    throw new Error('`get` must be run inside a handler function')
  }

  async set (key, value) {
    return await setApplicationBytes(this.storeName, key, value)
  }
}

export const getApplicationStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new ApplicationStringStore(storeName)
}

export const getApplicationKeyStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new ApplicationKeyStore(storeName)
}

export const getApplicationBytesStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new ApplicationBytesStore(storeName)
}
