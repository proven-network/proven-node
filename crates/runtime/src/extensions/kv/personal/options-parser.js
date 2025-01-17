class PersonalStringStore {
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

class PersonalKeyStore {
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

class PersonalBytesStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    throw new Error('`get` must be run inside a handler function')
  }

  async set (key, value) {
    return await setPersonalBytes(this.storeName, key, value)
  }
}

export const getPersonalStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new PersonalStringStore(storeName)
}

export const getPersonalKeyStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new PersonalKeyStore(storeName)
}

export const getPersonalBytesStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new PersonalBytesStore(storeName)
}
