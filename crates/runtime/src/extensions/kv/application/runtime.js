import { PrivateKey } from "proven:crypto";

function applicationKeys(storeName, storeType) {
  const { op_application_keys } = globalThis.Deno.core.ops;
  return op_application_keys(storeName, storeType);
}

function getApplicationBytes (storeName, key) {
  const { op_get_application_bytes } = globalThis.Deno.core.ops;
  return op_get_application_bytes(storeName, key)
}

function setApplicationBytes (storeName, key, value) {
  const { op_set_application_bytes } = globalThis.Deno.core.ops;
  return op_set_application_bytes(storeName, key, value)
}

function getApplicationKey (storeName, key) {
  const { op_get_application_key } = globalThis.Deno.core.ops;
  return op_get_application_key(storeName, key)
}

function setApplicationKey (storeName, key, keyId) {
  const { op_set_application_key } = globalThis.Deno.core.ops;
  return op_set_application_key(storeName, key, keyId)
}

function getApplicationString (storeName, key) {
  const { op_get_application_string } = globalThis.Deno.core.ops;
  return op_get_application_string(storeName, key)
}

function setApplicationString (storeName, key, value) {
  const { op_set_application_string } = globalThis.Deno.core.ops;
  return op_set_application_string(storeName, key, value)
}

class ApplicationStringStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    return await getApplicationString(this.storeName, key)
  }

  async keys () {
    return await applicationKeys(this.storeName, 'string')
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
    const keyId = await getApplicationKey(this.storeName, key)

    if (typeof keyId === 'number') {
      const privateKey = new PrivateKey(keyId);
      Object.freeze(privateKey);

      return privateKey;
    }
  }

  async keys () {
    return await applicationKeys(this.storeName, 'key')
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

  async keys () {
    return await applicationKeys(this.storeName, 'bytes')
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
