import { PrivateKey } from "proven:crypto";

function getPersonalBytes (storeName, key) {
  const { op_get_personal_bytes } = globalThis.Deno.core.ops;
  return op_get_personal_bytes(storeName, key)
}

function setPersonalBytes (storeName, key, value) {
  const { op_set_personal_bytes } = globalThis.Deno.core.ops;
  return op_set_personal_bytes(storeName, key, value)
}

function getPersonalKey (storeName, key) {
  const { op_get_personal_key } = globalThis.Deno.core.ops;
  return op_get_personal_key(storeName, key)
}

function setPersonalKey (storeName, key, value) {
  const { op_set_personal_key } = globalThis.Deno.core.ops;
  return op_set_personal_key(storeName, key, value)
}

function getPersonalString (storeName, key) {
  const { op_get_personal_string } = globalThis.Deno.core.ops;
  return op_get_personal_string(storeName, key)
}

function setPersonalString (storeName, key, value) {
  const { op_set_personal_string } = globalThis.Deno.core.ops;
  return op_set_personal_string(storeName, key, value)
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

class PersonalKeyStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    const keyId = await getPersonalKey(this.storeName, key)

    if (typeof keyId === 'number') {
      return new PrivateKey(keyId)
    }
  }

  async set (key, value) {
    return await setPersonalKey(this.storeName, key, value)
  }
}

export const getPersonalStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new PersonalStringStore(storeName)
}

export const getPersonalBytesStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new PersonalBytesStore(storeName)
}

export const getPersonalKeyStore = (storeName) => {
  if (!storeName) {
    throw new Error('storeName is required')
  }

  return new PersonalKeyStore(storeName)
}
