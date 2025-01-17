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
    const result = await getPersonalString(this.storeName, key);

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
    } else if (result === "None") {
      return undefined;
    } else {
      return result.Ok;
    }
  }

  async set (key, value) {
    const result = await setPersonalString(this.storeName, key, value);

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
    } else {
      return true;
    }
  }
}

class PersonalBytesStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    const result = await getPersonalBytes(this.storeName, key);

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
    } else if (result === "None") {
      return undefined;
    } else {
      return result.Ok;
    }
  }

  async set (key, value) {
    const result = await setPersonalBytes(this.storeName, key, value);

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
    } else {
      return true;
    }
  }
}

class PersonalKeyStore {
  constructor (storeName) {
    this.storeName = storeName
  }

  async get (key) {
    let keyId;

    const result = await getPersonalKey(this.storeName, key);

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
    } else if (result === "None") {
      return undefined;
    } else {
      keyId = result.Ok;
    }

    if (typeof keyId === 'number') {
      const privateKey = new PrivateKey(keyId);
      Object.freeze(privateKey);

      return privateKey;
    }
  }

  async set (key, value) {
    const result = await setPersonalKey(this.storeName, key, value.keyId);

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
    } else {
      return true;
    }
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
