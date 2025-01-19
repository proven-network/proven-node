import { PrivateKey } from "proven:crypto";

function personalKeys(storeName, storeType) {
  const { op_personal_keys } = globalThis.Deno.core.ops;
  return op_personal_keys(storeName, storeType);
}

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

function setPersonalKey (storeName, key, keyId) {
  const { op_set_personal_key } = globalThis.Deno.core.ops;
  return op_set_personal_key(storeName, key, keyId)
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
      return result.Some;
    }
  }

  async keys () {
    const result = await personalKeys(this.storeName, 'string');

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
    }

    return result.Some;
  }

  async set (key, value) {
    const result = await setPersonalString(this.storeName, key, value);

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
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
      return result.Some;
    }
  }

  async keys () {
    const result = await personalKeys(this.storeName, 'bytes');

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
    }

    return result.Some;
  }

  async set (key, value) {
    const result = await setPersonalBytes(this.storeName, key, value);

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
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
      keyId = result.Some;
    }

    if (typeof keyId === 'number') {
      const privateKey = new PrivateKey(keyId);
      Object.freeze(privateKey);

      return privateKey;
    }
  }

  async keys () {
    const result = await personalKeys(this.storeName, 'key');

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
    }

    return result.Some;
  }

  async set (key, value) {
    const result = await setPersonalKey(this.storeName, key, value.keyId);

    if (result === "NoPersonalContext") {
      throw new Error('No personal context');
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
