function getApplicationBytes (key) {
  const { op_get_application_bytes } = Deno.core.ops;
  return op_get_application_bytes(key)
}

function setApplicationBytes (key, value) {
  const { op_set_application_bytes } = Deno.core.ops;
  return op_set_application_bytes(key, value)
}

function getApplicationString (key) {
  const { op_get_application_string } = Deno.core.ops;
  return op_get_application_string(key)
}

function setApplicationString (key, value) {
  const { op_set_application_string } = Deno.core.ops;
  return op_set_application_string(key, value)
}

function getPersonalBytes (key) {
  const { op_get_personal_bytes } = Deno.core.ops;
  return op_get_personal_bytes(key)
}

function setPersonalBytes (key, value) {
  const { op_set_personal_bytes } = Deno.core.ops;
  return op_set_personal_bytes(key, value)
}

function getPersonalString (key) {
  const { op_get_personal_string } = Deno.core.ops;
  return op_get_personal_string(key)
}

function setPersonalString (key, value) {
  const { op_set_personal_string } = Deno.core.ops;
  return op_set_personal_string(key, value)
}

function getNftBytes (nft_id, key) {
  const { op_get_nft_bytes } = Deno.core.ops;
  return op_get_nft_bytes(nft_id, key)
}

function setNftBytes (nft_id, key, value) {
  const { op_set_nft_bytes } = Deno.core.ops;
  return op_set_nft_bytes(nft_id, key, value)
}

function getNftString (nft_id, key) {
  const { op_get_nft_string } = Deno.core.ops;
  return op_get_nft_string(nft_id, key)
}

function setNftString (nft_id, key, value) {
  const { op_set_nft_string } = Deno.core.ops;
  return op_set_nft_string(nft_id, key, value)
}

export const applicationStore = {
  bytes: {
    get: getApplicationBytes,
    set: setApplicationBytes
  },
  string: {
    get: getApplicationString,
    set: setApplicationString
  }
}

export const personalStore = {
  bytes: {
    get: getPersonalBytes,
    set: setPersonalBytes
  },
  string: {
    get: getPersonalString,
    set: setPersonalString
  }
}

export const nftStore = {
  bytes: {
    get: getNftBytes,
    set: setNftBytes
  },
  string: {
    get: getNftString,
    set: setNftString
  }
}
