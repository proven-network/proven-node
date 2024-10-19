export function getBytes (key) {
  const { op_get_application_bytes } = Deno.core.ops;
  return op_get_application_bytes(key)
}

export function setBytes (key, value) {
  const { op_set_application_bytes } = Deno.core.ops;
  return op_set_application_bytes(key, value)
}

export function getString (key) {
  const { op_get_application_string } = Deno.core.ops;
  return op_get_application_string(key)
}

export function setString (key, value) {
  const { op_set_application_string } = Deno.core.ops;
  return op_set_application_string(key, value)
}
