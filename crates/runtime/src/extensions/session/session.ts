export function getCurrentIdentity() {
  const { op_get_current_identity } = Deno.core.ops;
  return op_get_current_identity();
}

export function getCurrentAccounts() {
  const { op_get_current_accounts } = Deno.core.ops;
  return op_get_current_accounts();
}
