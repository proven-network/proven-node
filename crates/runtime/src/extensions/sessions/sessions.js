export function getCurrentIdentity() {
  const { op_get_current_identity } = Deno.core.ops;
  const identity = op_get_current_identity();

  return identity === "<NONE>" ? null : identity;
}

export function getCurrentAccounts() {
  const { op_get_current_accounts } = Deno.core.ops;
  const accounts = op_get_current_accounts();

  return accounts.split(",");
}
