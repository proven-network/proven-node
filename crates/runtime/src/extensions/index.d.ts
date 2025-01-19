declare namespace Deno {
  namespace core {
    const ops: {
      // session
      op_get_current_accounts: () => string[];
      op_get_current_identity: () => string | undefined;
    };
  }
}
