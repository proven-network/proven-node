#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use deno_core::{extension, op2};

static NULL_IDENTITY: &str = "<NONE>";

#[derive(Default)]
pub struct SessionState {
    pub identity: Option<String>,
    pub accounts: Option<Vec<String>>,
}

#[op2]
#[string]
pub fn op_get_current_identity(#[state] state: &SessionState) -> String {
    state
        .identity
        .clone()
        .unwrap_or_else(|| NULL_IDENTITY.to_string())
}

#[op2]
#[string]
pub fn op_get_current_accounts(#[state] state: &SessionState) -> String {
    state.accounts.clone().unwrap_or_default().join(",")
}

extension!(
    session_ext,
    ops = [op_get_current_identity, op_get_current_accounts],
    esm_entry_point = "proven:session",
    esm = [ dir "src/extensions/session", "proven:session" = "session.js" ],
    docs = "Functions for identity management"
);
