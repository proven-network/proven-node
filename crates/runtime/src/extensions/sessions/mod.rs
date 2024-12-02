#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use deno_core::{extension, op2};

static NULL_IDENTITY: &str = "<NONE>";

#[derive(Default)]
pub struct SessionsState {
    pub identity: Option<String>,
    pub accounts: Option<Vec<String>>,
}

#[op2]
#[string]
pub fn op_get_current_identity(#[state] state: &SessionsState) -> String {
    state
        .identity
        .clone()
        .unwrap_or_else(|| NULL_IDENTITY.to_string())
}

#[op2]
#[string]
pub fn op_get_current_accounts(#[state] state: &SessionsState) -> String {
    state.accounts.clone().unwrap_or_default().join(",")
}

extension!(
    sessions_ext,
    ops = [op_get_current_identity, op_get_current_accounts],
    esm_entry_point = "proven:sessions",
    esm = [ dir "src/extensions/sessions", "proven:sessions" = "sessions.js" ],
    docs = "Functions for identity management"
);
