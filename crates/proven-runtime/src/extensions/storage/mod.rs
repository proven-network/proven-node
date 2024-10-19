use proven_store::Store;

use deno_core::{extension, op2, OpDecl};

#[derive(Clone, Default)]
pub struct StorageState<AS: Store> {
    pub application_store: AS,
}

#[op2]
#[buffer]
pub fn op_get_application_bytes<AS: Store>(
    #[state] state: &mut StorageState<AS>,
    #[string] key: String,
) -> Option<Vec<u8>> {
    state
        .application_store
        .get_blocking(key)
        .unwrap_or_default()
}

#[op2(fast)]
pub fn op_set_application_bytes<AS: Store>(
    #[state] state: &mut StorageState<AS>,
    #[string] key: String,
    #[arraybuffer(copy)] value: Vec<u8>,
) -> bool {
    state.application_store.put_blocking(key, value).is_ok()
}

#[op2]
#[string]
pub fn op_get_application_string<AS: Store>(
    #[state] state: &mut StorageState<AS>,
    #[string] key: String,
) -> Option<String> {
    match state.application_store.get_blocking(key) {
        Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).to_string()),
        _ => None,
    }
}

#[op2(fast)]
pub fn op_set_application_string<AS: Store>(
    #[state] state: &mut StorageState<AS>,
    #[string] key: String,
    #[string] value: String,
) -> bool {
    state
        .application_store
        .put_blocking(key, value.as_bytes().to_vec())
        .is_ok()
}

extension!(
    storage_ext,
    parameters = [ AS: Store ],
    ops_fn = get_ops<AS>,
    esm_entry_point = "proven:storage",
    esm = [ dir "src/extensions/storage", "proven:storage" = "storage.js" ],
    docs = "Functions for accessing secure storage"
);

pub fn get_ops<AS: Store + 'static>() -> Vec<OpDecl> {
    let get_application_bytes = op_get_application_bytes::<AS>();
    let set_application_bytes = op_set_application_bytes::<AS>();
    let get_application_string = op_get_application_string::<AS>();
    let set_application_string = op_set_application_string::<AS>();

    vec![
        get_application_bytes,
        set_application_bytes,
        get_application_string,
        set_application_string,
    ]
}
