use proven_store::Store;

use deno_core::{extension, op2, OpDecl};

#[derive(Clone, Default)]
pub struct StorageState<AS: Store> {
    pub application_store: Option<AS>,
}

#[op2]
#[buffer]
pub fn op_get_application_bytes<AS: Store>(
    #[state] state: &mut StorageState<AS>,
    #[string] key: String,
) -> Option<Vec<u8>> {
    if let Some(store) = state.application_store.as_ref() {
        store.get_blocking(key).unwrap_or_default()
    } else {
        None
    }
}

#[op2(fast)]
pub fn op_set_application_bytes<AS: Store>(
    #[state] state: &mut StorageState<AS>,
    #[string] key: String,
    #[arraybuffer(copy)] value: Vec<u8>,
) -> bool {
    if let Some(store) = state.application_store.as_ref() {
        store.put_blocking(key, value).is_ok()
    } else {
        false
    }
}

#[op2]
#[string]
pub fn op_get_application_string<AS: Store>(
    #[state] state: &mut StorageState<AS>,
    #[string] key: String,
) -> Option<String> {
    if let Some(store) = state.application_store.as_ref() {
        match store.get_blocking(key) {
            Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).to_string()),
            _ => None,
        }
    } else {
        None
    }
}

#[op2(fast)]
pub fn op_set_application_string<AS: Store>(
    #[state] state: &mut StorageState<AS>,
    #[string] key: String,
    #[string] value: String,
) -> bool {
    if let Some(store) = state.application_store.as_ref() {
        store.put_blocking(key, value.as_bytes().to_vec()).is_ok()
    } else {
        false
    }
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
