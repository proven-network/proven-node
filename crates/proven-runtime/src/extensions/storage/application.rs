use proven_store::Store;

use deno_core::{extension, op2, OpDecl};

#[op2]
#[buffer]
pub fn op_get_application_bytes<AS: Store>(
    #[state] application_store: &mut AS,
    #[string] key: String,
) -> Option<Vec<u8>> {
    application_store.get_blocking(key).unwrap_or_default()
}

#[op2(fast)]
pub fn op_set_application_bytes<AS: Store>(
    #[state] application_store: &mut AS,
    #[string] key: String,
    #[arraybuffer(copy)] value: Vec<u8>,
) -> bool {
    application_store.put_blocking(key, value).is_ok()
}

#[op2]
#[string]
pub fn op_get_application_string<AS: Store>(
    #[state] application_store: &mut AS,
    #[string] key: String,
) -> Option<String> {
    match application_store.get_blocking(key) {
        Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).to_string()),
        _ => None,
    }
}

#[op2(fast)]
pub fn op_set_application_string<AS: Store>(
    #[state] application_store: &mut AS,
    #[string] key: String,
    #[string] value: String,
) -> bool {
    application_store
        .put_blocking(key, value.as_bytes().to_vec())
        .is_ok()
}

extension!(
    storage_application_ext,
    parameters = [ AS: Store ],
    ops_fn = get_ops<AS>,
);

fn get_ops<AS: Store + 'static>() -> Vec<OpDecl> {
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
