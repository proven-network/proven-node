use proven_store::Store;

use deno_core::{extension, op2, OpDecl};

#[op2]
#[buffer]
pub fn op_get_personal_bytes<PS: Store>(
    #[state] personal_store: &mut Option<PS>,
    #[string] key: String,
) -> Option<Vec<u8>> {
    if let Some(store) = personal_store.as_ref() {
        store.get_blocking(key).unwrap_or_default()
    } else {
        None
    }
}

#[op2(fast)]
pub fn op_set_personal_bytes<PS: Store>(
    #[state] personal_store: &mut Option<PS>,
    #[string] key: String,
    #[arraybuffer(copy)] value: Vec<u8>,
) -> bool {
    if let Some(store) = personal_store.as_mut() {
        store.put_blocking(key, value).is_ok()
    } else {
        false
    }
}

#[op2]
#[string]
pub fn op_get_personal_string<PS: Store>(
    #[state] personal_store: &mut Option<PS>,
    #[string] key: String,
) -> Option<String> {
    if let Some(store) = personal_store.as_ref() {
        match store.get_blocking(key) {
            Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).to_string()),
            _ => None,
        }
    } else {
        None
    }
}

#[op2(fast)]
pub fn op_set_personal_string<PS: Store>(
    #[state] personal_store: &mut Option<PS>,
    #[string] key: String,
    #[string] value: String,
) -> bool {
    if let Some(store) = personal_store.as_mut() {
        store.put_blocking(key, value.as_bytes().to_vec()).is_ok()
    } else {
        false
    }
}

extension!(
    storage_personal_ext,
    parameters = [ PS: Store ],
    ops_fn = get_ops<PS>,
);

fn get_ops<PS: Store + 'static>() -> Vec<OpDecl> {
    let get_personal_bytes = op_get_personal_bytes::<PS>();
    let set_personal_bytes = op_set_personal_bytes::<PS>();
    let get_personal_string = op_get_personal_string::<PS>();
    let set_personal_string = op_set_personal_string::<PS>();

    vec![
        get_personal_bytes,
        set_personal_bytes,
        get_personal_string,
        set_personal_string,
    ]
}
