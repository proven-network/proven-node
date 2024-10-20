use proven_store::{Store, Store1};

use deno_core::{extension, op2, OpDecl};

#[op2]
#[buffer]
pub fn op_get_nft_bytes<NS: Store1>(
    #[state] nft_store: &mut Option<NS>,
    #[string] nft_id: String,
    #[string] key: String,
) -> Option<Vec<u8>> {
    if let Some(store) = nft_store.as_ref() {
        store.scope(nft_id).get_blocking(key).unwrap_or_default()
    } else {
        None
    }
}

#[op2(fast)]
pub fn op_set_nft_bytes<NS: Store1>(
    #[state] nft_store: &mut Option<NS>,
    #[string] nft_id: String,
    #[string] key: String,
    #[arraybuffer(copy)] value: Vec<u8>,
) -> bool {
    if let Some(store) = nft_store.as_mut() {
        store.scope(nft_id).put_blocking(key, value).is_ok()
    } else {
        false
    }
}

#[op2]
#[string]
pub fn op_get_nft_string<NS: Store1>(
    #[state] nft_store: &mut Option<NS>,
    #[string] nft_id: String,
    #[string] key: String,
) -> Option<String> {
    if let Some(store) = nft_store.as_ref() {
        match store.scope(nft_id).get_blocking(key) {
            Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).to_string()),
            _ => None,
        }
    } else {
        None
    }
}

#[op2(fast)]
pub fn op_set_nft_string<NS: Store1>(
    #[state] nft_store: &mut Option<NS>,
    #[string] nft_id: String,
    #[string] key: String,
    #[string] value: String,
) -> bool {
    if let Some(store) = nft_store.as_mut() {
        store
            .scope(nft_id)
            .put_blocking(key, value.as_bytes().to_vec())
            .is_ok()
    } else {
        false
    }
}

extension!(
    storage_nft_ext,
    parameters = [ NS: Store1 ],
    ops_fn = get_ops<NS>,
);

fn get_ops<NS: Store1 + 'static>() -> Vec<OpDecl> {
    let get_nft_bytes = op_get_nft_bytes::<NS>();
    let set_nft_bytes = op_set_nft_bytes::<NS>();
    let get_nft_string = op_get_nft_string::<NS>();
    let set_nft_string = op_set_nft_string::<NS>();

    vec![get_nft_bytes, set_nft_bytes, get_nft_string, set_nft_string]
}
