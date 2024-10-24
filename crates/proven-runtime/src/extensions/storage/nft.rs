use std::cell::RefCell;
use std::rc::Rc;

use proven_store::{Store, Store1, Store2};

use deno_core::{extension, op2, OpDecl, OpState};

#[op2(async)]
#[buffer]
pub async fn op_get_nft_bytes<NS: Store2>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] nft_id: String,
    #[string] key: String,
) -> Option<Vec<u8>> {
    let nft_store = {
        loop {
            let nft_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<NS>>()
            };

            match nft_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = if let Some(store) = nft_store.as_ref() {
        store
            .scope(format!("{}:bytes", store_name))
            .scope(nft_id)
            .get(key)
            .await
            .unwrap_or_default()
    } else {
        None
    };

    state.borrow_mut().put(nft_store);

    result
}

#[op2(async)]
pub async fn op_set_nft_bytes<NS: Store2>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] nft_id: String,
    #[string] key: String,
    #[arraybuffer(copy)] value: Vec<u8>,
) -> bool {
    let nft_store = {
        loop {
            let nft_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<NS>>()
            };

            match nft_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = if let Some(store) = nft_store.as_ref() {
        store
            .scope(format!("{}:bytes", store_name))
            .scope(nft_id)
            .put(key, value)
            .await
            .is_ok()
    } else {
        false
    };

    state.borrow_mut().put(nft_store);

    result
}

#[op2(async)]
#[string]
pub async fn op_get_nft_string<NS: Store2>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] nft_id: String,
    #[string] key: String,
) -> Option<String> {
    let nft_store = {
        loop {
            let nft_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<NS>>()
            };

            match nft_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = if let Some(store) = nft_store.as_ref() {
        match store
            .scope(format!("{}:string", store_name))
            .scope(nft_id)
            .get(key)
            .await
        {
            Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).to_string()),
            _ => None,
        }
    } else {
        None
    };

    state.borrow_mut().put(nft_store);

    result
}

#[op2(async)]
pub async fn op_set_nft_string<NS: Store2>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] nft_id: String,
    #[string] key: String,
    #[string] value: String,
) -> bool {
    let nft_store = {
        loop {
            let nft_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<NS>>()
            };

            match nft_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = if let Some(store) = nft_store.as_ref() {
        store
            .scope(format!("{}:string", store_name))
            .scope(nft_id)
            .put(key, value.as_bytes().to_vec())
            .await
            .is_ok()
    } else {
        false
    };

    state.borrow_mut().put(nft_store);

    result
}

extension!(
    storage_nft_ext,
    parameters = [ NS: Store2 ],
    ops_fn = get_ops<NS>,
);

fn get_ops<NS: Store2>() -> Vec<OpDecl> {
    let get_nft_bytes = op_get_nft_bytes::<NS>();
    let set_nft_bytes = op_set_nft_bytes::<NS>();
    let get_nft_string = op_get_nft_string::<NS>();
    let set_nft_string = op_set_nft_string::<NS>();

    vec![get_nft_bytes, set_nft_bytes, get_nft_string, set_nft_string]
}
