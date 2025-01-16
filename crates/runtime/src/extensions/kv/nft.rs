#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

use std::cell::RefCell;
use std::rc::Rc;

use bytes::{Bytes, BytesMut};
use deno_core::{extension, op2, OpState};
use proven_radix_nft_verifier::RadixNftVerifier;
use proven_store::{Store, Store1, Store2};

#[op2(async)]
#[buffer]
pub async fn op_get_nft_bytes<NS: Store2, RNV: RadixNftVerifier>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] nft_id: String,
    #[string] key: String,
) -> Option<BytesMut> {
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
            .scope(format!("{store_name}:bytes"))
            .scope(nft_id)
            .get(key)
            .await
            .map(|bytes| bytes.map(BytesMut::from))
            .unwrap_or_default()
    } else {
        None
    };

    state.borrow_mut().put(nft_store);

    result
}

#[op2(async)]
pub async fn op_set_nft_bytes<NS: Store2, RNV: RadixNftVerifier>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] nft_id: String,
    #[string] key: String,
    #[buffer(copy)] value: Bytes,
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
            .scope(format!("{store_name}:bytes"))
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
pub async fn op_get_nft_string<NS: Store2, RNV: RadixNftVerifier>(
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
            .scope(format!("{store_name}:string"))
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
pub async fn op_set_nft_string<NS: Store2, RNV: RadixNftVerifier>(
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
            .scope(format!("{store_name}:string"))
            .scope(nft_id)
            .put(key, Bytes::from(value))
            .await
            .is_ok()
    } else {
        false
    };

    state.borrow_mut().put(nft_store);

    result
}

extension!(
    kv_nft_ext,
    parameters = [ NS: Store2, RNV: RadixNftVerifier ],
    ops = [
        op_get_nft_bytes<NS, RNV>,
        op_set_nft_bytes<NS, RNV>,
        op_get_nft_string<NS, RNV>,
        op_set_nft_string<NS, RNV>,
    ],
);
