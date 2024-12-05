#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

use std::cell::RefCell;
use std::rc::Rc;

use bytes::{Bytes, BytesMut};
use deno_core::{extension, op2, OpDecl, OpState};
use proven_store::{Store, Store1};

#[op2(async)]
#[buffer]
pub async fn op_get_personal_bytes<PS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Option<BytesMut> {
    let personal_store = {
        loop {
            let personal_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<PS>>()
            };

            match personal_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    // TODO: should probably err instead of returning None
    let result = if let Some(store) = personal_store.as_ref() {
        store
            .scope_1(format!("{store_name}:bytes"))
            .get(key)
            .await
            .map(|bytes| bytes.map(BytesMut::from))
            .unwrap_or_default()
    } else {
        None
    };

    state.borrow_mut().put(personal_store);

    result
}

#[op2(async)]
pub async fn op_set_personal_bytes<PS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
    #[buffer(copy)] value: Bytes,
) -> bool {
    let personal_store = {
        loop {
            let personal_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<PS>>()
            };

            match personal_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = if let Some(store) = personal_store.as_ref() {
        store
            .scope_1(format!("{store_name}:bytes"))
            .put(key, value)
            .await
            .is_ok()
    } else {
        false
    };

    state.borrow_mut().put(personal_store);

    result
}

#[op2(async)]
#[string]
pub async fn op_get_personal_string<PS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Option<String> {
    let personal_store = {
        loop {
            let personal_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<PS>>()
            };

            match personal_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = if let Some(store) = personal_store.as_ref() {
        match store.scope_1(format!("{store_name}:string")).get(key).await {
            Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).to_string()),
            _ => None,
        }
    } else {
        None
    };

    state.borrow_mut().put(personal_store);

    result
}

#[op2(async)]
pub async fn op_set_personal_string<PS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
    #[string] value: String,
) -> bool {
    let personal_store = {
        loop {
            let personal_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<PS>>()
            };

            match personal_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = if let Some(store) = personal_store.as_ref() {
        store
            .scope_1(format!("{store_name}:string"))
            .put(key, Bytes::from(value))
            .await
            .is_ok()
    } else {
        false
    };

    state.borrow_mut().put(personal_store);

    result
}

extension!(
    kv_personal_ext,
    parameters = [ PS: Store1 ],
    ops_fn = get_ops<PS>,
);

fn get_ops<PS: Store1>() -> Vec<OpDecl> {
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
