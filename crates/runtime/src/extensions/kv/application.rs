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
pub async fn op_get_application_bytes<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Option<BytesMut> {
    let application_store = {
        loop {
            let application_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<AS>()
            };

            match application_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = match application_store
        .scope(format!("{store_name}:bytes"))
        .get(key)
        .await
    {
        Ok(Some(bytes)) => Some(BytesMut::from(bytes)),
        _ => None,
    };

    state.borrow_mut().put(application_store);

    result
}

#[op2(async)]
pub async fn op_set_application_bytes<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
    #[buffer(copy)] value: Bytes,
) -> bool {
    let application_store = {
        loop {
            let application_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<AS>()
            };

            match application_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = application_store
        .scope(format!("{store_name}:bytes"))
        .put(key, value)
        .await
        .is_ok();

    state.borrow_mut().put(application_store);

    result
}

#[op2(async)]
#[string]
pub async fn op_get_application_string<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Option<String> {
    let application_store = {
        loop {
            let application_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<AS>()
            };

            match application_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = match application_store
        .scope(format!("{store_name}:string"))
        .get(key)
        .await
    {
        Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).to_string()),
        _ => None,
    };

    state.borrow_mut().put(application_store);

    result
}

#[op2(async)]
pub async fn op_set_application_string<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
    #[string] value: String,
) -> bool {
    let application_store = {
        loop {
            let application_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<AS>()
            };

            match application_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = application_store
        .scope(format!("{store_name}:string"))
        .put(key, Bytes::from(value))
        .await
        .is_ok();

    state.borrow_mut().put(application_store);

    result
}

extension!(
    kv_application_ext,
    parameters = [ AS: Store1 ],
    ops_fn = get_ops<AS>,
);

fn get_ops<AS: Store1>() -> Vec<OpDecl> {
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
