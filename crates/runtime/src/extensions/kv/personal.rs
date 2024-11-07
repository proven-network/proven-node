use std::cell::RefCell;
use std::rc::Rc;

use proven_store::{Store, Store1};

use deno_core::{extension, op2, OpDecl, OpState};

#[op2(async)]
#[buffer]
pub async fn op_get_personal_bytes<PS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Option<Vec<u8>> {
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
            .scope(format!("{}:bytes", store_name))
            .get(key)
            .await
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
    #[arraybuffer(copy)] value: Vec<u8>,
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
            .scope(format!("{}:bytes", store_name))
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
        match store.scope(format!("{}:string", store_name)).get(key).await {
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
            .scope(format!("{}:string", store_name))
            .put(key, value.as_bytes().to_vec())
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
