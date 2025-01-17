#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

use super::super::{CryptoState, Key};
use super::StoredKey;

use std::cell::RefCell;
use std::rc::Rc;

use bytes::{Bytes, BytesMut};
use deno_core::{op2, OpState};
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
pub async fn op_get_application_key<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Option<u32> {
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
        .scope(format!("{store_name}:key"))
        .get(key)
        .await
    {
        Ok(Some(bytes)) => {
            let stored_key: StoredKey = match ciborium::de::from_reader(&*bytes) {
                Ok(key) => key,
                Err(_) => return None,
            };

            let key_id = state
                .borrow_mut()
                .borrow_mut::<CryptoState>()
                .load_existing_key(match stored_key {
                    StoredKey::Ed25519(signing_key_bytes) => {
                        Key::Ed25519(ed25519_dalek::SigningKey::from_bytes(
                            &signing_key_bytes.as_slice().try_into().unwrap(),
                        ))
                    }
                });

            Some(key_id)
        }
        _ => None,
    };

    state.borrow_mut().put(application_store);

    result
}

#[op2(async)]
pub async fn op_set_application_key<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
    key_id: u32,
) -> bool {
    let crypto_key_bytes = {
        let crypto_state_binding = state.borrow();
        let crypto_key = crypto_state_binding.borrow::<CryptoState>().get_key(key_id);
        match crypto_key {
            Key::Ed25519(signing_key) => {
                let stored_key = StoredKey::Ed25519(signing_key.to_bytes().to_vec());
                let mut bytes = Vec::new();
                ciborium::ser::into_writer(&stored_key, &mut bytes).unwrap();
                BytesMut::from(&bytes[..])
            }
        }
    };

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
        .scope(format!("{store_name}:key"))
        .put(key.clone(), crypto_key_bytes.into())
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
