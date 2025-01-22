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
#[serde]
pub async fn op_application_keys<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] store_type: String,
) -> Result<Vec<String>, AS::Error> {
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
        .scope(format!("{store_name}:{store_type}"))
        .keys()
        .await;

    state.borrow_mut().put(application_store);

    result
}

#[op2(async)]
#[buffer]
pub async fn op_get_application_bytes<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Result<Option<BytesMut>, AS::Error> {
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
        Ok(Some(bytes)) => Ok(Some(BytesMut::from(bytes))),
        Ok(None) => Ok(None),
        Err(e) => Err(e),
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
) -> Result<(), AS::Error> {
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
        .await;

    state.borrow_mut().put(application_store);

    result
}

#[op2(async)]
pub async fn op_get_application_key<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Result<Option<u32>, AS::Error> {
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
                Err(_) => return Ok(None),
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

            Ok(Some(key_id))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(e),
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
) -> Result<(), AS::Error> {
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
        .await;

    state.borrow_mut().put(application_store);

    result
}

#[op2(async)]
#[string]
pub async fn op_get_application_string<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Result<Option<String>, AS::Error> {
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
        Ok(Some(bytes)) => Ok(Some(String::from_utf8_lossy(&bytes).to_string())),
        Ok(None) => Ok(None),
        Err(e) => Err(e),
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
) -> Result<(), AS::Error> {
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
        .await;

    state.borrow_mut().put(application_store);

    result
}

#[cfg(test)]
mod tests {
    use crate::test_utils::create_test_runtime_options;
    use crate::{ExecutionRequest, Worker};

    #[tokio::test]
    async fn test_application_bytes_store() {
        let runtime_options = create_test_runtime_options("kv/test_application_bytes_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
    }

    #[tokio::test]
    async fn test_application_key_store() {
        let runtime_options = create_test_runtime_options("kv/test_application_key_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
    }

    #[tokio::test]
    async fn test_application_string_store() {
        let runtime_options = create_test_runtime_options("kv/test_application_string_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
    }
}
