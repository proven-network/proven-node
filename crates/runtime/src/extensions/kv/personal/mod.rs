#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

use super::StoredKey;
use crate::extensions::{CryptoState, Key};

use std::cell::RefCell;
use std::rc::Rc;

use bytes::{Bytes, BytesMut};
use deno_core::{op2, OpState};
use proven_store::{Store, Store1};
use serde::Serialize;

#[derive(Serialize)]
enum PersonalStoreGetResponse<T> {
    NoPersonalContext,
    None,
    Some(T),
}

#[derive(Serialize)]
enum PersonalStoreSetResponse {
    NoPersonalContext,
    Ok,
}

#[op2(async)]
#[serde]
pub async fn op_get_personal_bytes<PS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Result<PersonalStoreGetResponse<Bytes>, PS::Error> {
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

    if let Some(store) = personal_store.as_ref() {
        let result = store.scope(format!("{store_name}:bytes")).get(key).await;

        state.borrow_mut().put(personal_store);

        match result {
            Ok(Some(bytes)) => Ok(PersonalStoreGetResponse::Some(bytes)),
            Ok(None) => Ok(PersonalStoreGetResponse::None),
            Err(e) => Err(e),
        }
    } else {
        state.borrow_mut().put(personal_store);

        Ok(PersonalStoreGetResponse::NoPersonalContext)
    }
}

#[op2(async)]
#[serde]
pub async fn op_set_personal_bytes<PS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
    #[buffer(copy)] value: Bytes,
) -> Result<PersonalStoreSetResponse, PS::Error> {
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
            .scope(format!("{store_name}:bytes"))
            .put(key, value)
            .await
            .map(|()| PersonalStoreSetResponse::Ok)
    } else {
        Ok(PersonalStoreSetResponse::NoPersonalContext)
    };

    state.borrow_mut().put(personal_store);

    result
}

#[op2(async)]
#[serde]
pub async fn op_get_personal_key<PS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Result<PersonalStoreGetResponse<u32>, PS::Error> {
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

    if let Some(store) = personal_store.as_ref() {
        let result = match store.scope(format!("{store_name}:key")).get(key).await {
            Ok(Some(bytes)) => {
                let stored_key: StoredKey = match ciborium::de::from_reader(&*bytes) {
                    Ok(key) => key,
                    Err(_) => return Ok(PersonalStoreGetResponse::None),
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

                Ok(PersonalStoreGetResponse::Some(key_id))
            }
            Ok(None) => Ok(PersonalStoreGetResponse::None),
            Err(e) => Err(e),
        };

        state.borrow_mut().put(personal_store);

        result
    } else {
        state.borrow_mut().put(personal_store);

        Ok(PersonalStoreGetResponse::NoPersonalContext)
    }
}

#[op2(async)]
#[serde]
pub async fn op_set_personal_key<PS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
    key_id: u32,
) -> Result<PersonalStoreSetResponse, PS::Error> {
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
    }
    .freeze();

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
            .scope(format!("{store_name}:key"))
            .put(key, crypto_key_bytes)
            .await
            .map(|()| PersonalStoreSetResponse::Ok)
    } else {
        Ok(PersonalStoreSetResponse::NoPersonalContext)
    };

    state.borrow_mut().put(personal_store);

    result
}

#[op2(async)]
#[serde]
pub async fn op_get_personal_string<PS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
) -> Result<PersonalStoreGetResponse<String>, PS::Error> {
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

    if let Some(store) = personal_store.as_ref() {
        let result = store.scope(format!("{store_name}:string")).get(key).await;

        state.borrow_mut().put(personal_store);

        match result {
            Ok(Some(bytes)) => Ok(PersonalStoreGetResponse::Some(
                String::from_utf8_lossy(&bytes).to_string(),
            )),
            Ok(None) => Ok(PersonalStoreGetResponse::None),
            Err(e) => Err(e),
        }
    } else {
        state.borrow_mut().put(personal_store);

        Ok(PersonalStoreGetResponse::NoPersonalContext)
    }
}

#[op2(async)]
#[serde]
pub async fn op_set_personal_string<PS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] key: String,
    #[string] value: String,
) -> Result<PersonalStoreSetResponse, PS::Error> {
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
            .scope(format!("{store_name}:string"))
            .put(key, Bytes::from(value))
            .await
            .map(|()| PersonalStoreSetResponse::Ok)
    } else {
        Ok(PersonalStoreSetResponse::NoPersonalContext)
    };

    state.borrow_mut().put(personal_store);

    result
}

#[cfg(test)]
mod tests {
    use crate::test_utils::create_runtime_options;
    use crate::{ExecutionRequest, Worker};

    #[tokio::test]
    async fn test_personal_bytes_store() {
        let runtime_options = create_runtime_options("kv/test_personal_bytes_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            accounts: None,
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: Some("identity_123".to_string()),
        };
        let result = worker.execute(request).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_personal_bytes_store_no_context() {
        let runtime_options = create_runtime_options("kv/test_personal_bytes_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            accounts: None,
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: None,
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_personal_key_store() {
        let runtime_options = create_runtime_options("kv/test_personal_key_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            accounts: None,
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: Some("identity_123".to_string()),
        };
        let result = worker.execute(request).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_personal_string_key_no_context() {
        let runtime_options = create_runtime_options("kv/test_personal_key_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            accounts: None,
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: None,
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_personal_string_store() {
        let runtime_options = create_runtime_options("kv/test_personal_string_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            accounts: None,
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: Some("identity_123".to_string()),
        };
        let result = worker.execute(request).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_personal_string_store_no_context() {
        let runtime_options = create_runtime_options("kv/test_personal_string_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            accounts: None,
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: None,
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }
}
