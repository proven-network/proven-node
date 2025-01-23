#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

mod error;

use super::StoredKey;
use crate::extensions::{CryptoState, Key, SessionState};
use error::Error;

use std::cell::RefCell;
use std::rc::Rc;

use bytes::{Bytes, BytesMut};
use deno_core::{op2, OpState};
use proven_radix_nft_verifier::{RadixNftVerificationResult, RadixNftVerifier};
use proven_store::{Store, Store1, Store2};
use serde::Serialize;

#[derive(Serialize)]
enum NftStoreGetResponse<T> {
    NftDoesNotExist,
    NoAccountsInContext,
    None,
    OwnershipInvalid(String),
    Some(T),
}

#[derive(Serialize)]
enum NftStoreSetResponse {
    NftDoesNotExist,
    NoAccountsInContext,
    Ok,
    OwnershipInvalid(String),
}

#[op2(async)]
#[serde]
pub async fn op_nft_keys<NS: Store2, RNV: RadixNftVerifier>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] store_type: String,
    #[string] resource_address: String,
    #[string] nft_id: String,
) -> Result<NftStoreGetResponse<Vec<String>>, Error> {
    let accounts = match state.borrow().borrow::<SessionState>().accounts.clone() {
        Some(accounts) if !accounts.is_empty() => accounts,
        Some(_) | None => return Ok(NftStoreGetResponse::NoAccountsInContext),
    };

    let verifier = { state.borrow().borrow::<RNV>().clone() };
    let verification = verifier
        .verify_ownership(&accounts, resource_address.clone(), nft_id.clone())
        .await
        .map_err(|e| Error::Verification(e.to_string()))?;

    if let RadixNftVerificationResult::NotOwned(account) = verification {
        return Ok(NftStoreGetResponse::OwnershipInvalid(account));
    }

    if matches!(verification, RadixNftVerificationResult::NftDoesNotExist) {
        return Ok(NftStoreGetResponse::NftDoesNotExist);
    }

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

    if let Some(store) = nft_store.as_ref() {
        let result = store
            .scope(format!("{store_name}:{store_type}"))
            .scope(format!("{resource_address}:{nft_id}"))
            .keys()
            .await
            .map(NftStoreGetResponse::Some)
            .map_err(|e| Error::Store(e.to_string()));

        state.borrow_mut().put(nft_store);

        result
    } else {
        state.borrow_mut().put(nft_store);

        Ok(NftStoreGetResponse::NoAccountsInContext)
    }
}

#[op2(async)]
#[serde]
pub async fn op_get_nft_bytes<NS: Store2, RNV: RadixNftVerifier>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] resource_address: String,
    #[string] nft_id: String,
    #[string] key: String,
) -> Result<NftStoreGetResponse<Bytes>, Error> {
    let accounts = match state.borrow().borrow::<SessionState>().accounts.clone() {
        Some(accounts) if !accounts.is_empty() => accounts,
        Some(_) | None => return Ok(NftStoreGetResponse::NoAccountsInContext),
    };

    let verifier = { state.borrow().borrow::<RNV>().clone() };
    let verification = verifier
        .verify_ownership(&accounts, resource_address.clone(), nft_id.clone())
        .await
        .map_err(|e| Error::Verification(e.to_string()))?;

    if let RadixNftVerificationResult::NotOwned(account) = verification {
        return Ok(NftStoreGetResponse::OwnershipInvalid(account));
    }

    if matches!(verification, RadixNftVerificationResult::NftDoesNotExist) {
        return Ok(NftStoreGetResponse::NftDoesNotExist);
    }

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

    if let Some(store) = nft_store.as_ref() {
        let result = store
            .scope(format!("{store_name}:bytes"))
            .scope(format!("{resource_address}:{nft_id}"))
            .get(key)
            .await;

        state.borrow_mut().put(nft_store);

        match result {
            Ok(Some(bytes)) => Ok(NftStoreGetResponse::Some(bytes)),
            Ok(None) => Ok(NftStoreGetResponse::None),
            Err(e) => Err(Error::Store(e.to_string())),
        }
    } else {
        state.borrow_mut().put(nft_store);

        Ok(NftStoreGetResponse::NoAccountsInContext)
    }
}

#[op2(async)]
#[serde]
pub async fn op_set_nft_bytes<NS: Store2, RNV: RadixNftVerifier>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] resource_address: String,
    #[string] nft_id: String,
    #[string] key: String,
    #[buffer(copy)] value: Bytes,
) -> Result<NftStoreSetResponse, Error> {
    let accounts = match state.borrow().borrow::<SessionState>().accounts.clone() {
        Some(accounts) if !accounts.is_empty() => accounts,
        Some(_) | None => return Ok(NftStoreSetResponse::NoAccountsInContext),
    };

    let verifier = { state.borrow().borrow::<RNV>().clone() };
    let verification = verifier
        .verify_ownership(&accounts, resource_address.clone(), nft_id.clone())
        .await
        .map_err(|e| Error::Verification(e.to_string()))?;

    if let RadixNftVerificationResult::NotOwned(account) = verification {
        return Ok(NftStoreSetResponse::OwnershipInvalid(account));
    }

    if matches!(verification, RadixNftVerificationResult::NftDoesNotExist) {
        return Ok(NftStoreSetResponse::NftDoesNotExist);
    }

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
            .scope(format!("{resource_address}:{nft_id}"))
            .put(key, value)
            .await
            .map(|()| NftStoreSetResponse::Ok)
            .map_err(|e| Error::Store(e.to_string()))
    } else {
        Ok(NftStoreSetResponse::NoAccountsInContext)
    };

    state.borrow_mut().put(nft_store);

    result
}

#[op2(async)]
#[serde]
pub async fn op_get_nft_key<NS: Store2, RNV: RadixNftVerifier>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] resource_address: String,
    #[string] nft_id: String,
    #[string] key: String,
) -> Result<NftStoreGetResponse<u32>, Error> {
    let accounts = match state.borrow().borrow::<SessionState>().accounts.clone() {
        Some(accounts) if !accounts.is_empty() => accounts,
        Some(_) | None => return Ok(NftStoreGetResponse::NoAccountsInContext),
    };

    let verifier = { state.borrow().borrow::<RNV>().clone() };
    let verification = verifier
        .verify_ownership(&accounts, resource_address.clone(), nft_id.clone())
        .await
        .map_err(|e| Error::Verification(e.to_string()))?;

    if let RadixNftVerificationResult::NotOwned(account) = verification {
        return Ok(NftStoreGetResponse::OwnershipInvalid(account));
    }

    if matches!(verification, RadixNftVerificationResult::NftDoesNotExist) {
        return Ok(NftStoreGetResponse::NftDoesNotExist);
    }

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

    if let Some(store) = nft_store.as_ref() {
        let result = match store
            .scope(format!("{store_name}:key"))
            .scope(format!("{resource_address}:{nft_id}"))
            .get(key)
            .await
        {
            Ok(Some(bytes)) => {
                let stored_key: StoredKey = match ciborium::de::from_reader(&*bytes) {
                    Ok(key) => key,
                    Err(_) => return Ok(NftStoreGetResponse::None),
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

                Ok(NftStoreGetResponse::Some(key_id))
            }
            Ok(None) => Ok(NftStoreGetResponse::None),
            Err(e) => Err(Error::Store(e.to_string())),
        };

        state.borrow_mut().put(nft_store);

        result
    } else {
        state.borrow_mut().put(nft_store);

        Ok(NftStoreGetResponse::NoAccountsInContext)
    }
}

#[op2(async)]
#[serde]
pub async fn op_set_nft_key<NS: Store2, RNV: RadixNftVerifier>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] resource_address: String,
    #[string] nft_id: String,
    #[string] key: String,
    key_id: u32,
) -> Result<NftStoreSetResponse, Error> {
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

    let accounts = match state.borrow().borrow::<SessionState>().accounts.clone() {
        Some(accounts) if !accounts.is_empty() => accounts,
        Some(_) | None => return Ok(NftStoreSetResponse::NoAccountsInContext),
    };

    let verifier = { state.borrow().borrow::<RNV>().clone() };
    let verification = verifier
        .verify_ownership(&accounts, resource_address.clone(), nft_id.clone())
        .await
        .map_err(|e| Error::Verification(e.to_string()))?;

    if let RadixNftVerificationResult::NotOwned(account) = verification {
        return Ok(NftStoreSetResponse::OwnershipInvalid(account));
    }

    if matches!(verification, RadixNftVerificationResult::NftDoesNotExist) {
        return Ok(NftStoreSetResponse::NftDoesNotExist);
    }

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
            .scope(format!("{store_name}:key"))
            .scope(format!("{resource_address}:{nft_id}"))
            .put(key, crypto_key_bytes)
            .await
            .map(|()| NftStoreSetResponse::Ok)
            .map_err(|e| Error::Store(e.to_string()))
    } else {
        Ok(NftStoreSetResponse::NoAccountsInContext)
    };

    state.borrow_mut().put(nft_store);

    result
}

#[op2(async)]
#[serde]
pub async fn op_get_nft_string<NS: Store2, RNV: RadixNftVerifier>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] resource_address: String,
    #[string] nft_id: String,
    #[string] key: String,
) -> Result<NftStoreGetResponse<String>, Error> {
    let accounts = match state.borrow().borrow::<SessionState>().accounts.clone() {
        Some(accounts) if !accounts.is_empty() => accounts,
        Some(_) | None => return Ok(NftStoreGetResponse::NoAccountsInContext),
    };

    let verifier = { state.borrow().borrow::<RNV>().clone() };
    let verification = verifier
        .verify_ownership(&accounts, resource_address.clone(), nft_id.clone())
        .await
        .map_err(|e| Error::Verification(e.to_string()))?;

    if let RadixNftVerificationResult::NotOwned(account) = verification {
        return Ok(NftStoreGetResponse::OwnershipInvalid(account));
    }

    if matches!(verification, RadixNftVerificationResult::NftDoesNotExist) {
        return Ok(NftStoreGetResponse::NftDoesNotExist);
    }

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

    if let Some(store) = nft_store.as_ref() {
        let result = store
            .scope(format!("{store_name}:string"))
            .scope(format!("{resource_address}:{nft_id}"))
            .get(key)
            .await;

        state.borrow_mut().put(nft_store);

        match result {
            Ok(Some(bytes)) => Ok(NftStoreGetResponse::Some(
                String::from_utf8_lossy(&bytes).to_string(),
            )),
            Ok(None) => Ok(NftStoreGetResponse::None),
            Err(e) => Err(Error::Store(e.to_string())),
        }
    } else {
        state.borrow_mut().put(nft_store);

        Ok(NftStoreGetResponse::NoAccountsInContext)
    }
}

#[op2(async)]
#[serde]
pub async fn op_set_nft_string<NS: Store2, RNV: RadixNftVerifier>(
    state: Rc<RefCell<OpState>>,
    #[string] store_name: String,
    #[string] resource_address: String,
    #[string] nft_id: String,
    #[string] key: String,
    #[string] value: String,
) -> Result<NftStoreSetResponse, Error> {
    let accounts = match state.borrow().borrow::<SessionState>().accounts.clone() {
        Some(accounts) if !accounts.is_empty() => accounts,
        Some(_) | None => return Ok(NftStoreSetResponse::NoAccountsInContext),
    };

    let verifier = { state.borrow().borrow::<RNV>().clone() };
    let verification = verifier
        .verify_ownership(&accounts, resource_address.clone(), nft_id.clone())
        .await
        .map_err(|e| Error::Verification(e.to_string()))?;

    if let RadixNftVerificationResult::NotOwned(account) = verification {
        return Ok(NftStoreSetResponse::OwnershipInvalid(account));
    }

    if matches!(verification, RadixNftVerificationResult::NftDoesNotExist) {
        return Ok(NftStoreSetResponse::NftDoesNotExist);
    }

    drop(verification);

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
            .scope(format!("{resource_address}:{nft_id}"))
            .put(key, Bytes::from(value))
            .await
            .map(|()| NftStoreSetResponse::Ok)
            .map_err(|e| Error::Store(e.to_string()))
    } else {
        Ok(NftStoreSetResponse::NoAccountsInContext)
    };

    state.borrow_mut().put(nft_store);

    result
}

#[cfg(test)]
mod tests {
    use crate::{ExecutionRequest, RuntimeOptions, Worker};

    #[tokio::test]
    async fn test_nft_bytes_store() {
        let mut runtime_options = RuntimeOptions::for_test_code("kv/test_nft_bytes_store", "test");

        runtime_options
            .radix_nft_verifier
            .insert_ownership(
                "my_account",
                "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi",
                "#420#",
            )
            .await;

        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec!["my_account".to_string()],
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
    async fn test_nft_bytes_store_nft_doesnt_exist() {
        let runtime_options = RuntimeOptions::for_test_code("kv/test_nft_bytes_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec!["my_account".to_string()],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nft_bytes_store_nft_not_owned() {
        let mut runtime_options = RuntimeOptions::for_test_code("kv/test_nft_bytes_store", "test");

        runtime_options
            .radix_nft_verifier
            .insert_ownership(
                "some_other_account",
                "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi",
                "#420#",
            )
            .await;

        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec!["my_account".to_string()],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nft_bytes_store_no_accounts() {
        let runtime_options = RuntimeOptions::for_test_code("kv/test_nft_bytes_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nft_key_store() {
        let mut runtime_options = RuntimeOptions::for_test_code("kv/test_nft_key_store", "test");

        runtime_options
            .radix_nft_verifier
            .insert_ownership(
                "my_account",
                "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi",
                "#420#",
            )
            .await;

        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec!["my_account".to_string()],
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
    async fn test_nft_key_store_nft_doesnt_exist() {
        let runtime_options = RuntimeOptions::for_test_code("kv/test_nft_key_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec!["my_account".to_string()],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nft_key_store_nft_not_owned() {
        let mut runtime_options = RuntimeOptions::for_test_code("kv/test_nft_key_store", "test");

        runtime_options
            .radix_nft_verifier
            .insert_ownership(
                "some_other_account",
                "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi",
                "#420#",
            )
            .await;

        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec!["my_account".to_string()],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nft_key_store_no_accounts() {
        let runtime_options = RuntimeOptions::for_test_code("kv/test_nft_key_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nft_string_store() {
        let mut runtime_options = RuntimeOptions::for_test_code("kv/test_nft_string_store", "test");

        runtime_options
            .radix_nft_verifier
            .insert_ownership(
                "my_account",
                "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi",
                "#420#",
            )
            .await;

        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec!["my_account".to_string()],
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
    async fn test_nft_string_store_nft_doesnt_exist() {
        let runtime_options = RuntimeOptions::for_test_code("kv/test_nft_string_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec!["my_account".to_string()],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nft_string_store_nft_not_owned() {
        let mut runtime_options = RuntimeOptions::for_test_code("kv/test_nft_string_store", "test");

        runtime_options
            .radix_nft_verifier
            .insert_ownership(
                "some_other_account",
                "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi",
                "#420#",
            )
            .await;

        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec!["my_account".to_string()],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nft_string_store_no_accounts() {
        let runtime_options = RuntimeOptions::for_test_code("kv/test_nft_string_store", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: "my_identity".to_string(),
        };
        let result = worker.execute(request).await;

        assert!(result.is_err());
    }
}
