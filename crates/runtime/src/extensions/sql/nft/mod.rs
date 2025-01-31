#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

mod connection_manager;
mod error;

use super::{SqlParamListManager, SqlQueryResultsManager};
pub use connection_manager::NftSqlConnectionManager;
use error::Error;

use std::cell::RefCell;
use std::rc::Rc;

use deno_core::{op2, OpState};
use futures::StreamExt;
use proven_radix_nft_verifier::{RadixNftVerificationResult, RadixNftVerifier};
use proven_sql::{SqlConnection, SqlParam, SqlStore2};
use serde::Serialize;

use crate::extensions::SessionState;

#[derive(Serialize)]
enum NftDbResponse<T> {
    NftDoesNotExist,
    NoAccountsInContext,
    Ok(T),
    OwnershipInvalid(String),
}

#[op2(async)]
#[serde]
#[allow(clippy::cast_possible_truncation)]
pub async fn op_execute_nft_sql<NSS: SqlStore2, RNV: RadixNftVerifier>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] resource_address: String,
    #[string] nft_id: String,
    #[string] query: String,
    param_list_id_opt: Option<u32>,
) -> Result<NftDbResponse<u32>, Error> {
    let accounts = match state.borrow().borrow::<SessionState>() {
        SessionState::Session { accounts, .. } if !accounts.is_empty() => accounts.clone(),
        _ => return Ok(NftDbResponse::NoAccountsInContext),
    };

    let verifier = { state.borrow().borrow::<RNV>().clone() };
    let verification = verifier
        .verify_ownership(&accounts, resource_address.clone(), nft_id.clone())
        .await
        .map_err(|e| Error::Verification(e.to_string()))?;

    if let RadixNftVerificationResult::NotOwned(account) = verification {
        return Ok(NftDbResponse::OwnershipInvalid(account));
    }

    if matches!(verification, RadixNftVerificationResult::NftDoesNotExist) {
        return Ok(NftDbResponse::NftDoesNotExist);
    }

    let connection_manager_opt = {
        loop {
            let connection_manager_opt = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<NftSqlConnectionManager<NSS>>>()
            };

            match connection_manager_opt {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let connection = if let Some(connection_manager) = connection_manager_opt.as_ref() {
        let result = connection_manager
            .connect(db_name, resource_address, nft_id)
            .await
            .map_err(|e| Error::SqlStore(e.to_string()));

        state.borrow_mut().put(connection_manager_opt);

        result
    } else {
        state.borrow_mut().put(connection_manager_opt);

        return Ok(NftDbResponse::NoAccountsInContext);
    }?;

    if let Some(param_list_id) = param_list_id_opt {
        let mut params_lists = {
            loop {
                let params_lists = {
                    let mut borrowed_state = state.borrow_mut();

                    borrowed_state.try_take::<SqlParamListManager>()
                };

                match params_lists {
                    Some(params_lists) => break params_lists,
                    None => {
                        tokio::task::yield_now().await;
                    }
                }
            }
        };

        let params = params_lists.finialize_param_list(param_list_id);

        state.borrow_mut().put(params_lists);

        connection
            .execute(query, params)
            .await
            .map(|i| i as u32)
            .map(NftDbResponse::Ok)
            .map_err(|e| Error::SqlStore(e.to_string()))
    } else {
        connection
            .execute(query, vec![])
            .await
            .map(|i| i as u32)
            .map(NftDbResponse::Ok)
            .map_err(|e| Error::SqlStore(e.to_string()))
    }
}

#[op2(async)]
#[serde]
pub async fn op_query_nft_sql<NSS: SqlStore2, RNV: RadixNftVerifier>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] resource_address: String,
    #[string] nft_id: String,
    #[string] query: String,
    param_list_id_opt: Option<u32>,
) -> Result<NftDbResponse<Option<(Vec<SqlParam>, u32)>>, Error> {
    let accounts = match state.borrow().borrow::<SessionState>() {
        SessionState::Session { accounts, .. } if !accounts.is_empty() => accounts.clone(),
        _ => return Ok(NftDbResponse::NoAccountsInContext),
    };

    let verifier = { state.borrow().borrow::<RNV>().clone() };
    let verification = verifier
        .verify_ownership(&accounts, resource_address.clone(), nft_id.clone())
        .await
        .map_err(|e| Error::Verification(e.to_string()))?;

    if let RadixNftVerificationResult::NotOwned(account) = verification {
        return Ok(NftDbResponse::OwnershipInvalid(account));
    }

    if matches!(verification, RadixNftVerificationResult::NftDoesNotExist) {
        return Ok(NftDbResponse::NftDoesNotExist);
    }

    let connection_manager_opt = {
        loop {
            let connection_manager_opt = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<NftSqlConnectionManager<NSS>>>()
            };

            match connection_manager_opt {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let connection = if let Some(connection_manager) = connection_manager_opt.as_ref() {
        let result = connection_manager
            .connect(db_name, resource_address, nft_id)
            .await
            .map_err(|e| Error::SqlStore(e.to_string()));

        state.borrow_mut().put(connection_manager_opt);

        result
    } else {
        state.borrow_mut().put(connection_manager_opt);

        return Ok(NftDbResponse::NoAccountsInContext);
    }?;

    let mut stream = if let Some(param_list_id) = param_list_id_opt {
        let mut params_lists = {
            loop {
                let params_lists = {
                    let mut borrowed_state = state.borrow_mut();
                    borrowed_state.try_take::<SqlParamListManager>()
                };

                match params_lists {
                    Some(params_lists) => break params_lists,
                    None => {
                        tokio::task::yield_now().await;
                    }
                }
            }
        };

        let params = params_lists.finialize_param_list(param_list_id);
        state.borrow_mut().put(params_lists);

        connection
            .query(query, params)
            .await
            .map_err(|e| Error::SqlStore(e.to_string()))?
    } else {
        connection
            .query(query, vec![])
            .await
            .map_err(|e| Error::SqlStore(e.to_string()))?
    };

    if let Some(first_row) = stream.next().await {
        let mut query_results_manager = {
            loop {
                let query_results_manager = {
                    let mut borrowed_state = state.borrow_mut();
                    borrowed_state.try_take::<SqlQueryResultsManager>()
                };

                match query_results_manager {
                    Some(store) => break store,
                    None => {
                        tokio::task::yield_now().await;
                    }
                }
            }
        };

        let row_stream_id = query_results_manager.save_stream(stream);
        state.borrow_mut().put(query_results_manager);

        Ok(NftDbResponse::Ok(Some((first_row, row_stream_id))))
    } else {
        Ok(NftDbResponse::Ok(None))
    }
}

#[cfg(test)]
mod tests {
    use crate::{ExecutionRequest, ExecutionResult, HandlerSpecifier, RuntimeOptions, Worker};

    #[tokio::test]
    async fn test_nft_db() {
        let mut runtime_options = RuntimeOptions::for_test_code("sql/test_nft_db");
        runtime_options
            .radix_nft_verifier
            .insert_ownership(
                "my_account",
                "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi",
                "#420#",
            )
            .await;

        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            accounts: vec!["my_account".to_string()],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identity: "my_identity".to_string(),
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert_eq!(output, "alice@example.com");
            }
            Ok(ExecutionResult::Error { error, .. }) => {
                panic!("Unexpected js error: {error:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_nft_db_multiple() {
        let mut runtime_options = RuntimeOptions::for_test_code("sql/test_nft_db_multiple");
        runtime_options
            .radix_nft_verifier
            .insert_ownership(
                "my_account",
                "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi",
                "#420#",
            )
            .await;

        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            accounts: vec!["my_account".to_string()],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identity: "my_identity".to_string(),
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Ok { output, .. }) => {
                assert!(output.is_array());
                assert_eq!(output.as_array().unwrap().len(), 2);
            }
            Ok(ExecutionResult::Error { error, .. }) => {
                panic!("Unexpected js error: {error:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_nft_db_nft_doesnt_exist() {
        let runtime_options = RuntimeOptions::for_test_code("sql/test_nft_db");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            accounts: vec!["my_account".to_string()],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identity: "my_identity".to_string(),
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Error { .. }) => {
                // Expected - NFT doesn't exist
            }
            Ok(ExecutionResult::Ok { output, .. }) => {
                panic!("Expected error but got success: {output:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_nft_db_nft_not_owned() {
        let mut runtime_options = RuntimeOptions::for_test_code("sql/test_nft_db");
        runtime_options
            .radix_nft_verifier
            .insert_ownership(
                "some_other_account",
                "resource_1qlq38wvrvh5m4kaz6etaac4389qtuycnp89atc8acdfi",
                "#420#",
            )
            .await;

        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            accounts: vec!["my_account".to_string()],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identity: "my_identity".to_string(),
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Error { .. }) => {
                // Expected - NFT not owned
            }
            Ok(ExecutionResult::Ok { output, .. }) => {
                panic!("Expected error but got success: {output:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_nft_db_no_accounts() {
        let runtime_options = RuntimeOptions::for_test_code("sql/test_nft_db");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::RpcWithUserContext {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identity: "my_identity".to_string(),
        };

        match worker.execute(request).await {
            Ok(ExecutionResult::Error { .. }) => {
                // Expected - no accounts in context
            }
            Ok(ExecutionResult::Ok { output, .. }) => {
                panic!("Expected error but got success: {output:?}");
            }
            Err(error) => {
                panic!("Unexpected error: {error:?}");
            }
        }
    }
}
