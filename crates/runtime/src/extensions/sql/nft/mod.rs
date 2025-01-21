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

use deno_core::{extension, op2, OpState};
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
) -> Result<NftDbResponse<u32>, Error<NSS::Error, RNV::Error>> {
    let accounts = match state.borrow().borrow::<SessionState>().accounts.clone() {
        Some(accounts) if !accounts.is_empty() => accounts,
        Some(_) | None => return Ok(NftDbResponse::NoAccountsInContext),
    };

    let verifier = { state.borrow().borrow::<RNV>().clone() };
    let verification = verifier
        .verify_ownership(&accounts, resource_address.clone(), nft_id.clone())
        .await
        .map_err(Error::Verification)?;

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
            .map_err(Error::SqlStore);

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
            .map_err(Error::SqlStore)
    } else {
        connection
            .execute(query, vec![])
            .await
            .map(|i| i as u32)
            .map(NftDbResponse::Ok)
            .map_err(Error::SqlStore)
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
) -> Result<NftDbResponse<Option<(Vec<SqlParam>, u32)>>, Error<NSS::Error, RNV::Error>> {
    let accounts = match state.borrow().borrow::<SessionState>().accounts.clone() {
        Some(accounts) if !accounts.is_empty() => accounts,
        Some(_) | None => return Ok(NftDbResponse::NoAccountsInContext),
    };

    let verifier = { state.borrow().borrow::<RNV>().clone() };
    let verification = verifier
        .verify_ownership(&accounts, resource_address.clone(), nft_id.clone())
        .await
        .map_err(Error::Verification)?;

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
            .map_err(Error::SqlStore);

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
            .map_err(Error::SqlStore)?
    } else {
        connection
            .query(query, vec![])
            .await
            .map_err(Error::SqlStore)?
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

extension!(
    sql_nft_ext,
    parameters = [ NSS: SqlStore2, RNV: RadixNftVerifier ],
    ops = [
        op_execute_nft_sql<NSS, RNV>,
        op_query_nft_sql<NSS, RNV>,
    ]
);

#[cfg(test)]
mod tests {
    use crate::test_utils::create_runtime_options;
    use crate::{ExecutionRequest, Worker};

    #[tokio::test]
    async fn test_nft_db() {
        let mut runtime_options = create_runtime_options("sql/test_nft_db", "test");

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
        assert_eq!(result.unwrap().output, "alice@example.com");
    }

    #[tokio::test]
    async fn test_nft_db_multiple() {
        let mut runtime_options = create_runtime_options("sql/test_nft_db_multiple", "test");

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

        let execution_result = result.unwrap();
        assert!(execution_result.output.is_array());
        assert_eq!(execution_result.output.as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_nft_db_nft_doesnt_exist() {
        let runtime_options = create_runtime_options("sql/test_nft_db", "test");
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
    async fn test_nft_db_nft_not_owned() {
        let mut runtime_options = create_runtime_options("sql/test_nft_db", "test");

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
    async fn test_nft_db_no_accounts() {
        let runtime_options = create_runtime_options("sql/test_nft_db", "test");
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
