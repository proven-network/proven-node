#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

mod connection_manager;

use super::{SqlParamListManager, SqlQueryResultsManager};
pub use connection_manager::PersonalSqlConnectionManager;

use std::cell::RefCell;
use std::rc::Rc;

use deno_core::{op2, OpState};
use futures::StreamExt;
use proven_sql::{SqlConnection, SqlParam, SqlStore1};
use serde::Serialize;

#[derive(Serialize)]
enum PersonalDbResponse<T> {
    NoPersonalContext,
    Ok(T),
}

#[op2(async)]
#[serde]
#[allow(clippy::cast_possible_truncation)]
pub async fn op_execute_personal_sql<PSS: SqlStore1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] query: String,
    param_list_id_opt: Option<u32>,
) -> Result<PersonalDbResponse<u32>, PSS::Error> {
    let connection_manager_opt = {
        loop {
            let connection_manager_opt = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<PersonalSqlConnectionManager<PSS>>>()
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
        let result = connection_manager.connect(db_name).await;

        state.borrow_mut().put(connection_manager_opt);

        result
    } else {
        state.borrow_mut().put(connection_manager_opt);

        return Ok(PersonalDbResponse::NoPersonalContext);
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
            .map(PersonalDbResponse::Ok)
    } else {
        connection
            .execute(query, vec![])
            .await
            .map(|i| i as u32)
            .map(PersonalDbResponse::Ok)
    }
}

#[op2(async)]
#[serde]
pub async fn op_query_personal_sql<PSS: SqlStore1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] query: String,
    param_list_id_opt: Option<u32>,
) -> Result<PersonalDbResponse<Option<(Vec<SqlParam>, u32)>>, PSS::Error> {
    let connection_manager_opt = {
        loop {
            let connection_manager_opt = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<Option<PersonalSqlConnectionManager<PSS>>>()
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
        let result = connection_manager.connect(db_name).await;

        state.borrow_mut().put(connection_manager_opt);

        result
    } else {
        state.borrow_mut().put(connection_manager_opt);

        return Ok(PersonalDbResponse::NoPersonalContext);
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

        connection.query(query, params).await?
    } else {
        connection.query(query, vec![]).await?
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

        Ok(PersonalDbResponse::Ok(Some((first_row, row_stream_id))))
    } else {
        Ok(PersonalDbResponse::Ok(None))
    }
}

#[cfg(test)]
mod tests {
    use crate::{ExecutionRequest, HandlerSpecifier, RuntimeOptions, Worker};

    #[tokio::test]
    async fn test_personal_db() {
        let runtime_options = RuntimeOptions::for_test_code("sql/test_personal_db");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::HttpWithUserContext {
            accounts: vec![],
            body: None,
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identity: "my_identity".to_string(),
            method: http::Method::GET,
            path: "/test".to_string(),
            query: None,
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }
        assert_eq!(result.unwrap().output, "alice@example.com");
    }

    #[tokio::test]
    async fn test_personal_db_no_context() {
        let runtime_options = RuntimeOptions::for_test_code("sql/test_personal_db");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Http {
            body: None,
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            method: http::Method::GET,
            path: "/test".to_string(),
            query: None,
        };

        let result = worker.execute(request).await;

        assert!(result.is_err());
    }
}
