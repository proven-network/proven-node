#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

mod connection_manager;

use super::SqlParamListManager;
pub use connection_manager::PersonalSqlConnectionManager;

use std::cell::RefCell;
use std::rc::Rc;

use deno_core::{extension, op2, OpState};
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
) -> Result<PersonalDbResponse<Vec<Vec<SqlParam>>>, PSS::Error> {
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

        let result = connection.query(query, params).await?;
        let collected = result.collect::<Vec<Vec<SqlParam>>>().await;
        Ok(PersonalDbResponse::Ok(collected))
    } else {
        let result = connection.query(query, vec![]).await?;
        let collected = result.collect::<Vec<Vec<SqlParam>>>().await;
        Ok(PersonalDbResponse::Ok(collected))
    }
}

extension!(
    sql_personal_ext,
    parameters = [ PSS: SqlStore1 ],
    ops = [
        op_execute_personal_sql<PSS>,
        op_query_personal_sql<PSS>,
    ]
);

#[cfg(test)]
mod tests {
    use crate::test_utils::create_runtime_options;
    use crate::{ExecutionRequest, Worker};

    #[tokio::test]
    async fn test_personal_db() {
        let runtime_options = create_runtime_options("sql/test_personal_db", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            accounts: None,
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: Some("identity_123".to_string()),
        };

        let result = worker.execute(request).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().output, "alice@example.com");
    }

    #[tokio::test]
    async fn test_personal_db_no_context() {
        let runtime_options = create_runtime_options("sql/test_personal_db", "test");
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
