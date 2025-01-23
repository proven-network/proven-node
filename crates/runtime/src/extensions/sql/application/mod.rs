#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

mod connection_manager;

use super::{SqlParamListManager, SqlQueryResultsManager};
pub use connection_manager::ApplicationSqlConnectionManager;

use std::cell::RefCell;
use std::rc::Rc;

use deno_core::{extension, op2, OpState};
use futures::StreamExt;
use proven_sql::{SqlConnection, SqlParam, SqlStore1};

#[op2(async)]
#[allow(clippy::cast_possible_truncation)]
async fn op_execute_application_sql<ASS: SqlStore1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] query: String,
    param_list_id_opt: Option<u32>,
) -> Result<u32, ASS::Error> {
    let connection_manager = {
        loop {
            let connection_manager = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<ApplicationSqlConnectionManager<ASS>>()
            };

            match connection_manager {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let connection_result = connection_manager.connect(db_name).await;
    state.borrow_mut().put(connection_manager);
    let connection = connection_result?;

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

        connection.execute(query, params).await.map(|i| i as u32)
    } else {
        connection.execute(query, vec![]).await.map(|i| i as u32)
    }
}

#[op2(async)]
#[serde]
async fn op_query_application_sql<ASS: SqlStore1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] query: String,
    param_list_id_opt: Option<u32>,
) -> Result<Option<(Vec<SqlParam>, u32)>, ASS::Error> {
    let connection_manager = {
        loop {
            let connection_manager = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<ApplicationSqlConnectionManager<ASS>>()
            };

            match connection_manager {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let connection_result = connection_manager.connect(db_name).await;
    state.borrow_mut().put(connection_manager);
    let connection = connection_result?;

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

        Ok(Some((first_row, row_stream_id)))
    } else {
        Ok(None)
    }
}

extension!(
    sql_application_ext,
    parameters = [ ASS: SqlStore1 ],
    ops = [
        op_execute_application_sql<ASS>,
        op_query_application_sql<ASS>,
    ]
);

#[cfg(test)]
mod tests {
    use crate::{ExecutionRequest, RuntimeOptions, Worker};

    #[tokio::test]
    async fn test_application_db() {
        let runtime_options = RuntimeOptions::for_test_code("sql/test_application_db", "test");
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
        assert_eq!(result.unwrap().output, "alice@example.com");
    }

    #[tokio::test]
    async fn test_application_db_multiple() {
        let runtime_options =
            RuntimeOptions::for_test_code("sql/test_application_db_multiple", "test");
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

        let execution_result = result.unwrap();
        assert!(execution_result.output.is_array());
        assert_eq!(execution_result.output.as_array().unwrap().len(), 2);
    }
}
