#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

mod connection_manager;
mod param_list_manager;

pub use connection_manager::ApplicationSqlConnectionManager;
pub use param_list_manager::ApplicationSqlParamListManager;

use std::cell::RefCell;
use std::rc::Rc;

use bytes::Bytes;
use deno_core::{extension, op2, OpState};
use futures::StreamExt;
use proven_sql::{SqlConnection, SqlParam, SqlStore1};

#[op2(fast)]
#[bigint]
fn op_create_application_params_list(
    #[state] param_lists: &mut ApplicationSqlParamListManager,
) -> u64 {
    param_lists.create_param_list()
}

#[op2(fast)]
fn op_add_application_blob_param(
    #[state] param_lists: &mut ApplicationSqlParamListManager,
    #[bigint] param_list_id: u64,
    #[buffer(copy)] value: Bytes,
) {
    param_lists.push_blob_param(param_list_id, value);
}

#[op2(fast)]
fn op_add_application_integer_param(
    #[state] param_lists: &mut ApplicationSqlParamListManager,
    #[bigint] param_list_id: u64,
    #[bigint] value: i64,
) {
    param_lists.push_integer_param(param_list_id, value);
}

#[op2(fast)]
fn op_add_application_null_param(
    #[state] param_lists: &mut ApplicationSqlParamListManager,
    #[bigint] param_list_id: u64,
) {
    param_lists.push_null_param(param_list_id);
}

#[op2(fast)]
fn op_add_application_real_param(
    #[state] param_lists: &mut ApplicationSqlParamListManager,
    #[bigint] param_list_id: u64,
    value: f64,
) {
    param_lists.push_real_param(param_list_id, value);
}

#[op2(fast)]
fn op_add_application_text_param(
    #[state] param_lists: &mut ApplicationSqlParamListManager,
    #[bigint] param_list_id: u64,
    #[string] value: String,
) {
    param_lists.push_text_param(param_list_id, value);
}

#[op2(async)]
#[allow(clippy::cast_possible_truncation)]
async fn op_execute_application_sql<ASS: SqlStore1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] query: String,
    #[bigint] param_list_id_opt: Option<u64>,
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

                    borrowed_state.try_take::<ApplicationSqlParamListManager>()
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
    #[bigint] param_list_id_opt: Option<u64>,
) -> Result<Vec<Vec<SqlParam>>, ASS::Error> {
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

    let stream = if let Some(param_list_id) = param_list_id_opt {
        let mut params_lists = {
            loop {
                let params_lists = {
                    let mut borrowed_state = state.borrow_mut();

                    borrowed_state.try_take::<ApplicationSqlParamListManager>()
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

    Ok(stream.collect().await)
}

extension!(
    sql_application_ext,
    parameters = [ ASS: SqlStore1 ],
    ops = [
        op_add_application_blob_param,
        op_add_application_integer_param,
        op_add_application_null_param,
        op_add_application_real_param,
        op_add_application_text_param,
        op_create_application_params_list,
        op_execute_application_sql<ASS>,
        op_query_application_sql<ASS>,
    ]
);

#[cfg(test)]
mod tests {
    use crate::test_utils::create_runtime_options;
    use crate::{ExecutionRequest, Worker};

    #[tokio::test]
    async fn test_application_db() {
        let runtime_options = create_runtime_options("sql/test_application_db", "test");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest {
            accounts: None,
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            identity: None,
        };

        let result = worker.execute(request).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().output, "alice@example.com");
    }
}
