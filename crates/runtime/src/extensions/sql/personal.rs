use super::PersonalSqlConnectionManager;

use std::cell::RefCell;
use std::rc::Rc;

use deno_core::{extension, op2, OpDecl, OpState};
use proven_sql::{Rows, SqlConnection, SqlParam, SqlStore1};

#[op2(async)]
#[bigint]
pub async fn op_execute_personal_sql<PSS: SqlStore1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] query: String,
    #[serde] params: Vec<SqlParam>,
) -> u64 {
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

    // TODO: should probably err instead of returning None
    let connection_opt = if let Some(connection_manager) = connection_manager_opt.as_ref() {
        Some(connection_manager.connect(db_name).await.unwrap())
    } else {
        None
    };
    state.borrow_mut().put(connection_manager_opt);

    connection_opt
        .unwrap()
        .execute(query, params)
        .await
        .unwrap()
}

#[op2(async)]
#[serde]
pub async fn op_query_personal_sql<PSS: SqlStore1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] query: String,
    #[serde] params: Vec<SqlParam>,
) -> Rows {
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

    // TODO: should probably err instead of returning None
    let connection_opt = if let Some(connection_manager) = connection_manager_opt.as_ref() {
        Some(connection_manager.connect(db_name).await.unwrap())
    } else {
        None
    };
    state.borrow_mut().put(connection_manager_opt);

    connection_opt.unwrap().query(query, params).await.unwrap()
}

fn get_ops<PSS: SqlStore1>() -> Vec<OpDecl> {
    let execute_personal_sql = op_execute_personal_sql::<PSS>();
    let query_personal_sql = op_query_personal_sql::<PSS>();

    vec![execute_personal_sql, query_personal_sql]
}

extension!(
    sql_personal_ext,
    parameters = [ PSS: SqlStore1 ],
    ops_fn = get_ops<PSS>,
);
