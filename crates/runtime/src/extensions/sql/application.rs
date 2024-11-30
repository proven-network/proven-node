use std::cell::RefCell;
use std::rc::Rc;

use deno_core::{extension, op2, OpDecl, OpState};
use proven_sql::{Rows, SqlConnection, SqlParam, SqlStore, SqlStore1};

#[op2(async)]
#[bigint]
pub async fn op_execute_application_sql<ASS: SqlStore1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] query: String,
    #[serde] params: Vec<SqlParam>,
) -> u64 {
    let application_sql_store = {
        loop {
            let application_sql_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<ASS>()
            };

            match application_sql_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = application_sql_store
        .scope(db_name)
        .connect()
        .await
        .unwrap() // TODO: handle properly
        .execute(query, params)
        .await
        .unwrap();

    state.borrow_mut().put(application_sql_store);

    result
}

#[op2(async)]
#[serde]
pub async fn op_query_application_sql<ASS: SqlStore1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] query: String,
    #[serde] params: Vec<SqlParam>,
) -> Rows {
    let application_sql_store = {
        loop {
            let application_sql_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<ASS>()
            };

            match application_sql_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = application_sql_store
        .scope(db_name)
        .connect()
        .await
        .unwrap() // TODO: handle properly
        .query(query, params)
        .await
        .unwrap();

    state.borrow_mut().put(application_sql_store);

    result
}

fn get_ops<ASS: SqlStore1>() -> Vec<OpDecl> {
    let execute_application_sql = op_execute_application_sql::<ASS>();
    let query_application_sql = op_query_application_sql::<ASS>();

    vec![execute_application_sql, query_application_sql]
}

extension!(
    sql_application_ext,
    parameters = [ ASS: SqlStore1 ],
    ops_fn = get_ops<ASS>,
);
