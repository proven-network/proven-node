#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

mod connection_manager;

pub use connection_manager::NftSqlConnectionManager;

use std::cell::RefCell;
use std::rc::Rc;

use deno_core::{extension, op2, OpDecl, OpState};
use proven_sql::{Rows, SqlConnection, SqlParam, SqlStore2};

#[op2(async)]
#[bigint]
pub async fn op_execute_nft_sql<NSS: SqlStore2>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] nft_id: String,
    #[string] query: String,
    #[serde] params: Vec<SqlParam>,
) -> u64 {
    let connection_manager = {
        loop {
            let connection_manager = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<NftSqlConnectionManager<NSS>>()
            };

            match connection_manager {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    // TODO: handle errors properly
    let connection = connection_manager.connect(db_name, nft_id).await.unwrap();
    state.borrow_mut().put(connection_manager);

    connection.execute(query, params).await.unwrap()
}

#[op2(async)]
#[serde]
pub async fn op_query_nft_sql<NSS: SqlStore2>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] nft_id: String,
    #[string] query: String,
    #[serde] params: Vec<SqlParam>,
) -> Rows {
    let connection_manager = {
        loop {
            let connection_manager = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<NftSqlConnectionManager<NSS>>()
            };

            match connection_manager {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    // TODO: handle errors properly
    let connection = connection_manager.connect(db_name, nft_id).await.unwrap();
    state.borrow_mut().put(connection_manager);

    connection.query(query, params).await.unwrap()
}

fn get_ops<NSS: SqlStore2>() -> Vec<OpDecl> {
    let execute_nft_sql = op_execute_nft_sql::<NSS>();
    let query_nft_sql = op_query_nft_sql::<NSS>();

    vec![execute_nft_sql, query_nft_sql]
}

extension!(
    sql_nft_ext,
    parameters = [ NSS: SqlStore2 ],
    ops_fn = get_ops<NSS>,
);
