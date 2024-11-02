use std::cell::RefCell;
use std::rc::Rc;

use proven_store::{Store, Store1};

use deno_core::{extension, op2, OpDecl, OpState};

#[op2(async)]
#[buffer]
pub async fn op_execute_schema_change_sql<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] key: String,
) -> Option<Vec<u8>> {
    let application_store = {
        loop {
            let application_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<AS>()
            };

            match application_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = match application_store
        .scope(format!("{}:bytes", db_name))
        .get(key)
        .await
    {
        Ok(Some(bytes)) => Some(bytes),
        _ => None,
    };

    state.borrow_mut().put(application_store);

    result
}

#[op2(async)]
pub async fn op_execute_mutation_sql<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] key: String,
    #[arraybuffer(copy)] value: Vec<u8>,
) -> bool {
    let application_store = {
        loop {
            let application_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<AS>()
            };

            match application_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = application_store
        .scope(format!("{}:bytes", db_name))
        .put(key, value)
        .await
        .is_ok();

    state.borrow_mut().put(application_store);

    result
}

#[op2(async)]
#[string]
pub async fn op_run_query_sql<AS: Store1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] key: String,
) -> Option<String> {
    let application_store = {
        loop {
            let application_store = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<AS>()
            };

            match application_store {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let result = match application_store
        .scope(format!("{}:string", db_name))
        .get(key)
        .await
    {
        Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).to_string()),
        _ => None,
    };

    state.borrow_mut().put(application_store);

    result
}

extension!(
    sql_application_ext,
    parameters = [ AS: Store1 ],
    ops_fn = get_ops<AS>,
);

fn get_ops<AS: Store1>() -> Vec<OpDecl> {
    let execute_mutation_sql = op_execute_mutation_sql::<AS>();
    let execute_schema_change_sql = op_execute_schema_change_sql::<AS>();
    let run_query_sql = op_run_query_sql::<AS>();

    vec![
        execute_mutation_sql,
        execute_schema_change_sql,
        run_query_sql,
    ]
}
