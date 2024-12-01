use super::PersonalSqlConnectionManager;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use bytes::Bytes;
use deno_core::{extension, op2, OpDecl, OpState};
use proven_sql::{Rows, SqlConnection, SqlParam, SqlStore1};

pub struct PersonalSqlParamListManager {
    next_param_list_id: u64,
    personal_param_lists: HashMap<u64, Vec<SqlParam>>,
}

impl PersonalSqlParamListManager {
    pub fn new() -> Self {
        Self {
            next_param_list_id: 0,
            personal_param_lists: HashMap::new(),
        }
    }

    pub fn create_param_list(&mut self) -> u64 {
        let param_list_id = self.next_param_list_id;
        self.next_param_list_id += 1;

        self.personal_param_lists.insert(param_list_id, Vec::new());

        param_list_id
    }

    pub fn finialize_param_list(&mut self, param_list_id: u64) -> Vec<SqlParam> {
        self.personal_param_lists.remove(&param_list_id).unwrap()
    }

    pub fn push_blob_param(&mut self, param_list_id: u64, value: Bytes) {
        let param = SqlParam::Blob(value);
        self.personal_param_lists
            .get_mut(&param_list_id)
            .unwrap()
            .push(param);
    }

    pub fn push_integer_param(&mut self, param_list_id: u64, value: i64) {
        let param = SqlParam::Integer(value);
        self.personal_param_lists
            .get_mut(&param_list_id)
            .unwrap()
            .push(param);
    }

    pub fn push_null_param(&mut self, param_list_id: u64) {
        let param = SqlParam::Null;
        self.personal_param_lists
            .get_mut(&param_list_id)
            .unwrap()
            .push(param);
    }

    pub fn push_real_param(&mut self, param_list_id: u64, value: f64) {
        let param = SqlParam::Real(value);
        self.personal_param_lists
            .get_mut(&param_list_id)
            .unwrap()
            .push(param);
    }

    pub fn push_text_param(&mut self, param_list_id: u64, value: String) {
        let param = SqlParam::Text(value);
        self.personal_param_lists
            .get_mut(&param_list_id)
            .unwrap()
            .push(param);
    }
}

#[op2(fast)]
#[bigint]
fn op_create_personal_params_list(#[state] param_lists: &mut PersonalSqlParamListManager) -> u64 {
    param_lists.create_param_list()
}

#[op2(fast)]
fn op_add_personal_blob_param(
    #[state] param_lists: &mut PersonalSqlParamListManager,
    #[bigint] param_list_id: u64,
    #[buffer(copy)] value: Bytes,
) {
    param_lists.push_blob_param(param_list_id, value);
}

#[op2(fast)]
fn op_add_personal_integer_param(
    #[state] param_lists: &mut PersonalSqlParamListManager,
    #[bigint] param_list_id: u64,
    #[bigint] value: i64,
) {
    param_lists.push_integer_param(param_list_id, value);
}

#[op2(fast)]
fn op_add_personal_null_param(
    #[state] param_lists: &mut PersonalSqlParamListManager,
    #[bigint] param_list_id: u64,
) {
    param_lists.push_null_param(param_list_id);
}

#[op2(fast)]
fn op_add_personal_real_param(
    #[state] param_lists: &mut PersonalSqlParamListManager,
    #[bigint] param_list_id: u64,
    value: f64,
) {
    param_lists.push_real_param(param_list_id, value);
}

#[op2(fast)]
fn op_add_personal_text_param(
    #[state] param_lists: &mut PersonalSqlParamListManager,
    #[bigint] param_list_id: u64,
    #[string] value: String,
) {
    param_lists.push_text_param(param_list_id, value);
}

#[op2(async)]
#[bigint]
pub async fn op_execute_personal_sql<PSS: SqlStore1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] query: String,
    #[bigint] param_list_id_opt: Option<u64>,
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

    if let Some(param_list_id) = param_list_id_opt {
        let mut params_lists = {
            loop {
                let params_lists = {
                    let mut borrowed_state = state.borrow_mut();

                    borrowed_state.try_take::<PersonalSqlParamListManager>()
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

        connection_opt
            .unwrap()
            .execute(query, params)
            .await
            .unwrap()
    } else {
        connection_opt
            .unwrap()
            .execute(query, vec![])
            .await
            .unwrap()
    }
}

#[op2(async)]
#[serde]
pub async fn op_query_personal_sql<PSS: SqlStore1>(
    state: Rc<RefCell<OpState>>,
    #[string] db_name: String,
    #[string] query: String,
    #[bigint] param_list_id_opt: Option<u64>,
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

    if let Some(param_list_id) = param_list_id_opt {
        let mut params_lists = {
            loop {
                let params_lists = {
                    let mut borrowed_state = state.borrow_mut();

                    borrowed_state.try_take::<PersonalSqlParamListManager>()
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

        connection_opt.unwrap().query(query, params).await.unwrap()
    } else {
        connection_opt.unwrap().query(query, vec![]).await.unwrap()
    }
}

fn get_ops<PSS: SqlStore1>() -> Vec<OpDecl> {
    vec![
        op_add_personal_blob_param(),
        op_add_personal_integer_param(),
        op_add_personal_null_param(),
        op_add_personal_real_param(),
        op_add_personal_text_param(),
        op_create_personal_params_list(),
        op_execute_personal_sql::<PSS>(),
        op_query_personal_sql::<PSS>(),
    ]
}

extension!(
    sql_personal_ext,
    parameters = [ PSS: SqlStore1 ],
    ops_fn = get_ops<PSS>,
);
