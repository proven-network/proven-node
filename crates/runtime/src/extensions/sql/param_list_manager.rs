#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use deno_core::op2;
use proven_sql::SqlParam;

pub struct SqlParamListManager {
    next_param_list_id: AtomicU32,
    param_lists: HashMap<u32, Vec<SqlParam>>,
}

impl SqlParamListManager {
    pub fn new() -> Self {
        Self {
            next_param_list_id: AtomicU32::new(0),
            param_lists: HashMap::new(),
        }
    }

    pub fn create_param_list(&mut self) -> u32 {
        let param_list_id = self.next_param_list_id.fetch_add(1, Ordering::Relaxed);

        self.param_lists.insert(param_list_id, Vec::new());

        param_list_id
    }

    pub fn finialize_param_list(&mut self, param_list_id: u32) -> Vec<SqlParam> {
        self.param_lists.remove(&param_list_id).unwrap()
    }

    pub fn push_blob_param(&mut self, param_list_id: u32, value: Bytes) {
        let param = SqlParam::Blob(value);
        self.param_lists
            .get_mut(&param_list_id)
            .unwrap()
            .push(param);
    }

    pub fn push_integer_param(&mut self, param_list_id: u32, value: i64) {
        let param = SqlParam::Integer(value);
        self.param_lists
            .get_mut(&param_list_id)
            .unwrap()
            .push(param);
    }

    pub fn push_null_param(&mut self, param_list_id: u32) {
        let param = SqlParam::Null;
        self.param_lists
            .get_mut(&param_list_id)
            .unwrap()
            .push(param);
    }

    pub fn push_real_param(&mut self, param_list_id: u32, value: f64) {
        let param = SqlParam::Real(value);
        self.param_lists
            .get_mut(&param_list_id)
            .unwrap()
            .push(param);
    }

    pub fn push_text_param(&mut self, param_list_id: u32, value: String) {
        let param = SqlParam::Text(value);
        self.param_lists
            .get_mut(&param_list_id)
            .unwrap()
            .push(param);
    }
}

#[op2(fast)]
pub fn op_create_params_list(#[state] param_lists: &mut SqlParamListManager) -> u32 {
    param_lists.create_param_list()
}

#[op2(fast)]
pub fn op_add_blob_param(
    #[state] param_lists: &mut SqlParamListManager,
    param_list_id: u32,
    #[buffer(copy)] value: Bytes,
) {
    param_lists.push_blob_param(param_list_id, value);
}

#[op2(fast)]
pub fn op_add_integer_param(
    #[state] param_lists: &mut SqlParamListManager,
    param_list_id: u32,
    #[bigint] value: i64,
) {
    param_lists.push_integer_param(param_list_id, value);
}

#[op2(fast)]
pub fn op_add_null_param(#[state] param_lists: &mut SqlParamListManager, param_list_id: u32) {
    param_lists.push_null_param(param_list_id);
}

#[op2(fast)]
pub fn op_add_real_param(
    #[state] param_lists: &mut SqlParamListManager,
    param_list_id: u32,
    value: f64,
) {
    param_lists.push_real_param(param_list_id, value);
}

#[op2(fast)]
pub fn op_add_text_param(
    #[state] param_lists: &mut SqlParamListManager,
    param_list_id: u32,
    #[string] value: String,
) {
    param_lists.push_text_param(param_list_id, value);
}
