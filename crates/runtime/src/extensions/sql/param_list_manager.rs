use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use proven_sql::SqlParam;

pub struct SqlParamListManager {
    param_lists: HashMap<u32, Vec<SqlParam>>,
    next_param_list_id: AtomicU32,
}

impl SqlParamListManager {
    pub fn new() -> Self {
        Self {
            param_lists: HashMap::new(),
            next_param_list_id: AtomicU32::new(0),
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
