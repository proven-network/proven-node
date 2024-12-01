use std::collections::HashMap;

use bytes::Bytes;
use proven_sql::SqlParam;

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
