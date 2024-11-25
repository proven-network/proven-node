use crate::SqlParam;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Rows {
    pub column_count: u16,
    pub column_names: Vec<String>,
    pub column_types: Vec<String>,
    pub rows: Vec<Vec<SqlParam>>,
}
