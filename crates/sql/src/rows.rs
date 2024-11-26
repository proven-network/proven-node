use crate::SqlParam;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Rows {
    pub column_count: u16,
    pub column_names: Vec<String>,
    pub column_types: Vec<String>,
    pub rows: Vec<Vec<SqlParam>>,
}

#[derive(Debug)]
pub struct Row<'a> {
    column_names: &'a [String],
    values: &'a [SqlParam],
}

impl Row<'_> {
    pub fn get_by_name(&self, name: &str) -> Option<&SqlParam> {
        self.column_names
            .iter()
            .position(|n| n == name)
            .and_then(|idx| self.values.get(idx))
    }

    pub fn get_text(&self, idx: usize) -> Option<&str> {
        match self.values.get(idx)? {
            SqlParam::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn get_text_by_name(&self, name: &str) -> Option<&str> {
        match self.get_by_name(name)? {
            SqlParam::Text(s) => Some(s),
            _ => None,
        }
    }
}

impl Rows {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = Row> {
        self.rows.iter().map(|values| Row {
            column_names: &self.column_names,
            values,
        })
    }

    pub fn row(&self, idx: usize) -> Option<Row> {
        self.rows.get(idx).map(|values| Row {
            column_names: &self.column_names,
            values,
        })
    }
}
