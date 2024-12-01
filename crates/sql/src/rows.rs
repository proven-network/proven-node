use crate::SqlParam;

use serde::{Deserialize, Serialize};

/// Represents a set of rows returned from a SQL query
#[derive(Debug, Deserialize, Serialize)]
pub struct Rows {
    /// The number of columns in the result set
    pub column_count: u16,

    /// The names of the columns in the result set
    pub column_names: Vec<String>,

    /// The types of the columns in the result set
    pub column_types: Vec<String>,

    /// The rows in the result set
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
    /// Check if the result set is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Creates an iterator over the rows in the result set
    pub fn iter(&self) -> impl Iterator<Item = Row> {
        self.rows.iter().map(|values| Row {
            column_names: &self.column_names,
            values,
        })
    }

    /// Get the row at the given index
    #[must_use]
    pub fn row(&self, idx: usize) -> Option<Row> {
        self.rows.get(idx).map(|values| Row {
            column_names: &self.column_names,
            values,
        })
    }
}
