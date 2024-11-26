use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Clone, Debug, PartialEq)]
pub enum SqlType {
    Migration,
    Mutation,
    Query,
}

impl Display for SqlType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            SqlType::Migration => write!(f, "Migration"),
            SqlType::Mutation => write!(f, "Mutation"),
            SqlType::Query => write!(f, "Query"),
        }
    }
}
