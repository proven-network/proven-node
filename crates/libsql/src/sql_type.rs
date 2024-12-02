use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlType {
    Migration,
    Mutation,
    Query,
}

impl Display for SqlType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Migration => write!(f, "Migration"),
            Self::Mutation => write!(f, "Mutation"),
            Self::Query => write!(f, "Query"),
        }
    }
}
