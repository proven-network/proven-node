use std::fmt::{Display, Formatter, Result as FmtResult};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StatementType {
    Migration,
    Mutation,
    Query,
    Unknown,
}

impl Display for StatementType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Migration => write!(f, "migration"),
            Self::Mutation => write!(f, "mutation"),
            Self::Query => write!(f, "query"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}
