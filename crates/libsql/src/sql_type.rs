use std::fmt::{Display, Formatter, Result as FmtResult};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SqlType {
    Migration,
    Mutation,
    Query,
}

impl Display for SqlType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::Migration => write!(f, "migration"),
            Self::Mutation => write!(f, "mutation"),
            Self::Query => write!(f, "query"),
        }
    }
}
