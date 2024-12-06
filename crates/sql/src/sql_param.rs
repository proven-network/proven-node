use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Represents a parameter for a SQL query
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SqlParam {
    /// Represents a BLOB value
    Blob(Bytes),

    /// Represents an INTEGER value
    Integer(i64),

    /// Represents a NULL value
    Null,

    /// Represents a REAL value
    Real(f64),

    /// Represents a TEXT value
    Text(String),
}
