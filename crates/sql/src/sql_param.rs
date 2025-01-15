use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Represents a parameter for a SQL query
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SqlParam {
    /// Represents a BLOB value
    Blob(Bytes),

    /// Represents a BLOB value with column name
    BlobWithName(String, Bytes),

    /// Represents an INTEGER value
    Integer(i64),

    /// Represents an INTEGER value with column name
    IntegerWithName(String, i64),

    /// Represents a NULL value
    Null,

    /// Represents a NULL value with column name
    NullWithName(String),

    /// Represents a REAL value
    Real(f64),

    /// Represents a REAL value with column name
    RealWithName(String, f64),

    /// Represents a TEXT value
    Text(String),

    /// Represents a TEXT value with column name
    TextWithName(String, String),
}
