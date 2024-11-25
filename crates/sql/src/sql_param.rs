use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub enum SqlParam {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Bytes),
}
