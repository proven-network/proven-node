use crate::Error;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum SqlParam {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    Execute(String, Vec<SqlParam>),
    Query(String, Vec<SqlParam>),
}

impl TryFrom<Bytes> for Request {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref()).map_err(|e| e.into())
    }
}

impl TryInto<Bytes> for Request {
    type Error = Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
