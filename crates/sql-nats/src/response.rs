use crate::Error;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Rows {
    pub column_count: u16,
    pub column_names: Vec<String>,
    pub column_types: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    Execute(u64),
    Query(Rows),
}

impl TryFrom<Bytes> for Response {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref()).map_err(|e| e.into())
    }
}

impl TryInto<Bytes> for Response {
    type Error = Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
