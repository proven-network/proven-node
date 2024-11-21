use crate::Error;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub signing_key: Vec<u8>,
    pub verifying_key: Vec<u8>,
    pub dapp_definition_address: String,
    pub expected_origin: String,
    pub identity_address: String,
    pub account_addresses: Vec<String>,
}

impl TryFrom<Bytes> for Session {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref()).map_err(|e| e.into())
    }
}

impl TryInto<Bytes> for Session {
    type Error = Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
