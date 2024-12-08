use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    /// The account addresses.
    pub account_addresses: Vec<String>,

    /// The dApp definition address.
    pub dapp_definition_address: String,

    /// The expected origin of future requests.
    pub expected_origin: String,

    /// The identity address.
    pub identity_address: String,

    /// The session ID.
    pub session_id: String,

    /// The node signing key.
    pub signing_key: Vec<u8>,

    /// The node verifying key.
    pub verifying_key: Vec<u8>,
}

impl TryFrom<Bytes> for Session {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for Session {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
