use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Represents an application.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Application {
    /// The unique identifier for the application.
    pub id: String,

    /// The address of the owner's identity.
    pub owner_identity_address: String,

    /// The addresses of the dApp definitions.
    pub dapp_definition_addresses: Vec<String>,
}

impl TryFrom<Bytes> for Application {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for Application {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
