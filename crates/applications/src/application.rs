use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Application {
    pub application_id: String,
    pub owner_identity_address: String,
    pub dapp_definition_addresses: Vec<String>,
}

impl TryFrom<Bytes> for Application {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref())
    }
}

impl TryInto<Bytes> for Application {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut buffer = Vec::new();
        ciborium::ser::into_writer(&self, &mut buffer)?;
        Ok(Bytes::from(buffer))
    }
}
