use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents an identity.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Identity {
    /// The unique identifier for the identity.
    pub id: Uuid,
}

impl TryFrom<Bytes> for Identity {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for Identity {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
