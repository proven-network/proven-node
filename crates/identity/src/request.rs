use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Commands that can be sent to the identity service to modify identity state.
/// These commands are processed through the command stream and may succeed or fail.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum IdentityCommand {
    /// Get an existing identity by PRF public key, or create a new one if it doesn't exist.
    GetOrCreateIdentityByPrfPublicKey {
        /// The PRF public key bytes (32 bytes).
        prf_public_key: Bytes,
    },

    /// Link a PRF public key to an existing identity.
    LinkPrfPublicKey {
        /// The unique identifier of the identity.
        identity_id: Uuid,
        /// The PRF public key bytes (32 bytes).
        prf_public_key: Bytes,
    },
}

impl TryFrom<Bytes> for IdentityCommand {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for IdentityCommand {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
