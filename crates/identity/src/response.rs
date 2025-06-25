use crate::Identity;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Responses returned by the identity service after processing commands.
/// These indicate the result of command execution.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum IdentityCommandResponse {
    /// Command failed with an error message.
    Error {
        /// Description of what went wrong.
        message: String,
    },

    /// An identity was successfully retrieved or created.
    IdentityRetrieved {
        /// The retrieved or created identity.
        identity: Identity,
    },

    /// PRF public key was successfully linked to an identity.
    PrfPublicKeyLinked,
}

impl TryFrom<Bytes> for IdentityCommandResponse {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, <Self as TryFrom<Bytes>>::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for IdentityCommandResponse {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, <Self as TryInto<Bytes>>::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
