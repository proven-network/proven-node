use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Events that represent state changes in the identity lifecycle.
/// These events are published to the event stream and consumed by various subsystems.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Event {
    /// An identity was successfully created.
    Created {
        /// The unique identifier of the created identity.
        identity_id: Uuid,

        /// The timestamp when the identity was created.
        created_at: chrono::DateTime<chrono::Utc>,
    },

    /// A PRF public key was linked to an identity.
    PrfPublicKeyLinked {
        /// The unique identifier of the identity.
        identity_id: Uuid,

        /// The PRF public key bytes (32 bytes).
        prf_public_key: Bytes,

        /// The timestamp when the PRF public key was linked.
        linked_at: chrono::DateTime<chrono::Utc>,
    },
}

impl TryFrom<Bytes> for Event {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for Event {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
