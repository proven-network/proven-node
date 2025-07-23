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
