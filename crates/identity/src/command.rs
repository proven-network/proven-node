use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Commands that can be sent to the identity service to modify identity state.
/// These commands are processed through the command stream and may succeed or fail.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Command {
    /// Create a new identity with a PRF public key.
    /// Fails if the PRF public key is already associated with an identity.
    CreateIdentityWithPrfPublicKey {
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
