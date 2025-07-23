use serde::{Deserialize, Serialize};

use crate::Identity;

/// Responses that can be sent from the identity service.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Response {
    /// An error occurred while processing the command.
    Error {
        /// The error message.
        message: String,
    },

    /// An identity was created with a PRF public key.
    IdentityCreated {
        /// The newly created identity.
        identity: Identity,

        /// The last event sequence processed.
        last_event_seq: u64,
    },

    /// An internal error occurred.
    InternalError {
        /// The error message.
        message: String,
    },

    /// A PRF public key was linked to an identity.
    PrfPublicKeyLinked {
        /// The last event sequence processed.
        last_event_seq: u64,
    },
}
