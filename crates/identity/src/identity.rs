use serde::{Deserialize, Serialize};
use uuid::Uuid;
use webauthn_rs::prelude::Passkey;

/// A session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Identity {
    /// The identity ID.
    pub identity_id: Uuid,

    /// Device passkeys for confirming important interactions.
    pub passkeys: Vec<Passkey>,
}
