use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// An identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Identity {
    /// The identity ID.
    pub identity_id: Uuid,
}
