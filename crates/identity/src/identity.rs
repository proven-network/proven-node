use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents an identity.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Identity {
    /// The unique identifier for the identity.
    pub id: Uuid,
}
