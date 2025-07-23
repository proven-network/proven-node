use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// An identity in the system.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Identity {
    /// The unique identifier of the identity.
    pub id: Uuid,
}

impl Identity {
    /// Create a new identity.
    #[must_use]
    pub const fn new(id: Uuid) -> Self {
        Self { id }
    }
}
