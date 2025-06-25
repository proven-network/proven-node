use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents an application.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Application {
    /// The timestamp when the application was created.
    pub created_at: chrono::DateTime<chrono::Utc>,

    /// The unique identifier for the application.
    pub id: Uuid,

    /// The HTTP domains linked to the application.
    pub linked_http_domains: Vec<String>,

    /// The ID of the owner's identity.
    pub owner_id: Uuid,
}
