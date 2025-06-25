use thiserror::Error;
use uuid::Uuid;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Application not found
    #[error("application not found: {0}")]
    ApplicationNotFound(Uuid),

    /// Messaging client error
    #[error("messaging client error: {0}")]
    Client(String),

    /// Command processing error
    #[error("command error: {0}")]
    Command(String),

    /// Messaging service error
    #[error("messaging service error: {0}")]
    Service(String),

    /// Stream error
    #[error("stream error: {0}")]
    Stream(String),

    /// Unexpected response type
    #[error("unexpected response type")]
    UnexpectedResponse,
}
