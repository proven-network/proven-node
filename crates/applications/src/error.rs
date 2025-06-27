use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
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
