use thiserror::Error;

/// Errors that can occur in a SQL stream handler.
#[derive(Debug, Error)]
pub enum Error {
    /// An error occurred in the stream.
    #[error("stream error: {0}")]
    Stream(String),
}
