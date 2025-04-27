use thiserror::Error;

/// Errors that can occur during HTTP proxy handling.
#[derive(Debug, Error)]
pub enum Error {
    /// An error occurred during the downstream HTTP request via reqwest.
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
}
