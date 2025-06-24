use thiserror::Error;

/// Errors that can occur when working with sessions.
#[derive(Debug, Error)]
pub enum Error {
    /// An error occurred while working with the attestor.
    #[error("Attestation error: {0}")]
    Attestation(String),

    /// An error occurred while working with the session store.
    #[error("Session store error: {0}")]
    SessionStore(String),
}
