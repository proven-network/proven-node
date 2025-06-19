use thiserror::Error;

/// Errors that can occur when managing sessions.
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Attestation error.
    #[error("attestation error: {0}")]
    Attestation(String),

    /// Identity store error.
    #[error("identity store error: {0}")]
    IdentityStore(String),

    /// Challenge store error.
    #[error("passkey store error: {0}")]
    PasskeyStore(String),

    /// Session store error.
    #[error("session store error: {0}")]
    SessionStore(String),
}
