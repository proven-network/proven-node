use thiserror::Error;

/// Errors that can occur when managing passkeys.
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Passkey store error.
    #[error("passkey store error: {0}")]
    PasskeyStore(String),
}
