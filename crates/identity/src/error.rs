use thiserror::Error;

/// Errors that can occur when managing identities and passkeys.
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Identity store error.
    #[error("identity store error: {0}")]
    IdentityStore(String),

    /// Passkey store error.
    #[error("passkey store error: {0}")]
    PasskeyStore(String),
}
