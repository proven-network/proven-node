use thiserror::Error;

/// Errors that can occur when managing identities.
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Identity store error.
    #[error("identity store error: {0}")]
    IdentityStore(String),
}
