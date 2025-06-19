use std::sync::Arc;

use thiserror::Error;

/// Errors that can occur when managing sessions.
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Attestation error.
    #[error("attestation error: {0}")]
    Attestation(String),

    /// CBOR deserialization error.
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    /// CBOR serialization error.
    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    /// Identity store error.
    #[error("identity store error: {0}")]
    IdentityStore(String),

    /// Challenge store error.
    #[error("challenge store error: {0}")]
    ChallengeStore(String),

    /// Session store error.
    #[error("session store error: {0}")]
    SessionStore(String),

    /// Signed challenge is invalid.
    #[error("signed challenge is invalid")]
    SignedChallengeInvalid,
}

impl From<ciborium::de::Error<std::io::Error>> for Error {
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborDeserialize(Arc::new(error))
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::CborSerialize(Arc::new(error))
    }
}
