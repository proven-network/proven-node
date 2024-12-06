use std::sync::Arc;

use proven_attestation::AttestorError;
use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur when managing sessions.
#[derive(Clone, Debug, Error)]
pub enum Error<AE, CSE, SSE>
where
    AE: AttestorError,
    CSE: StoreError,
    SSE: StoreError,
{
    /// Attestation error.
    #[error(transparent)]
    Attestation(AE),

    /// CBOR deserialization error.
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    /// CBOR serialization error.
    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    /// Challenge store error.
    #[error(transparent)]
    ChallengeStore(CSE),

    /// Session store error.
    #[error(transparent)]
    SessionStore(SSE),

    /// Signed challenge is invalid.
    #[error("signed challenge is invalid")]
    SignedChallengeInvalid,
}

impl<AE, CSE, SSE> From<ciborium::de::Error<std::io::Error>> for Error<AE, CSE, SSE>
where
    AE: AttestorError,
    CSE: StoreError,
    SSE: StoreError,
{
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborDeserialize(Arc::new(error))
    }
}

impl<AE, CSE, SSE> From<ciborium::ser::Error<std::io::Error>> for Error<AE, CSE, SSE>
where
    AE: AttestorError,
    CSE: StoreError,
    SSE: StoreError,
{
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::CborSerialize(Arc::new(error))
    }
}
