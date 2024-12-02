use std::sync::Arc;

use proven_attestation::Attestor;
use proven_store::Store1;
use thiserror::Error;

/// Errors that can occur when managing sessions.
#[derive(Clone, Debug, Error)]
pub enum Error<A, CS, SS>
where
    A: Attestor,
    CS: Store1,
    SS: Store1,
{
    /// Attestation error.
    #[error(transparent)]
    Attestation(A::Error),

    /// CBOR deserialization error.
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    /// CBOR serialization error.
    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    /// Challenge store error.
    #[error(transparent)]
    ChallengeStore(CS::Error),

    /// Session store error.
    #[error(transparent)]
    SessionStore(SS::Error),

    /// Signed challenge is invalid.
    #[error("signed challenge is invalid")]
    SignedChallengeInvalid,
}

impl<A, CS, SS> From<ciborium::de::Error<std::io::Error>> for Error<A, CS, SS>
where
    A: Attestor,
    CS: Store1,
    SS: Store1,
{
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborDeserialize(Arc::new(error))
    }
}

impl<A, CS, SS> From<ciborium::ser::Error<std::io::Error>> for Error<A, CS, SS>
where
    A: Attestor,
    CS: Store1,
    SS: Store1,
{
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::CborSerialize(Arc::new(error))
    }
}
