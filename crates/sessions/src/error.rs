use std::sync::Arc;

use proven_store::StoreError;
use thiserror::Error;

pub type Result<T, CSE, SSE> = std::result::Result<T, Error<CSE, SSE>>;

#[derive(Clone, Debug, Error)]
pub enum Error<CSE, SSE> {
    #[error("attestation error")]
    Attestation,

    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    #[error(transparent)]
    ChallengeStore(CSE),

    #[error(transparent)]
    SessionStore(SSE),

    #[error("signed challenge is invalid")]
    SignedChallengeInvalid,
}

impl<CSE: StoreError, SSE: StoreError> From<ciborium::de::Error<std::io::Error>>
    for Error<CSE, SSE>
{
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Error::CborDeserialize(Arc::new(error))
    }
}

impl<CSE: StoreError, SSE: StoreError> From<ciborium::ser::Error<std::io::Error>>
    for Error<CSE, SSE>
{
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Error::CborSerialize(Arc::new(error))
    }
}
