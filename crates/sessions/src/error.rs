use std::sync::Arc;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub enum Error {
    Attestation,
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),
    ChallengeStore,
    SessionStore,
    SignedChallengeInvalid,
}

impl From<ciborium::de::Error<std::io::Error>> for Error {
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Error::CborDeserialize(Arc::new(error))
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Error::CborSerialize(Arc::new(error))
    }
}
