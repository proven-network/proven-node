use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("bad response type")]
    BadResponseType,

    #[error(transparent)]
    CborDeserialize(#[from] ciborium::de::Error<std::io::Error>),

    #[error(transparent)]
    CborSerialize(#[from] ciborium::ser::Error<std::io::Error>),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
