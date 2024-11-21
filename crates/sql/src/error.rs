use derive_more::From;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, From)]
pub enum Error {
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),
    Libsql(Arc<libsql::Error>),

    #[from]
    Utf8(std::string::FromUtf8Error),
}

impl From<libsql::Error> for Error {
    fn from(error: libsql::Error) -> Self {
        Error::Libsql(Arc::new(error))
    }
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

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::CborSerialize(e) => write!(f, "CBOR serialization error: {}", e),
            Error::CborDeserialize(e) => write!(f, "CBOR deserialization error: {}", e),
            Error::Libsql(e) => write!(f, "Libsql error: {}", e),
            Error::Utf8(e) => write!(f, "Utf8 error: {}", e),
        }
    }
}

impl std::error::Error for Error {}
