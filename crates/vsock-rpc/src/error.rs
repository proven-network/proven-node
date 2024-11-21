use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    BadResponseType,

    #[from]
    CborSerialize(ciborium::ser::Error<std::io::Error>),

    #[from]
    CborDeserialize(ciborium::de::Error<std::io::Error>),

    #[from]
    Io(std::io::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::BadResponseType => write!(f, "bad response type"),
            Error::CborSerialize(e) => write!(f, "{}", e),
            Error::CborDeserialize(e) => write!(f, "{}", e),
            Error::Io(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
