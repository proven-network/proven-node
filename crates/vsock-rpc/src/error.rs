use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    BadResponseType,
    Cbor,

    #[from]
    Io(std::io::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::BadResponseType => write!(f, "bad response type"),
            Error::Cbor => write!(f, "CBOR error"),
            Error::Io(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
