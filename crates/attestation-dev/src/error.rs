use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    Cbor,

    #[from]
    Cose(coset::CoseError),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::Cbor => write!(f, "CBOR error"),
            Error::Cose(e) => write!(f, "COSE error: {}", e),
        }
    }
}

impl std::error::Error for Error {}
