#[derive(Debug)]
pub enum Error {
    AlreadyStarted,
    CertRetrieve,
    CertStore,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::AlreadyStarted => write!(f, "The server has already been started"),
            Error::CertRetrieve => write!(f, "Failed to retrieve certificate"),
            Error::CertStore => write!(f, "Failed to store certificate"),
        }
    }
}

impl std::error::Error for Error {}
