#[derive(Debug)]
pub enum Error {
    AlreadyStarted,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::AlreadyStarted => write!(f, "The server has already been started"),
        }
    }
}

impl std::error::Error for Error {}
