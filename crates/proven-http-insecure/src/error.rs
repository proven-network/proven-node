#[derive(Debug)]
pub enum Error {
    AlreadyStarted,
    Bind(std::io::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::AlreadyStarted => write!(f, "The server has already been started"),
            Error::Bind(e) => write!(f, "Failed to bind to address: {}", e),
        }
    }
}

impl std::error::Error for Error {}
