use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    #[from]
    Async(tokio::task::JoinError),

    #[from]
    Io(std::io::Error),

    #[from]
    SetTracing(tracing::dispatcher::SetGlobalDefaultError),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::Async(e) => write!(f, "{}", e),
            Error::Io(e) => write!(f, "{}", e),
            Error::SetTracing(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
