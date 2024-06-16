use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    #[from]
    Io(std::io::Error),

    #[from]
    ProxyError(proven_vsock_proxy::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{}", e),
            Error::ProxyError(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
