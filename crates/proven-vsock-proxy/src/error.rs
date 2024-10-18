use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    #[from]
    Async(tokio::task::JoinError),

    Callback(Box<dyn std::error::Error + Sync + Send>),

    #[from]
    Io(std::io::Error),

    #[cfg(target_os = "linux")]
    #[from]
    Tun(tokio_tun::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::Async(e) => write!(f, "{}", e),
            Error::Callback(e) => write!(f, "{}", e),
            Error::Io(e) => write!(f, "{}", e),
            #[cfg(target_os = "linux")]
            Error::Tun(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
