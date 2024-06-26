use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    AlreadyStarted,

    #[from]
    Custom(String),

    #[from]
    AddrParse(std::net::AddrParseError),

    #[from]
    Async(tokio::task::JoinError),

    #[from]
    Axum(axum::Error),

    #[from]
    Io(std::io::Error),

    #[from]
    Rpc(crate::rpc::RpcHandlerError), // shouldn't actually be here; handle in transport
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::AlreadyStarted => write!(f, "Server already started"),
            Error::Custom(e) => write!(f, "{}", e),
            Error::AddrParse(e) => write!(f, "{}", e),
            Error::Async(e) => write!(f, "{}", e),
            Error::Axum(e) => write!(f, "{}", e),
            Error::Io(e) => write!(f, "{}", e),
            Error::Rpc(e) => write!(f, "{:?}", e),
        }
    }
}

impl std::error::Error for Error {}
