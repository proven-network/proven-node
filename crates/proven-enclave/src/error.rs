use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    #[from]
    Custom(String),

    #[from]
    AddrParse(std::net::AddrParseError),

    #[from]
    Async(tokio::task::JoinError),

    #[from]
    Cac(proven_vsock_cac::Error),

    #[from]
    Cidr(cidr::errors::NetworkParseError),

    #[from]
    Imds(proven_imds::Error),

    #[from]
    Io(std::io::Error),

    #[from]
    Netlink(rtnetlink::Error),

    #[from]
    Proxy(proven_vsock_proxy::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::Custom(e) => write!(f, "{}", e),
            Error::AddrParse(e) => write!(f, "{}", e),
            Error::Async(e) => write!(f, "{}", e),
            Error::Cac(e) => write!(f, "{}", e),
            Error::Cidr(e) => write!(f, "{}", e),
            Error::Imds(e) => write!(f, "{}", e),
            Error::Io(e) => write!(f, "{}", e),
            Error::Netlink(e) => write!(f, "{}", e),
            Error::Proxy(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
