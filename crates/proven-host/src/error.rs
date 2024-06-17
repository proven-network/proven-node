use std::path::PathBuf;

use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    EifDoesNotExist(PathBuf),

    #[from]
    Io(std::io::Error),

    #[from]
    VsockProxy(proven_vsock_proxy::Error),

    #[from]
    VsockRpc(proven_vsock_rpc::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::EifDoesNotExist(path) => write!(f, "eif does not exist: {:?}", path),
            Error::Io(e) => write!(f, "{}", e),
            Error::VsockProxy(e) => write!(f, "{}", e),
            Error::VsockRpc(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
