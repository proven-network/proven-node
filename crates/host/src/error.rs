use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("eif does not exist: {0:?}")]
    EifDoesNotExist(PathBuf),

    #[error(transparent)]
    Http(#[from] proven_http_insecure::Error),

    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    #[error("must be root")]
    NotRoot,

    #[error(transparent)]
    TracingService(#[from] super::vsock_tracing::TracingServiceError),

    #[error(transparent)]
    VsockProxy(#[from] proven_vsock_proxy::Error),

    #[error(transparent)]
    VsockRpc(#[from] proven_vsock_rpc::Error),
}
