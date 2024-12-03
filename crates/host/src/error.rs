use std::path::PathBuf;
use std::process::ExitStatus;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    BadUtf8(#[from] std::string::FromUtf8Error),

    #[error("eif does not exist: {0:?}")]
    EifDoesNotExist(PathBuf),

    #[error(transparent)]
    Http(#[from] proven_http_insecure::Error),

    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// JSON decode error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("{0} unexpectedly exited with non-zero code: {1}")]
    NonZeroExit(&'static str, ExitStatus),

    #[error("must be root")]
    NotRoot,

    #[error(transparent)]
    VsockProxy(#[from] proven_vsock_proxy::Error),

    #[error(transparent)]
    VsockRpc(#[from] proven_vsock_rpc::Error),

    #[error(transparent)]
    VsockTracing(#[from] proven_vsock_tracing::host::Error),
}
