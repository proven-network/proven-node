use std::path::PathBuf;
use std::process::ExitStatus;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    BadUtf8(#[from] std::string::FromUtf8Error),

    #[error(transparent)]
    Bootable(Box<dyn std::error::Error + Send + Sync>),

    #[error("eif does not exist: {0:?}")]
    EifDoesNotExist(PathBuf),

    #[error("enclave already running")]
    EnclaveAlreadyRunning,

    #[error("enclave did not shutdown")]
    EnclaveDidNotShutdown,

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

    #[error("no running enclave")]
    NoRunningEnclave,

    #[error("must be root")]
    NotRoot,

    /// Could not set global default subscriber.
    #[error("could not set global default subscriber: {0}")]
    SetTracing(#[from] tracing::dispatcher::SetGlobalDefaultError),

    #[error(transparent)]
    VsockProxy(#[from] proven_vsock_proxy::Error),

    #[error(transparent)]
    VsockRpcCac(#[from] proven_vsock_cac::Error),

    #[error(transparent)]
    LoggerVsock(#[from] proven_vsock_tracing::Error),
}
