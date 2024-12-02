use proven_http::HttpServerError;
use thiserror::Error;

/// The result type for this crate.
pub type Result<T, HE> = std::result::Result<T, Error<HE>>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error<HE>
where
    HE: HttpServerError,
{
    /// Can't parse address.
    #[error(transparent)]
    AddrParse(std::net::AddrParseError),

    /// Core already started.
    #[error("Server already started")]
    AlreadyStarted,

    /// Tokio error.
    #[error(transparent)]
    Async(tokio::task::JoinError),

    /// Axum error.
    #[error(transparent)]
    Axum(axum::Error),

    /// HTTP error.
    #[error(transparent)]
    Http(axum::http::Error),

    /// HTTP server error.
    #[error(transparent)]
    HttpServer(HE),

    /// Http server not running.
    #[error("HTTP server stopped")]
    HttpServerStopped,

    /// IO error.
    #[error(transparent)]
    Io(std::io::Error),

    /// RPC error.
    #[error(transparent)]
    Rpc(#[from] crate::rpc::RpcHandlerError),
}

impl<HE> From<HE> for Error<HE>
where
    HE: HttpServerError,
{
    fn from(err: HE) -> Self {
        Self::HttpServer(err)
    }
}
