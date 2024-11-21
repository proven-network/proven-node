use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    AddrParse(#[from] std::net::AddrParseError),

    #[error("Server already started")]
    AlreadyStarted,

    #[error(transparent)]
    Async(#[from] tokio::task::JoinError),

    #[error(transparent)]
    Axum(#[from] axum::Error),

    #[error("HTTP server stopped")]
    HttpServerStopped,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Rpc(#[from] crate::rpc::RpcHandlerError),
}
