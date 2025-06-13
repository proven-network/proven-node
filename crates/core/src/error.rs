use proven_bootable::BootableError;
use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
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

    /// Code package error.
    #[error(transparent)]
    CodePackage(#[from] proven_code_package::Error),

    /// Governance error.
    #[error("governance error: {0}")]
    Governance(String),

    /// HTTP error.
    #[error(transparent)]
    Http(axum::http::Error),

    /// HTTP server error.
    #[error("http server error: {0}")]
    HttpServer(String),

    /// IO error.
    #[error(transparent)]
    Io(std::io::Error),

    /// Network error.
    #[error("network error: {0}")]
    Network(String),

    /// Overlapping routes error.
    #[error("overlapping routes detected: {0} and {1}")]
    OverlappingRoutes(String, String),

    /// Runtime error.
    #[error(transparent)]
    Runtime(#[from] proven_runtime::Error),
}

impl BootableError for Error {}
