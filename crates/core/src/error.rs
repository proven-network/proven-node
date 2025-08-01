use thiserror::Error;

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

    /// Consensus error.
    #[error("consensus error: {0}")]
    Consensus(String),

    /// Topology error.
    #[error("topology error: {0}")]
    Topology(String),

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
    #[error("runtime error: {0}")]
    Runtime(String),
}
