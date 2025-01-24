use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Error from eszip.
    #[error("failure handling eszip")]
    CodePackage(String),

    /// Fmt error.
    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    /// Hash not known to pool.
    #[error("hash not known to pool")]
    HashUnknown,

    /// Execution request type doesn't match handler type.
    #[error("execution request type doesn't match handler type")]
    MismatchedExecutionRequest,

    /// Failed to parse regex pattern.
    #[error(transparent)]
    RegexParse(#[from] regex::Error),

    /// Root not found in graph.
    #[error("root not found in graph")]
    SpecifierNotFoundInCodePackage,

    /// Rustyscript error.
    #[error(transparent)]
    RustyScript(#[from] rustyscript::Error),

    /// Serde JSON error.
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}
