use deno_core::ModuleSpecifier;
use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Channel communication error.
    #[error("channel communication error")]
    ChannelCommunicationError,

    /// Error from eszip.
    #[error("failure handling eszip")]
    CodePackage(String),

    /// Fmt error.
    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    /// Hash not known to pool.
    #[error("hash not known to pool")]
    HashUnknown,

    /// Failed to parse regex pattern.
    #[error(transparent)]
    RegexParse(#[from] regex::Error),

    /// Runtime error.
    #[error(transparent)]
    RuntimeError(#[from] rustyscript::Error),

    /// Serde JSON error.
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// Root not found in graph.
    #[error("root not found in graph")]
    SpecifierNotFoundInCodePackage(ModuleSpecifier),
}
