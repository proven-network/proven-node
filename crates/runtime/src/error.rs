use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Fmt error.
    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    /// Hash not known to pool.
    #[error("hash not known to pool")]
    HashUnknown,

    /// Failed to parse regex pattern.
    #[error(transparent)]
    RegexParse(#[from] regex::Error),

    /// Rustyscript error.
    #[error(transparent)]
    RustyScript(#[from] rustyscript::Error),
}
