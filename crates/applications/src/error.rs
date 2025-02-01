use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Errors passed through from underlying SQL store.
    #[error("sql store error: {0}")]
    SqlStore(String),

    /// Errors passed through from underlying SQL store.
    #[error("kv store error: {0}")]
    Store(String),
}
