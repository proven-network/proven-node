use proven_sql::SqlStoreError;
use thiserror::Error;

/// The result type for this crate.
pub type Result<T, SE> = std::result::Result<T, Error<SE>>;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
pub enum Error<SE>
where
    SE: SqlStoreError,
{
    /// Errors passed through from underlying SQL store.
    #[error(transparent)]
    SqlStore(SE),
}
