use proven_sql::SqlStoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error<SE>
where
    SE: SqlStoreError,
{
    /// Errors passed through from underlying SQL store.
    #[error(transparent)]
    SqlStore(SE),
}
