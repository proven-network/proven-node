use proven_sql::SqlStoreError;
use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error<SE, SSE>
where
    SE: StoreError,
    SSE: SqlStoreError,
{
    /// Errors passed through from underlying SQL store.
    #[error(transparent)]
    SqlStore(SSE),

    /// Errors passed through from underlying SQL store.
    #[error(transparent)]
    Store(SE),
}
