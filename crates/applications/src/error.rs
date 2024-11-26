use proven_sql::SqlStoreError;
use thiserror::Error;

pub type Result<T, SE> = std::result::Result<T, Error<SE>>;

#[derive(Clone, Debug, Error)]
pub enum Error<SE>
where
    SE: SqlStoreError,
{
    #[error(transparent)]
    Store(#[from] SE),
}
