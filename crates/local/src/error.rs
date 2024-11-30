use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Applications(#[from] proven_applications::Error<proven_sql_direct::Error>),

    #[error(transparent)]
    Core(#[from] proven_core::Error),
}
