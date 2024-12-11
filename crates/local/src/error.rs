use proven_http_insecure::Error as HttpError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Applications(#[from] proven_sql_direct::Error),

    #[error(transparent)]
    Core(#[from] proven_core::Error<HttpError>),

    /// Could not set global default subscriber.
    #[error("could not set global default subscriber: {0}")]
    SetTracing(#[from] tracing::dispatcher::SetGlobalDefaultError),
}
