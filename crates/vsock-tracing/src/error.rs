use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Async(#[from] tokio::task::JoinError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    SetTracing(#[from] tracing::dispatcher::SetGlobalDefaultError),
}
