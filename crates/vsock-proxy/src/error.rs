use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Async(#[from] tokio::task::JoinError),

    #[error("{0}")]
    Callback(Box<dyn std::error::Error + Sync + Send>),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[cfg(target_os = "linux")]
    #[error(transparent)]
    Tun(#[from] tokio_tun::Error),
}
