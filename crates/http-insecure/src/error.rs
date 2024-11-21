use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("The server has already been started")]
    AlreadyStarted,

    #[error("Failed to bind to address: {0}")]
    Bind(#[from] std::io::Error),
}
