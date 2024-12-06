use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    /// The stream has already started.
    #[error("The stream has already started")]
    AlreadyStarted,
}
