use proven_locks::LockManagerError;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
#[error("error")]
pub struct Error;

impl LockManagerError for Error {}
