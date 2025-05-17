use proven_locks::LockManagerError;
use thiserror::Error;

/// Error type for the In-Memory Lock Manager.
#[derive(Clone, Debug, Error)]
pub enum Error {
    // No errors possible for in-memory version
}

impl LockManagerError for Error {}
