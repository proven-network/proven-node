use std::fmt::Debug;

use thiserror::Error;

/// Error type for memory stream subscription handlers.
#[derive(Debug, Error)]
pub enum Error {
    /// An error occurred while sending data.
    #[error("An error occurred while sending data")]
    Send,
}
