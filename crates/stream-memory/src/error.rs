use proven_stream::{StreamError, StreamHandlerError};
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
pub enum Error<HE: StreamHandlerError> {
    /// Handler error.
    #[error("Handler error: {0}")]
    Handler(HE),

    /// Channel send error.
    #[error("Channel send error")]
    Send,

    /// Channel receive error.
    #[error("Channel receive error")]
    Receive,
}

impl<HE: StreamHandlerError> StreamError for Error<HE> {}
