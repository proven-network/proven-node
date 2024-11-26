use proven_stream::{StreamError, StreamHandlerError};
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error<HE: StreamHandlerError> {
    #[error("Handler error: {0}")]
    Handler(HE),

    #[error("Channel send error")]
    Send,

    #[error("Channel receive error")]
    Receive,
}

impl<HE: StreamHandlerError> StreamError for Error<HE> {}
