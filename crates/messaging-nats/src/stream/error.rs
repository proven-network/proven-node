use std::fmt::Debug;

use proven_messaging::stream::StreamError;
use std::error::Error as StdError;
use thiserror::Error;

/// Error type for memory stream operations.
#[derive(Debug, Error)]
pub enum Error<DE, SE>
where
    DE: Debug + Send + StdError + Sync + 'static,
    SE: Debug + Send + StdError + Sync + 'static,
{
    /// Deserialization error.
    #[error(transparent)]
    Deserialize(DE),

    /// Direct get error.
    #[error("Failed to get message: {0}")]
    DirectGet(async_nats::jetstream::stream::DirectGetErrorKind),

    /// Stream info error.
    #[error("Failed to get stream info: {0}")]
    Info(async_nats::jetstream::context::RequestErrorKind),

    /// Publish error.
    #[error("Failed to publish: {0}")]
    Publish(async_nats::jetstream::context::PublishErrorKind),

    /// Serialization error.
    #[error(transparent)]
    Serialize(SE),

    /// An error occured while subscribing to a subject.
    #[error(transparent)]
    Subject(#[from] crate::subject::Error<DE, SE>),
}

impl<DE, SE> StreamError for Error<DE, SE>
where
    DE: Debug + Send + StdError + Sync + 'static,
    SE: Debug + Send + StdError + Sync + 'static,
{
}
