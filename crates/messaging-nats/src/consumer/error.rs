use std::fmt::Debug;

use proven_messaging::{consumer::ConsumerError, consumer_handler::ConsumerHandlerError};
use thiserror::Error;

/// Errors that can occur in a consumer.
#[derive(Debug, Error)]
pub enum Error<HE>
where
    HE: ConsumerHandlerError,
{
    /// Consumer create error.
    #[error("Failed to create consumer: {0}")]
    Create(async_nats::jetstream::stream::ConsumerErrorKind),

    /// Handler error.
    #[error("Handler error: {0}")]
    Handler(HE),

    /// Consumer messages error.
    #[error("Failed to get consumer messages: {0}")]
    Messages(async_nats::jetstream::consumer::pull::MessagesErrorKind),

    /// Consumer stream error.
    #[error("Consumer stream error: {0}")]
    Stream(async_nats::jetstream::consumer::StreamErrorKind),
}

impl<HE> ConsumerError for Error<HE> where HE: ConsumerHandlerError {}
