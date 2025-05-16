use std::fmt::Debug;

use proven_bootable::BootableError;
use proven_messaging::consumer::ConsumerError;
use thiserror::Error;

/// Errors that can occur in a consumer.
#[derive(Debug, Error)]
pub enum Error {
    /// Already running.
    #[error("Already running")]
    AlreadyRunning,

    /// Consumer create error.
    #[error("Failed to create consumer: {0}")]
    Create(async_nats::jetstream::stream::ConsumerErrorKind),

    /// Consumer info error.
    #[error("Failed to get consumer info: {0}")]
    Info(async_nats::jetstream::context::ConsumerInfoErrorKind),

    /// Handler error.
    #[error("Handler error")]
    Handler,

    /// Consumer messages error.
    #[error("Failed to get consumer messages: {0}")]
    Messages(async_nats::jetstream::consumer::pull::MessagesErrorKind),

    /// Consumer stream error.
    #[error("Consumer stream error: {0}")]
    Stream(async_nats::jetstream::consumer::StreamErrorKind),
}

impl BootableError for Error {}
impl ConsumerError for Error {}
