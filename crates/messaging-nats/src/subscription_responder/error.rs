use std::fmt::Debug;

use proven_messaging::subscription_responder::SubscriptionResponderError;
use thiserror::Error;

/// Errors that can occur in a consumer.
#[derive(Debug, Error)]
pub enum Error {
    /// Consumer create error.
    #[error("Failed to create consumer: {0}")]
    Create(async_nats::jetstream::stream::ConsumerErrorKind),

    /// Consumer info error.
    #[error("Failed to get consumer info: {0}")]
    Info(async_nats::jetstream::context::RequestErrorKind),

    /// Consumer messages error.
    #[error("Failed to get consumer messages: {0}")]
    Messages(async_nats::jetstream::consumer::pull::MessagesErrorKind),

    /// Consumer stream error.
    #[error("Consumer stream error: {0}")]
    Stream(async_nats::jetstream::consumer::StreamErrorKind),
}

impl SubscriptionResponderError for Error {}
