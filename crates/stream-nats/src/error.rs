use std::fmt::Debug;
use std::sync::Arc;

use proven_stream::{StreamError, StreamHandler};
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
pub enum Error<H>
where
    H: StreamHandler,
{
    /// Acknowledgment failed.
    #[error("Consumer acknowledgment failed")]
    ConsumerAck,

    /// Consumer create error.
    #[error("Failed to create consumer: {0}")]
    ConsumerCreate(async_nats::jetstream::stream::ConsumerErrorKind),

    /// Consumer info error.
    #[error("Failed to get consumer info: {0}")]
    ConsumerInfo(async_nats::jetstream::context::RequestErrorKind),

    /// Consumer messages error.
    #[error("Failed to get consumer messages: {0}")]
    ConsumerMessages(async_nats::jetstream::consumer::pull::MessagesErrorKind),

    /// Consumer stream error.
    #[error("Consumer stream error: {0}")]
    ConsumerStream(async_nats::jetstream::consumer::StreamErrorKind),

    /// Direct get error.
    #[error("Failed to get message: {0}")]
    DirectGet(async_nats::jetstream::stream::DirectGetErrorKind),

    /// Handler error.
    #[error("Handler error: {0}")]
    Handler(H::Error),

    /// No message info available.
    #[error("No message info available")]
    NoInfo,

    /// Publish error.
    #[error("Failed to publish: {0}")]
    Publish(async_nats::client::PublishErrorKind),

    /// Reply delete error.
    #[error("Failed to delete reply message: {0}")]
    ReplyDelete(async_nats::jetstream::stream::DeleteMessageErrorKind),

    /// Reply direct get error.
    #[error("Failed to get reply message: {0}")]
    ReplyDirectGet(async_nats::jetstream::stream::DirectGetErrorKind),

    /// Reply publish error.
    #[error("Failed to publish reply: {0}")]
    ReplyPublish(async_nats::client::PublishErrorKind),

    /// Request error.
    #[error("Request failed: {0}")]
    Request(async_nats::client::RequestErrorKind),

    /// Request delete error.
    #[error("Failed to delete request message: {0}")]
    RequestDelete(async_nats::jetstream::stream::DeleteMessageErrorKind),

    /// JSON deserialization error.
    #[error("JSON deserialization error: {0}")]
    SerdeJson(Arc<serde_json::Error>),

    /// Stream create error.
    #[error("Failed to create stream: {0}")]
    StreamCreate(async_nats::jetstream::context::CreateStreamErrorKind),

    /// Stream info error.
    #[error("Failed to get stream info: {0}")]
    StreamInfo(async_nats::jetstream::context::RequestErrorKind),
}

impl<H> From<serde_json::Error> for Error<H>
where
    H: StreamHandler,
{
    fn from(error: serde_json::Error) -> Self {
        Self::SerdeJson(Arc::new(error))
    }
}

impl<H> StreamError for Error<H> where H: StreamHandler {}
