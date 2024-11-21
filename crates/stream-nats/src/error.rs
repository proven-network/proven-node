use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error<HE: Clone + Debug + StdError + Send + Sync> {
    #[error("Consumer acknowledgment failed")]
    ConsumerAck,

    #[error("Failed to create consumer: {0}")]
    ConsumerCreate(async_nats::jetstream::stream::ConsumerErrorKind),

    #[error("Failed to get consumer messages: {0}")]
    ConsumerMessages(async_nats::jetstream::consumer::pull::MessagesErrorKind),

    #[error("Consumer stream error: {0}")]
    ConsumerStream(async_nats::jetstream::consumer::StreamErrorKind),

    #[error("Handler error: {0}")]
    Handler(HE),

    #[error("No message info available")]
    NoInfo,

    #[error("Failed to delete reply message: {0}")]
    ReplyDelete(async_nats::jetstream::stream::DeleteMessageErrorKind),

    #[error("Failed to get reply message: {0}")]
    ReplyDirectGet(async_nats::jetstream::stream::DirectGetErrorKind),

    #[error("Failed to publish reply: {0}")]
    ReplyPublish(async_nats::client::PublishErrorKind),

    #[error("Request failed: {0}")]
    Request(async_nats::client::RequestErrorKind),

    #[error("JSON serialization error: {0}")]
    SerdeJson(Arc<serde_json::Error>),

    #[error("Failed to create stream: {0}")]
    StreamCreate(async_nats::jetstream::context::CreateStreamErrorKind),
}

impl<HE: Clone + Debug + StdError + Send + Sync> From<serde_json::Error> for Error<HE> {
    fn from(error: serde_json::Error) -> Self {
        Error::SerdeJson(Arc::new(error))
    }
}
