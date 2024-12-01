use std::fmt::Debug;
use std::sync::Arc;

use proven_stream::{StreamError, StreamHandlerError};
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error<HE: StreamHandlerError> {
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    #[error("Consumer acknowledgment failed")]
    ConsumerAck,

    #[error("Failed to create consumer: {0}")]
    ConsumerCreate(async_nats::jetstream::stream::ConsumerErrorKind),

    #[error("Failed to get consumer info: {0}")]
    ConsumerInfo(async_nats::jetstream::context::RequestErrorKind),

    #[error("Failed to get consumer messages: {0}")]
    ConsumerMessages(async_nats::jetstream::consumer::pull::MessagesErrorKind),

    #[error("Consumer stream error: {0}")]
    ConsumerStream(async_nats::jetstream::consumer::StreamErrorKind),

    #[error("Handler error: {0}")]
    Handler(HE),

    #[error("No message info available")]
    NoInfo,

    #[error("Failed to publish: {0}")]
    Publish(async_nats::client::PublishErrorKind),

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

    #[error("Failed to get stream info: {0}")]
    StreamInfo(async_nats::jetstream::context::RequestErrorKind),
}

impl<HE: StreamHandlerError> From<ciborium::de::Error<std::io::Error>> for Error<HE> {
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Error::CborDeserialize(Arc::new(error))
    }
}

impl<HE: StreamHandlerError> From<ciborium::ser::Error<std::io::Error>> for Error<HE> {
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Error::CborSerialize(Arc::new(error))
    }
}

impl<HE: StreamHandlerError> From<serde_json::Error> for Error<HE> {
    fn from(error: serde_json::Error) -> Self {
        Error::SerdeJson(Arc::new(error))
    }
}

impl<HE: StreamHandlerError> StreamError for Error<HE> {}
