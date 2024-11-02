use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum Error<HE: Clone + Debug + StdError + Send + Sync> {
    ConsumerAck,
    ConsumerCreate(async_nats::jetstream::stream::ConsumerErrorKind),
    ConsumerMessages(async_nats::jetstream::consumer::pull::MessagesErrorKind),
    ConsumerStream(async_nats::jetstream::consumer::StreamErrorKind),
    Handler(HE),
    NoInfo,
    ReplyDelete(async_nats::jetstream::stream::DeleteMessageErrorKind),
    ReplyDirectGet(async_nats::jetstream::stream::DirectGetErrorKind),
    ReplyPublish(async_nats::client::PublishErrorKind),
    Request(async_nats::client::RequestErrorKind),
    SerdeJson(Arc<serde_json::Error>),
    StreamCreate(async_nats::jetstream::context::CreateStreamErrorKind),
}

impl<HE: Clone + Debug + StdError + Send + Sync> From<serde_json::Error> for Error<HE> {
    fn from(error: serde_json::Error) -> Self {
        Error::SerdeJson(Arc::new(error))
    }
}

impl<HE: Clone + Debug + StdError + Send + Sync> std::fmt::Display for Error<HE> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Stream error")
    }
}

impl<HE: Clone + Debug + StdError + Send + Sync> std::error::Error for Error<HE> {}
