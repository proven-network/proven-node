use std::error::Error;
use std::fmt::Debug;

use derive_more::From;

#[derive(Clone, Debug, From)]
pub enum SubscriberError<HE: Clone + Debug + Error + Send + Sync> {
    ConsumerAck,

    #[from]
    ConsumerCreate(async_nats::jetstream::stream::ConsumerErrorKind),

    #[from]
    ConsumerMessages(async_nats::jetstream::consumer::pull::MessagesErrorKind),

    #[from]
    ConsumerStream(async_nats::jetstream::consumer::StreamErrorKind),

    Handler(HE),

    #[from]
    ReplyPublish(async_nats::client::PublishErrorKind),

    #[from]
    StreamCreate(async_nats::jetstream::context::CreateStreamErrorKind),
}

impl<HE: Clone + Debug + Error + Send + Sync> std::fmt::Display for SubscriberError<HE> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Store error")
    }
}

impl<HE: Clone + Debug + Error + Send + Sync> std::error::Error for SubscriberError<HE> {}
