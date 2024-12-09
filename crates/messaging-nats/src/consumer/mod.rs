mod error;

pub use error::Error;

use std::fmt::Debug;

use async_nats::jetstream::consumer::pull::Config as NatsConsumerConfig;
use async_nats::jetstream::consumer::Consumer as NatsConsumerType;
use async_nats::jetstream::Context;
use async_nats::Client as NatsClient;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_messaging::consumer::{Consumer, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::stream::Stream;
use proven_messaging::Message;

use crate::stream::NatsStream;

/// Options for the nats consumer.
#[derive(Clone, Debug)]
pub struct NatsConsumerOptions {
    durable_name: Option<String>,
    nats_client: NatsClient,
    nats_jetstream_context: Context,
}
impl ConsumerOptions for NatsConsumerOptions {}

/// A NATS consumer.
#[derive(Clone, Debug)]
pub struct NatsConsumer<X, T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
    X: ConsumerHandler<T>,
{
    handler: X,
    _nats_client: NatsClient,
    nats_consumer: NatsConsumerType<NatsConsumerConfig>,
    _nats_jetstream_context: Context,
    stream: <Self as Consumer<X, T>>::StreamType,
}

impl<X, T> NatsConsumer<X, T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
    X: ConsumerHandler<T>,
{
    /// Creates a new NATS consumer.
    async fn process_messages(
        nats_consumer: NatsConsumerType<NatsConsumerConfig>,
        handler: X,
    ) -> Result<(), Error<X::Error>> {
        loop {
            let mut messages = nats_consumer
                .messages()
                .await
                .map_err(|e| Error::Stream(e.kind()))?;

            while let Some(message) = messages.next().await {
                let message = message.map_err(|e| Error::Messages(e.kind()))?;

                let headers = message.headers.clone();
                let payload: T = message.payload.clone().try_into().unwrap();

                handler
                    .handle(Message { headers, payload })
                    .await
                    .map_err(Error::Handler)?;
            }
        }
    }
}

#[async_trait]
impl<X, T> Consumer<X, T> for NatsConsumer<X, T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
    X: ConsumerHandler<T>,
{
    type Error = Error<X::Error>;

    type Options = NatsConsumerOptions;

    type StreamType = NatsStream<T>;

    #[allow(clippy::significant_drop_tightening)]
    async fn new(
        name: String,
        stream: Self::StreamType,
        options: NatsConsumerOptions,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let nats_consumer = options
            .nats_jetstream_context
            .create_consumer_on_stream(
                NatsConsumerConfig {
                    name: Some(name),
                    durable_name: options.durable_name,
                    ..Default::default()
                },
                stream.name().as_str(),
            )
            .await
            .map_err(|e| Error::Create(e.kind()))?;

        tokio::spawn(Self::process_messages(
            nats_consumer.clone(),
            handler.clone(),
        ));

        Ok(Self {
            handler,
            _nats_client: options.nats_client,
            nats_consumer,
            _nats_jetstream_context: options.nats_jetstream_context,
            stream,
        })
    }

    fn handler(&self) -> X {
        self.handler.clone()
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        let seq = self
            .nats_consumer
            .clone()
            .info()
            .await
            .map_err(|e| Error::Info(e.kind()))?
            .ack_floor
            .stream_sequence;

        Ok(seq)
    }

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}
