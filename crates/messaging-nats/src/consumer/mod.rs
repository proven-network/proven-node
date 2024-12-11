mod error;

pub use error::Error;

use std::error::Error as StdError;
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
use proven_messaging::stream::InitializedStream;
use proven_messaging::Message;

use crate::stream::InitializedNatsStream;

/// Options for the nats consumer.
#[derive(Clone, Debug)]
pub struct NatsConsumerOptions {
    durable_name: Option<String>,
    nats_client: NatsClient,
    nats_jetstream_context: Context,
}
impl ConsumerOptions for NatsConsumerOptions {}

/// A NATS consumer.
#[derive(Debug)]
pub struct NatsConsumer<P, X, T, D, S>
where
    P: InitializedStream<T, D, S> + Clone + Debug + Send + Sync + 'static,
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    nats_client: NatsClient,
    nats_consumer: NatsConsumerType<NatsConsumerConfig>,
    nats_jetstream_context: Context,
    stream: <Self as Consumer<P, X, T, D, S>>::StreamType,
}

impl<P, X, T, D, S> Clone for NatsConsumer<P, X, T, D, S>
where
    P: InitializedStream<T, D, S> + Clone + Debug + Send + Sync + 'static,
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            nats_client: self.nats_client.clone(),
            nats_consumer: self.nats_consumer.clone(),
            nats_jetstream_context: self.nats_jetstream_context.clone(),
            stream: self.stream.clone(),
        }
    }
}

impl<P, X, T, D, S> NatsConsumer<P, X, T, D, S>
where
    P: InitializedStream<T, D, S> + Clone + Debug + Send + Sync + 'static,
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    /// Creates a new NATS consumer.
    async fn process_messages(
        nats_consumer: NatsConsumerType<NatsConsumerConfig>,
        handler: X,
    ) -> Result<(), Error> {
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
                    .map_err(|_| Error::Handler)?;
            }
        }
    }
}

#[async_trait]
impl<P, X, T, D, S> Consumer<P, X, T, D, S> for NatsConsumer<P, X, T, D, S>
where
    P: InitializedStream<T, D, S> + Clone + Debug + Send + Sync + 'static,
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    type Error = Error;

    type Options = NatsConsumerOptions;

    type StreamType = InitializedNatsStream<T, D, S>;

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
            nats_client: options.nats_client,
            nats_consumer,
            nats_jetstream_context: options.nats_jetstream_context,
            stream,
        })
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
}
