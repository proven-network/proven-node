mod error;

use crate::stream::InitializedNatsStream;
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;

use async_nats::jetstream::consumer::pull::Config as NatsConsumerConfig;
use async_nats::jetstream::consumer::Consumer as NatsConsumerType;
use async_nats::jetstream::Context;
use async_nats::Client as NatsClient;
use async_nats::HeaderMap;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_messaging::service::{Service, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::InitializedStream;
use proven_messaging::Message;

/// Options for the nats service.
#[derive(Clone, Debug)]
pub struct NatsServiceOptions {
    /// The NATS client.
    pub client: NatsClient,

    /// The durable name of the consumer.
    pub durable_name: Option<String>,

    /// The jetstream context.
    pub jetstream_context: Context,
}
impl ServiceOptions for NatsServiceOptions {}

/// A NATS service.
#[derive(Debug)]
pub struct NatsService<X, T, D, S>
where
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
    stream: <Self as Service<X, T, D, S>>::StreamType,
}

impl<X, T, D, S> Clone for NatsService<X, T, D, S>
where
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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

impl<X, T, D, S> NatsService<X, T, D, S>
where
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
    /// Process requests.
    async fn process_requests(
        nats_client: NatsClient,
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
                let handler = handler.clone();
                let nats_client = nats_client.clone();

                let payload: T = message.payload.clone().try_into().unwrap();

                let headers = if let Some(headers) = message.headers.as_ref() {
                    headers.clone()
                } else {
                    continue;
                };

                let reply_stream_header = headers.get("reply-stream-name").cloned();
                let request_id_header = headers.get("request-id").cloned();

                if let (Some(reply_stream_name), Some(request_id)) =
                    (reply_stream_header, request_id_header)
                {
                    let reply_stream_name = reply_stream_name.clone().to_string();
                    let request_id = request_id.clone().to_string();

                    let result = handler
                        .handle(Message {
                            headers: None,
                            payload,
                        })
                        .await
                        .map_err(|_| Error::Handler)
                        .unwrap();

                    let result_bytes: Bytes = result.message.payload.try_into().unwrap();

                    let mut headers = HeaderMap::new();
                    headers.insert("request-id", request_id.clone());

                    nats_client
                        .publish_with_headers(reply_stream_name, headers, result_bytes)
                        .await
                        .unwrap();

                    message.ack().await.unwrap();
                }
            }
        }
    }
}

#[async_trait]
impl<X, T, D, S> Service<X, T, D, S> for NatsService<X, T, D, S>
where
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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

    type Options = NatsServiceOptions;

    type StreamType = InitializedNatsStream<T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        options: NatsServiceOptions,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let nats_consumer = options
            .jetstream_context
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

        // TODO: Actually wait for catch up
        let _ = handler.on_caught_up().await;

        tokio::spawn(Self::process_requests(
            options.client.clone(),
            nats_consumer.clone(),
            handler.clone(),
        ));

        Ok(Self {
            nats_client: options.client,
            nats_consumer,
            nats_jetstream_context: options.jetstream_context,
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

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}
