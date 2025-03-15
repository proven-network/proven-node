mod error;

use crate::service_responder::{NatsServiceResponder, NatsUsedServiceResponder};
use crate::stream::InitializedNatsStream;
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;

use async_nats::Client as NatsClient;
use async_nats::jetstream::Context;
use async_nats::jetstream::consumer::Consumer as NatsConsumerType;
use async_nats::jetstream::consumer::pull::Config as NatsConsumerConfig;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_messaging::service::{Service, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::InitializedStream;

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
        handler: X,
        nats_consumer: NatsConsumerType<NatsConsumerConfig>,
        stream: InitializedNatsStream<T, D, S>,
    ) -> Result<(), Error>
    where
        X: ServiceHandler<T, D, S>,
    {
        let initial_stream_seq = stream.last_seq().await.unwrap_or(0);
        let mut caught_up = initial_stream_seq == 0;

        if caught_up {
            let _ = handler.on_caught_up().await;
        }

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

                let request_id_header = headers.get("Reply-Id").cloned();
                let reply_stream_header = headers.get("Reply-Stream").cloned();

                if let (Some(reply_stream_name), Some(request_id)) =
                    (reply_stream_header, request_id_header)
                {
                    let responder = NatsServiceResponder::new(
                        caught_up,
                        nats_client.clone(),
                        reply_stream_name.clone().to_string(),
                        request_id.clone().to_string(),
                        stream.clone(),
                        message.info().unwrap().stream_sequence,
                    );

                    handler
                        .handle(payload, responder)
                        .await
                        .map_err(|_| Error::Handler)?;

                    message.ack().await.unwrap();
                }

                // This check will re-fetch the last sequence number from the
                // stream in case it's changed since the service initialized
                if !caught_up
                    && message.info().unwrap().stream_sequence >= initial_stream_seq
                    && stream.last_seq().await.unwrap_or(0)
                        == message.info().unwrap().stream_sequence
                {
                    caught_up = true;
                    let _ = handler.on_caught_up().await;
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

    type Responder = NatsServiceResponder<
        T,
        D,
        S,
        X::ResponseType,
        X::ResponseDeserializationError,
        X::ResponseSerializationError,
    >;

    type UsedResponder = NatsUsedServiceResponder;

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

        tokio::spawn(Self::process_requests(
            options.client.clone(),
            handler.clone(),
            nats_consumer.clone(),
            stream.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::{NatsStream, NatsStreamOptions};

    use std::sync::Arc;

    use async_trait::async_trait;
    use proven_messaging::stream::Stream;
    use proven_messaging::{service_handler::ServiceHandler, service_responder::ServiceResponder};
    use serde::{Deserialize, Serialize};
    use tokio::sync::Mutex;

    async fn cleanup_stream(client: &async_nats::Client, stream_name: &str) {
        let js = async_nats::jetstream::new(client.clone());
        let _ = js.delete_stream(stream_name).await;
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct TestMessage {
        content: String,
    }

    impl TryFrom<Bytes> for TestMessage {
        type Error = serde_json::Error;

        fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
            serde_json::from_slice(&bytes)
        }
    }

    impl TryInto<Bytes> for TestMessage {
        type Error = serde_json::Error;

        fn try_into(self) -> Result<Bytes, Self::Error> {
            Ok(Bytes::from(serde_json::to_vec(&self)?))
        }
    }

    #[derive(Debug, Clone)]
    struct MockHandler {
        caught_up_called: Arc<Mutex<bool>>,
        caught_up_count: Arc<Mutex<u32>>,
        messages_processed: Arc<Mutex<u32>>,
    }

    impl MockHandler {
        fn new() -> Self {
            Self {
                caught_up_called: Arc::new(Mutex::new(false)),
                caught_up_count: Arc::new(Mutex::new(0)),
                messages_processed: Arc::new(Mutex::new(0)),
            }
        }
    }

    #[async_trait]
    impl ServiceHandler<TestMessage, serde_json::Error, serde_json::Error> for MockHandler {
        type Error = serde_json::Error;
        type ResponseType = TestMessage;
        type ResponseDeserializationError = serde_json::Error;
        type ResponseSerializationError = serde_json::Error;

        async fn handle<R>(
            &self,
            msg: TestMessage,
            responder: R,
        ) -> Result<R::UsedResponder, Self::Error>
        where
            R: ServiceResponder<
                    TestMessage,
                    serde_json::Error,
                    serde_json::Error,
                    Self::ResponseType,
                    Self::ResponseDeserializationError,
                    Self::ResponseSerializationError,
                >,
        {
            let mut count = self.messages_processed.lock().await;
            *count += 1;
            drop(count);

            Ok(responder
                .reply(TestMessage {
                    content: format!("handled: {}", msg.content),
                })
                .await)
        }

        async fn on_caught_up(&self) -> Result<(), Self::Error> {
            let mut called = self.caught_up_called.lock().await;
            *called = true;
            drop(called);

            let mut count = self.caught_up_count.lock().await;
            *count += 1;
            drop(count);

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_on_caught_up_called() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        cleanup_stream(&client, "test_on_caught_up_called").await;

        // Create stream using NatsStream API
        let stream = NatsStream::<TestMessage, serde_json::Error, serde_json::Error>::new(
            "test_on_caught_up_called",
            NatsStreamOptions {
                client: client.clone(),
            },
        );

        let initialized_stream = stream.init().await.unwrap();

        // Publish exactly 3 messages before creating the service
        for i in 1..=3 {
            // Just needed to service actually will process messages (simulates client publishing messages)
            let mut dummy_headers = async_nats::HeaderMap::new();
            dummy_headers.insert("Reply-Id", i.to_string());
            dummy_headers.insert("Reply-Stream", "test_on_caught_up_called_reply");

            let message = TestMessage {
                content: format!("message_{i}"),
            };
            client
                .publish_with_headers(
                    "test_on_caught_up_called",
                    dummy_headers.clone(),
                    message.try_into().unwrap(),
                )
                .await
                .expect("Failed to publish message");
        }

        let handler = MockHandler::new();
        let caught_up_called = handler.caught_up_called.clone();
        let caught_up_count = handler.caught_up_count.clone();
        let messages_processed = handler.messages_processed.clone();

        let _service = initialized_stream
            .start_service(
                "test_on_caught_up_called",
                NatsServiceOptions {
                    client: client.clone(),
                    durable_name: None,
                    jetstream_context: async_nats::jetstream::new(client.clone()),
                },
                handler,
            )
            .await
            .unwrap();

        // Give enough time for messages to be processed
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        assert!(
            *caught_up_called.lock().await,
            "on_caught_up should have been called"
        );
        assert_eq!(
            *caught_up_count.lock().await,
            1,
            "on_caught_up should have been called exactly once"
        );
        assert_eq!(
            *messages_processed.lock().await,
            3,
            "exactly 3 messages should have been processed before on_caught_up"
        );
    }
}
