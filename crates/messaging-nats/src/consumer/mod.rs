mod error;

pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;

use crate::stream::InitializedNatsStream;
use async_nats::Client as NatsClient;
use async_nats::jetstream::Context;
use async_nats::jetstream::consumer::pull::Config as NatsConsumerConfig;
use async_nats::jetstream::consumer::{Consumer as NatsConsumerType, DeliverPolicy};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_bootable::Bootable;
use proven_messaging::consumer::{Consumer, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::stream::InitializedStream;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::debug;

/// Options for the nats consumer.
#[derive(Clone, Debug)]
pub struct NatsConsumerOptions {
    /// The NATS client.
    pub client: NatsClient,

    /// The durable name of the consumer.
    pub durable_name: Option<String>,

    /// The jetstream context.
    pub jetstream_context: Context,
}
impl ConsumerOptions for NatsConsumerOptions {}

/// A NATS consumer.
#[derive(Debug)]
#[allow(clippy::struct_field_names)]
pub struct NatsConsumer<X, T, D, S>
where
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
    handler: X,
    name: String,
    nats_client: NatsClient,
    nats_consumer: Arc<OnceCell<NatsConsumerType<NatsConsumerConfig>>>,
    nats_jetstream_context: Context,
    durable_name: Option<String>,
    shutdown_token: CancellationToken,
    stream: <Self as Consumer<X, T, D, S>>::StreamType,
    task_tracker: TaskTracker,
}

impl<X, T, D, S> Clone for NatsConsumer<X, T, D, S>
where
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
            handler: self.handler.clone(),
            name: self.name.clone(),
            nats_client: self.nats_client.clone(),
            nats_consumer: self.nats_consumer.clone(),
            nats_jetstream_context: self.nats_jetstream_context.clone(),
            durable_name: self.durable_name.clone(),
            shutdown_token: self.shutdown_token.clone(),
            stream: self.stream.clone(),
            task_tracker: self.task_tracker.clone(),
        }
    }
}

impl<X, T, D, S> NatsConsumer<X, T, D, S>
where
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
    async fn ensure_consumer_initialized(
        &self,
    ) -> Result<&NatsConsumerType<NatsConsumerConfig>, Error> {
        self.nats_consumer
            .get_or_try_init(|| async {
                self.nats_jetstream_context
                    .create_consumer_on_stream(
                        NatsConsumerConfig {
                            name: None, // Setting to none as it interferes with deliver_policy. TODO: Investigate more later.
                            deliver_policy: DeliverPolicy::All,
                            durable_name: self.durable_name.clone(),
                            ..Default::default()
                        },
                        self.stream.name().as_str(),
                    )
                    .await
                    .map_err(|e| Error::Create(e.kind()))
            })
            .await
    }

    /// Process messages.
    async fn process_messages(
        nats_consumer: NatsConsumerType<NatsConsumerConfig>,
        handler: X,
        stream: InitializedNatsStream<T, D, S>,
        shutdown_token: CancellationToken,
    ) -> Result<(), Error> {
        let initial_stream_msgs = stream.messages().await.unwrap_or(0);
        let mut caught_up = initial_stream_msgs == 0;
        let mut msgs_processed = 0;

        if caught_up {
            let _ = handler.on_caught_up().await;
        }

        let mut messages = nats_consumer
            .messages()
            .await
            .map_err(|e| Error::Stream(e.kind()))?;

        loop {
            tokio::select! {
                biased;
                () = shutdown_token.cancelled() => {
                    debug!("shutdown token cancelled, exiting message processing loop");
                    break;
                }
                message = messages.next() => {
                    if let Some(message) = message {
                        let message = message.map_err(|e| Error::Messages(e.kind()))?;
                        let payload: T = message.payload.clone().try_into().unwrap();
                        let stream_sequence = message.info().unwrap().stream_sequence;

                        handler.handle(payload, stream_sequence).await.map_err(|_| Error::Handler)?;

                        message.ack().await.unwrap();

                        if !caught_up {
                            // Only count while not caught up
                            msgs_processed += 1;

                            // This check will re-fetch the messages count from the
                            // stream in case it's changed since the service initialized
                            if msgs_processed >= initial_stream_msgs
                                && msgs_processed >= stream.messages().await.unwrap_or(0)
                            {
                                caught_up = true;
                                let _ = handler.on_caught_up().await;
                            }
                        }
                    } else {
                        // Stream closed
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<X, T, D, S> Consumer<X, T, D, S> for NatsConsumer<X, T, D, S>
where
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
    ) -> Result<Self, Error> {
        Ok(Self {
            handler,
            name,
            nats_client: options.client,
            nats_consumer: Arc::new(OnceCell::new()),
            nats_jetstream_context: options.jetstream_context,
            durable_name: options.durable_name,
            shutdown_token: CancellationToken::new(),
            stream,
            task_tracker: TaskTracker::new(),
        })
    }

    async fn last_seq(&self) -> Result<u64, Error> {
        let consumer = self.ensure_consumer_initialized().await?;
        let seq = consumer
            .clone()
            .info()
            .await
            .map_err(|e| Error::Info(e.kind()))?
            .ack_floor
            .stream_sequence;

        Ok(seq)
    }
}

#[async_trait]
impl<X, T, D, S> Bootable for NatsConsumer<X, T, D, S>
where
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
    fn bootable_name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.task_tracker.is_closed() {
            return Err(Box::new(Error::AlreadyRunning));
        }

        // Ensure consumer is initialized before starting
        let consumer = self.ensure_consumer_initialized().await?;

        self.task_tracker.spawn(Self::process_messages(
            consumer.clone(),
            self.handler.clone(),
            self.stream.clone(),
            self.shutdown_token.clone(),
        ));

        self.task_tracker.close();

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("shutting down nats consumer");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        Ok(())
    }

    async fn wait(&self) {
        self.task_tracker.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::{NatsStream, NatsStreamOptions};

    use std::sync::Arc;

    use async_trait::async_trait;
    use proven_messaging::stream::Stream;
    use serde::{Deserialize, Serialize};
    use tokio::sync::Mutex;

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
    impl ConsumerHandler<TestMessage, serde_json::Error, serde_json::Error> for MockHandler {
        type Error = serde_json::Error;

        async fn handle(
            &self,
            _msg: TestMessage,
            _stream_sequence: u64,
        ) -> Result<(), Self::Error> {
            let mut count = self.messages_processed.lock().await;
            *count += 1;
            drop(count);
            Ok(())
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

    async fn cleanup_stream(client: &async_nats::Client, stream_name: &str) {
        let js = async_nats::jetstream::new(client.clone());
        let _ = js.delete_stream(stream_name).await;
    }

    #[tokio::test]
    async fn test_consumer_on_caught_up_called() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        cleanup_stream(&client, "test_consumer_on_caught_up").await;

        // Create stream using NatsStream API
        let stream = NatsStream::<TestMessage, serde_json::Error, serde_json::Error>::new(
            "test_consumer_on_caught_up",
            NatsStreamOptions {
                client: client.clone(),
                num_replicas: 1,
            },
        );

        let initialized_stream = stream.init().await.unwrap();

        // Publish exactly 3 messages before creating the consumer
        for i in 1..=3 {
            let message = TestMessage {
                content: format!("message_{i}"),
            };
            initialized_stream
                .publish(message)
                .await
                .expect("Failed to publish message");
        }

        let handler = MockHandler::new();
        let caught_up_called = handler.caught_up_called.clone();
        let caught_up_count = handler.caught_up_count.clone();
        let messages_processed = handler.messages_processed.clone();

        let consumer = initialized_stream
            .consumer(
                "test_consumer_on_caught_up",
                NatsConsumerOptions {
                    client: client.clone(),
                    durable_name: None,
                    jetstream_context: async_nats::jetstream::new(client.clone()),
                },
                handler,
            )
            .await
            .unwrap();

        consumer.start().await.unwrap();

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
