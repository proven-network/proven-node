mod error;

use crate::client::NatsClient;
use crate::consumer::{NatsConsumer, NatsConsumerOptions};
use crate::service::{NatsService, NatsServiceOptions};
use crate::subject::NatsUnpublishableSubject;
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_nats::Client as AsyncNatsClient;
use async_nats::HeaderMap;
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::jetstream::stream::{Config as NatsStreamConfig, Stream as NatsStreamType};
use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::client::Client;
use proven_messaging::consumer::Consumer;
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::service::Service;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::{
    InitializedStream, Stream, Stream1, Stream2, Stream3, StreamOptions,
};

/// Options for the NATS stream.
#[derive(Clone, Debug)]
pub struct NatsStreamOptions {
    /// The NATS client.
    pub client: AsyncNatsClient,

    /// The number of replicas for the stream.
    pub num_replicas: usize,
}
impl StreamOptions for NatsStreamOptions {}

/// An in-memory stream.
#[derive(Debug)]
pub struct InitializedNatsStream<T, D, S>
where
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
    jetstream_context: JetStreamContext,
    name: String,
    nats_stream: NatsStreamType,
    _marker: PhantomData<T>,
}

impl<T, D, S> Clone for InitializedNatsStream<T, D, S>
where
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
            jetstream_context: self.jetstream_context.clone(),
            name: self.name.clone(),
            nats_stream: self.nats_stream.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, D, S> InitializedStream<T, D, S> for InitializedNatsStream<T, D, S>
where
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

    type Options = NatsStreamOptions;

    type Subject = NatsUnpublishableSubject<T, D, S>;

    type Client<X>
        = NatsClient<X, T, D, S>
    where
        X: ServiceHandler<T, D, S>;

    type Consumer<X>
        = NatsConsumer<X, T, D, S>
    where
        X: ConsumerHandler<T, D, S>;

    type Service<X>
        = NatsService<X, T, D, S>
    where
        X: ServiceHandler<T, D, S>;

    /// Creates a new stream.
    /// TODO: Use stream metadata to ensure you can't overlap streams with different types.
    /// (through that should probably be some tuple of type + ser/de)
    async fn new<N>(stream_name: N, options: NatsStreamOptions) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
    {
        let jetstream_context = async_nats::jetstream::new(options.client.clone());

        let nats_stream = jetstream_context
            .get_or_create_stream(NatsStreamConfig {
                name: stream_name.clone().into(),
                allow_direct: true,
                allow_rollup: true,
                num_replicas: options.num_replicas,
                ..Default::default()
            })
            .await
            .map_err(|e| Error::CreateStream(e.kind()))?;

        Ok(Self {
            jetstream_context,
            name: stream_name.into(),
            nats_stream,
            _marker: PhantomData,
        })
    }

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new_with_subjects<N, J>(
        stream_name: N,
        options: NatsStreamOptions,
        subjects: Vec<J>,
    ) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
        J: Into<Self::Subject> + Clone + Send,
    {
        let jetstream_context = async_nats::jetstream::new(options.client.clone());

        let nats_stream = jetstream_context
            .get_or_create_stream(NatsStreamConfig {
                name: stream_name.clone().into(),
                allow_direct: true,
                allow_rollup: true,
                subjects: subjects
                    .iter()
                    .map(|s| {
                        let subject: Self::Subject = s.clone().into();
                        let string: String = subject.into();
                        string
                    })
                    .collect(),
                ..Default::default()
            })
            .await
            .unwrap();

        Ok(Self {
            jetstream_context,
            name: stream_name.into(),
            nats_stream,
            _marker: PhantomData,
        })
    }

    async fn client<N, X>(
        &self,
        service_name: N,
        options: <Self::Client<X> as Client<X, T, D, S>>::Options,
    ) -> Result<Self::Client<X>, <Self::Client<X> as Client<X, T, D, S>>::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T, D, S>,
    {
        let client = NatsClient::new(
            format!("{}_{}", self.name(), service_name.into()),
            self.clone(),
            options,
        )
        .await?;

        Ok(client)
    }

    /// Deletes the message with the given sequence number.
    async fn delete(&self, seq: u64) -> Result<(), Self::Error> {
        self.nats_stream
            .delete_message(seq)
            .await
            .map_err(|e| Error::Delete(e.kind()))?;

        Ok(())
    }

    /// Gets the message with the given sequence number.
    async fn get(&self, seq: u64) -> Result<Option<T>, Self::Error> {
        match self.nats_stream.direct_get(seq).await {
            Ok(message) => {
                let payload: T = message
                    .payload
                    .try_into()
                    .map_err(|e: D| Error::Deserialize(e.to_string()))?;

                Ok(Some(payload))
            }
            Err(e) => match e.kind() {
                async_nats::jetstream::stream::DirectGetErrorKind::NotFound => Ok(None),
                _ => Err(Error::DirectGet(e.kind())),
            },
        }
    }

    /// The last message in the stream.
    async fn last_message(&self) -> Result<Option<T>, Self::Error> {
        self.get(self.last_seq().await?).await
    }

    /// The last sequence number in the stream.
    async fn last_seq(&self) -> Result<u64, Self::Error> {
        Ok(self
            .nats_stream
            .clone()
            .info()
            .await
            .map_err(|e| Error::Info(e.kind()))?
            .state
            .last_sequence)
    }

    /// The number of messages in the stream.
    async fn messages(&self) -> Result<u64, Self::Error> {
        Ok(self
            .nats_stream
            .clone()
            .info()
            .await
            .map_err(|e| Error::Info(e.kind()))?
            .state
            .messages)
    }

    /// Returns the name of the stream.
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Publishes a message directly to the stream.
    async fn publish(&self, message: T) -> Result<u64, Self::Error> {
        let payload: Bytes = message
            .try_into()
            .map_err(|e| Error::Serialize(e.to_string()))?;

        let seq = self
            .jetstream_context
            .publish(self.name(), payload)
            .await
            .map_err(|e| Error::Publish(e.kind()))?
            .await
            .map_err(|e| Error::Publish(e.kind()))?
            .sequence;

        Ok(seq)
    }

    /// Publishes a batch of messages atomically to the stream.
    /// Returns the sequence number of the last published message.
    ///
    /// TODO: Update this implementation once NATS JetStream supports atomic batch publishing.
    /// For now, we publish messages sequentially and return the last sequence number.
    async fn publish_batch(&self, messages: Vec<T>) -> Result<u64, Self::Error> {
        if messages.is_empty() {
            return Err(Error::EmptyBatch);
        }

        let mut last_seq = 0;

        // Publish each message sequentially
        // TODO: Replace with atomic batch publish when NATS supports it
        for message in messages {
            last_seq = self.publish(message).await?;
        }

        Ok(last_seq)
    }

    /// Publishes a rollup message directly to the stream - purges all prior messages.
    /// Must provide expected sequence number for optimistic concurrency control.
    async fn rollup(&self, message: T, expected_seq: u64) -> Result<u64, Self::Error> {
        let payload: Bytes = message
            .try_into()
            .map_err(|e| Error::Serialize(e.to_string()))?;

        let mut headers = HeaderMap::new();
        headers.insert("Nats-Rollup", "all");
        headers.insert("Nats-Expected-Last-Sequence", expected_seq.to_string());

        let seq = self
            .jetstream_context
            .publish_with_headers(self.name(), headers, payload)
            .await
            .map_err(|e| Error::Publish(e.kind()))?
            .await
            .map_err(|e| Error::Publish(e.kind()))?
            .sequence;

        Ok(seq)
    }

    /// Consumes the stream with the given consumer.
    async fn consumer<N, X>(
        &self,
        consumer_name: N,
        options: NatsConsumerOptions,
        handler: X,
    ) -> Result<Self::Consumer<X>, <Self::Consumer<X> as Consumer<X, T, D, S>>::Error>
    where
        N: Clone + Into<String> + Send,
        X: ConsumerHandler<T, D, S>,
    {
        let consumer = NatsConsumer::new(
            format!("{}_{}", self.name(), consumer_name.into()),
            self.clone(),
            options,
            handler,
        )
        .await?;

        Ok(consumer)
    }

    /// Consumes the stream with the given service.
    async fn service<N, X>(
        &self,
        service_name: N,
        options: NatsServiceOptions,
        handler: X,
    ) -> Result<Self::Service<X>, <Self::Service<X> as Service<X, T, D, S>>::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T, D, S>,
    {
        let service = NatsService::new(
            format!("{}_{}", self.name(), service_name.into()),
            self.clone(),
            options,
            handler,
        )
        .await?;

        Ok(service)
    }
}

/// All scopes applied and can initialize.
#[derive(Debug)]
pub struct NatsStream<T, D, S>
where
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
    full_name: String,
    options: NatsStreamOptions,
    _marker: PhantomData<T>,
}

impl<T, D, S> Clone for NatsStream<T, D, S>
where
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
            full_name: self.full_name.clone(),
            options: self.options.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, D, S> Stream<T, D, S> for NatsStream<T, D, S>
where
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
    type Options = NatsStreamOptions;

    /// The initialized stream type.
    type Initialized = InitializedNatsStream<T, D, S>;

    type Subject = NatsUnpublishableSubject<T, D, S>;

    /// Creates a new `NatsStream`.
    fn new<K>(stream_name: K, options: NatsStreamOptions) -> Self
    where
        K: Clone + Into<String> + Send,
    {
        Self {
            full_name: stream_name.into(),
            options,
            _marker: PhantomData,
        }
    }

    async fn init(
        &self,
    ) -> Result<Self::Initialized, <Self::Initialized as InitializedStream<T, D, S>>::Error> {
        let stream =
            InitializedNatsStream::new(self.full_name.clone(), self.options.clone()).await?;

        Ok(stream)
    }

    async fn init_with_subjects<J>(
        &self,
        subjects: Vec<J>,
    ) -> Result<Self::Initialized, <Self::Initialized as InitializedStream<T, D, S>>::Error>
    where
        J: Into<Self::Subject> + Clone + Send,
    {
        let stream = InitializedNatsStream::new_with_subjects(
            self.full_name.clone(),
            self.options.clone(),
            subjects,
        )
        .await?;

        Ok(stream)
    }
}

macro_rules! impl_scoped_stream {
    ($index:expr_2021, $parent:ident, $parent_trait:ident, $doc:expr_2021) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Debug)]
            pub struct [< NatsStream $index >]<T, D, S>
            where
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
                full_name: String,
                options: NatsStreamOptions,
                _marker: PhantomData<T>,
            }

            impl<T, D, S> Clone for [< NatsStream $index >]<T, D, S>
            where
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
                        full_name: self.full_name.clone(),
                        options: self.options.clone(),
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> [< NatsStream $index >]<T, D, S>
            where
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
                /// Creates a new `[< NatsStream $index >]`.
                #[must_use]
                pub fn new<K>(stream_name: K, options: NatsStreamOptions) -> Self
                where
                    K: Clone + Into<String> + Send,
                {
                    Self {
                        full_name: stream_name.into(),
                        options,
                        _marker: PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T, D, S> [< Stream $index >]<T, D, S> for [< NatsStream $index >]<T, D, S>
            where
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
                type Options = NatsStreamOptions;

                type Scoped = $parent<T, D, S>;

                fn scope<K>(&self, scope: K) -> $parent<T, D, S>
                where
                    K: AsRef<str> + Send,
                {
                    $parent::<T, D, S> {
                        full_name: format!("{}_{}", self.full_name, scope.as_ref()),
                        options: self.options.clone(),
                        _marker: PhantomData,
                    }
                }
            }
        }
    };
}
impl_scoped_stream!(1, NatsStream, Stream1, "A single-scoped NATs stream.");
impl_scoped_stream!(2, NatsStream1, Stream1, "A double-scoped NATs stream.");
impl_scoped_stream!(3, NatsStream2, Stream2, "A triple-scoped NATs stream.");

#[cfg(test)]
mod tests {
    use super::*;
    use async_nats::ConnectOptions;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    async fn cleanup_stream(client: &AsyncNatsClient, stream_name: &str) {
        let js = async_nats::jetstream::new(client.clone());
        // Ignore errors since the stream might not exist
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

    #[tokio::test]
    async fn test_stream_get() {
        // Connect to NATS
        let client = ConnectOptions::default()
            .connection_timeout(Duration::from_secs(5))
            .connect("localhost:4222")
            .await
            .expect("Failed to connect to NATS");

        cleanup_stream(&client, "test_stream").await;

        // Create stream
        let stream = NatsStream::<TestMessage, serde_json::Error, serde_json::Error>::new(
            "test_stream",
            NatsStreamOptions {
                client,
                num_replicas: 1,
            },
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        // Create test message
        let test_message = TestMessage {
            content: "test content".to_string(),
        };

        // Publish message
        let seq = initialized_stream
            .publish(test_message.clone())
            .await
            .expect("Failed to publish message");

        // Get message
        let retrieved_message = initialized_stream
            .get(seq)
            .await
            .expect("Failed to get message")
            .expect("Message not found");

        // Verify message content
        assert_eq!(retrieved_message, test_message);
    }

    #[tokio::test]
    async fn test_stream_delete() {
        // Connect to NATS
        let client = ConnectOptions::default()
            .connection_timeout(Duration::from_secs(5))
            .connect("localhost:4222")
            .await
            .expect("Failed to connect to NATS");

        cleanup_stream(&client, "test_delete_stream").await;

        // Create stream
        let stream = NatsStream::<TestMessage, serde_json::Error, serde_json::Error>::new(
            "test_delete_stream",
            NatsStreamOptions {
                client,
                num_replicas: 1,
            },
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        // Create and publish test message
        let test_message = TestMessage {
            content: "delete me".to_string(),
        };

        // Publish message and get sequence number
        let seq = initialized_stream
            .publish(test_message)
            .await
            .expect("Failed to publish message");

        // Delete the message
        initialized_stream
            .delete(seq)
            .await
            .expect("Failed to delete message");

        // Verify message is deleted by attempting to get it
        let retrieved_message = initialized_stream
            .get(seq)
            .await
            .expect("Failed to query message");

        assert!(
            retrieved_message.is_none(),
            "Message should have been deleted"
        );
    }

    #[tokio::test]
    async fn test_rollup() {
        // Connect to NATS
        let client = ConnectOptions::default()
            .connection_timeout(Duration::from_secs(5))
            .connect("localhost:4222")
            .await
            .expect("Failed to connect to NATS");

        cleanup_stream(&client, "test_rollup").await;

        // Create stream
        let stream = NatsStream::new(
            "test_rollup",
            NatsStreamOptions {
                client,
                num_replicas: 1,
            },
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        let message1 = Bytes::from("test_message1");
        let message2 = Bytes::from("test_message2");
        let message3 = Bytes::from("test_message3");

        initialized_stream.publish(message1.clone()).await.unwrap();
        initialized_stream.publish(message2.clone()).await.unwrap();

        // Perform rollup with message3, expecting the sequence number to be 2
        let seq = initialized_stream
            .rollup(message3.clone(), 2)
            .await
            .unwrap();

        // Verify the sequence number returned by rollup
        assert_eq!(seq, 3);

        // Verify that only message3 is present in the stream
        assert_eq!(initialized_stream.get(1).await.unwrap(), None);
        assert_eq!(initialized_stream.get(2).await.unwrap(), None);
        assert_eq!(
            initialized_stream.get(3).await.unwrap(),
            Some(message3.clone())
        );

        // Verify that the last message is message3
        assert_eq!(
            initialized_stream.last_message().await.unwrap(),
            Some(message3)
        );
    }

    #[tokio::test]
    async fn test_rollup_with_outdated_seq() {
        // Connect to NATS
        let client = ConnectOptions::default()
            .connection_timeout(Duration::from_secs(5))
            .connect("localhost:4222")
            .await
            .expect("Failed to connect to NATS");

        cleanup_stream(&client, "test_rollup_with_outdated_seq").await;

        // Create stream
        let stream = NatsStream::new(
            "test_rollup_with_outdated_seq",
            NatsStreamOptions {
                client,
                num_replicas: 1,
            },
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        let message1 = Bytes::from("test_message1");
        let message2 = Bytes::from("test_message2");
        let message3 = Bytes::from("test_message3");

        initialized_stream.publish(message1.clone()).await.unwrap();
        initialized_stream.publish(message2.clone()).await.unwrap();

        // Attempt rollup with message3, providing an outdated expected sequence number
        let result = initialized_stream.rollup(message3.clone(), 1).await;

        // Verify that an error is returned
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_publish_batch() {
        // Connect to NATS
        let client = ConnectOptions::default()
            .connection_timeout(Duration::from_secs(5))
            .connect("localhost:4222")
            .await
            .expect("Failed to connect to NATS");

        cleanup_stream(&client, "test_publish_batch").await;

        // Create stream
        let stream = NatsStream::<TestMessage, serde_json::Error, serde_json::Error>::new(
            "test_publish_batch",
            NatsStreamOptions {
                client,
                num_replicas: 1,
            },
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        let message1 = TestMessage {
            content: "batch_message1".to_string(),
        };
        let message2 = TestMessage {
            content: "batch_message2".to_string(),
        };
        let message3 = TestMessage {
            content: "batch_message3".to_string(),
        };
        let all_messages = vec![message1.clone(), message2.clone(), message3.clone()];

        // Publish batch (currently sequential, but will be atomic when NATS supports it)
        let last_seq = initialized_stream
            .publish_batch(all_messages)
            .await
            .expect("Failed to publish batch");

        // Verify all messages were stored and last_seq is correct
        // Note: NATS sequences are 1-indexed, so first message is seq 1
        assert_eq!(last_seq, 3); // Last sequence should be 3

        // Verify all messages can be retrieved
        assert_eq!(initialized_stream.get(1).await.unwrap(), Some(message1));
        assert_eq!(initialized_stream.get(2).await.unwrap(), Some(message2));
        assert_eq!(
            initialized_stream.get(3).await.unwrap(),
            Some(message3.clone())
        );

        // Verify stream metadata
        assert_eq!(initialized_stream.last_seq().await.unwrap(), 3);
        assert_eq!(initialized_stream.messages().await.unwrap(), 3);
        assert_eq!(
            initialized_stream.last_message().await.unwrap(),
            Some(message3)
        );
    }

    #[tokio::test]
    async fn test_publish_batch_empty() {
        // Connect to NATS
        let client = ConnectOptions::default()
            .connection_timeout(Duration::from_secs(5))
            .connect("localhost:4222")
            .await
            .expect("Failed to connect to NATS");

        cleanup_stream(&client, "test_publish_batch_empty").await;

        // Create stream
        let stream = NatsStream::<TestMessage, serde_json::Error, serde_json::Error>::new(
            "test_publish_batch_empty",
            NatsStreamOptions {
                client,
                num_replicas: 1,
            },
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        // Attempt to publish empty batch
        let result = initialized_stream.publish_batch(vec![]).await;

        // Verify that an error is returned
        assert!(result.is_err());
        if matches!(result, Err(Error::EmptyBatch)) {
            // Expected error
        } else {
            panic!("Expected Error::EmptyBatch");
        }
    }
}
