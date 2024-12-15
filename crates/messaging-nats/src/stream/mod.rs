mod error;

use crate::client::NatsClient;
use crate::consumer::{NatsConsumer, NatsConsumerOptions};
use crate::service::{NatsService, NatsServiceOptions};
use crate::subject::NatsUnpublishableSubject;
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_nats::jetstream::stream::{Config as NatsStreamConfig, Stream as NatsStreamType};
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::Client as AsyncNatsClient;
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
    type Error = Error<D, S>;

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
            .create_stream(NatsStreamConfig {
                name: stream_name.clone().into(),
                allow_direct: true,
                allow_rollup: true,
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
            .create_stream(NatsStreamConfig {
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
                    .map(Into::into)
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
        handler: X,
    ) -> Result<Self::Client<X>, <Self::Client<X> as Client<X, T, D, S>>::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T, D, S>,
    {
        let client = NatsClient::new(
            format!("{}_{}", self.name(), service_name.into()),
            self.clone(),
            options,
            handler,
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
                    .map_err(|e| Error::Deserialize(e))?;

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

    /// Returns the name of the stream.
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Publishes a message directly to the stream.
    async fn publish(&self, message: T) -> Result<u64, Self::Error> {
        let payload: Bytes = message.try_into().map_err(|e| Error::Serialize(e))?;

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

    /// Consumes the stream with the given consumer.
    async fn start_consumer<N, X>(
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
    async fn start_service<N, X>(
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
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
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

                fn scope<K: Clone + Into<String> + Send>(&self, scope: K) -> $parent<T, D, S> {
                    $parent::<T, D, S> {
                        full_name: format!("{}_{}", self.full_name, scope.into()),
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
            NatsStreamOptions { client },
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
            NatsStreamOptions { client },
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
}
