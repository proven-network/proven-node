//! Engine-based streams for messaging.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::{info, warn};

use proven_engine::{Client as EngineClient, stream::StreamConfig};
use proven_messaging::client::Client;
use proven_messaging::consumer::Consumer;
use proven_messaging::service::Service;
use proven_messaging::stream::{
    InitializedStream, Stream, Stream1, Stream2, Stream3, StreamOptions,
};

use crate::client::EngineMessagingClient;
use crate::consumer::EngineMessagingConsumer;
use crate::error::MessagingEngineError;
use crate::service::EngineMessagingService;
use crate::subject::EngineMessagingSubject;

/// Options for the engine stream.
#[derive(Debug, Clone, Default)]
pub struct EngineStreamOptions {
    /// Optional stream configuration
    pub stream_config: Option<StreamConfig>,
}

impl StreamOptions for EngineStreamOptions {}

/// An initialized engine stream.
#[derive(Debug)]
pub struct InitializedEngineStream<T, D, S>
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
    /// Stream name.
    name: String,

    /// Engine client.
    client: Arc<dyn ClientWrapper>,

    /// Stream options.
    options: EngineStreamOptions,

    /// Local cache of stream data.
    cache: Arc<RwLock<HashMap<u64, T>>>,

    /// Type markers.
    _marker: PhantomData<(T, D, S)>,
}

/// Trait to abstract over the engine client's concrete types
#[async_trait]
trait ClientWrapper: Debug + Send + Sync + 'static {
    /// Create a stream
    async fn create_stream(
        &self,
        name: String,
        config: StreamConfig,
    ) -> Result<(), MessagingEngineError>;

    /// Publish to a stream
    async fn publish(
        &self,
        stream: String,
        payload: Vec<u8>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> Result<u64, MessagingEngineError>;

    /// Check if stream exists
    async fn stream_exists(&self, name: &str) -> Result<bool, MessagingEngineError>;

    /// Read messages from a stream
    async fn read_stream(
        &self,
        stream_name: String,
        start_sequence: u64,
        count: u64,
    ) -> Result<Vec<proven_engine::stream::StoredMessage>, MessagingEngineError>;

    /// Get stream info
    #[allow(dead_code)]
    async fn get_stream_info(
        &self,
        name: &str,
    ) -> Result<Option<proven_engine::stream::StreamMetadata>, MessagingEngineError>;
}

/// Wrapper implementation for the engine client
struct ClientWrapperImpl<T, G, L>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    L: proven_storage::LogStorage + 'static,
{
    inner: EngineClient<T, G, L>,
}

impl<T, G, L> Debug for ClientWrapperImpl<T, G, L>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    L: proven_storage::LogStorage + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientWrapperImpl").finish()
    }
}

#[async_trait]
impl<T, G, L> ClientWrapper for ClientWrapperImpl<T, G, L>
where
    T: proven_transport::Transport + Debug + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    L: proven_storage::LogStorage + Debug + 'static,
{
    async fn create_stream(
        &self,
        name: String,
        config: StreamConfig,
    ) -> Result<(), MessagingEngineError> {
        self.inner
            .create_stream(name, config)
            .await
            .map(|_| ())
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))
    }

    async fn publish(
        &self,
        stream: String,
        payload: Vec<u8>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> Result<u64, MessagingEngineError> {
        let _response = self
            .inner
            .publish(stream, payload, metadata)
            .await
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))?;

        // Extract sequence number from GroupResponse
        // For now, we'll use a placeholder since we don't know the exact structure
        // TODO: Extract actual sequence number from GroupResponse
        Ok(1)
    }

    async fn stream_exists(&self, name: &str) -> Result<bool, MessagingEngineError> {
        self.inner
            .get_stream_info(name)
            .await
            .map(|info| info.is_some())
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))
    }

    async fn read_stream(
        &self,
        stream_name: String,
        start_sequence: u64,
        count: u64,
    ) -> Result<Vec<proven_engine::stream::StoredMessage>, MessagingEngineError> {
        self.inner
            .read_stream(stream_name, start_sequence, count)
            .await
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))
    }

    async fn get_stream_info(
        &self,
        name: &str,
    ) -> Result<Option<proven_engine::stream::StreamMetadata>, MessagingEngineError> {
        // Get stream info from engine
        let info = self
            .inner
            .get_stream_info(name)
            .await
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))?;

        // Convert StreamInfo to StreamMetadata
        Ok(info.map(|i| proven_engine::stream::StreamMetadata {
            name: i.name.into(),
            group_id: i.group_id,
            created_at: 0,    // TODO: Engine doesn't expose created_at
            leader: None,     // TODO: Engine doesn't expose leader in stream config
            replicas: vec![], // TODO: Engine doesn't expose replicas in stream config
        }))
    }
}

impl<T, D, S> Clone for InitializedEngineStream<T, D, S>
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
            name: self.name.clone(),
            client: self.client.clone(),
            options: self.options.clone(),
            cache: self.cache.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, D, S> InitializedStream<T, D, S> for InitializedEngineStream<T, D, S>
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
    type Error = MessagingEngineError;
    type Options = EngineStreamOptions;
    type Subject = EngineMessagingSubject<T, D, S>;

    type Client<X>
        = EngineMessagingClient<X, T, D, S>
    where
        X: proven_messaging::service_handler::ServiceHandler<T, D, S>;

    type Consumer<X>
        = EngineMessagingConsumer<X, T, D, S>
    where
        X: proven_messaging::consumer_handler::ConsumerHandler<T, D, S>;

    type Service<X>
        = EngineMessagingService<X, T, D, S>
    where
        X: proven_messaging::service_handler::ServiceHandler<T, D, S>;

    /// Creates a new engine stream.
    async fn new<N>(stream_name: N, _options: Self::Options) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
    {
        let _name = stream_name.into();

        // This is just a type definition - actual client must be provided
        // when creating the EngineStream wrapper
        Err(MessagingEngineError::Engine(
            "InitializedEngineStream cannot be created directly, use EngineStream instead"
                .to_string(),
        ))
    }

    /// Creates a new stream with subjects.
    async fn new_with_subjects<N, J>(
        _stream_name: N,
        _options: Self::Options,
        _subjects: Vec<J>,
    ) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
        J: Into<Self::Subject> + Clone + Send,
    {
        // This is just a type definition - actual implementation in EngineStream
        Err(MessagingEngineError::Engine(
            "InitializedEngineStream cannot be created directly, use EngineStream instead"
                .to_string(),
        ))
    }

    /// Creates a client for this stream.
    async fn client<N, X>(
        &self,
        service_name: N,
        options: <Self::Client<X> as proven_messaging::client::Client<X, T, D, S>>::Options,
    ) -> Result<
        Self::Client<X>,
        <Self::Client<X> as proven_messaging::client::Client<X, T, D, S>>::Error,
    >
    where
        N: Clone + Into<String> + Send,
        X: proven_messaging::service_handler::ServiceHandler<T, D, S>,
    {
        EngineMessagingClient::new(service_name.into(), self.clone(), options).await
    }

    /// Creates a consumer for this stream.
    async fn consumer<N, X>(
        &self,
        consumer_name: N,
        options: <Self::Consumer<X> as proven_messaging::consumer::Consumer<X, T, D, S>>::Options,
        handler: X,
    ) -> Result<
        Self::Consumer<X>,
        <Self::Consumer<X> as proven_messaging::consumer::Consumer<X, T, D, S>>::Error,
    >
    where
        N: Clone + Into<String> + Send,
        X: proven_messaging::consumer_handler::ConsumerHandler<T, D, S>,
    {
        EngineMessagingConsumer::new(consumer_name.into(), self.clone(), options, handler).await
    }

    /// Creates a service for this stream.
    async fn service<N, X>(
        &self,
        service_name: N,
        options: <Self::Service<X> as proven_messaging::service::Service<X, T, D, S>>::Options,
        handler: X,
    ) -> Result<
        Self::Service<X>,
        <Self::Service<X> as proven_messaging::service::Service<X, T, D, S>>::Error,
    >
    where
        N: Clone + Into<String> + Send,
        X: proven_messaging::service_handler::ServiceHandler<T, D, S>,
    {
        EngineMessagingService::new(service_name.into(), self.clone(), options, handler).await
    }

    /// Deletes a message at the given sequence number.
    async fn delete(&self, seq: u64) -> Result<(), Self::Error> {
        // Engine streams don't support deletion - just remove from cache
        self.cache.write().await.remove(&seq);
        info!(
            "Removed message {} from cache for stream '{}'",
            seq, self.name
        );
        Ok(())
    }

    /// Gets a message by sequence number.
    async fn get(&self, seq: u64) -> Result<Option<T>, Self::Error> {
        // First check local cache
        {
            let cache = self.cache.read().await;
            if let Some(message) = cache.get(&seq) {
                return Ok(Some(message.clone()));
            }
        }

        // Query from engine storage
        let messages = self.client.read_stream(self.name.clone(), seq, 1).await?;
        if let Some(stored_msg) = messages.first() {
            let bytes = stored_msg.data.payload.clone();
            match T::try_from(bytes) {
                Ok(message) => {
                    // Cache the message for future access
                    self.cache.write().await.insert(seq, message.clone());
                    Ok(Some(message))
                }
                Err(e) => {
                    warn!("Failed to deserialize message at seq {}: {:?}", seq, e);
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    /// Gets the last message in the stream.
    async fn last_message(&self) -> Result<Option<T>, Self::Error> {
        let last_seq = self.last_seq().await?;
        if last_seq > 0 {
            self.get(last_seq).await
        } else {
            Ok(None)
        }
    }

    /// Gets the last sequence number.
    async fn last_seq(&self) -> Result<u64, Self::Error> {
        // TODO: Engine doesn't expose last sequence directly
        // For now, return a placeholder
        Ok(0)
    }

    /// Gets the total number of messages.
    async fn messages(&self) -> Result<u64, Self::Error> {
        // TODO: Engine doesn't expose message count directly
        // For now, return a placeholder
        Ok(0)
    }

    /// Gets the stream name.
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Publishes a message to the stream.
    async fn publish(&self, message: T) -> Result<u64, Self::Error> {
        // Convert message to bytes
        let bytes: Bytes = message.clone().try_into().map_err(|e| {
            MessagingEngineError::Serialization(format!("Failed to serialize message: {e:?}"))
        })?;

        // Publish through engine client
        let seq = self
            .client
            .publish(self.name.clone(), bytes.to_vec(), None)
            .await?;

        // Cache the message locally
        self.cache.write().await.insert(seq, message);

        info!(
            "Published message to stream '{}' at sequence {}",
            self.name, seq
        );
        Ok(seq)
    }

    /// Publishes multiple messages as a batch.
    async fn publish_batch(&self, messages: Vec<T>) -> Result<u64, Self::Error> {
        if messages.is_empty() {
            return self.last_seq().await;
        }

        // Engine doesn't support batch publish directly, so publish individually
        let mut last_seq = 0;
        for message in messages {
            last_seq = self.publish(message).await?;
        }

        info!(
            "Published batch to stream '{}', ending at sequence {}",
            self.name, last_seq
        );
        Ok(last_seq)
    }

    /// Replaces all previous messages with a single rollup message.
    async fn rollup(&self, message: T, _expected_seq: u64) -> Result<u64, Self::Error> {
        // Engine doesn't support rollup directly
        // Just publish the rollup message and clear cache
        let seq = self.publish(message.clone()).await?;

        // Clear local cache and add the rollup message
        {
            let mut cache = self.cache.write().await;
            cache.clear();
            cache.insert(seq, message);
        }

        info!(
            "Performed rollup on stream '{}' at sequence {}",
            self.name, seq
        );
        Ok(seq)
    }
}

impl<T, D, S> InitializedEngineStream<T, D, S>
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
    /// Create a new initialized stream with an engine client
    #[must_use]
    pub fn with_client<Tr, G, L>(
        name: String,
        client: EngineClient<Tr, G, L>,
        options: EngineStreamOptions,
    ) -> Self
    where
        Tr: proven_transport::Transport + Debug + 'static,
        G: proven_topology::TopologyAdaptor + 'static,
        L: proven_storage::LogStorage + Debug + 'static,
    {
        Self {
            name,
            client: Arc::new(ClientWrapperImpl { inner: client }),
            options,
            cache: Arc::new(RwLock::new(HashMap::new())),
            _marker: PhantomData,
        }
    }

    /// Publish a message with metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the publish operation fails.
    pub async fn publish_with_metadata(
        &self,
        message: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<u64, MessagingEngineError> {
        // Publish through engine with metadata
        let seq = self
            .client
            .publish(self.name.clone(), message.to_vec(), Some(metadata))
            .await?;

        info!(
            "Published message with metadata to stream '{}' at sequence {}",
            self.name, seq
        );

        Ok(seq)
    }
}

/// An engine-backed stream.
#[derive(Debug)]
pub struct EngineStream<T, D, S>
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
    name: String,
    client: Arc<dyn ClientWrapper>,
    options: EngineStreamOptions,
    _marker: PhantomData<(T, D, S)>,
}

impl<T, D, S> EngineStream<T, D, S>
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
    /// Create a new engine stream
    #[must_use]
    pub fn new<Tr, G, L>(
        name: String,
        client: EngineClient<Tr, G, L>,
        options: EngineStreamOptions,
    ) -> Self
    where
        Tr: proven_transport::Transport + Debug + 'static,
        G: proven_topology::TopologyAdaptor + 'static,
        L: proven_storage::LogStorage + Debug + 'static,
    {
        Self {
            name,
            client: Arc::new(ClientWrapperImpl { inner: client }),
            options,
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> Clone for EngineStream<T, D, S>
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
            name: self.name.clone(),
            client: self.client.clone(),
            options: self.options.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, D, S> Stream<T, D, S> for EngineStream<T, D, S>
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
    type Options = EngineStreamOptions;
    type Initialized = InitializedEngineStream<T, D, S>;
    type Subject = EngineMessagingSubject<T, D, S>;

    fn new<K>(_stream_name: K, _options: Self::Options) -> Self
    where
        K: Clone + Into<String> + Send,
    {
        panic!(
            "EngineStream::new should not be called directly, use new() with engine client instead"
        )
    }

    async fn init(&self) -> Result<Self::Initialized, MessagingEngineError> {
        // Ensure the stream exists
        let stream_config = self.options.stream_config.clone().unwrap_or_default();

        if !self.client.stream_exists(&self.name).await? {
            self.client
                .create_stream(self.name.clone(), stream_config)
                .await?;
            info!("Created stream: {}", self.name);
        }

        Ok(InitializedEngineStream {
            name: self.name.clone(),
            client: self.client.clone(),
            options: self.options.clone(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            _marker: PhantomData,
        })
    }

    async fn init_with_subjects<J>(
        &self,
        _subjects: Vec<J>,
    ) -> Result<Self::Initialized, MessagingEngineError>
    where
        J: Into<Self::Subject> + Clone + Send,
    {
        // Initialize the stream first
        let initialized = self.init().await?;

        // TODO: Implement subject-based routing if needed
        // For now, just return the initialized stream
        Ok(initialized)
    }
}

macro_rules! impl_scoped_stream {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Debug)]
            pub struct [< EngineStream $index >]<T, D, S>
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
                name: String,
                client: Arc<dyn ClientWrapper>,
                options: EngineStreamOptions,
                _marker: PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [< EngineStream $index >]<T, D, S>
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
                        name: self.name.clone(),
                        client: self.client.clone(),
                        options: self.options.clone(),
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> [< EngineStream $index >]<T, D, S>
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
                /// Creates a new scoped engine stream
                #[must_use] pub fn new<Tr, G, L>(
                    name: String,
                    client: EngineClient<Tr, G, L>,
                    options: EngineStreamOptions,
                ) -> Self
                where
                    Tr: proven_transport::Transport + Debug + 'static,
                    G: proven_topology::TopologyAdaptor + 'static,
                    L: proven_storage::LogStorage + Debug + 'static,
                {
                    Self {
                        name,
                        client: Arc::new(ClientWrapperImpl { inner: client }),
                        options,
                        _marker: PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T, D, S> [< Stream $index >]<T, D, S> for [< EngineStream $index >]<T, D, S>
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
                type Options = EngineStreamOptions;
                type Scoped = $parent<T, D, S>;

                fn scope<K>(&self, scope: K) -> $parent<T, D, S>
                where
                    K: AsRef<str> + Send,
                {
                    $parent::<T, D, S> {
                        name: format!("{}_{}", self.name, scope.as_ref()),
                        client: self.client.clone(),
                        options: self.options.clone(),
                        _marker: PhantomData,
                    }
                }
            }
        }
    };
}

impl_scoped_stream!(1, EngineStream, Stream1, "A single-scoped engine stream.");

impl_scoped_stream!(2, EngineStream1, Stream1, "A double-scoped engine stream.");

impl_scoped_stream!(3, EngineStream2, Stream2, "A triple-scoped engine stream.");

// TODO: Add tests for engine-based messaging
