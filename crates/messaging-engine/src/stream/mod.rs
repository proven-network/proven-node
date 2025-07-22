//! Engine-based streams for messaging.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::NonZero;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_engine::consensus::GroupResponse;
use proven_logger::{info, warn};
use tokio::sync::RwLock;

use proven_engine::{Client as EngineClient, StreamConfig};
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
pub struct EngineStreamOptions<T, G, St>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
{
    /// Optional stream configuration
    pub stream_config: Option<StreamConfig>,
    /// Engine client
    pub client: EngineClient<T, G, St>,
}

impl<T, G, St> Clone for EngineStreamOptions<T, G, St>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
{
    fn clone(&self) -> Self {
        Self {
            stream_config: self.stream_config.clone(),
            client: self.client.clone(),
        }
    }
}

impl<T, G, St> Default for EngineStreamOptions<T, G, St>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
{
    fn default() -> Self {
        panic!("EngineStreamOptions requires a client to be provided")
    }
}

impl<T, G, St> EngineStreamOptions<T, G, St>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
{
    /// Create new engine stream options with an engine client
    #[must_use]
    pub const fn new(client: EngineClient<T, G, St>, stream_config: Option<StreamConfig>) -> Self {
        Self {
            stream_config,
            client,
        }
    }
}

impl<T, G, St> Debug for EngineStreamOptions<T, G, St>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineStreamOptions")
            .field("stream_config", &self.stream_config)
            .field("client", &"<EngineClient>")
            .finish()
    }
}

impl<T, G, St> StreamOptions for EngineStreamOptions<T, G, St>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
{
}

/// An initialized engine stream.
pub struct InitializedEngineStream<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    client: EngineClient<Tr, G, St>,

    /// Stream options.
    options: EngineStreamOptions<Tr, G, St>,

    /// Local cache of stream data.
    cache: Arc<RwLock<HashMap<u64, T>>>,

    /// Type markers.
    _marker: PhantomData<(T, D, S)>,
}

impl<Tr, G, St, T, D, S> Debug for InitializedEngineStream<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitializedEngineStream")
            .field("name", &self.name)
            .field("client", &"<EngineClient>")
            .field("options", &"<EngineStreamOptions>")
            .field("cache", &"<Cache>")
            .finish()
    }
}

impl<Tr, G, St, T, D, S> Clone for InitializedEngineStream<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
impl<Tr, G, St, T, D, S> InitializedStream<T, D, S> for InitializedEngineStream<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    type Options = EngineStreamOptions<Tr, G, St>;
    type Subject = EngineMessagingSubject<Tr, G, St, T, D, S>;

    type Client<X>
        = EngineMessagingClient<Tr, G, St, X, T, D, S>
    where
        X: proven_messaging::service_handler::ServiceHandler<T, D, S>;

    type Consumer<X>
        = EngineMessagingConsumer<Tr, G, St, X, T, D, S>
    where
        X: proven_messaging::consumer_handler::ConsumerHandler<T, D, S>;

    type Service<X>
        = EngineMessagingService<Tr, G, St, X, T, D, S>
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
        // Delete from engine
        let response = self
            .client
            .delete_message(self.name.clone(), NonZero::new(seq).unwrap())
            .await
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))?;

        match response {
            proven_engine::consensus::group::GroupResponse::Deleted { .. }
            | proven_engine::consensus::group::GroupResponse::Success => {
                // Also remove from local cache
                self.cache.write().await.remove(&seq);
                info!("Deleted message {} from stream '{}'", seq, self.name);
                Ok(())
            }
            proven_engine::consensus::group::GroupResponse::Error { message } => {
                Err(MessagingEngineError::Engine(format!(
                    "Failed to delete message {seq} from stream '{}': {message}",
                    self.name
                )))
            }
            _ => Err(MessagingEngineError::Engine(format!(
                "Unexpected response from delete message {seq} from stream '{}': {response:?}",
                self.name
            ))),
        }
    }

    /// Gets a message by sequence number.
    async fn get(&self, seq: u64) -> Result<Option<T>, Self::Error> {
        // Query from engine storage first to ensure we get the latest state
        let messages = self
            .client
            .read_stream(
                self.name.clone(),
                NonZero::new(seq).unwrap(),
                NonZero::new(1).unwrap(),
            )
            .await
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))?;

        if let Some(stored_msg) = messages.first() {
            let bytes = stored_msg.data.payload.clone();
            match T::try_from(bytes) {
                Ok(message) => {
                    // Cache the message for future access
                    self.cache.write().await.insert(seq, message.clone());
                    Ok(Some(message))
                }
                Err(e) => {
                    warn!("Failed to deserialize message at seq {seq}: {e:?}");
                    Ok(None)
                }
            }
        } else {
            // Message not found in storage - ensure it's not in cache either
            self.cache.write().await.remove(&seq);
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
        // Get stream info from engine
        if let Some(info) = self
            .client
            .get_stream_info(&self.name)
            .await
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))?
        {
            // The StreamInfo has a last_sequence field
            Ok(info.last_sequence.get())
        } else {
            // Stream doesn't exist or no messages yet
            Ok(0)
        }
    }

    /// Gets the total number of messages.
    async fn messages(&self) -> Result<u64, Self::Error> {
        // Get stream info from engine
        if let Some(info) = self
            .client
            .get_stream_info(&self.name)
            .await
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))?
        {
            // Use the accurate message_count from StreamInfo
            Ok(info.message_count)
        } else {
            // Stream doesn't exist or no messages yet
            Ok(0)
        }
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
        let response = self
            .client
            .publish(self.name.clone(), bytes.to_vec(), None)
            .await
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))?;

        // Extract sequence number from GroupResponse
        let GroupResponse::Appended {
            stream: _,
            sequence: seq,
            ..
        } = response
        else {
            return Err(MessagingEngineError::Engine(format!(
                "Unexpected response from publish to stream '{}': expected Appended, got {response:?}",
                self.name
            )));
        };

        // Cache the message locally
        self.cache.write().await.insert(seq.get(), message);

        info!(
            "Published message to stream '{}' at sequence {}",
            self.name, seq
        );
        Ok(seq.get())
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

impl<Tr, G, St, T, D, S> InitializedEngineStream<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
        let response = self
            .client
            .publish(self.name.clone(), message.to_vec(), Some(metadata))
            .await
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))?;

        // Extract sequence number from GroupResponse
        let GroupResponse::Appended {
            stream: _,
            sequence: seq,
            ..
        } = response
        else {
            return Err(MessagingEngineError::Engine(format!(
                "Unexpected response from publish to stream '{}': expected Appended, got {response:?}",
                self.name
            )));
        };

        info!(
            "Published message with metadata to stream '{}' at sequence {}",
            self.name, seq
        );

        Ok(seq.get())
    }
}

/// An engine-backed stream.
pub struct EngineStream<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    options: EngineStreamOptions<Tr, G, St>,
    _marker: PhantomData<(T, D, S)>,
}

impl<Tr, G, St, T, D, S> Debug for EngineStream<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineStream")
            .field("name", &self.name)
            .field("options", &"<EngineStreamOptions>")
            .finish()
    }
}

impl<Tr, G, St, T, D, S> Clone for EngineStream<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
            options: self.options.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<Tr, G, St, T, D, S> Stream<T, D, S> for EngineStream<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    type Options = EngineStreamOptions<Tr, G, St>;
    type Initialized = InitializedEngineStream<Tr, G, St, T, D, S>;
    type Subject = EngineMessagingSubject<Tr, G, St, T, D, S>;

    fn new<K>(stream_name: K, options: Self::Options) -> Self
    where
        K: Clone + Into<String> + Send,
    {
        Self {
            name: stream_name.into(),
            options,
            _marker: PhantomData,
        }
    }

    async fn init(&self) -> Result<Self::Initialized, MessagingEngineError> {
        // Get the client from options
        let client = &self.options.client;

        // Ensure the stream exists
        let stream_config = self.options.stream_config.clone().unwrap_or_default();

        let stream_exists = client
            .get_stream_info(&self.name)
            .await
            .map_err(|e| MessagingEngineError::Engine(e.to_string()))?
            .is_some();

        if !stream_exists {
            client
                .create_stream(self.name.clone(), stream_config)
                .await
                .map_err(|e| MessagingEngineError::Engine(e.to_string()))?;
            info!("Created stream: {}", self.name);
        }

        Ok(InitializedEngineStream {
            name: self.name.clone(),
            client: client.clone(),
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
            pub struct [< EngineStream $index >]<Tr, G, St, T, D, S>
            where
                Tr: proven_transport::Transport + 'static,
                G: proven_topology::TopologyAdaptor + 'static,
                St: proven_storage::StorageAdaptor + 'static,
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
                options: EngineStreamOptions<Tr, G, St>,
                _marker: PhantomData<(T, D, S)>,
            }

            impl<Tr, G, St, T, D, S> [< EngineStream $index >]<Tr, G, St, T, D, S>
            where
                Tr: proven_transport::Transport + 'static,
                G: proven_topology::TopologyAdaptor + 'static,
                St: proven_storage::StorageAdaptor + 'static,
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
                /// Create a new engine stream.
                #[must_use]
                pub const fn new(name: String, options: EngineStreamOptions<Tr, G, St>) -> Self {
                    Self {
                        name,
                        options,
                        _marker: PhantomData,
                    }
                }
            }

            impl<Tr, G, St, T, D, S> Debug for [< EngineStream $index >]<Tr, G, St, T, D, S>
            where
                Tr: proven_transport::Transport + 'static,
                G: proven_topology::TopologyAdaptor + 'static,
                St: proven_storage::StorageAdaptor + 'static,
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
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.debug_struct(stringify!([< EngineStream $index >]))
                        .field("name", &self.name)
                        .field("options", &"<EngineStreamOptions>")
                        .finish()
                }
            }

            impl<Tr, G, St, T, D, S> Clone for [< EngineStream $index >]<Tr, G, St, T, D, S>
            where
                Tr: proven_transport::Transport + 'static,
                G: proven_topology::TopologyAdaptor + 'static,
                St: proven_storage::StorageAdaptor + 'static,
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
                        options: self.options.clone(),
                        _marker: PhantomData,
                    }
                }
            }


            #[async_trait]
            impl<Tr, G, St, T, D, S> [< Stream $index >]<T, D, S> for [< EngineStream $index >]<Tr, G, St, T, D, S>
            where
                Tr: proven_transport::Transport + 'static,
                G: proven_topology::TopologyAdaptor + 'static,
                St: proven_storage::StorageAdaptor + 'static,
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
                type Options = EngineStreamOptions<Tr, G, St>;
                type Scoped = $parent<Tr, G, St, T, D, S>;

                fn scope<K>(&self, scope: K) -> $parent<Tr, G, St, T, D, S>
                where
                    K: AsRef<str> + Send,
                {
                    $parent::<Tr, G, St, T, D, S> {
                        name: format!("{}_{}", self.name, scope.as_ref()),
                        options: self.options.clone(),
                        _marker: PhantomData,
                    }
                }
            }
        }
    };
}

impl_scoped_stream!(1, EngineStream, Stream, "A single-scoped engine stream.");

impl_scoped_stream!(2, EngineStream1, Stream1, "A double-scoped engine stream.");

impl_scoped_stream!(3, EngineStream2, Stream2, "A triple-scoped engine stream.");
