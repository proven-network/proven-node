//! Clients send requests to services in the engine messaging system.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use proven_logger::{debug, warn};
use tokio::sync::{Mutex as TokioMutex, oneshot};
use tokio::time::timeout;

use proven_messaging::client::{Client, ClientError, ClientOptions, ClientResponseType};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::InitializedStream;

use crate::error::MessagingEngineError;
use crate::stream::InitializedEngineStream;

/// Type alias for response map - maps request IDs to response channels
type ResponseMap<R> = HashMap<usize, oneshot::Sender<ClientResponseType<R>>>;

/// Options for engine messaging clients.
#[derive(Clone, Debug)]
pub struct EngineMessagingClientOptions {
    /// Timeout for requests.
    pub timeout: std::time::Duration,
}

impl Default for EngineMessagingClientOptions {
    fn default() -> Self {
        Self {
            timeout: std::time::Duration::from_secs(30),
        }
    }
}

impl ClientOptions for EngineMessagingClientOptions {}

/// Error type for engine messaging clients.
#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)] // TODO: Improve this
pub enum EngineMessagingClientError {
    /// Engine error.
    #[error("Engine error: {0}")]
    Engine(#[from] MessagingEngineError),
    /// No response received within timeout.
    #[error("No response received within timeout")]
    NoResponse,
    /// Serialization error.
    #[error("Serialization error")]
    Serialization,
}

impl ClientError for EngineMessagingClientError {}

/// An engine messaging client.
pub struct EngineMessagingClient<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    X: ServiceHandler<T, D, S>,
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
    stream: InitializedEngineStream<Tr, G, St, T, D, S>,
    options: EngineMessagingClientOptions,
    /// Counter for generating unique request IDs
    request_id_counter: Arc<AtomicUsize>,
    /// Map of pending requests to their response channels
    response_map: Arc<TokioMutex<ResponseMap<X::ResponseType>>>,
    /// Response stream name for this client
    response_stream_name: String,
    _marker: PhantomData<(Tr, G, St, X)>,
}

impl<Tr, G, St, X, T, D, S> Debug for EngineMessagingClient<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    X: ServiceHandler<T, D, S>,
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
        f.debug_struct("EngineMessagingClient")
            .field("name", &self.name)
            .field("stream", &self.stream)
            .field("options", &self.options)
            .field("request_id_counter", &self.request_id_counter)
            .field("response_stream_name", &self.response_stream_name)
            .finish_non_exhaustive()
    }
}

impl<Tr, G, St, X, T, D, S> Clone for EngineMessagingClient<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    X: ServiceHandler<T, D, S>,
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
            stream: self.stream.clone(),
            options: self.options.clone(),
            request_id_counter: self.request_id_counter.clone(),
            response_map: self.response_map.clone(),
            response_stream_name: self.response_stream_name.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<Tr, G, St, X, T, D, S> Client<X, T, D, S> for EngineMessagingClient<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    X: ServiceHandler<T, D, S>,
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
    type Error = EngineMessagingClientError;
    type Options = EngineMessagingClientOptions;
    type ResponseType = X::ResponseType;
    type StreamType = InitializedEngineStream<Tr, G, St, T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
    ) -> Result<Self, Self::Error> {
        let response_stream_name = format!("{name}_responses");

        Ok(Self {
            name,
            stream,
            options,
            request_id_counter: Arc::new(AtomicUsize::new(0)),
            response_map: Arc::new(TokioMutex::new(HashMap::new())),
            response_stream_name,
            _marker: PhantomData,
        })
    }

    async fn request(
        &self,
        request: T,
    ) -> Result<ClientResponseType<X::ResponseType>, Self::Error> {
        // Generate unique request ID
        let request_id = self.request_id_counter.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = oneshot::channel();

        // Store the response channel for correlation
        {
            let mut map = self.response_map.lock().await;
            map.insert(request_id, sender);
        }

        debug!("Sending request {} to engine stream", request_id);

        // Convert request to bytes
        let request_bytes: Bytes = request
            .try_into()
            .map_err(|_| EngineMessagingClientError::Serialization)?;

        // Publish request to engine stream with metadata
        let mut metadata = HashMap::new();
        metadata.insert("request_id".to_string(), request_id.to_string());
        metadata.insert(
            "response_stream".to_string(),
            self.response_stream_name.clone(),
        );
        metadata.insert("client_name".to_string(), self.name.clone());

        // Publish to engine stream with the request metadata
        let sequence = self
            .stream
            .publish_with_metadata(request_bytes, metadata)
            .await
            .map_err(EngineMessagingClientError::Engine)?;

        debug!(
            "Published request {} at sequence {} to engine stream - response expected on {}",
            request_id, sequence, self.response_stream_name
        );

        // Wait for response with timeout
        match timeout(self.options.timeout, receiver).await {
            Ok(Ok(response)) => {
                debug!("Received response for request {request_id}");
                Ok(response)
            }
            Ok(Err(_)) => {
                warn!("Response channel closed for request {request_id}");
                Err(EngineMessagingClientError::NoResponse)
            }
            Err(_) => {
                warn!(
                    "Request {} timed out after {:?}",
                    request_id, self.options.timeout
                );
                // Clean up response map on timeout
                let mut map = self.response_map.lock().await;
                map.remove(&request_id);
                drop(map);
                Err(EngineMessagingClientError::NoResponse)
            }
        }
    }

    async fn delete(&self, seq: u64) -> Result<(), Self::Error> {
        debug!("Deleting message at sequence {} from engine stream", seq);

        // Use the stream's delete method directly
        self.stream
            .delete(seq)
            .await
            .map_err(EngineMessagingClientError::Engine)?;

        debug!(
            "Successfully deleted message at sequence {} from engine stream",
            seq
        );
        Ok(())
    }
}
