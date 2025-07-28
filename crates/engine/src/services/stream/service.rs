//! Stream service implementation
//!
//! This service manages stream storage and provides stream operations following
//! the established service patterns in the consensus engine.

use std::pin::Pin;
use std::sync::Arc;

use dashmap::DashMap;
use proven_attestation::Attestor;
use proven_network::NetworkManager;
use proven_storage::{
    LogIndex, LogStorage, StorageAdaptor, StorageManager, StorageNamespace, StreamStorage,
};
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;
use tokio::sync::{RwLock, oneshot, watch};
use tokio_stream::{Stream, StreamExt};
use tracing::{info, warn};

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::events::EventBus;
use crate::foundation::{
    Message, PersistenceType, RoutingTable, StreamConfig, StreamName, traits::ServiceLifecycle,
};
use crate::services::stream::internal::storage::{StreamStorageImpl, StreamStorageReader};

/// Type alias for stream storage map
type StreamStorageMap<S> =
    DashMap<StreamName, Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>>;

/// Stream service configuration
#[derive(Debug, Clone)]
pub struct StreamServiceConfig {
    /// Default persistence type for new streams
    pub default_persistence: PersistenceType,
}

impl Default for StreamServiceConfig {
    fn default() -> Self {
        Self {
            default_persistence: PersistenceType::Persistent,
        }
    }
}

/// Stream service for managing stream storage and operations
pub struct StreamService<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    /// Service configuration
    config: StreamServiceConfig,

    /// Stream storage instances by stream name
    pub(crate) streams: Arc<StreamStorageMap<S>>,

    /// Stream configurations
    stream_configs: Arc<DashMap<StreamName, StreamConfig>>,

    /// Stream append notifiers - watchers are notified when new messages are appended
    /// The value is the latest sequence number in the stream
    stream_notifiers: Arc<DashMap<StreamName, watch::Sender<LogIndex>>>,

    /// Storage manager for persistent streams
    storage_manager: Arc<StorageManager<S>>,

    /// Event bus reference
    event_bus: Arc<EventBus>,

    /// Routing table for determining stream locations
    routing_table: Option<Arc<RoutingTable>>,

    /// Network manager for forwarding requests
    network_manager: Option<Arc<NetworkManager<T, G, A>>>,

    /// Local node ID
    node_id: Option<NodeId>,

    /// Service running state
    is_running: Arc<RwLock<bool>>,

    /// Shutdown signal
    shutdown: Arc<RwLock<Option<oneshot::Sender<()>>>>,

    /// Background task handles
    background_tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
}

impl<T, G, A, S> StreamService<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new stream service
    pub fn new(
        config: StreamServiceConfig,
        node_id: NodeId,
        storage_manager: Arc<StorageManager<S>>,
        network_manager: Arc<NetworkManager<T, G, A>>,
        routing_table: Arc<RoutingTable>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            config,
            streams: Arc::new(DashMap::new()),
            stream_configs: Arc::new(DashMap::new()),
            stream_notifiers: Arc::new(DashMap::new()),
            storage_manager,
            event_bus,
            routing_table: Some(routing_table),
            network_manager: Some(network_manager),
            node_id: Some(node_id),
            is_running: Arc::new(RwLock::new(false)),
            shutdown: Arc::new(RwLock::new(None)),
            background_tasks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Get stream storage for reading
    pub async fn get_stream(
        &self,
        stream_name: &StreamName,
    ) -> Option<Arc<StreamStorageImpl<StreamStorage<S>>>> {
        // First check if we have it in memory
        if let Some(storage) = self.streams.get(stream_name) {
            return Some(storage.clone());
        }

        // Get stream config to determine persistence type
        let persistence_type = self
            .stream_configs
            .get(stream_name)
            .map(|entry| entry.persistence_type);

        // For persistent streams (or if we don't know the type), try to create storage on demand
        match persistence_type {
            Some(PersistenceType::Persistent) | None => {
                // Try to access the storage - if the stream exists in storage, we can read from it
                let namespace =
                    proven_storage::StorageNamespace::new(format!("stream_{stream_name}"));

                // Check if there's any data in this namespace
                let stream_storage = self.storage_manager.stream_storage();
                match stream_storage.bounds(&namespace).await {
                    Ok(Some(_)) => {
                        // Stream exists in storage, create the storage wrapper
                        let storage = Arc::new(StreamStorageImpl::persistent(
                            stream_name.clone(),
                            self.storage_manager.stream_storage(),
                            namespace,
                        ));

                        // Store it for future use
                        self.streams.insert(stream_name.clone(), storage.clone());

                        // Also create a notifier if it doesn't exist
                        if !self.stream_notifiers.contains_key(stream_name) {
                            let (tx, _rx) = watch::channel(LogIndex::new(1).unwrap());
                            self.stream_notifiers.insert(stream_name.clone(), tx);
                        }

                        Some(storage)
                    }
                    _ => {
                        // No data found in storage
                        None
                    }
                }
            }
            Some(PersistenceType::Ephemeral) => {
                // Ephemeral streams don't persist, so return None if not in memory
                None
            }
        }
    }

    /// Get stream configuration
    pub async fn get_stream_config(&self, stream_name: &StreamName) -> Option<StreamConfig> {
        self.stream_configs
            .get(stream_name)
            .map(|entry| entry.clone())
    }

    /// Read messages from a stream
    pub async fn read_messages(
        &self,
        stream_name: &str,
        start_sequence: LogIndex,
        count: LogIndex,
    ) -> ConsensusResult<Vec<Message>> {
        let stream_name = StreamName::new(stream_name);

        // Try to get the stream storage (which will check persistent storage)
        if let Some(stream_storage) = self.get_stream(&stream_name).await {
            // Read the requested range
            let end_sequence = LogIndex::new(start_sequence.get().saturating_add(count.get()))
                .unwrap_or(start_sequence);
            return stream_storage
                .read_range(start_sequence, end_sequence)
                .await
                .map_err(|e| Error::with_context(ErrorKind::Storage, e.to_string()));
        }

        // If get_stream returned None, the stream doesn't exist
        Err(Error::with_context(
            ErrorKind::NotFound,
            format!("Stream {stream_name} not found"),
        ))
    }

    /// Stream messages from a stream
    ///
    /// Returns a stream of messages starting from the given sequence.
    /// If end_sequence is None, streams until the last available message.
    pub async fn stream_messages(
        &self,
        stream_name: &str,
        start_sequence: LogIndex,
        end_sequence: Option<LogIndex>,
    ) -> ConsensusResult<Pin<Box<dyn Stream<Item = ConsensusResult<(Message, u64, u64)>> + Send>>>
    {
        let stream_name_str = stream_name.to_string();
        let stream_name = StreamName::new(stream_name);

        // Check if the stream exists and get its config
        let stream_config = self.get_stream_config(&stream_name).await;

        // Default to persistent if we don't have config (for restored streams)
        let persistence_type = stream_config
            .map(|c| c.persistence_type)
            .unwrap_or(PersistenceType::Persistent);

        // For persistent streams, use storage streaming
        if persistence_type == PersistenceType::Persistent {
            // Use proven_storage::LogStorageStreaming if available
            let namespace = StorageNamespace::new(format!("stream_{stream_name_str}"));
            let stream_storage = self.storage_manager.stream_storage();

            // Check if the storage supports streaming
            if let Ok(storage_stream) = proven_storage::LogStorageStreaming::stream_range(
                &stream_storage,
                &namespace,
                start_sequence,
                end_sequence,
            )
            .await
            {
                // Convert storage stream to (Message, timestamp, sequence) stream
                let mapped_stream = storage_stream.map(move |result| {
                    result
                        .and_then(|(_seq, bytes)| {
                            match crate::foundation::deserialize_entry(&bytes) {
                                Ok((message, timestamp, sequence)) => {
                                    Ok((message, timestamp, sequence))
                                }
                                Err(e) => {
                                    Err(proven_storage::StorageError::InvalidValue(e.to_string()))
                                }
                            }
                        })
                        .map_err(|e| Error::with_context(ErrorKind::Storage, e.to_string()))
                });

                return Ok(Box::pin(mapped_stream));
            }
        }

        // Fallback to batch reading for ephemeral streams or if streaming not supported
        let storage = match self.get_stream(&stream_name).await {
            Some(s) => s,
            None => {
                return Err(Error::with_context(
                    ErrorKind::NotFound,
                    format!("Stream {stream_name} not found"),
                ));
            }
        };

        // Create a stream that reads in batches
        let batch_size = 100;
        let stream = async_stream::stream! {
            let mut current_seq = start_sequence;

            loop {
                let batch_end = match end_sequence {
                    Some(end) => {
                        let next = LogIndex::new(current_seq.get().saturating_add(batch_size)).unwrap_or(current_seq);
                        if next.get() > end.get() { end } else { next }
                    },
                    None => LogIndex::new(current_seq.get().saturating_add(batch_size)).unwrap_or(current_seq),
                };

                match storage.read_range_with_metadata(current_seq, batch_end).await {
                    Ok(messages) => {
                        if messages.is_empty() {
                            // No more messages
                            break;
                        }

                        for (msg, timestamp, sequence) in messages {
                            current_seq = LogIndex::new(current_seq.get().saturating_add(1)).unwrap_or(current_seq);
                            yield Ok((msg, timestamp, sequence));
                        }

                        // If we've reached the end sequence, stop
                        if let Some(end) = end_sequence && current_seq >= end {
                            break;
                        }
                    }
                    Err(e) => {
                        yield Err(Error::with_context(ErrorKind::Storage, e.to_string()));
                        break;
                    }
                }
            }
        };

        Ok(Box::pin(stream))
    }

    /// Read messages in a range
    pub async fn read_range(
        &self,
        stream_name: &StreamName,
        start: LogIndex,
        end: LogIndex,
    ) -> ConsensusResult<Vec<Message>> {
        // Read messages and convert to MessageData
        let count = LogIndex::new(end.get().saturating_sub(start.get()))
            .unwrap_or(LogIndex::new(1).unwrap());
        let messages = self
            .read_messages(stream_name.as_str(), start, count)
            .await?;
        Ok(messages)
    }

    /// Read messages from a start sequence as a stream
    pub async fn read_from(
        &self,
        stream_name: &StreamName,
        start: LogIndex,
    ) -> ConsensusResult<Pin<Box<dyn Stream<Item = (Message, u64, u64)> + Send>>> {
        // Use stream_messages with no end sequence
        let stream = self
            .stream_messages(stream_name.as_str(), start, None)
            .await?;
        let mapped_stream =
            futures::StreamExt::filter_map(stream, |result| futures::future::ready(result.ok()));
        Ok(Box::pin(mapped_stream))
    }

    /// Register message handlers for network communication
    async fn register_message_handlers(&self) -> ConsensusResult<()> {
        if let Some(network_manager) = &self.network_manager {
            use super::handler::StreamHandler;

            let handler = StreamHandler::new(Arc::new(self.clone()));
            network_manager
                .register_service(handler)
                .await
                .map_err(|e| {
                    Error::with_context(
                        ErrorKind::Network,
                        format!("Failed to register stream service: {e}"),
                    )
                })?;
        }

        Ok(())
    }

    /// Check if a stream can be served locally (we're in the group that owns it)
    pub async fn can_serve_stream(&self, stream_name: &StreamName) -> ConsensusResult<bool> {
        // Get stream's group from routing table
        if let Some(routing_table) = &self.routing_table
            && let Ok(Some(stream_route)) =
                routing_table.get_stream_route(stream_name.as_str()).await
        {
            // Check if we're in that group
            return routing_table
                .is_group_local(stream_route.group_id)
                .await
                .map_err(|e| {
                    Error::with_context(
                        ErrorKind::Internal,
                        format!("Failed to check group location: {e}"),
                    )
                });
        }
        // If no routing info, assume we can't serve it
        Ok(false)
    }
}

// Implement Clone for StreamService to match other services
impl<T, G, A, S> Clone for StreamService<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            streams: self.streams.clone(),
            stream_configs: self.stream_configs.clone(),
            stream_notifiers: self.stream_notifiers.clone(),
            storage_manager: self.storage_manager.clone(),
            event_bus: self.event_bus.clone(),
            network_manager: self.network_manager.clone(),
            routing_table: self.routing_table.clone(),
            node_id: self.node_id.clone(),
            is_running: self.is_running.clone(),
            shutdown: self.shutdown.clone(),
            background_tasks: self.background_tasks.clone(),
        }
    }
}

#[async_trait::async_trait]
impl<T, G, A, S> ServiceLifecycle for StreamService<T, G, A, S>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
    S: StorageAdaptor + Send + Sync + 'static,
{
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        info!("Starting StreamService");

        // Check if already running
        if *self.is_running.read().await {
            return Err(Error::with_context(
                ErrorKind::InvalidState,
                "StreamService is already running",
            ));
        }

        // Register command handlers
        use crate::services::stream::command_handlers;
        use crate::services::stream::commands::*;

        // Register PersistMessages handler for group consensus
        let persist_handler = command_handlers::PersistMessagesHandler::new(
            self.streams.clone(),
            self.storage_manager.clone(),
            self.stream_notifiers.clone(),
        );
        self.event_bus
            .handle_requests::<PersistMessages, _>(persist_handler)
            .expect("Failed to register PersistMessages handler");

        // Register CreateStream handler
        let create_handler = command_handlers::CreateStreamHandler::new(
            self.streams.clone(),
            self.stream_configs.clone(),
            self.stream_notifiers.clone(),
            self.storage_manager.clone(),
            self.config.default_persistence,
        );
        self.event_bus
            .handle_requests::<CreateStream, _>(create_handler)
            .expect("Failed to register CreateStream handler");

        // Register DeleteStream handler
        let delete_handler = command_handlers::DeleteStreamHandler::new(
            self.streams.clone(),
            self.stream_configs.clone(),
            self.stream_notifiers.clone(),
        );
        self.event_bus
            .handle_requests::<DeleteStream, _>(delete_handler)
            .expect("Failed to register DeleteStream handler");

        // Register StreamMessages streaming handler (needs network dependencies)
        if let (Some(node_id), Some(routing_table), Some(network_manager)) = (
            self.node_id.clone(),
            self.routing_table.clone(),
            self.network_manager.clone(),
        ) {
            let stream_handler = command_handlers::StreamMessagesHandler::new(
                node_id,
                self.streams.clone(),
                self.stream_notifiers.clone(),
                routing_table,
                network_manager,
            );
            self.event_bus
                .handle_streams::<StreamMessages, _>(stream_handler)
                .expect("Failed to register StreamMessages handler");
        }

        info!("StreamService: Registered command handlers");

        // Register network message handlers
        self.register_message_handlers().await?;

        // Mark as running
        *self.is_running.write().await = true;

        // Set up shutdown channel
        let (shutdown_tx, _shutdown_rx) = oneshot::channel();
        *self.shutdown.write().await = Some(shutdown_tx);

        info!("StreamService started successfully");
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        info!("Stopping StreamService");

        // Check if running
        if !*self.is_running.read().await {
            return Ok(());
        }

        // Unregister from network service
        if let Some(network_manager) = &self.network_manager {
            let _ = network_manager.unregister_service("stream").await;
        }

        // Unregister all event handlers to allow re-registration on restart
        use crate::services::stream::commands::*;

        let _ = self
            .event_bus
            .unregister_request_handler::<PersistMessages>();
        let _ = self.event_bus.unregister_request_handler::<CreateStream>();
        let _ = self.event_bus.unregister_request_handler::<DeleteStream>();
        let _ = self.event_bus.unregister_stream_handler::<StreamMessages>();

        // Signal shutdown
        if let Some(shutdown_tx) = self.shutdown.write().await.take() {
            let _ = shutdown_tx.send(());
        }

        // Wait for background tasks to complete
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            if let Err(e) = task.await {
                warn!("Error waiting for background task: {}", e);
            }
        }

        // Clear all stream storage instances to release storage references
        self.streams.clear();
        self.stream_configs.clear();
        self.stream_notifiers.clear();

        // Mark as not running
        *self.is_running.write().await = false;

        info!("StreamService stopped");
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        *self.is_running.read().await
    }

    async fn status(&self) -> crate::foundation::traits::lifecycle::ServiceStatus {
        use crate::foundation::traits::lifecycle::ServiceStatus;
        if *self.is_running.read().await {
            ServiceStatus::Running
        } else {
            ServiceStatus::Stopped
        }
    }
}
