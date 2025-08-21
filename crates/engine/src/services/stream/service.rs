//! Stream service implementation
//!
//! This service manages stream storage and provides stream operations following
//! the established service patterns in the consensus engine.

use std::sync::Arc;

use dashmap::DashMap;
use proven_attestation::Attestor;
use proven_network::NetworkManager;
use proven_storage::{StorageAdaptor, StorageManager};
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;
use tokio::sync::{RwLock, oneshot};
use tracing::{info, warn};

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::events::EventBus;
use crate::foundation::{
    PersistenceType, RoutingTable, StreamConfig, StreamName, traits::ServiceLifecycle,
};
use crate::services::stream::internal::registry::StreamRegistry;
use crate::services::stream::internal::storage::StreamStorageImpl;

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

    /// Register message handlers for network communication
    async fn register_message_handlers(&self) -> ConsensusResult<()> {
        if let Some(network_manager) = &self.network_manager {
            use super::handler::StreamHandler;
            use super::internal::registry::StreamRegistry;

            // Create a stream registry for the handler
            let stream_registry = Arc::new(StreamRegistry::new(
                self.streams.clone(),
                self.stream_configs.clone(),
                self.storage_manager.clone(),
            ));

            // Create handler with specific dependencies
            let handler = StreamHandler::new(
                stream_registry,
                self.routing_table.as_ref().unwrap().clone(),
                self.storage_manager.clone(),
            );

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
            storage_manager: self.storage_manager.clone(),
            event_bus: self.event_bus.clone(),
            network_manager: self.network_manager.clone(),
            routing_table: self.routing_table.clone(),
            node_id: self.node_id,
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
        );
        self.event_bus
            .handle_requests::<PersistMessages, _>(persist_handler)
            .expect("Failed to register PersistMessages handler");

        // Register CreateStream handler
        let create_handler = command_handlers::CreateStreamHandler::new(
            self.streams.clone(),
            self.stream_configs.clone(),
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
        );
        self.event_bus
            .handle_requests::<DeleteStream, _>(delete_handler)
            .expect("Failed to register DeleteStream handler");

        // Register RegisterStream handler
        let register_handler =
            command_handlers::RegisterStreamHandler::new(self.stream_configs.clone());
        self.event_bus
            .handle_requests::<RegisterStream, _>(register_handler)
            .expect("Failed to register RegisterStream handler");

        // Register StreamMessages streaming handler (needs network dependencies)
        if let (Some(node_id), Some(routing_table), Some(network_manager)) = (
            self.node_id,
            self.routing_table.clone(),
            self.network_manager.clone(),
        ) {
            // Create a stream registry for the handler
            let stream_registry = Arc::new(StreamRegistry::new(
                self.streams.clone(),
                self.stream_configs.clone(),
                self.storage_manager.clone(),
            ));

            let stream_handler = command_handlers::StreamMessagesHandler::new(
                node_id,
                stream_registry.clone(),
                routing_table,
                network_manager.clone(),
            );
            self.event_bus
                .handle_streams::<StreamMessages, _>(stream_handler)
                .expect("Failed to register StreamMessages handler");

            // Register the streaming service for handling incoming stream requests
            let streaming_service =
                crate::services::stream::streaming::StreamMessagesStreamingService::new(
                    stream_registry,
                );
            network_manager
                .register_streaming_service(streaming_service)
                .await
                .expect("Failed to register stream messages streaming service");
        }

        info!("StreamService: Registered command handlers and streaming service");

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
            let _ = network_manager
                .unregister_streaming_service("stream_messages")
                .await;
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
