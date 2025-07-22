//! Network manager for handling message routing and peer communication

use crate::error::{NetworkError, NetworkResult};
use crate::message::ServiceMessage;
use crate::peer::Peer;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use proven_bootable::Bootable;
use proven_logger::{debug, error, info, warn};
use proven_topology::TopologyAdaptor;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::{Transport, TransportEnvelope};
use tokio::sync::{RwLock, oneshot};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

/// Pending request tracking
struct PendingRequest {
    tx: oneshot::Sender<Bytes>,
    sent_at: std::time::Instant,
}

/// Type-erased service handler
#[async_trait]
trait ServiceHandler: Send + Sync {
    /// Handle a message and return a response
    async fn handle(
        &self,
        sender: NodeId,
        payload: Bytes,
        correlation_id: Option<Uuid>,
    ) -> NetworkResult<Option<Bytes>>;
}

/// Concrete implementation of a service handler
struct TypedServiceHandler<M, F>
where
    M: ServiceMessage,
    F: Fn(NodeId, M) -> std::pin::Pin<Box<dyn Future<Output = NetworkResult<M::Response>> + Send>>
        + Send
        + Sync,
{
    handler: F,
    _phantom: std::marker::PhantomData<M>,
}

#[async_trait]
impl<M, F> ServiceHandler for TypedServiceHandler<M, F>
where
    M: ServiceMessage,
    F: Fn(NodeId, M) -> std::pin::Pin<Box<dyn Future<Output = NetworkResult<M::Response>> + Send>>
        + Send
        + Sync,
{
    async fn handle(
        &self,
        sender: NodeId,
        payload: Bytes,
        _correlation_id: Option<Uuid>,
    ) -> NetworkResult<Option<Bytes>> {
        // Deserialize the message
        let message: M = ciborium::from_reader(payload.as_ref()).map_err(|e| {
            NetworkError::Serialization(format!("Failed to deserialize message: {e}"))
        })?;

        // Call the handler
        let response = (self.handler)(sender, message).await?;

        // Serialize the response
        let mut bytes = Vec::new();
        ciborium::into_writer(&response, &mut bytes).map_err(|e| {
            NetworkError::Serialization(format!("Failed to serialize response: {e}"))
        })?;

        Ok(Some(Bytes::from(bytes)))
    }
}

/// Network manager for handling message routing and peer communication
pub struct NetworkManager<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Our local node ID
    local_node_id: NodeId,
    /// Transport layer
    pub(crate) transport: Arc<T>,
    /// Topology manager
    topology: Arc<TopologyManager<G>>,
    /// Pending requests waiting for responses
    pending_requests: Arc<RwLock<DashMap<Uuid, PendingRequest>>>,
    /// Service handlers
    service_handlers: Arc<RwLock<HashMap<&'static str, Arc<dyn ServiceHandler>>>>,
    /// Task tracker for background tasks
    task_tracker: TaskTracker,
    /// Cancellation token for graceful shutdown
    cancellation_token: CancellationToken,
}

impl<T, G> NetworkManager<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Create a new network manager
    pub fn new(
        local_node_id: NodeId,
        transport: Arc<T>,
        topology: Arc<TopologyManager<G>>,
    ) -> Self {
        Self {
            local_node_id,
            transport,
            topology,
            pending_requests: Arc::new(RwLock::new(DashMap::new())),
            service_handlers: Arc::new(RwLock::new(HashMap::new())),
            task_tracker: TaskTracker::new(),
            cancellation_token: CancellationToken::new(),
        }
    }

    /// Start the network manager background tasks
    pub async fn start_tasks(&self) -> NetworkResult<()> {
        // Start router task
        {
            let transport = self.transport.clone();
            let pending_requests = self.pending_requests.clone();
            let service_handlers = self.service_handlers.clone();
            let cancellation = self.cancellation_token.clone();

            self.task_tracker.spawn(async move {
                tokio::select! {
                    _ = Self::message_router_loop_static(transport, pending_requests, service_handlers) => {
                        error!("Message router loop exited unexpectedly");
                    }
                    _ = cancellation.cancelled() => {
                        debug!("Message router loop cancelled");
                    }
                }
            });
        }

        // Start cleanup task
        {
            let pending_requests = self.pending_requests.clone();
            let cancellation = self.cancellation_token.clone();

            self.task_tracker.spawn(async move {
                tokio::select! {
                    _ = Self::cleanup_loop_static(pending_requests) => {
                        error!("Cleanup loop exited unexpectedly");
                    }
                    _ = cancellation.cancelled() => {
                        debug!("Cleanup loop cancelled");
                    }
                }
            });
        }

        info!("Network manager started for node {}", self.local_node_id);
        Ok(())
    }

    /// Get local node ID
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Get a peer by node ID
    pub async fn get_peer(self: &Arc<Self>, node_id: &NodeId) -> NetworkResult<Peer<T, G>> {
        self.topology
            .get_node(node_id)
            .await
            .ok_or_else(|| NetworkError::PeerNotFound(node_id.to_string()))
            .map(|node| Peer::new(node, Arc::clone(self)))
    }

    /// Send a typed message to a peer
    pub async fn send<M>(&self, target: NodeId, message: M) -> NetworkResult<()>
    where
        M: crate::message::NetworkMessage,
    {
        let message_type = message.message_type().to_string();

        // Serialize the message
        let bytes = message.serialize()?;

        self.transport
            .send_envelope(&target, &bytes, &message_type, None)
            .await
            .map_err(NetworkError::Transport)
    }

    /// Send a typed request and wait for typed response
    pub async fn request<M>(
        &self,
        target: NodeId,
        message: M,
        timeout_duration: Duration,
    ) -> NetworkResult<M::Response>
    where
        M: crate::message::HandledMessage,
        M::Response: crate::message::NetworkMessage + serde::de::DeserializeOwned,
    {
        let correlation_id = Uuid::new_v4();
        let message_type = message.message_type().to_string();

        // Serialize the request
        let request_bytes = message.serialize()?;

        // Create oneshot channel for response
        let (tx, rx) = oneshot::channel();

        // Store pending request
        {
            let pending = self.pending_requests.read().await;
            pending.insert(
                correlation_id,
                PendingRequest {
                    tx,
                    sent_at: std::time::Instant::now(),
                },
            );
        }

        // Send the message
        // Send the request via transport
        self.transport
            .send_envelope(&target, &request_bytes, &message_type, Some(correlation_id))
            .await
            .map_err(NetworkError::Transport)?;

        // Wait for response with timeout
        let response_bytes = match timeout(timeout_duration, rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                self.cleanup_request(correlation_id).await;
                return Err(NetworkError::ChannelClosed(
                    "Response channel closed".to_string(),
                ));
            }
            Err(_) => {
                self.cleanup_request(correlation_id).await;
                return Err(NetworkError::Timeout(format!(
                    "Request to {target} timed out after {timeout_duration:?}"
                )));
            }
        };

        // Deserialize the response using CBOR
        ciborium::from_reader(response_bytes.as_ref()).map_err(|e| {
            NetworkError::Serialization(format!("Failed to deserialize response: {e}"))
        })
    }

    /// Register a service handler
    pub async fn register_service<M, F>(&self, handler: F) -> NetworkResult<()>
    where
        M: ServiceMessage,
        F: Fn(
                NodeId,
                M,
            )
                -> std::pin::Pin<Box<dyn Future<Output = NetworkResult<M::Response>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let service_id = M::service_id();

        // Check if already registered
        let mut handlers = self.service_handlers.write().await;
        if handlers.contains_key(service_id) {
            return Err(NetworkError::Internal(format!(
                "Service '{service_id}' already registered"
            )));
        }

        let typed_handler = TypedServiceHandler {
            handler,
            _phantom: std::marker::PhantomData,
        };

        handlers.insert(service_id, Arc::new(typed_handler));
        info!("Registered service handler for '{service_id}'");
        Ok(())
    }

    /// Unregister a service handler
    pub async fn unregister_service<M>(&self) -> NetworkResult<()>
    where
        M: ServiceMessage,
    {
        let service_id = M::service_id();

        let mut handlers = self.service_handlers.write().await;
        if handlers.remove(service_id).is_some() {
            info!("Unregistered service handler for '{service_id}'");
            Ok(())
        } else {
            warn!("Attempted to unregister non-existent service '{service_id}'");
            Ok(()) // Don't error on double-unregister
        }
    }

    /// Send a typed request to a service and wait for response
    pub async fn service_request<M>(
        &self,
        target: NodeId,
        message: M,
        timeout: Duration,
    ) -> NetworkResult<M::Response>
    where
        M: ServiceMessage,
    {
        let correlation_id = Uuid::new_v4();
        let service_id = M::service_id();

        // Serialize the message
        let mut message_bytes = Vec::new();
        ciborium::into_writer(&message, &mut message_bytes).map_err(|e| {
            NetworkError::Serialization(format!("Failed to serialize message: {e}"))
        })?;

        // Create oneshot channel for response
        let (tx, rx) = oneshot::channel();

        // Store pending request
        {
            let pending = self.pending_requests.read().await;
            pending.insert(
                correlation_id,
                PendingRequest {
                    tx,
                    sent_at: std::time::Instant::now(),
                },
            );
        }

        debug!(
            "Sending request to {target} for service '{service_id}' with correlation_id {correlation_id}"
        );

        // Send the request via transport
        self.transport
            .send_envelope(
                &target,
                &Bytes::from(message_bytes),
                service_id,
                Some(correlation_id),
            )
            .await
            .map_err(NetworkError::Transport)?;

        // Wait for response with timeout
        let response_bytes = match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                self.cleanup_request(correlation_id).await;
                return Err(NetworkError::ChannelClosed(
                    "Response channel closed".to_string(),
                ));
            }
            Err(_) => {
                self.cleanup_request(correlation_id).await;
                return Err(NetworkError::Timeout(format!(
                    "Request to {target} timed out after {timeout:?}"
                )));
            }
        };

        // Deserialize the response
        ciborium::from_reader(response_bytes.as_ref()).map_err(|e| {
            NetworkError::Serialization(format!("Failed to deserialize response: {e}"))
        })
    }

    /// Message router loop - processes incoming messages (static version)
    async fn message_router_loop_static(
        transport: Arc<T>,
        pending_requests: Arc<RwLock<DashMap<Uuid, PendingRequest>>>,
        service_handlers: Arc<RwLock<HashMap<&'static str, Arc<dyn ServiceHandler>>>>,
    ) {
        debug!("Message router loop started");
        let mut incoming = transport.incoming();

        while let Some(envelope) = incoming.next().await {
            debug!(
                "Received envelope from transport: sender={}, message_type={}, correlation_id={:?}",
                envelope.sender, envelope.message_type, envelope.correlation_id
            );
            let pending_requests = pending_requests.clone();
            let service_handlers = service_handlers.clone();
            let transport = transport.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::process_incoming_message_static(
                    envelope,
                    pending_requests,
                    service_handlers,
                    transport,
                )
                .await
                {
                    error!("Error processing incoming message: {e}");
                }
            });
        }

        debug!("Message router loop exited");
    }

    /// Process an incoming message (static version)
    async fn process_incoming_message_static(
        envelope: TransportEnvelope,
        pending_requests: Arc<RwLock<DashMap<Uuid, PendingRequest>>>,
        service_handlers: Arc<RwLock<HashMap<&'static str, Arc<dyn ServiceHandler>>>>,
        transport: Arc<T>,
    ) -> NetworkResult<()> {
        let TransportEnvelope {
            sender,
            payload: payload_bytes,
            message_type: service_id,
            correlation_id,
        } = envelope;

        // Check if this is a response to a pending request
        if let Some(correlation_id) = correlation_id {
            debug!("Received message with correlation_id {correlation_id} from {sender}");

            let pending = pending_requests.read().await;
            if let Some((_, request)) = pending.remove(&correlation_id) {
                debug!("Found pending request for correlation_id {correlation_id}");
                let _ = request.tx.send(payload_bytes);
                return Ok(());
            }

            debug!("No pending request found for correlation_id {correlation_id}");
        }

        // Try to find a service handler
        // Clone the handler Arc to avoid holding the read lock during execution
        let handler = {
            let handlers = service_handlers.read().await;
            handlers.get(service_id.as_str()).cloned()
        };

        if let Some(handler) = handler {
            debug!("Found handler for service '{service_id}', correlation_id: {correlation_id:?}");

            match handler
                .handle(sender.clone(), payload_bytes, correlation_id)
                .await
            {
                Ok(Some(response_bytes)) => {
                    if let Some(corr_id) = correlation_id {
                        debug!(
                            "Handler returned response, sending back to {sender} with correlation_id {corr_id}"
                        );
                        // Send response back with the same service_id and correlation_id
                        transport
                            .send_envelope(&sender, &response_bytes, &service_id, Some(corr_id))
                            .await
                            .map_err(NetworkError::Transport)?;
                    }
                }
                Ok(None) => {
                    // No response needed
                }
                Err(e) => {
                    warn!("Service handler error: {e}");
                }
            }
        } else {
            warn!(
                "No handler found for service '{service_id}', correlation_id: {correlation_id:?}"
            );
        }

        Ok(())
    }

    /// Cleanup old pending requests
    async fn cleanup_request(&self, correlation_id: Uuid) {
        let pending = self.pending_requests.read().await;
        pending.remove(&correlation_id);
    }

    /// Periodic cleanup of old pending requests (static version)
    async fn cleanup_loop_static(pending_requests: Arc<RwLock<DashMap<Uuid, PendingRequest>>>) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            let now = std::time::Instant::now();
            let pending = pending_requests.read().await;

            // Remove requests older than 5 minutes
            let old_requests: Vec<Uuid> = pending
                .iter()
                .filter(|entry| {
                    now.duration_since(entry.value().sent_at) > Duration::from_secs(300)
                })
                .map(|entry| *entry.key())
                .collect();

            for id in old_requests {
                pending.remove(&id);
            }
        }
    }

    /// Shutdown the network manager
    pub async fn shutdown(&self) -> NetworkResult<()> {
        info!("Shutting down network manager");

        // Cancel all tasks
        self.cancellation_token.cancel();

        // Close the task tracker and wait for all tasks to complete
        self.task_tracker.close();
        self.task_tracker.wait().await;

        Ok(())
    }
}

// Implement Bootable for NetworkManager
#[async_trait]
impl<T, G> Bootable for NetworkManager<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    fn bootable_name(&self) -> &str {
        "NetworkManager"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting NetworkManager as bootable service");
        self.start_tasks().await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Shutting down NetworkManager");

        // Cancel all tasks
        self.cancellation_token.cancel();

        // Shutdown transport
        self.transport.shutdown().await?;

        // Close the task tracker and wait for all tasks to complete
        self.task_tracker.close();

        match tokio::time::timeout(Duration::from_secs(5), self.task_tracker.wait()).await {
            Ok(()) => {
                debug!("NetworkManager tasks shut down cleanly");
            }
            Err(_) => {
                error!("NetworkManager tasks did not shut down within timeout");
            }
        }

        info!("NetworkManager shutdown complete");
        Ok(())
    }

    async fn wait(&self) {
        self.task_tracker.wait().await;
    }
}
