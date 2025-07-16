//! Network manager for handling message routing and peer communication

use crate::error::{NetworkError, NetworkResult};
use crate::namespace::{MessageType, NamespaceManager};
use crate::peer::Peer;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use proven_bootable::Bootable;
use proven_topology::TopologyAdaptor;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::{Transport, TransportEnvelope};
use tokio::sync::{RwLock, oneshot};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Pending request tracking (legacy - will be removed)
struct PendingRequest {
    tx: oneshot::Sender<Bytes>,
    sent_at: std::time::Instant,
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
    /// Pending requests waiting for responses (legacy - will be removed)
    pending_requests: Arc<RwLock<DashMap<Uuid, PendingRequest>>>,
    /// Namespace manager
    namespace_manager: Arc<NamespaceManager>,
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
            namespace_manager: Arc::new(NamespaceManager::new()),
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
            let namespace_manager = self.namespace_manager.clone();
            let cancellation = self.cancellation_token.clone();

            self.task_tracker.spawn(async move {
                tokio::select! {
                    _ = Self::message_router_loop_static(transport, pending_requests, namespace_manager) => {
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
            let namespace_manager = self.namespace_manager.clone();
            let cancellation = self.cancellation_token.clone();

            self.task_tracker.spawn(async move {
                tokio::select! {
                    _ = Self::cleanup_loop_static(pending_requests, namespace_manager) => {
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

    /// Register a namespace
    pub async fn register_namespace(&self, namespace: &str) -> NetworkResult<()> {
        self.namespace_manager.register_namespace(namespace).await
    }

    /// Send a typed request with namespace and wait for typed response
    pub async fn request_namespaced<Req, Resp>(
        &self,
        namespace: &str,
        target: NodeId,
        request: Req,
        timeout: Duration,
    ) -> NetworkResult<Resp>
    where
        Req: MessageType
            + serde::Serialize
            + serde::de::DeserializeOwned
            + Send
            + Sync
            + Debug
            + 'static,
        Resp: serde::de::DeserializeOwned,
    {
        let correlation_id = Uuid::new_v4();
        let message_type = MessageType::message_type(&request);
        let full_message_type =
            crate::namespace::NamespaceManager::combine_message_type(namespace, message_type);

        // Serialize the request
        let request_bytes = crate::message::NetworkMessage::serialize(&request)?;

        // Get or create namespace
        let namespace_state = self
            .namespace_manager
            .get_or_create_namespace(namespace)
            .await;

        // Create oneshot channel for response
        let (tx, rx) = oneshot::channel();

        // Store pending request in namespace
        namespace_state.pending_requests.insert(
            correlation_id,
            crate::namespace::PendingRequest {
                tx,
                sent_at: std::time::Instant::now(),
                timeout: Some(timeout),
            },
        );

        // Update metrics
        namespace_state.metrics.record_request();

        debug!(
            "Sending request to {} with correlation_id {} in namespace '{}', message_type: '{}'",
            target, correlation_id, namespace, full_message_type
        );

        // Send the request via transport
        self.transport
            .send_envelope(
                &target,
                &request_bytes,
                &full_message_type,
                Some(correlation_id),
            )
            .await
            .map_err(NetworkError::Transport)?;

        // Wait for response with timeout
        let response_bytes = match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(response)) => {
                namespace_state.metrics.record_response();
                response
            }
            Ok(Err(_)) => {
                namespace_state.pending_requests.remove(&correlation_id);
                return Err(NetworkError::ChannelClosed(
                    "Response channel closed".to_string(),
                ));
            }
            Err(_) => {
                namespace_state.pending_requests.remove(&correlation_id);
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

    /// Register a handler for a specific message type within a namespace
    pub async fn register_namespaced_handler<M, F, Fut>(
        &self,
        namespace: &str,
        message_type: &str,
        handler: F,
    ) -> NetworkResult<()>
    where
        M: crate::message::NetworkMessage + serde::de::DeserializeOwned + 'static,
        F: Fn(NodeId, M) -> Fut + Clone + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let namespace_state = self
            .namespace_manager
            .get_or_create_namespace(namespace)
            .await;

        // Create type-erased handler
        let handler = Arc::new(
            move |sender: NodeId,
                  payload: Bytes,
                  _msg_type: &str,
                  _correlation_id: Option<Uuid>| {
                let handler = handler.clone();
                Box::pin(async move {
                    // Deserialize the message
                    let message: M = match ciborium::from_reader(payload.as_ref()) {
                        Ok(msg) => msg,
                        Err(e) => {
                            warn!("Failed to deserialize message: {}", e);
                            return Ok(None);
                        }
                    };

                    // Call the handler
                    handler(sender, message).await;
                    Ok(None)
                })
                    as std::pin::Pin<
                        Box<
                            dyn std::future::Future<
                                    Output = crate::handler::HandlerResult<Option<Bytes>>,
                                > + Send,
                        >,
                    >
            },
        );

        namespace_state
            .handlers
            .insert(message_type.to_string(), handler);
        Ok(())
    }

    /// Register a request handler that returns a response
    pub async fn register_namespaced_request_handler<Req, Resp, F, Fut>(
        &self,
        namespace: &str,
        message_type: &str,
        handler: F,
    ) -> NetworkResult<()>
    where
        Req: crate::message::NetworkMessage + serde::de::DeserializeOwned + 'static,
        Resp: MessageType
            + serde::Serialize
            + serde::de::DeserializeOwned
            + Send
            + Sync
            + Debug
            + 'static,
        F: Fn(NodeId, Req) -> Fut + Clone + Send + Sync + 'static,
        Fut: std::future::Future<Output = NetworkResult<Resp>> + Send + 'static,
    {
        let namespace_state = self
            .namespace_manager
            .get_or_create_namespace(namespace)
            .await;

        // Create type-erased handler
        let handler = Arc::new(
            move |sender: NodeId,
                  payload: Bytes,
                  _msg_type: &str,
                  _correlation_id: Option<Uuid>| {
                let handler = handler.clone();
                Box::pin(async move {
                    // Deserialize the request
                    let request: Req = match ciborium::from_reader(payload.as_ref()) {
                        Ok(msg) => msg,
                        Err(e) => {
                            warn!("Failed to deserialize request: {}", e);
                            return Ok(None);
                        }
                    };

                    // Call the handler
                    match handler(sender, request).await {
                        Ok(response) => {
                            // Serialize the response
                            match crate::message::NetworkMessage::serialize(&response) {
                                Ok(bytes) => Ok(Some(bytes)),
                                Err(e) => {
                                    warn!("Failed to serialize response: {}", e);
                                    Ok(None)
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Handler returned error: {}", e);
                            Ok(None)
                        }
                    }
                })
                    as std::pin::Pin<
                        Box<
                            dyn std::future::Future<
                                    Output = crate::handler::HandlerResult<Option<Bytes>>,
                                > + Send,
                        >,
                    >
            },
        );

        namespace_state
            .handlers
            .insert(message_type.to_string(), handler);
        Ok(())
    }

    /// Message router loop - processes incoming messages (static version)
    async fn message_router_loop_static(
        transport: Arc<T>,
        pending_requests: Arc<RwLock<DashMap<Uuid, PendingRequest>>>,
        namespace_manager: Arc<NamespaceManager>,
    ) {
        debug!("Message router loop started");
        let mut incoming = transport.incoming();

        while let Some(envelope) = incoming.next().await {
            debug!(
                "Received envelope from transport: sender={}, message_type={}, correlation_id={:?}",
                envelope.sender, envelope.message_type, envelope.correlation_id
            );
            let pending_requests = pending_requests.clone();
            let namespace_manager = namespace_manager.clone();
            let transport = transport.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::process_incoming_message_static(
                    envelope,
                    pending_requests,
                    namespace_manager,
                    transport,
                )
                .await
                {
                    error!("Error processing incoming message: {}", e);
                }
            });
        }

        debug!("Message router loop exited");
    }

    /// Process an incoming message (static version)
    async fn process_incoming_message_static(
        envelope: TransportEnvelope,
        pending_requests: Arc<RwLock<DashMap<Uuid, PendingRequest>>>,
        namespace_manager: Arc<NamespaceManager>,
        transport: Arc<T>,
    ) -> NetworkResult<()> {
        // The transport has already verified the COSE signature and extracted metadata
        let TransportEnvelope {
            sender,
            payload: payload_bytes,
            message_type,
            correlation_id,
        } = envelope;

        // Extract namespace from message type
        let (namespace, local_message_type) =
            crate::namespace::NamespaceManager::extract_namespace(&message_type);

        // Check if this is a response to a pending request in namespace
        if let Some(correlation_id) = correlation_id {
            debug!(
                "Received message with correlation_id {} from {}",
                correlation_id, sender
            );

            // First check namespace-based pending requests
            let namespace_state = namespace_manager.get_or_create_namespace(namespace).await;
            if let Some((_, request)) = namespace_state.pending_requests.remove(&correlation_id) {
                debug!(
                    "Found pending request in namespace '{}' for correlation_id {}",
                    namespace, correlation_id
                );
                let _ = request.tx.send(payload_bytes);
                namespace_state.metrics.record_response();
                return Ok(());
            }

            // Fall back to legacy pending requests
            let pending = pending_requests.read().await;
            if let Some((_, request)) = pending.remove(&correlation_id) {
                debug!(
                    "Found pending request in legacy map for correlation_id {}",
                    correlation_id
                );
                let _ = request.tx.send(payload_bytes);
                return Ok(());
            }

            debug!(
                "No pending request found for correlation_id {} in namespace '{}' or legacy map",
                correlation_id, namespace
            );
        }

        // Try namespace-based handler first
        let namespace_state = namespace_manager.get_or_create_namespace(namespace).await;
        namespace_state.metrics.record_message();

        if let Some(handler) = namespace_state.handlers.get(local_message_type) {
            debug!(
                "Found handler for message type '{}' in namespace '{}', correlation_id: {:?}",
                local_message_type, namespace, correlation_id
            );
            match handler(
                sender.clone(),
                payload_bytes.clone(),
                local_message_type,
                correlation_id,
            )
            .await
            {
                Ok(Some(response_bytes)) => {
                    debug!(
                        "Handler returned response, sending back to {} with correlation_id {:?}",
                        sender, correlation_id
                    );
                    // Send response back via transport with the same correlation_id
                    // For responses, we use the original message type so the correlation works
                    transport
                        .send_envelope(&sender, &response_bytes, &message_type, correlation_id)
                        .await
                        .map_err(NetworkError::Transport)?;
                    return Ok(());
                }
                Ok(None) => {
                    // No response needed
                    return Ok(());
                }
                Err(e) => {
                    warn!("Namespace handler error: {}", e);
                    // Fall through to legacy handler
                }
            }
        }

        // If no namespace handler found, log a warning
        warn!(
            "No handler found for message type '{}' (local: '{}') in namespace '{}', correlation_id: {:?}",
            message_type, local_message_type, namespace, correlation_id
        );

        Ok(())
    }

    /// Cleanup old pending requests
    async fn cleanup_request(&self, correlation_id: Uuid) {
        let pending = self.pending_requests.read().await;
        pending.remove(&correlation_id);
    }

    /// Periodic cleanup of old pending requests (static version)
    async fn cleanup_loop_static(
        pending_requests: Arc<RwLock<DashMap<Uuid, PendingRequest>>>,
        namespace_manager: Arc<NamespaceManager>,
    ) {
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

            // Also cleanup namespace-based pending requests
            namespace_manager
                .cleanup_all_namespaces(Duration::from_secs(300))
                .await;
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
