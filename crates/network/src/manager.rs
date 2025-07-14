//! Network manager for handling message routing and peer communication

use crate::error::{NetworkError, NetworkResult};
use crate::handler::HandlerRegistry;
use crate::peer::Peer;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use proven_governance::Governance;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::{Transport, TransportEnvelope};
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Pending request tracking
struct PendingRequest {
    tx: oneshot::Sender<Bytes>,
    sent_at: std::time::Instant,
}

/// Bootable state for background tasks
pub(crate) struct BootableState {
    pub(crate) router_task: Option<JoinHandle<()>>,
    pub(crate) cleanup_task: Option<JoinHandle<()>>,
    pub(crate) shutdown_tx: Option<mpsc::Sender<()>>,
}

/// Network manager for handling message routing and peer communication
pub struct NetworkManager<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Our local node ID
    local_node_id: NodeId,
    /// Transport layer
    pub(crate) transport: T,
    /// Topology manager
    topology: Arc<TopologyManager<G>>,
    /// Handler registry
    handler_registry: Arc<HandlerRegistry>,
    /// Pending requests waiting for responses
    pending_requests: Arc<RwLock<DashMap<Uuid, PendingRequest>>>,
    /// Bootable state
    pub(crate) bootable_state: Arc<RwLock<BootableState>>,
}

impl<T, G> NetworkManager<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new network manager
    pub fn new(local_node_id: NodeId, transport: T, topology: Arc<TopologyManager<G>>) -> Self {
        Self {
            local_node_id,
            transport,
            topology,
            handler_registry: Arc::new(HandlerRegistry::new()),
            pending_requests: Arc::new(RwLock::new(DashMap::new())),
            bootable_state: Arc::new(RwLock::new(BootableState {
                router_task: None,
                cleanup_task: None,
                shutdown_tx: None,
            })),
        }
    }

    /// Start the network manager background tasks
    pub async fn start_tasks(self: Arc<Self>) -> NetworkResult<()> {
        let (shutdown_tx, mut _shutdown_rx) = mpsc::channel::<()>(1);

        // Start router task
        let router_task = {
            let manager = Arc::clone(&self);
            tokio::spawn(async move {
                manager.message_router_loop().await;
            })
        };

        // Start cleanup task
        let cleanup_task = {
            let manager = Arc::clone(&self);
            tokio::spawn(async move {
                manager.cleanup_loop().await;
            })
        };

        // Store task handles
        {
            let mut state = self.bootable_state.write().await;
            state.router_task = Some(router_task);
            state.cleanup_task = Some(cleanup_task);
            state.shutdown_tx = Some(shutdown_tx);
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

    /// Register a message handler
    pub fn register_handler<M, F, Fut>(&self, handler: F) -> NetworkResult<()>
    where
        M: crate::message::HandledMessage + serde::de::DeserializeOwned,
        M::Response: serde::de::DeserializeOwned,
        F: Fn(NodeId, M, Option<Uuid>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = crate::handler::HandlerResult<M::Response>>
            + Send
            + 'static,
    {
        self.handler_registry.register(handler)
    }

    /// Get the handler registry
    pub(crate) fn handler_registry(&self) -> &Arc<HandlerRegistry> {
        &self.handler_registry
    }

    /// Message router loop - processes incoming messages
    async fn message_router_loop(self: Arc<Self>) {
        let mut incoming = self.transport.incoming();

        while let Some(transport_msg) = incoming.next().await {
            let manager = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = manager.process_incoming_message(transport_msg).await {
                    error!("Error processing incoming message: {}", e);
                }
            });
        }

        debug!("Message router loop exited");
    }

    /// Process an incoming message
    async fn process_incoming_message(&self, envelope: TransportEnvelope) -> NetworkResult<()> {
        // The transport has already verified the COSE signature and extracted metadata
        let TransportEnvelope {
            sender,
            payload: payload_bytes,
            message_type,
            correlation_id,
        } = envelope;

        // Check if this is a response to a pending request
        if let Some(correlation_id) = correlation_id {
            let pending = self.pending_requests.read().await;
            if let Some((_, request)) = pending.remove(&correlation_id) {
                let _ = request.tx.send(payload_bytes);
                return Ok(());
            }
        }

        // Otherwise, try to handle the message
        match self
            .handler_registry
            .handle_raw_message(
                sender.clone(),
                payload_bytes.clone(),
                message_type.clone(),
                correlation_id,
            )
            .await
        {
            Ok(Some(response_bytes)) => {
                // Send response back via transport
                self.transport
                    .send_envelope(&sender, &response_bytes, &message_type, correlation_id)
                    .await
                    .map_err(NetworkError::Transport)?;
            }
            Ok(None) => {
                // No response needed
            }
            Err(e) => {
                warn!("Handler error: {}", e);
            }
        }

        Ok(())
    }

    /// Cleanup old pending requests
    async fn cleanup_request(&self, correlation_id: Uuid) {
        let pending = self.pending_requests.read().await;
        pending.remove(&correlation_id);
    }

    /// Periodic cleanup of old pending requests
    async fn cleanup_loop(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            let now = std::time::Instant::now();
            let pending = self.pending_requests.read().await;

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

        let mut state = self.bootable_state.write().await;

        // Send shutdown signal
        if let Some(tx) = state.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }

        // Abort tasks
        if let Some(task) = state.router_task.take() {
            task.abort();
        }
        if let Some(task) = state.cleanup_task.take() {
            task.abort();
        }

        Ok(())
    }
}
