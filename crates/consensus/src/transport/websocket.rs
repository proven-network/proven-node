//! WebSocket-based transport implementation for consensus networking.

use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use ciborium;
use futures::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ed25519_dalek::SigningKey;
use proven_attestation::Attestor;
use proven_governance::Governance;

use super::{
    ClusterDiscoveryRequest, ClusterDiscoveryResponse, ConsensusMessage, ConsensusTransport,
    PeerConnection,
};
use crate::attestation::AttestationVerifier;
use crate::cose::CoseHandler;
use crate::error::ConsensusError;
use crate::topology::TopologyManager;

/// Maximum message size (1MB)
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// Handshake timeout for WebSocket connections
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// WebSocket consensus message with COSE signing
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebSocketMessage {
    /// Message ID for tracking responses
    pub id: Uuid,
    /// The actual consensus message
    pub message: ConsensusMessage,
    /// Message timestamp
    pub timestamp: u64,
    /// Sender's node ID
    pub sender_id: String,
}

/// Handshake message for WebSocket connection establishment (matches TCP format)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebSocketHandshakeMessage {
    /// Node identifier
    pub node_id: String,
    /// Node's attestation document
    pub attestation: Bytes,
    /// Protocol version
    pub protocol_version: u16,
    /// Connection timestamp
    pub timestamp: u64,
}

/// Handshake response for WebSocket connections (matches TCP format)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebSocketHandshakeResponse {
    /// Whether handshake was accepted
    pub accepted: bool,
    /// Error message if rejected
    pub error: Option<String>,
    /// Responder's attestation document
    pub attestation: Option<Bytes>,
}

/// Represents an active WebSocket peer connection
#[derive(Clone)]
struct WebSocketPeer {
    /// Node ID
    node_id: String,
    /// Remote address
    address: SocketAddr,
    /// Message sender to this peer
    sender: mpsc::UnboundedSender<Bytes>,
    /// Whether attestation is verified
    attestation_verified: bool,
    /// Last activity timestamp
    last_activity: SystemTime,
}

/// WebSocket server state shared across connections
#[derive(Clone)]
pub struct WebSocketServerState<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Local node ID
    node_id: String,
    /// Attestation verifier
    attestation_verifier: Arc<AttestationVerifier<G, A>>,
    /// COSE handler for secure message signing/verification
    cose_handler: Arc<CoseHandler<G>>,
    /// Connected peers
    peers: Arc<RwLock<HashMap<String, WebSocketPeer>>>,
    /// Channel for sending messages to consensus protocol
    consensus_tx: mpsc::UnboundedSender<(String, ConsensusMessage)>,
    /// Temporary channel for collecting cluster discovery responses
    discovery_response_tx:
        Arc<std::sync::Mutex<Option<mpsc::UnboundedSender<ClusterDiscoveryResponse>>>>,
}

/// WebSocket-based transport for consensus networking
pub struct WebSocketTransport<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Local node ID
    node_id: String,
    /// Local listening address
    listen_addr: SocketAddr,
    /// Topology manager for peer discovery
    #[allow(dead_code)]
    topology: Arc<TopologyManager<G>>,
    /// Shared server state
    state: WebSocketServerState<G, A>,
    /// Channel for receiving messages from consensus protocol
    #[allow(dead_code)]
    network_rx: Arc<Mutex<mpsc::UnboundedReceiver<(String, ConsensusMessage)>>>,
    /// Shutdown signal sender
    shutdown_tx: Arc<std::sync::Mutex<Option<broadcast::Sender<()>>>>,
    /// Background task handles for lifecycle management
    task_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
}

impl<G, A> WebSocketTransport<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Creates a new WebSocket-based consensus transport
    #[allow(clippy::needless_pass_by_value)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: String,
        listen_addr: SocketAddr,
        topology: Arc<TopologyManager<G>>,
        governance: Arc<G>,
        attestor: Arc<A>,
        signing_key: SigningKey,
        consensus_tx: mpsc::UnboundedSender<(String, ConsensusMessage)>,
        network_rx: mpsc::UnboundedReceiver<(String, ConsensusMessage)>,
    ) -> Self {
        let attestation_verifier = Arc::new(AttestationVerifier::new(
            (*governance).clone(),
            (*attestor).clone(),
        ));

        let cose_handler = Arc::new(CoseHandler::new(
            signing_key,
            node_id.clone(),
            (*governance).clone(),
        ));

        let state = WebSocketServerState {
            node_id: node_id.clone(),
            attestation_verifier,
            cose_handler,
            peers: Arc::new(RwLock::new(HashMap::new())),
            consensus_tx,
            discovery_response_tx: Arc::new(std::sync::Mutex::new(None)),
        };

        Self {
            node_id,
            listen_addr,
            topology,
            state,
            network_rx: Arc::new(Mutex::new(network_rx)),
            shutdown_tx: Arc::new(std::sync::Mutex::new(None)),
            task_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Create a WebSocket router that can be mounted into an existing axum application
    pub fn create_router(&self) -> Router<WebSocketServerState<G, A>> {
        Router::new()
            .route("/consensus/:node_id", get(ws_consensus_handler))
            .with_state(self.state.clone())
    }

    /// Performs secure handshake with a connecting peer
    #[allow(clippy::too_many_lines)]
    async fn perform_handshake(
        sender: &mut futures::stream::SplitSink<WebSocket, Message>,
        receiver: &mut futures::stream::SplitStream<WebSocket>,
        connecting_node_id: &str,
        state: &WebSocketServerState<G, A>,
    ) -> Result<String, ConsensusError> {
        // Wait for handshake message from peer
        let handshake_timeout = tokio::time::timeout(HANDSHAKE_TIMEOUT, receiver.next()).await;

        let handshake_data = match handshake_timeout {
            Ok(Some(Ok(Message::Binary(data)))) => data,
            Ok(Some(Ok(_))) => {
                return Err(ConsensusError::AttestationFailure(
                    "Expected binary handshake message".to_string(),
                ));
            }
            Ok(Some(Err(e))) => {
                return Err(ConsensusError::Network(
                    format!("WebSocket error during handshake: {e}").into(),
                ));
            }
            Ok(None) => {
                return Err(ConsensusError::AttestationFailure(
                    "Connection closed during handshake".to_string(),
                ));
            }
            Err(_) => {
                return Err(ConsensusError::AttestationFailure(
                    "Handshake timeout".to_string(),
                ));
            }
        };

        // Deserialize and verify COSE-signed handshake
        let cose_message = state
            .cose_handler
            .deserialize_cose_message(&handshake_data)
            .map_err(|e| ConsensusError::InvalidMessage(format!("COSE deserialize failed: {e}")))?;

        let payload = state
            .cose_handler
            .verify_signed_message(&cose_message)
            .await
            .map_err(|e| ConsensusError::InvalidMessage(format!("COSE verify failed: {e}")))?;

        // Extract handshake from payload
        let handshake: WebSocketHandshakeMessage = ciborium::de::from_reader(payload.data.as_ref())
            .map_err(|e| {
                ConsensusError::InvalidMessage(format!("Handshake deserialize failed: {e}"))
            })?;

        // Verify node ID matches path parameter
        if handshake.node_id != connecting_node_id {
            return Err(ConsensusError::AttestationFailure(format!(
                "Node ID mismatch: path={}, handshake={}",
                connecting_node_id, handshake.node_id
            )));
        }

        // Verify attestation
        let authorized = state
            .attestation_verifier
            .authorize_peer(handshake.attestation.clone())
            .await?;

        // Generate response
        let response = if authorized {
            // Generate our attestation for the response
            let our_attestation = state
                .attestation_verifier
                .generate_peer_attestation(None)
                .await
                .ok(); // Optional - we might not have attestation ready

            WebSocketHandshakeResponse {
                accepted: true,
                error: None,
                attestation: our_attestation,
            }
        } else {
            WebSocketHandshakeResponse {
                accepted: false,
                error: Some("Attestation verification failed".to_string()),
                attestation: None,
            }
        };

        // Serialize and sign response
        let mut response_data = Vec::new();
        ciborium::ser::into_writer(&response, &mut response_data).map_err(|e| {
            ConsensusError::InvalidMessage(format!("Response serialize failed: {e}"))
        })?;

        let response_payload = crate::cose::MessagePayload::handshake_message(
            Bytes::from(response_data),
            state.node_id.clone(),
        );

        let cose_response = state
            .cose_handler
            .create_signed_message(&response_payload, None)
            .map_err(|e| ConsensusError::InvalidMessage(format!("COSE signing failed: {e}")))?;

        let cose_response_bytes = state
            .cose_handler
            .serialize_cose_message(&cose_response)
            .map_err(|e| {
                ConsensusError::InvalidMessage(format!("COSE serialization failed: {e}"))
            })?;

        // Send response
        if let Err(e) = sender
            .send(Message::Binary(Bytes::from(cose_response_bytes)))
            .await
        {
            return Err(ConsensusError::Network(
                format!("Failed to send handshake response: {e}").into(),
            ));
        }

        if authorized {
            debug!("Handshake successful with peer {}", handshake.node_id);
            Ok(handshake.node_id)
        } else {
            Err(ConsensusError::AttestationFailure(
                "Peer not authorized".to_string(),
            ))
        }
    }

    /// Process a COSE-signed message from a peer
    async fn process_peer_message(
        peer_node_id: &str,
        data: Bytes,
        state: &WebSocketServerState<G, A>,
    ) -> Result<(), ConsensusError> {
        if data.len() > MAX_MESSAGE_SIZE {
            return Err(ConsensusError::InvalidMessage("Message too large".into()));
        }

        // First, try to parse as COSE-signed message (for cluster discovery)
        if let Some(consensus_message) =
            Self::try_parse_cose_message(&data, peer_node_id, state).await
        {
            // Handle cluster discovery messages specially
            if let ConsensusMessage::ClusterDiscoveryResponse(response) = &consensus_message {
                if let Ok(discovery_tx) = state.discovery_response_tx.lock() {
                    if let Some(tx) = discovery_tx.as_ref() {
                        let _ = tx.send(response.clone());
                        return Ok(());
                    }
                }
            }

            // Send to consensus protocol
            if let Err(e) = state
                .consensus_tx
                .send((peer_node_id.to_string(), consensus_message))
            {
                error!("Failed to send message to consensus: {}", e);
            }
            return Ok(());
        }

        // If not COSE-signed, try to parse as regular WebSocket message
        let ws_message: WebSocketMessage = ciborium::from_reader(&data[..])
            .map_err(|e| ConsensusError::InvalidMessage(format!("CBOR deserialize: {e}")))?;

        // Verify sender ID matches peer
        if ws_message.sender_id != peer_node_id {
            return Err(ConsensusError::InvalidMessage(format!(
                "Sender ID mismatch: expected {}, got {}",
                peer_node_id, ws_message.sender_id
            )));
        }

        // Handle special messages
        match &ws_message.message {
            ConsensusMessage::ClusterDiscovery(request) => {
                // Respond to cluster discovery request
                Self::handle_cluster_discovery_request(peer_node_id, request, state).await;
                Ok(())
            }
            ConsensusMessage::ClusterDiscoveryResponse(response) => {
                // Forward to discovery response collector
                if let Ok(discovery_tx) = state.discovery_response_tx.lock() {
                    if let Some(tx) = discovery_tx.as_ref() {
                        let _ = tx.send(response.clone());
                    }
                }
                Ok(())
            }
            _ => {
                // Send other messages to consensus protocol
                if let Err(e) = state
                    .consensus_tx
                    .send((peer_node_id.to_string(), ws_message.message))
                {
                    error!("Failed to send message to consensus: {}", e);
                }
                Ok(())
            }
        }
    }

    /// Handle cluster discovery request by sending a COSE-signed response
    async fn handle_cluster_discovery_request(
        peer_node_id: &str,
        _request: &ClusterDiscoveryRequest,
        state: &WebSocketServerState<G, A>,
    ) {
        debug!(
            "Received cluster discovery request from {}, sending response",
            peer_node_id
        );

        // Create discovery response (similar to TCP implementation)
        let discovery_response = ClusterDiscoveryResponse {
            responder_id: state.node_id.clone(),
            has_active_cluster: true, // Report active cluster when node is running
            current_term: Some(1),    // Basic term for bootstrap
            current_leader: Some(state.node_id.clone()), // This node claims to be leader
            cluster_size: Some(1),    // Start with 1, will grow as nodes join
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Serialize response
        let mut response_data = Vec::new();
        if ciborium::ser::into_writer(&discovery_response, &mut response_data).is_err() {
            warn!("Failed to serialize cluster discovery response");
            return;
        }

        // Create COSE-signed message
        let message_payload = crate::cose::MessagePayload::new(
            Bytes::from(response_data),
            state.node_id.clone(),
            "cluster_discovery_response".to_string(),
        );

        let cose_message = match state
            .cose_handler
            .create_signed_message(&message_payload, None)
        {
            Ok(msg) => msg,
            Err(e) => {
                warn!("Failed to create COSE message: {}", e);
                return;
            }
        };

        let cose_data = match state.cose_handler.serialize_cose_message(&cose_message) {
            Ok(data) => data,
            Err(e) => {
                warn!("Failed to serialize COSE message: {}", e);
                return;
            }
        };

        // Send response to peer
        let peers_read = state.peers.read().await;
        if let Some(peer) = peers_read.get(peer_node_id) {
            if let Err(e) = peer.sender.send(cose_data.into()) {
                warn!(
                    "Failed to send discovery response to {}: {}",
                    peer_node_id, e
                );
            } else {
                debug!("Sent cluster discovery response to {}", peer_node_id);
            }
        }
    }

    /// Try to parse a COSE-signed message (for cluster discovery)
    async fn try_parse_cose_message(
        data: &Bytes,
        peer_node_id: &str,
        state: &WebSocketServerState<G, A>,
    ) -> Option<ConsensusMessage> {
        // Try to deserialize as COSE message
        let Ok(cose_message) = state.cose_handler.deserialize_cose_message(data) else {
            return None; // Not a COSE message
        };

        // Verify the COSE signature
        let payload = match state
            .cose_handler
            .verify_signed_message(&cose_message)
            .await
        {
            Ok(payload) => payload,
            Err(e) => {
                warn!(
                    "COSE verification failed for message from {}: {}",
                    peer_node_id, e
                );
                return None;
            }
        };

        // Parse based on message type
        match payload.message_type.as_str() {
            "cluster_discovery" => {
                match ciborium::de::from_reader::<ClusterDiscoveryRequest, _>(payload.data.as_ref())
                {
                    Ok(discovery_request) => {
                        debug!(
                            "Received COSE-signed cluster discovery request from {}",
                            peer_node_id
                        );
                        Some(ConsensusMessage::ClusterDiscovery(discovery_request))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to deserialize cluster discovery request from {}: {}",
                            peer_node_id, e
                        );
                        None
                    }
                }
            }
            "cluster_discovery_response" => {
                match ciborium::de::from_reader::<ClusterDiscoveryResponse, _>(
                    payload.data.as_ref(),
                ) {
                    Ok(discovery_response) => {
                        debug!(
                            "Received COSE-signed cluster discovery response from {}",
                            peer_node_id
                        );
                        Some(ConsensusMessage::ClusterDiscoveryResponse(
                            discovery_response,
                        ))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to deserialize cluster discovery response from {}: {}",
                            peer_node_id, e
                        );
                        None
                    }
                }
            }
            "consensus_message" => {
                match ciborium::de::from_reader::<ConsensusMessage, _>(payload.data.as_ref()) {
                    Ok(consensus_message) => {
                        debug!(
                            "Received COSE-signed consensus message from {}",
                            peer_node_id
                        );
                        Some(consensus_message)
                    }
                    Err(e) => {
                        warn!(
                            "Failed to deserialize consensus message from {}: {}",
                            peer_node_id, e
                        );
                        None
                    }
                }
            }
            _ => {
                debug!("Unknown COSE message type: {}", payload.message_type);
                None
            }
        }
    }
}

/// WebSocket handler for incoming consensus connections
async fn ws_consensus_handler<G, A>(
    Path(connecting_node_id): Path<String>,
    State(state): State<WebSocketServerState<G, A>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    ws.on_upgrade(move |socket| handle_incoming_websocket(socket, connecting_node_id, addr, state))
}

/// Handle an incoming WebSocket connection with secure handshake
async fn handle_incoming_websocket<G, A>(
    socket: WebSocket,
    connecting_node_id: String,
    addr: SocketAddr,
    state: WebSocketServerState<G, A>,
) where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    info!(
        "Incoming WebSocket connection from {} at {}",
        connecting_node_id, addr
    );

    let (mut sender, mut receiver) = socket.split();

    // Perform secure handshake
    let authenticated_node_id = match WebSocketTransport::<G, A>::perform_handshake(
        &mut sender,
        &mut receiver,
        &connecting_node_id,
        &state,
    )
    .await
    {
        Ok(node_id) => {
            info!("Handshake successful with peer {} from {}", node_id, addr);
            node_id
        }
        Err(e) => {
            warn!(
                "Handshake failed with {} from {}: {}",
                connecting_node_id, addr, e
            );
            return;
        }
    };

    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();

    // Create authenticated peer entry
    let peer = WebSocketPeer {
        node_id: authenticated_node_id.clone(),
        address: addr,
        sender: tx,
        attestation_verified: true, // Verified during handshake
        last_activity: SystemTime::now(),
    };

    // Add to peers map
    state
        .peers
        .write()
        .await
        .insert(authenticated_node_id.clone(), peer);

    // Spawn sender task
    let peer_node_id_clone = authenticated_node_id.clone();
    let mut send_task = tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            match sender.send(Message::Binary(data)).await {
                Ok(()) => {}
                Err(e) => {
                    error!("Error sending to peer {}: {}", peer_node_id_clone, e);
                    break;
                }
            }
        }
    });

    // Spawn receiver task
    let peer_node_id_clone = authenticated_node_id.clone();
    let state_clone = state.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(message)) = receiver.next().await {
            match message {
                Message::Binary(data) => {
                    if let Err(e) = WebSocketTransport::<G, A>::process_peer_message(
                        &peer_node_id_clone,
                        data,
                        &state_clone,
                    )
                    .await
                    {
                        error!(
                            "Error processing message from {}: {}",
                            peer_node_id_clone, e
                        );
                    }
                }
                Message::Close(_) => {
                    info!("Peer {} closed connection", peer_node_id_clone);
                    break;
                }
                _ => {} // Ignore other message types
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = (&mut send_task) => {
            recv_task.abort();
        },
        _ = (&mut recv_task) => {
            send_task.abort();
        }
    }

    // Remove peer from map
    state.peers.write().await.remove(&authenticated_node_id);
    info!("Disconnected from peer {}", authenticated_node_id);
}

#[async_trait]
impl<G, A> ConsensusTransport for WebSocketTransport<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    async fn send_message(
        &self,
        target_node_id: &str,
        message: ConsensusMessage,
    ) -> Result<(), ConsensusError> {
        // Find the peer and send the message directly
        let peers_read = self.state.peers.read().await;
        if let Some(peer) = peers_read.get(target_node_id) {
            // Use COSE signing for cluster discovery messages
            let message_data = match &message {
                ConsensusMessage::ClusterDiscovery(_)
                | ConsensusMessage::ClusterDiscoveryResponse(_) => {
                    // Serialize message for COSE signing
                    let mut message_payload = Vec::new();
                    ciborium::ser::into_writer(&message, &mut message_payload).map_err(|e| {
                        ConsensusError::InvalidMessage(format!("Failed to serialize message: {e}"))
                    })?;

                    // Create COSE message payload
                    let payload = match &message {
                        ConsensusMessage::ClusterDiscovery(_) => crate::cose::MessagePayload::new(
                            Bytes::from(message_payload),
                            self.node_id.clone(),
                            "cluster_discovery".to_string(),
                        ),
                        ConsensusMessage::ClusterDiscoveryResponse(_) => {
                            crate::cose::MessagePayload::new(
                                Bytes::from(message_payload),
                                self.node_id.clone(),
                                "cluster_discovery_response".to_string(),
                            )
                        }
                        _ => unreachable!(), // We already matched these above
                    };

                    // Sign with COSE
                    let cose_message = self
                        .state
                        .cose_handler
                        .create_signed_message(&payload, None)
                        .map_err(|e| {
                            ConsensusError::InvalidMessage(format!("COSE signing failed: {e}"))
                        })?;

                    // Serialize COSE message
                    self.state
                        .cose_handler
                        .serialize_cose_message(&cose_message)
                        .map_err(|e| {
                            ConsensusError::InvalidMessage(format!(
                                "COSE serialization failed: {e}"
                            ))
                        })?
                }
                _ => {
                    // Use regular WebSocket message for other message types
                    let ws_message = WebSocketMessage {
                        id: Uuid::new_v4(),
                        message,
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                        sender_id: self.node_id.clone(),
                    };

                    let mut serialized = Vec::new();
                    ciborium::into_writer(&ws_message, &mut serialized).map_err(|e| {
                        ConsensusError::InvalidMessage(format!("Failed to serialize message: {e}"))
                    })?;
                    serialized
                }
            };

            peer.sender.send(Bytes::from(message_data)).map_err(|e| {
                ConsensusError::Network(
                    format!("Failed to send to peer {target_node_id}: {e}").into(),
                )
            })?;

            Ok(())
        } else {
            Err(ConsensusError::Network(
                format!("Peer {target_node_id} not connected").into(),
            ))
        }
    }

    fn get_message_sender(&self) -> mpsc::UnboundedSender<(String, ConsensusMessage)> {
        self.state.consensus_tx.clone()
    }

    async fn get_connected_peers(&self) -> Result<Vec<PeerConnection>, ConsensusError> {
        let connected_peers = self
            .state
            .peers
            .read()
            .await
            .values()
            .map(|peer| PeerConnection {
                address: peer.address,
                attestation_verified: peer.attestation_verified,
                connected: true,
                last_activity: peer.last_activity,
                node_id: peer.node_id.clone(),
            })
            .collect();
        Ok(connected_peers)
    }

    async fn start(&self) -> Result<(), ConsensusError> {
        info!(
            "Starting WebSocket consensus transport on {}",
            self.listen_addr
        );

        // This is a placeholder - in the actual implementation, this would be integrated
        // into the core module's HTTP server rather than starting its own server
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), ConsensusError> {
        // Send shutdown signal
        let value = self.shutdown_tx.lock().unwrap().take();
        if let Some(shutdown_tx) = value {
            let _ = shutdown_tx.send(());
        }

        // Wait for all tasks to complete
        let handles = {
            let mut handles_lock = self.task_handles.lock().unwrap();
            std::mem::take(&mut *handles_lock)
        };

        for handle in handles {
            handle.abort();
        }

        info!("WebSocket transport shutdown complete");
        Ok(())
    }

    async fn discover_existing_clusters(
        &self,
    ) -> Result<Vec<ClusterDiscoveryResponse>, ConsensusError> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Set the discovery response channel
        {
            let mut discovery_tx = self.state.discovery_response_tx.lock().unwrap();
            *discovery_tx = Some(tx);
        }

        let discovery_request = ClusterDiscoveryRequest {
            requester_id: self.node_id.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Send discovery requests to connected peers
        let peers = self.state.peers.read().await;
        for peer_id in peers.keys() {
            if peer_id != &self.node_id {
                if let Err(e) = self
                    .send_message(
                        peer_id,
                        ConsensusMessage::ClusterDiscovery(discovery_request.clone()),
                    )
                    .await
                {
                    warn!("Failed to send discovery request to {}: {}", peer_id, e);
                }
            }
        }
        drop(peers);

        // Collect responses with timeout
        let mut responses = Vec::new();
        let timeout_duration = Duration::from_secs(10);
        let deadline = tokio::time::Instant::now() + timeout_duration;

        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(response)) => responses.push(response),
                Ok(None) => break,
                Err(_) => {} // Timeout, continue collecting
            }
        }

        // Clear the discovery response channel
        {
            let mut discovery_tx = self.state.discovery_response_tx.lock().unwrap();
            *discovery_tx = None;
        }

        Ok(responses)
    }

    fn local_address(&self) -> Option<SocketAddr> {
        Some(self.listen_addr)
    }

    fn create_router(&self) -> Router {
        // Create a stateless router by using with_state() which consumes the state type
        let stateful_router = Router::new()
            .route("/consensus/:node_id", get(ws_consensus_handler))
            .with_state(self.state.clone());

        // Convert to stateless router
        Router::new().nest("/", stateful_router)
    }
}

impl<G, A> Debug for WebSocketTransport<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WebSocketTransport")
    }
}
