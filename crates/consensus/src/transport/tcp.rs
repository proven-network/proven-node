//! TCP-based transport implementation for consensus networking.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::Router;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, error, warn};

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
const MAX_MESSAGE_SIZE: u32 = 1024 * 1024;

/// Connection timeout
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Handshake timeout
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// Network message with attestation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkMessage {
    /// The actual consensus message.
    pub message: ConsensusMessage,
    /// Sender's attestation document (for verification).
    pub attestation: Option<Bytes>,
    /// Message timestamp.
    pub timestamp: u64,
    /// Sender's node ID.
    pub sender_id: String,
}

/// Handshake message for connection establishment.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandshakeMessage {
    /// Node identifier.
    pub node_id: String,
    /// Node's attestation document.
    pub attestation: Bytes,
    /// Protocol version.
    pub protocol_version: u16,
    /// Connection timestamp.
    pub timestamp: u64,
}

/// Handshake response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandshakeResponse {
    /// Whether handshake was accepted.
    pub accepted: bool,
    /// Error message if rejected.
    pub error: Option<String>,
    /// Responder's attestation document.
    pub attestation: Option<Bytes>,
}

/// Message framing for TCP streams.
struct MessageFraming;

impl MessageFraming {
    /// Writes a length-prefixed message to a stream.
    async fn write_message<T: AsyncWriteExt + Unpin>(
        stream: &mut T,
        data: &[u8],
    ) -> Result<(), ConsensusError> {
        if data.len() > MAX_MESSAGE_SIZE as usize {
            return Err(ConsensusError::InvalidMessage(
                "Message too large".to_string(),
            ));
        }

        #[allow(clippy::cast_possible_truncation)]
        let len = data.len() as u32;
        let mut buf = BytesMut::with_capacity(4 + data.len());
        buf.put_u32(len);
        buf.extend_from_slice(data);

        stream
            .write_all(&buf)
            .await
            .map_err(|e| ConsensusError::Network(format!("Write failed: {e}").into()))?;

        Ok(())
    }

    /// Reads a length-prefixed message from a stream.
    async fn read_message<T: AsyncReadExt + Unpin>(
        stream: &mut T,
    ) -> Result<Vec<u8>, ConsensusError> {
        // Read length prefix
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| ConsensusError::Network(format!("Read length failed: {e}").into()))?;

        let len = u32::from_be_bytes(len_buf);
        if len > MAX_MESSAGE_SIZE {
            return Err(ConsensusError::InvalidMessage(
                "Message too large".to_string(),
            ));
        }

        // Read message data
        let mut data = vec![0u8; len as usize];
        stream
            .read_exact(&mut data)
            .await
            .map_err(|e| ConsensusError::Network(format!("Read data failed: {e}").into()))?;

        Ok(data)
    }
}

/// TCP-based transport for consensus networking.
#[derive(Debug)]
pub struct TcpTransport<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Local node ID.
    node_id: String,
    /// Local listening address.
    listen_addr: SocketAddr,
    /// Topology manager for peer discovery.
    topology: Arc<TopologyManager<G>>,
    /// Attestation verifier.
    attestation_verifier: Arc<AttestationVerifier<G, A>>,
    /// COSE handler for secure message signing/verification.
    cose_handler: Arc<CoseHandler<G>>,
    /// Connected peers.
    peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
    /// Active TCP connections to peers.
    connections: Arc<RwLock<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    /// Channel for sending messages to consensus protocol.
    consensus_tx: mpsc::UnboundedSender<(String, ConsensusMessage)>,
    /// Channel for receiving messages from consensus protocol.
    network_rx: Arc<Mutex<mpsc::UnboundedReceiver<(String, ConsensusMessage)>>>,
    /// Shutdown signal sender.
    shutdown_tx: Arc<std::sync::Mutex<Option<broadcast::Sender<()>>>>,
    /// Background task handles for lifecycle management.
    task_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
    /// Temporary channel for collecting cluster discovery responses.
    discovery_response_tx:
        Arc<std::sync::Mutex<Option<mpsc::UnboundedSender<ClusterDiscoveryResponse>>>>,
}

impl<G, A> TcpTransport<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Creates a new TCP-based consensus transport.
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

        Self {
            node_id,
            listen_addr,
            topology,
            attestation_verifier,
            cose_handler,
            peers: Arc::new(RwLock::new(HashMap::new())),
            connections: Arc::new(RwLock::new(HashMap::new())),
            consensus_tx,
            network_rx: Arc::new(Mutex::new(network_rx)),
            shutdown_tx: Arc::new(std::sync::Mutex::new(None)),
            task_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
            discovery_response_tx: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Starts the transport internally.
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP listener fails to bind to the configured address.
    ///
    /// # Panics
    ///
    /// Panics if the shutdown mutex is poisoned.
    async fn start_internal(&self) -> Result<(), ConsensusError> {
        debug!("Starting TCP consensus transport on {}", self.listen_addr);

        let (shutdown_tx, _) = broadcast::channel(1);
        *self.shutdown_tx.lock().unwrap() = Some(shutdown_tx.clone());

        // Start TCP listener
        let listener = TcpListener::bind(self.listen_addr)
            .await
            .map_err(|e| ConsensusError::Consensus(format!("Failed to bind: {e}")))?;

        // Start listener loop
        let attestation_verifier = self.attestation_verifier.clone();
        let consensus_tx = self.consensus_tx.clone();
        let peers = self.peers.clone();
        let connections = self.connections.clone();
        let node_id = self.node_id.clone();
        let cose_handler = self.cose_handler.clone();
        let discovery_response_tx = self.discovery_response_tx.clone();
        let mut shutdown_rx1 = shutdown_tx.subscribe();

        let handle1 = tokio::spawn(async move {
            tokio::select! {
                () = Self::listener_loop(
                    listener,
                    node_id,
                    attestation_verifier,
                    cose_handler,
                    consensus_tx,
                    peers,
                    connections,
                    discovery_response_tx,
                ) => {
                    debug!("Listener loop completed");
                }
                _ = shutdown_rx1.recv() => {
                    debug!("Network listener shutting down");
                }
            }
        });

        // Start outbound connection manager
        let topology = self.topology.clone();
        let attestation_verifier = self.attestation_verifier.clone();
        let cose_handler = self.cose_handler.clone();
        let peers = self.peers.clone();
        let connections = self.connections.clone();
        let node_id = self.node_id.clone();
        let mut shutdown_rx2 = shutdown_tx.subscribe();

        let handle2 = tokio::spawn(async move {
            tokio::select! {
                () = Self::connection_manager_loop(
                    node_id,
                    topology,
                    attestation_verifier,
                    cose_handler,
                    peers,
                    connections,
                ) => {
                    debug!("Connection manager loop completed");
                }
                _ = shutdown_rx2.recv() => {
                    debug!("Network connection manager shutting down");
                }
            }
        });

        // Start message sender loop
        let network_rx = self.network_rx.clone();
        let connections = self.connections.clone();
        let mut shutdown_rx3 = shutdown_tx.subscribe();

        let handle3 = tokio::spawn(async move {
            tokio::select! {
                () = Self::message_sender_loop(network_rx, connections) => {
                    debug!("Message sender loop completed");
                }
                _ = shutdown_rx3.recv() => {
                    debug!("Network message sender shutting down");
                }
            }
        });

        // Store handles for cleanup
        self.task_handles
            .lock()
            .unwrap()
            .extend(vec![handle1, handle2, handle3]);

        Ok(())
    }

    /// TCP listener loop for incoming connections.
    #[allow(clippy::too_many_arguments)]
    async fn listener_loop(
        listener: TcpListener,
        node_id: String,
        attestation_verifier: Arc<AttestationVerifier<G, A>>,
        cose_handler: Arc<CoseHandler<G>>,
        consensus_tx: mpsc::UnboundedSender<(String, ConsensusMessage)>,
        peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
        connections: Arc<RwLock<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        discovery_response_tx: Arc<
            std::sync::Mutex<Option<mpsc::UnboundedSender<ClusterDiscoveryResponse>>>,
        >,
    ) {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("Incoming connection from {}", addr);

                    let attestation_verifier = attestation_verifier.clone();
                    let cose_handler = cose_handler.clone();
                    let consensus_tx = consensus_tx.clone();
                    let peers = peers.clone();
                    let connections = connections.clone();
                    let discovery_response_tx = discovery_response_tx.clone();

                    let node_id_clone = node_id.clone();
                    tokio::spawn(async move {
                        Self::handle_incoming_connection(
                            stream,
                            addr,
                            node_id_clone,
                            attestation_verifier,
                            cose_handler,
                            consensus_tx,
                            peers,
                            connections,
                            discovery_response_tx,
                        )
                        .await;
                    });
                }
                Err(e) => {
                    warn!("Failed to accept connection: {}", e);
                    break;
                }
            }
        }
    }

    /// Connection manager loop for outbound connections.
    async fn connection_manager_loop(
        local_node_id: String,
        topology: Arc<TopologyManager<G>>,
        attestation_verifier: Arc<AttestationVerifier<G, A>>,
        cose_handler: Arc<CoseHandler<G>>,
        peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
        connections: Arc<RwLock<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;

            // Get current topology
            let topology_peers = topology.get_all_peers().await;
            let current_peers = {
                let peers_guard = peers.read().await;
                peers_guard.keys().cloned().collect::<Vec<_>>()
            };

            // Connect to new peers
            for peer_info in topology_peers {
                // Map public key to node ID using deterministic test configuration
                let peer_node_id = Self::public_key_to_node_id(&peer_info.public_key);

                if peer_node_id == local_node_id || current_peers.contains(&peer_node_id) {
                    continue; // Skip self and already connected peers
                }

                // Parse address
                if let Ok(addr) = peer_info.address.to_string().parse::<SocketAddr>() {
                    let attestation_verifier = attestation_verifier.clone();
                    let cose_handler = cose_handler.clone();
                    let peers = peers.clone();
                    let connections = connections.clone();
                    let local_node_id_clone = local_node_id.clone();

                    tokio::spawn(async move {
                        Self::establish_outbound_connection(
                            local_node_id_clone,
                            peer_node_id,
                            addr,
                            attestation_verifier,
                            cose_handler,
                            peers,
                            connections,
                        )
                        .await;
                    });
                }
            }
        }
    }

    /// Message sender loop for outbound messages.
    async fn message_sender_loop(
        network_rx: Arc<Mutex<mpsc::UnboundedReceiver<(String, ConsensusMessage)>>>,
        connections: Arc<RwLock<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) {
        let mut rx = network_rx.lock().await;

        while let Some((target_node_id, consensus_msg)) = rx.recv().await {
            let connections_guard = connections.read().await;
            if let Some(conn) = connections_guard.get(&target_node_id) {
                let conn = conn.clone();
                tokio::spawn(async move {
                    Self::send_message_to_peer(target_node_id, consensus_msg, conn).await;
                });
            } else {
                warn!("No connection to peer {}", target_node_id);
            }
        }
    }

    /// Handles incoming TCP connections.
    #[allow(clippy::too_many_arguments)]
    async fn handle_incoming_connection(
        mut stream: TcpStream,
        addr: SocketAddr,
        local_node_id: String,
        attestation_verifier: Arc<AttestationVerifier<G, A>>,
        cose_handler: Arc<CoseHandler<G>>,
        consensus_tx: mpsc::UnboundedSender<(String, ConsensusMessage)>,
        peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
        connections: Arc<RwLock<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        discovery_response_tx: Arc<
            std::sync::Mutex<Option<mpsc::UnboundedSender<ClusterDiscoveryResponse>>>,
        >,
    ) {
        // Handle handshake with timeout
        let handshake_result = timeout(
            HANDSHAKE_TIMEOUT,
            Self::handle_incoming_handshake(&mut stream, &attestation_verifier, &cose_handler),
        )
        .await;

        let peer_node_id = match handshake_result {
            Ok(Ok(node_id)) => {
                debug!("Handshake successful with peer {} from {}", node_id, addr);
                node_id
            }
            Ok(Err(e)) => {
                warn!("Handshake failed with {}: {}", addr, e);
                return;
            }
            Err(_) => {
                warn!("Handshake timeout with {}", addr);
                return;
            }
        };

        // Update peer status
        {
            let mut peers_guard = peers.write().await;
            peers_guard.insert(
                peer_node_id.clone(),
                PeerConnection {
                    address: addr,
                    connected: true,
                    attestation_verified: true,
                    node_id: peer_node_id.clone(),
                    last_activity: SystemTime::now(),
                },
            );
        }

        // Store connection
        {
            let mut connections_guard = connections.write().await;
            connections_guard.insert(peer_node_id.clone(), Arc::new(Mutex::new(stream)));
        }

        // Start message handling for this connection
        if let Some(conn) = {
            let connections_guard = connections.read().await;
            connections_guard.get(&peer_node_id).cloned()
        } {
            Self::handle_peer_messages(
                peer_node_id,
                conn,
                consensus_tx,
                peers,
                cose_handler,
                discovery_response_tx,
                local_node_id,
                connections,
            )
            .await;
        }
    }

    /// Establishes an outbound connection to a peer.
    async fn establish_outbound_connection(
        local_node_id: String,
        peer_node_id: String,
        addr: SocketAddr,
        attestation_verifier: Arc<AttestationVerifier<G, A>>,
        cose_handler: Arc<CoseHandler<G>>,
        peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
        connections: Arc<RwLock<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) {
        debug!("Connecting to peer {} at {}", peer_node_id, addr);

        // Connect with timeout
        let stream = match timeout(CONNECTION_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                warn!("Failed to connect to {}: {}", addr, e);
                return;
            }
            Err(_) => {
                warn!("Connection timeout to {}", addr);
                return;
            }
        };

        // Perform outbound handshake
        match Self::perform_outbound_handshake(
            stream,
            local_node_id,
            &attestation_verifier,
            &cose_handler,
        )
        .await
        {
            Ok(authenticated_stream) => {
                debug!(
                    "Successfully connected to peer {} at {}",
                    peer_node_id, addr
                );

                // Update peer status
                {
                    let mut peers_guard = peers.write().await;
                    peers_guard.insert(
                        peer_node_id.clone(),
                        PeerConnection {
                            address: addr,
                            connected: true,
                            attestation_verified: true,
                            node_id: peer_node_id.clone(),
                            last_activity: SystemTime::now(),
                        },
                    );
                }

                // Store connection
                {
                    let mut connections_guard = connections.write().await;
                    connections_guard
                        .insert(peer_node_id, Arc::new(Mutex::new(authenticated_stream)));
                }
            }
            Err(e) => {
                warn!("Handshake failed with {}: {}", addr, e);
            }
        }
    }

    /// Performs outbound handshake.
    async fn perform_outbound_handshake(
        mut stream: TcpStream,
        local_node_id: String,
        attestation_verifier: &AttestationVerifier<G, A>,
        cose_handler: &CoseHandler<G>,
    ) -> Result<TcpStream, ConsensusError> {
        // Generate our attestation
        let our_attestation = attestation_verifier.generate_peer_attestation(None).await?;

        // Create handshake message
        let handshake = HandshakeMessage {
            node_id: local_node_id.clone(),
            attestation: our_attestation,
            protocol_version: 1,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Serialize handshake to CBOR
        let mut handshake_data = Vec::new();
        ciborium::ser::into_writer(&handshake, &mut handshake_data)
            .map_err(|e| ConsensusError::InvalidMessage(format!("CBOR serialize failed: {e}")))?;

        // Create COSE message payload
        let payload = crate::cose::MessagePayload::handshake_message(
            Bytes::from(handshake_data),
            local_node_id,
        );

        // Sign the handshake with COSE
        let cose_message = cose_handler
            .create_signed_message(&payload, None)
            .map_err(|e| ConsensusError::InvalidMessage(format!("COSE signing failed: {e}")))?;

        // Serialize the COSE message
        let cose_bytes = cose_handler
            .serialize_cose_message(&cose_message)
            .map_err(|e| {
                ConsensusError::InvalidMessage(format!("COSE serialization failed: {e}"))
            })?;

        // Send the COSE-signed handshake
        MessageFraming::write_message(&mut stream, &cose_bytes).await?;

        // Read response
        let response_data = timeout(HANDSHAKE_TIMEOUT, MessageFraming::read_message(&mut stream))
            .await
            .map_err(|_| ConsensusError::AttestationFailure("Handshake timeout".to_string()))??;

        let response: HandshakeResponse = ciborium::de::from_reader(response_data.as_slice())
            .map_err(|e| ConsensusError::InvalidMessage(format!("CBOR deserialize failed: {e}")))?;

        if response.accepted {
            Ok(stream)
        } else {
            Err(ConsensusError::AttestationFailure(
                response
                    .error
                    .unwrap_or_else(|| "Handshake rejected".to_string()),
            ))
        }
    }

    /// Handles incoming handshake.
    async fn handle_incoming_handshake(
        stream: &mut TcpStream,
        attestation_verifier: &AttestationVerifier<G, A>,
        cose_handler: &CoseHandler<G>,
    ) -> Result<String, ConsensusError> {
        // Read handshake message
        let handshake_data = MessageFraming::read_message(stream).await?;
        // Deserialize COSE message and verify signature
        let cose_message = cose_handler
            .deserialize_cose_message(&handshake_data)
            .map_err(|e| ConsensusError::InvalidMessage(format!("COSE deserialize failed: {e}")))?;

        let payload = cose_handler
            .verify_signed_message(&cose_message)
            .await
            .map_err(|e| ConsensusError::InvalidMessage(format!("COSE verify failed: {e}")))?;

        // Extract handshake from payload
        let handshake: HandshakeMessage = ciborium::de::from_reader(payload.data.as_ref())
            .map_err(|e| {
                ConsensusError::InvalidMessage(format!("Handshake deserialize failed: {e}"))
            })?;

        // Verify attestation
        let authorized = attestation_verifier
            .authorize_peer(handshake.attestation.clone())
            .await?;

        let response = if authorized {
            // Generate our attestation for the response
            let our_attestation = attestation_verifier
                .generate_peer_attestation(None)
                .await
                .ok(); // Optional - we might not have attestation ready

            HandshakeResponse {
                accepted: true,
                error: None,
                attestation: our_attestation,
            }
        } else {
            HandshakeResponse {
                accepted: false,
                error: Some("Attestation verification failed".to_string()),
                attestation: None,
            }
        };

        // Send response
        let mut response_data = Vec::new();
        ciborium::ser::into_writer(&response, &mut response_data)
            .map_err(|e| ConsensusError::InvalidMessage(format!("CBOR serialize failed: {e}")))?;
        MessageFraming::write_message(stream, &response_data).await?;

        if authorized {
            Ok(handshake.node_id)
        } else {
            Err(ConsensusError::AttestationFailure(
                "Peer not authorized".to_string(),
            ))
        }
    }

    /// Sends a message to a specific peer.
    async fn send_message_to_peer(
        target_node_id: String,
        consensus_msg: ConsensusMessage,
        connection: Arc<Mutex<TcpStream>>,
    ) {
        let network_msg = NetworkMessage {
            message: consensus_msg,
            attestation: None, // Could add per-message attestation if needed
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            sender_id: target_node_id.clone(), // This should be our node ID, but we need to pass it
        };

        let mut msg_data = Vec::new();
        if let Err(e) = ciborium::ser::into_writer(&network_msg, &mut msg_data) {
            error!("Failed to serialize message: {}", e);
            return;
        }

        let mut stream = connection.lock().await;
        if let Err(e) = MessageFraming::write_message(&mut *stream, &msg_data).await {
            error!("Failed to send message to peer {}: {}", target_node_id, e);
        }
    }

    /// Maps a hex-encoded public key to a node ID.
    /// Now that we use public keys directly as node IDs, this simply returns the public key.
    fn public_key_to_node_id(public_key_hex: &str) -> String {
        public_key_hex.to_string()
    }

    /// Handles messages from a peer connection.
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    async fn handle_peer_messages(
        peer_node_id: String,
        connection: Arc<Mutex<TcpStream>>,
        consensus_tx: mpsc::UnboundedSender<(String, ConsensusMessage)>,
        peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
        cose_handler: Arc<CoseHandler<G>>,
        discovery_response_tx: Arc<
            std::sync::Mutex<Option<mpsc::UnboundedSender<ClusterDiscoveryResponse>>>,
        >,
        local_node_id: String,
        connections: Arc<RwLock<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) {
        loop {
            let message_result = {
                let mut stream = connection.lock().await;
                MessageFraming::read_message(&mut *stream).await
            };

            match message_result {
                Ok(msg_data) => {
                    // First, try to handle as attested cluster discovery message
                    let consensus_message = Self::try_parse_cluster_discovery_message(
                        &msg_data,
                        &peer_node_id,
                        &cose_handler,
                    )
                    .await;

                    let final_message = consensus_message.map_or_else(
                        || {
                            // Not a cluster discovery message, try regular network message
                            match ciborium::de::from_reader::<NetworkMessage, _>(
                                msg_data.as_slice(),
                            ) {
                                Ok(network_msg) => Some(network_msg.message),
                                Err(e) => {
                                    error!(
                                        "Failed to deserialize message from peer {}: {}",
                                        peer_node_id, e
                                    );
                                    None
                                }
                            }
                        },
                        Some,
                    );

                    if let Some(message) = final_message {
                        // Update last activity
                        {
                            let mut peers_guard = peers.write().await;
                            if let Some(peer) = peers_guard.get_mut(&peer_node_id) {
                                peer.last_activity = SystemTime::now();
                            }
                        }

                        // Handle discovery messages directly in network layer
                        match &message {
                            ConsensusMessage::ClusterDiscovery(_request) => {
                                // This is a discovery REQUEST - respond immediately with "no active cluster"
                                // In a real implementation, this would check actual cluster state from Raft
                                debug!(
                                    "Received cluster discovery request from {}, sending response",
                                    peer_node_id
                                );

                                // Create discovery response
                                let discovery_response = ClusterDiscoveryResponse {
                                    responder_id: local_node_id.clone(),
                                    has_active_cluster: true, // Report active cluster when node is running
                                    current_term: Some(1),    // Basic term for bootstrap
                                    current_leader: Some(local_node_id.clone()), // This node claims to be leader
                                    cluster_size: Some(1), // Start with 1, will grow as nodes join
                                    timestamp: SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs(),
                                };

                                // Send attested response back to peer
                                let response_message =
                                    ConsensusMessage::ClusterDiscoveryResponse(discovery_response);

                                // Create COSE-signed message
                                let mut payload_data = Vec::new();
                                if ciborium::ser::into_writer(&response_message, &mut payload_data)
                                    .is_ok()
                                {
                                    // Create MessagePayload struct for consensus message
                                    let message_payload =
                                        crate::cose::MessagePayload::consensus_message(
                                            payload_data.into(),
                                            local_node_id.clone(),
                                        );

                                    if let Ok(cose_message) =
                                        cose_handler.create_signed_message(&message_payload, None)
                                    {
                                        if let Ok(cose_data) =
                                            cose_handler.serialize_cose_message(&cose_message)
                                        {
                                            // Send response directly through connection
                                            let mut stream = connection.lock().await;
                                            if let Err(e) = MessageFraming::write_message(
                                                &mut *stream,
                                                &cose_data,
                                            )
                                            .await
                                            {
                                                warn!(
                                                    "Failed to send discovery response to {}: {}",
                                                    peer_node_id, e
                                                );
                                            } else {
                                                debug!(
                                                    "Sent cluster discovery response to {}",
                                                    peer_node_id
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            ConsensusMessage::ClusterDiscoveryResponse(response) => {
                                // This is a discovery RESPONSE - forward to response collector
                                debug!(
                                    "Received cluster discovery response from {}: has_cluster={}, leader={:?}",
                                    peer_node_id, response.has_active_cluster, response.current_leader
                                );

                                // Forward to response collector if we have one active
                                if let Ok(discovery_tx_guard) = discovery_response_tx.lock() {
                                    if let Some(ref tx) = *discovery_tx_guard {
                                        if tx.send(response.clone()).is_err() {
                                            debug!("Discovery response collector channel closed");
                                        }
                                    }
                                }
                            }
                            _ => {
                                // Forward non-discovery messages to consensus
                                if let Err(e) = consensus_tx.send((peer_node_id.clone(), message)) {
                                    error!("Failed to forward message to consensus: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("Connection to peer {} closed: {}", peer_node_id, e);
                    break;
                }
            }
        }

        // Cleanup connection and peer when loop exits
        {
            let mut connections_guard = connections.write().await;
            connections_guard.remove(&peer_node_id);
        }
        {
            let mut peers_guard = peers.write().await;
            if let Some(peer) = peers_guard.get_mut(&peer_node_id) {
                peer.connected = false;
            }
        }
        debug!("Peer {} message handler exited", peer_node_id);
    }

    /// Static method to try parsing cluster discovery messages with COSE verification.
    async fn try_parse_cluster_discovery_message(
        message_data: &[u8],
        sender_node_id: &str,
        cose_handler: &CoseHandler<G>,
    ) -> Option<ConsensusMessage> {
        // Try to deserialize as COSE message
        let Ok(cose_message) = cose_handler.deserialize_cose_message(message_data) else {
            return None; // Not a COSE message
        };

        // Verify the COSE signature
        let payload = match cose_handler.verify_signed_message(&cose_message).await {
            Ok(payload) => payload,
            Err(e) => {
                warn!(
                    "COSE verification failed for message from {}: {}",
                    sender_node_id, e
                );
                return None;
            }
        };

        // Check the message type to determine what kind of cluster discovery message this is
        match payload.message_type.as_str() {
            "cluster_discovery" => {
                // This is a cluster discovery request
                match ciborium::de::from_reader::<ClusterDiscoveryRequest, _>(payload.data.as_ref())
                {
                    Ok(discovery_request) => {
                        debug!(
                            "Received attested cluster discovery request from {}",
                            sender_node_id
                        );
                        Some(ConsensusMessage::ClusterDiscovery(discovery_request))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to deserialize cluster discovery request from {}: {}",
                            sender_node_id, e
                        );
                        None
                    }
                }
            }
            "cluster_discovery_response" => {
                // This is a cluster discovery response
                match ciborium::de::from_reader::<ClusterDiscoveryResponse, _>(
                    payload.data.as_ref(),
                ) {
                    Ok(discovery_response) => {
                        debug!(
                            "Received attested cluster discovery response from {}",
                            sender_node_id
                        );
                        Some(ConsensusMessage::ClusterDiscoveryResponse(
                            discovery_response,
                        ))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to deserialize cluster discovery response from {}: {}",
                            sender_node_id, e
                        );
                        None
                    }
                }
            }
            _ => {
                // Not a cluster discovery message
                None
            }
        }
    }
}

#[async_trait]
impl<G, A> ConsensusTransport for TcpTransport<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    async fn send_message(
        &self,
        target_node_id: &str,
        message: ConsensusMessage,
    ) -> Result<(), ConsensusError> {
        if let Err(e) = self
            .consensus_tx
            .send((target_node_id.to_string(), message))
        {
            return Err(ConsensusError::Network(
                format!("Failed to send message: {e}").into(),
            ));
        }
        Ok(())
    }

    fn get_message_sender(&self) -> mpsc::UnboundedSender<(String, ConsensusMessage)> {
        self.consensus_tx.clone()
    }

    async fn get_connected_peers(&self) -> Result<Vec<PeerConnection>, ConsensusError> {
        let peers = self.peers.read().await;
        Ok(peers
            .values()
            .filter(|peer| peer.connected && peer.attestation_verified)
            .cloned()
            .collect())
    }

    async fn start(&self) -> Result<(), ConsensusError> {
        self.start_internal().await
    }

    async fn shutdown(&self) -> Result<(), ConsensusError> {
        // Signal shutdown
        let shutdown_tx = self.shutdown_tx.lock().unwrap().take();
        if let Some(shutdown_tx) = shutdown_tx {
            let _ = shutdown_tx.send(());
        }

        // Wait for background tasks to complete
        let handles = {
            let mut task_handles = self.task_handles.lock().unwrap();
            std::mem::take(&mut *task_handles)
        };

        for handle in handles {
            if !handle.is_finished() {
                handle.abort();
            }
        }

        Ok(())
    }

    async fn discover_existing_clusters(
        &self,
    ) -> Result<Vec<ClusterDiscoveryResponse>, ConsensusError> {
        // Implementation from the original discover_existing_clusters method
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();
        *self.discovery_response_tx.lock().unwrap() = Some(response_tx);

        let peers = self.topology.get_all_peers().await;
        let discovery_request = ClusterDiscoveryRequest {
            requester_id: self.node_id.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        // Send discovery requests to all peers
        for peer in peers {
            let peer_node_id = &peer.public_key;
            if let Err(e) = self
                .send_attested_cluster_discovery(peer_node_id, &discovery_request)
                .await
            {
                warn!(
                    "Failed to send cluster discovery to peer {}: {}",
                    peer_node_id, e
                );
            }
        }

        // Collect responses with timeout
        let mut responses = Vec::new();
        let timeout_duration = Duration::from_secs(5);
        let start_time = tokio::time::Instant::now();

        while start_time.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_millis(100), response_rx.recv()).await {
                Ok(Some(response)) => {
                    responses.push(response);
                }
                Ok(None) => break, // Channel closed
                Err(_) => {}       // Timeout - keep trying
            }
        }

        // Clean up
        *self.discovery_response_tx.lock().unwrap() = None;

        Ok(responses)
    }

    fn local_address(&self) -> Option<SocketAddr> {
        Some(self.listen_addr)
    }

    fn create_router(&self) -> Router {
        panic!("TCP transport does not support HTTP routes - this method should never be called")
    }
}

impl<G, A> TcpTransport<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Sends an attested cluster discovery request to a peer.
    async fn send_attested_cluster_discovery(
        &self,
        peer_node_id: &str,
        discovery_request: &ClusterDiscoveryRequest,
    ) -> Result<(), ConsensusError> {
        let connection = {
            let connections_guard = self.connections.read().await;
            connections_guard
                .get(peer_node_id)
                .cloned()
                .ok_or_else(|| {
                    ConsensusError::Network(format!("No connection to peer {peer_node_id}").into())
                })?
        };

        // Serialize discovery request to CBOR
        let mut discovery_data = Vec::new();
        ciborium::ser::into_writer(discovery_request, &mut discovery_data)
            .map_err(|e| ConsensusError::InvalidMessage(format!("CBOR serialize failed: {e}")))?;

        // Create COSE message payload for cluster discovery
        let payload = crate::cose::MessagePayload::new(
            Bytes::from(discovery_data),
            self.node_id.clone(),
            "cluster_discovery".to_string(),
        );

        // Sign the discovery request with COSE
        let cose_message = self
            .cose_handler
            .create_signed_message(&payload, None)
            .map_err(|e| ConsensusError::InvalidMessage(format!("COSE signing failed: {e}")))?;

        // Serialize the COSE message
        let cose_bytes = self
            .cose_handler
            .serialize_cose_message(&cose_message)
            .map_err(|e| {
                ConsensusError::InvalidMessage(format!("COSE serialization failed: {e}"))
            })?;

        // Send the COSE-signed discovery request
        MessageFraming::write_message(&mut *connection.lock().await, &cose_bytes)
            .await
            .map_err(|e| ConsensusError::Network(format!("Send failed: {e}").into()))?;

        debug!(
            "Sent attested cluster discovery request to peer {}",
            peer_node_id
        );
        Ok(())
    }
}
