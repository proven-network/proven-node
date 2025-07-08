//! TCP transport implementation
//!
//! This transport is responsible ONLY for TCP networking - no business logic,
//! no COSE handling, no attestation verification. All of that is handled by
//! the consensus layer.

use crate::NodeId;
use crate::error::{NetworkError, NetworkResult};
use crate::topology::TopologyManager;
use crate::transport::{MessageHandler, NetworkTransport, PeerConnection};
use crate::verification::ConnectionVerification;

use std::any::Any;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use proven_governance::GovernanceNode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tracing::{debug, warn};

/// Maximum message size (16MB)
const MAX_MESSAGE_SIZE: u32 = 16 * 1024 * 1024;

/// Connection timeout
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);

/// Connection retry attempts
const CONNECTION_RETRY_ATTEMPTS: usize = 3;

/// Delay between connection retry attempts
const CONNECTION_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Type alias for TCP peer connection using channel-based sending
type TcpPeerConnection = PeerConnection<mpsc::UnboundedSender<Bytes>>;

/// Simple TCP transport for raw networking with verification
/// Uses separate incoming and outgoing connections for each peer pair
pub struct TcpTransport<G>
where
    G: proven_governance::Governance + Send + Sync + 'static,
{
    /// Local listening address
    listen_addr: SocketAddr,

    /// Outgoing connections - connections we initiated to other peers
    outgoing_peers: Arc<RwLock<HashMap<NodeId, TcpPeerConnection>>>,

    /// Incoming connections - connections other peers initiated to us
    incoming_peers: Arc<RwLock<HashMap<NodeId, TcpPeerConnection>>>,

    /// Connection verifier for secure handshakes
    verifier: Arc<dyn ConnectionVerification>,

    /// Topology manager for peer lookup
    topology_manager: Arc<TopologyManager<G>>,

    /// Shutdown signal
    shutdown_tx: Arc<std::sync::Mutex<Option<broadcast::Sender<()>>>>,

    /// Background task handles
    task_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,

    /// Message handler for incoming messages (set when listener starts)
    message_handler: Arc<std::sync::Mutex<Option<MessageHandler>>>,
}

impl<G> std::fmt::Debug for TcpTransport<G>
where
    G: proven_governance::Governance + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpTransport")
            .field("listen_addr", &self.listen_addr)
            .field("outgoing_peers", &self.outgoing_peers)
            .field("incoming_peers", &self.incoming_peers)
            .field("verifier", &"<ConnectionVerification>")
            .field("topology_manager", &"<TopologyManager>")
            .field("shutdown_tx", &"<broadcast::Sender>")
            .field("task_handles", &"<Vec<JoinHandle>>")
            .field("message_handler", &"<Option<MessageHandler>>")
            .finish()
    }
}

impl<G> TcpTransport<G>
where
    G: proven_governance::Governance + Send + Sync + 'static,
{
    /// Create a new TCP transport
    pub fn new(
        listen_addr: SocketAddr,
        verifier: Arc<dyn ConnectionVerification>,
        topology_manager: Arc<TopologyManager<G>>,
    ) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            listen_addr,
            outgoing_peers: Arc::new(RwLock::new(HashMap::new())),
            incoming_peers: Arc::new(RwLock::new(HashMap::new())),
            verifier,
            topology_manager,
            shutdown_tx: Arc::new(std::sync::Mutex::new(Some(shutdown_tx))),
            task_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
            message_handler: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Internal method to start the TCP listener
    async fn start_internal(&self, message_handler: MessageHandler) -> NetworkResult<()> {
        debug!("Starting TCP transport on {}", self.listen_addr);

        let (shutdown_tx, _) = broadcast::channel(1);
        *self.shutdown_tx.lock().unwrap() = Some(shutdown_tx.clone());

        // Start TCP listener
        let listener = TcpListener::bind(self.listen_addr)
            .await
            .map_err(|e| NetworkError::BindFailed(e.to_string()))?;

        // Start listener task
        let incoming_peers = self.incoming_peers.clone();
        let verifier = self.verifier.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            tokio::select! {
                () = Self::listener_loop(listener, incoming_peers, verifier, message_handler) => {
                    debug!("TCP listener loop completed");
                }
                _ = shutdown_rx.recv() => {
                    debug!("TCP listener shutting down");
                }
            }
        });

        // Store handle for cleanup
        self.task_handles.lock().unwrap().push(handle);

        Ok(())
    }

    /// TCP listener loop for incoming connections
    async fn listener_loop(
        listener: TcpListener,
        incoming_peers: Arc<RwLock<HashMap<NodeId, TcpPeerConnection>>>,
        verifier: Arc<dyn ConnectionVerification>,
        message_handler: MessageHandler,
    ) {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let incoming_peers = incoming_peers.clone();
                    let verifier = verifier.clone();
                    let message_handler = message_handler.clone();

                    tokio::spawn(async move {
                        Self::handle_incoming_connection(
                            stream,
                            addr,
                            incoming_peers,
                            verifier,
                            message_handler,
                        )
                        .await;
                    });
                }
                Err(e) => {
                    warn!("Failed to accept TCP connection: {}", e);
                    break;
                }
            }
        }
    }

    /// Handle an incoming TCP connection
    async fn handle_incoming_connection(
        stream: TcpStream,
        addr: SocketAddr,
        incoming_peers: Arc<RwLock<HashMap<NodeId, TcpPeerConnection>>>,
        verifier: Arc<dyn ConnectionVerification>,
        message_handler: MessageHandler,
    ) {
        // All connections must be verified - use connection ID for verification
        let connection_id = format!("tcp-{}", addr);

        // Initialize the connection for verification
        verifier.initialize_connection(connection_id.clone()).await;

        // Handle verification and then normal messages
        Self::handle_verified_connection(
            stream,
            connection_id,
            incoming_peers,
            verifier,
            message_handler,
        )
        .await;
    }

    /// Handle a connection with verification handshake
    async fn handle_verified_connection(
        stream: TcpStream,
        connection_id: String,
        incoming_peers: Arc<RwLock<HashMap<NodeId, TcpPeerConnection>>>,
        verifier: Arc<dyn ConnectionVerification>,
        message_handler: MessageHandler,
    ) {
        let stream = Arc::new(tokio::sync::Mutex::new(stream));

        // Handle verification messages until verification completes
        loop {
            let message_result = {
                let mut stream_guard = stream.lock().await;
                Self::read_message(&mut stream_guard).await
            };

            match message_result {
                Ok(data) => {
                    let bytes = Bytes::from(data);

                    // Process verification message
                    match verifier
                        .process_verification_message(connection_id.clone(), bytes)
                        .await
                    {
                        Ok(Some(response_data)) => {
                            // Send verification response
                            let mut stream_guard = stream.lock().await;
                            if let Err(e) =
                                Self::write_message(&mut stream_guard, &response_data).await
                            {
                                warn!("Failed to send verification response: {}", e);
                                break;
                            }
                        }
                        Ok(None) => {
                            // No response needed, continue verification
                        }
                        Err(e) => {
                            warn!(
                                "Verification failed for connection {}: {}",
                                connection_id, e
                            );
                            verifier.remove_connection(&connection_id).await;
                            return;
                        }
                    }

                    // Check if verification is complete
                    if verifier.is_connection_verified(&connection_id).await {
                        if let Some(verified_public_key) =
                            verifier.get_verified_public_key(&connection_id).await
                        {
                            // Create a channel for sending messages to this verified peer
                            let (tx, rx) = mpsc::unbounded_channel();

                            // Store the connection with channel sender
                            incoming_peers.write().insert(
                                verified_public_key.clone(),
                                TcpPeerConnection {
                                    node_id: verified_public_key.clone(),
                                    connected: true,
                                    last_activity: SystemTime::now(),
                                    connection: tx,
                                },
                            );

                            // Switch to normal message handling with verified identity
                            Self::handle_verified_tcp_messages(
                                stream,
                                connection_id,
                                verified_public_key,
                                rx,
                                incoming_peers,
                                message_handler,
                                false,
                            )
                            .await;
                            return;
                        }
                    }
                }
                Err(_) => {
                    verifier.remove_connection(&connection_id).await;
                    break;
                }
            }
        }
    }

    /// Handle messages from a verified TCP connection - UNIDIRECTIONAL
    /// - Incoming connections: Only read messages from remote peer
    /// - Outgoing connections: Only write messages to remote peer
    async fn handle_verified_tcp_messages(
        stream: Arc<tokio::sync::Mutex<TcpStream>>,
        _connection_id: String,
        verified_node_id: NodeId,
        mut outgoing_rx: mpsc::UnboundedReceiver<Bytes>,
        peers: Arc<RwLock<HashMap<NodeId, TcpPeerConnection>>>,
        message_handler: MessageHandler,
        is_outgoing: bool, // true for outgoing connections, false for incoming
    ) {
        if is_outgoing {
            while let Some(data) = outgoing_rx.recv().await {
                let mut stream = stream.lock().await;
                match Self::write_message(&mut stream, &data).await {
                    Ok(()) => {}
                    Err(_) => {
                        break;
                    }
                }
            }
        } else {
            loop {
                let message_result = {
                    let mut stream = stream.lock().await;
                    Self::read_message(&mut stream).await
                };

                match message_result {
                    Ok(data) => {
                        // Update last activity
                        if let Some(peer) = peers.write().get_mut(&verified_node_id) {
                            peer.last_activity = SystemTime::now();
                        }

                        // Forward raw bytes to message handler with verified node ID
                        message_handler(verified_node_id.clone(), Bytes::from(data));
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }

        // Clean up the peer connection when the handler ends
        peers.write().remove(&verified_node_id);
    }

    /// Read a length-prefixed message from a TCP stream
    async fn read_message(stream: &mut TcpStream) -> NetworkResult<Vec<u8>> {
        // Read length prefix with timeout
        let mut len_buf = [0u8; 4];

        let read_result = tokio::time::timeout(
            tokio::time::Duration::from_secs(30), // 30 second timeout
            stream.read_exact(&mut len_buf),
        )
        .await;

        let length_result = match read_result {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(NetworkError::ReceiveFailed(e.to_string())),
            Err(_) => Err(NetworkError::ReceiveFailed("Read timeout".to_string())),
        };

        length_result?;

        let len = u32::from_be_bytes(len_buf);
        if len > MAX_MESSAGE_SIZE {
            return Err(NetworkError::ReceiveFailed("Message too large".to_string()));
        }

        // Read message data with timeout
        let mut data = vec![0u8; len as usize];
        tokio::time::timeout(
            tokio::time::Duration::from_secs(30), // 30 second timeout
            stream.read_exact(&mut data),
        )
        .await
        .map_err(|_| NetworkError::ReceiveFailed("Read timeout".to_string()))?
        .map_err(|e| NetworkError::ReceiveFailed(e.to_string()))?;

        Ok(data)
    }

    /// Write a length-prefixed message to a TCP stream
    async fn write_message(stream: &mut TcpStream, data: &[u8]) -> NetworkResult<()> {
        if data.len() > MAX_MESSAGE_SIZE as usize {
            return Err(NetworkError::SendFailed("Message too large".to_string()));
        }

        let len = data.len() as u32;
        let mut buf = BytesMut::with_capacity(4 + data.len());
        buf.put_u32(len);
        buf.extend_from_slice(data);

        match stream.write_all(&buf).await {
            Ok(()) => match stream.flush().await {
                Ok(()) => Ok(()),
                Err(e) => Err(NetworkError::SendFailed(e.to_string())),
            },
            Err(e) => Err(NetworkError::SendFailed(e.to_string())),
        }
    }

    /// Handle outgoing connection verification handshake
    async fn handle_outgoing_verification(
        &self,
        stream: TcpStream,
        connection_id: String,
        expected_node_id: NodeId,
    ) -> NetworkResult<()> {
        // Initialize the connection for verification
        self.verifier
            .initialize_connection(connection_id.clone())
            .await;

        let stream = Arc::new(tokio::sync::Mutex::new(stream));

        // Send initial verification challenge
        let challenge_request = self
            .verifier
            .create_verification_request_for_connection(connection_id.clone())
            .await
            .map_err(|e| {
                NetworkError::SendFailed(format!("Failed to create verification request: {}", e))
            })?;

        {
            let mut stream_guard = stream.lock().await;
            Self::write_message(&mut stream_guard, &challenge_request).await?;
        }

        // Handle verification messages until verification completes
        loop {
            let message_result = {
                let mut stream_guard = stream.lock().await;
                Self::read_message(&mut stream_guard).await
            };

            match message_result {
                Ok(data) => {
                    let bytes = Bytes::from(data);

                    // Process verification message
                    match self
                        .verifier
                        .process_verification_message(connection_id.clone(), bytes)
                        .await
                    {
                        Ok(Some(response_data)) => {
                            // Send verification response
                            let mut stream_guard = stream.lock().await;
                            if let Err(e) =
                                Self::write_message(&mut stream_guard, &response_data).await
                            {
                                warn!("Failed to send verification response: {}", e);
                                self.verifier.remove_connection(&connection_id).await;
                                return Err(NetworkError::SendFailed(format!(
                                    "Failed to send verification response: {}",
                                    e
                                )));
                            }
                        }
                        Ok(None) => {
                            // No response needed, continue verification
                        }
                        Err(e) => {
                            warn!(
                                "Verification failed for outgoing connection {}: {}",
                                connection_id, e
                            );
                            self.verifier.remove_connection(&connection_id).await;
                            return Err(NetworkError::ConnectionFailed(format!(
                                "Verification failed: {}",
                                e
                            )));
                        }
                    }

                    // Check if verification is complete
                    if self.verifier.is_connection_verified(&connection_id).await {
                        if let Some(verified_public_key) =
                            self.verifier.get_verified_public_key(&connection_id).await
                        {
                            // Verify the public key matches what we expected
                            if verified_public_key != expected_node_id {
                                warn!(
                                    "Verified public key {} does not match expected {}",
                                    &verified_public_key, &expected_node_id
                                );
                                self.verifier.remove_connection(&connection_id).await;
                                return Err(NetworkError::ConnectionFailed(
                                    "Public key mismatch during verification".to_string(),
                                ));
                            }

                            // Create a channel for sending messages to this verified peer
                            let (tx, rx) = mpsc::unbounded_channel();

                            // Store the connection with channel sender
                            self.outgoing_peers.write().insert(
                                verified_public_key.clone(),
                                TcpPeerConnection {
                                    node_id: verified_public_key.clone(),
                                    connected: true,
                                    last_activity: SystemTime::now(),
                                    connection: tx,
                                },
                            );

                            // Clean up verifier state
                            self.verifier.remove_connection(&connection_id).await;

                            // Set up bidirectional message handling for outgoing connection
                            if let Some(message_handler) =
                                self.message_handler.lock().unwrap().clone()
                            {
                                let peers = self.outgoing_peers.clone();
                                let verified_public_key_clone = verified_public_key.clone();
                                let stream_clone = stream.clone();

                                // Start message handling task for this outgoing connection
                                let handle = tokio::spawn(async move {
                                    Self::handle_verified_tcp_messages(
                                        stream_clone,
                                        connection_id.clone(),
                                        verified_public_key_clone,
                                        rx,
                                        peers,
                                        message_handler,
                                        true,
                                    )
                                    .await;
                                });

                                // Store task handle for cleanup
                                self.task_handles.lock().unwrap().push(handle);
                            }

                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    self.verifier.remove_connection(&connection_id).await;
                    return Err(NetworkError::ConnectionFailed(format!(
                        "Connection closed during verification: {}",
                        e
                    )));
                }
            }
        }
    }

    /// Establish an on-demand connection to a peer with full verification
    /// This is used internally when send_bytes needs to create a new connection
    pub async fn establish_connection_on_demand(&self, node: &GovernanceNode) -> NetworkResult<()> {
        let node_id = NodeId::new(node.public_key);

        // Check if already connected (race condition protection)
        if self.outgoing_peers.read().contains_key(&node_id) {
            return Ok(());
        }

        // Get socket address using the Node's tcp_socket_addr method
        let node_wrapper = crate::Node::from(node.clone());
        let address = node_wrapper
            .tcp_socket_addr()
            .await
            .map_err(NetworkError::ConnectionFailed)?;

        debug!("Resolved {} to {}", node.origin, address);

        // Connection with retry logic
        let mut last_error = None;
        for attempt in 1..=CONNECTION_RETRY_ATTEMPTS {
            match timeout(CONNECTION_TIMEOUT, TcpStream::connect(address)).await {
                Ok(Ok(stream)) => {
                    // Handle the verification handshake
                    let connection_id = format!("tcp-outgoing-{}", address);
                    match self
                        .handle_outgoing_verification(stream, connection_id, node_id.clone())
                        .await
                    {
                        Ok(()) => {
                            return Ok(());
                        }
                        Err(e) => {
                            last_error = Some(e);
                        }
                    }
                }
                Ok(Err(e)) => {
                    last_error = Some(NetworkError::ConnectionFailed(e.to_string()));
                }
                Err(_) => {
                    last_error = Some(NetworkError::Timeout("Connection timeout".to_string()));
                }
            }

            // Wait before retry (except on last attempt)
            if attempt < CONNECTION_RETRY_ATTEMPTS {
                tokio::time::sleep(CONNECTION_RETRY_DELAY).await;
            }
        }

        // All attempts failed
        let error = last_error
            .unwrap_or_else(|| NetworkError::ConnectionFailed("Unknown error".to_string()));
        warn!(
            "Failed to establish on-demand connection to peer {} after {} attempts: {}",
            node_id, CONNECTION_RETRY_ATTEMPTS, error
        );
        Err(error)
    }
}

#[async_trait]
impl<G> NetworkTransport for TcpTransport<G>
where
    G: proven_governance::Governance + Send + Sync + 'static,
{
    async fn send_bytes(&self, target_node_id: &NodeId, data: Bytes) -> NetworkResult<()> {
        // For unidirectional connections, we only send via outgoing connections
        // If no outgoing connection exists, establish one on-demand with full verification

        // First try to find existing outgoing connection
        {
            let outgoing_peers = self.outgoing_peers.read();
            if let Some(peer) = outgoing_peers.get(target_node_id) {
                return peer
                    .connection
                    .send(data)
                    .map_err(|_| NetworkError::SendFailed("TCP send failed".to_string()));
            }
        }

        // No existing connection - look up peer in topology and establish connection on-demand
        if let Some(node) = self
            .topology_manager
            .get_peer_by_node_id(target_node_id)
            .await
        {
            // Try to establish connection
            self.establish_connection_on_demand(node.as_governance_node())
                .await?;

            // Now try to send again
            let outgoing_peers = self.outgoing_peers.read();
            if let Some(peer) = outgoing_peers.get(target_node_id) {
                return peer
                    .connection
                    .send(data)
                    .map_err(|_| NetworkError::SendFailed("TCP send failed".to_string()));
            }
        }

        // Still no connection available
        Err(NetworkError::PeerNotConnected)
    }

    async fn start_listener(&self, message_handler: MessageHandler) -> NetworkResult<()> {
        // Store the message handler for outgoing connections
        *self.message_handler.lock().unwrap() = Some(message_handler.clone());

        self.start_internal(message_handler).await
    }

    async fn shutdown(&self) -> NetworkResult<()> {
        debug!("Shutting down TCP transport");

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

    async fn get_connected_peers(&self) -> NetworkResult<Vec<(NodeId, bool, SystemTime)>> {
        let mut connected_peers = Vec::new();

        // Add outgoing connections
        let outgoing_peers = self.outgoing_peers.read();
        for peer in outgoing_peers.values() {
            if peer.connected {
                connected_peers.push((peer.node_id.clone(), peer.connected, peer.last_activity));
            }
        }
        drop(outgoing_peers);

        // Add incoming connections (avoid duplicates)
        let incoming_peers = self.incoming_peers.read();
        for peer in incoming_peers.values() {
            if peer.connected {
                // Check if we already have this peer from outgoing connections
                if !connected_peers
                    .iter()
                    .any(|(node_id, _, _)| node_id == &peer.node_id)
                {
                    connected_peers.push((
                        peer.node_id.clone(),
                        peer.connected,
                        peer.last_activity,
                    ));
                }
            }
        }

        Ok(connected_peers)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
