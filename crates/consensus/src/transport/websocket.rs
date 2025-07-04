//! WebSocket transport implementation
//!
//! This transport handles WebSocket networking and provides HTTP integration
//! capabilities. Like the TCP transport, it only handles raw networking -
//! no business logic, COSE, or attestation verification.

use crate::error::{NetworkError, NetworkResult};
use crate::topology::TopologyManager;
use crate::transport::{HttpIntegratedTransport, MessageHandler, NetworkTransport, PeerConnection};
use crate::types::NodeId;
use crate::verification::ConnectionVerification;

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum::Router;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::{Message, WebSocket};
use axum::response::Response;
use axum::routing::get;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use parking_lot::RwLock;
use proven_governance::GovernanceNode;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::protocol::Message as TungsteniteMessage,
};
use tracing::{debug, info, warn};
use url::Url;
use uuid;

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

type WebSocketPeerConnection =
    PeerConnection<tokio::sync::mpsc::UnboundedSender<axum::extract::ws::Message>>;

/// WebSocket transport for networking with HTTP integration
/// Uses separate incoming and outgoing connections for each peer pair
pub struct WebSocketTransport<G>
where
    G: proven_governance::Governance + Send + Sync + 'static,
{
    /// Outgoing connections - connections we initiated to other peers
    outgoing_peers: Arc<RwLock<HashMap<NodeId, WebSocketPeerConnection>>>,
    /// Incoming connections - connections other peers initiated to us
    incoming_peers: Arc<RwLock<HashMap<NodeId, WebSocketPeerConnection>>>,
    /// Connection verifier for secure handshakes
    verifier: Arc<dyn ConnectionVerification>,
    /// Topology manager for peer lookup
    topology_manager: Arc<TopologyManager<G>>,
    /// Shutdown signal
    shutdown_tx: Arc<std::sync::Mutex<Option<broadcast::Sender<()>>>>,
    /// Background task handles
    task_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
    /// Message handler for incoming messages
    message_handler: Arc<std::sync::Mutex<Option<MessageHandler>>>,
}

impl<G> std::fmt::Debug for WebSocketTransport<G>
where
    G: proven_governance::Governance + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketTransport")
            .field("outgoing_peers", &self.outgoing_peers)
            .field("incoming_peers", &self.incoming_peers)
            .field("topology_manager", &"<TopologyManager>")
            .field("message_handler", &"<MessageHandler>")
            .finish()
    }
}

impl<G> WebSocketTransport<G>
where
    G: proven_governance::Governance + Send + Sync + 'static,
{
    /// Create a new WebSocket transport
    pub fn new(
        verifier: Arc<dyn ConnectionVerification>,
        topology_manager: Arc<TopologyManager<G>>,
    ) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            outgoing_peers: Arc::new(RwLock::new(HashMap::new())),
            incoming_peers: Arc::new(RwLock::new(HashMap::new())),
            verifier,
            topology_manager,
            shutdown_tx: Arc::new(std::sync::Mutex::new(Some(shutdown_tx))),
            task_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
            message_handler: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Handle a WebSocket upgrade request
    async fn handle_websocket_upgrade(
        ws: WebSocketUpgrade,
        incoming_peers: Arc<RwLock<HashMap<NodeId, WebSocketPeerConnection>>>,
        verifier: Arc<dyn ConnectionVerification>,
        message_handler: MessageHandler,
    ) -> Response {
        ws.on_upgrade(move |socket| {
            Self::handle_websocket_connection(socket, incoming_peers, verifier, message_handler)
        })
    }

    /// Handle a WebSocket connection
    async fn handle_websocket_connection(
        socket: WebSocket,
        incoming_peers: Arc<RwLock<HashMap<NodeId, WebSocketPeerConnection>>>,
        verifier: Arc<dyn ConnectionVerification>,
        message_handler: MessageHandler,
    ) {
        // All connections must be verified
        // Generate a unique connection ID for this WebSocket connection
        let connection_id = format!("ws-{}", uuid::Uuid::new_v4());

        // Initialize the connection for verification
        verifier.initialize_connection(connection_id.clone()).await;

        // Handle verification handshake
        Self::handle_verified_websocket_connection(
            socket,
            connection_id,
            incoming_peers,
            verifier,
            message_handler,
        )
        .await;
    }

    /// Handle a WebSocket connection with verification handshake
    async fn handle_verified_websocket_connection(
        socket: WebSocket,
        connection_id: String,
        incoming_peers: Arc<RwLock<HashMap<NodeId, WebSocketPeerConnection>>>,
        verifier: Arc<dyn ConnectionVerification>,
        message_handler: MessageHandler,
    ) {
        let (mut sender, mut receiver) = socket.split();

        // Handle verification messages until verification completes
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    // Process verification message
                    match verifier
                        .process_verification_message(connection_id.clone(), data)
                        .await
                    {
                        Ok(Some(response_data)) => {
                            // Send verification response
                            if let Err(e) = sender.send(Message::Binary(response_data)).await {
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
                            info!(
                                "WebSocket connection {} verified with public key {}",
                                connection_id, &verified_public_key
                            );

                            // Create a channel for sending messages to this verified peer
                            let (tx, rx) = mpsc::unbounded_channel();

                            // Store the connection
                            incoming_peers.write().insert(
                                verified_public_key.clone(),
                                WebSocketPeerConnection {
                                    node_id: verified_public_key.clone(),
                                    connected: true,
                                    last_activity: SystemTime::now(),
                                    connection: tx.clone(),
                                },
                            );

                            // Switch to normal message handling with verified identity
                            Self::handle_verified_websocket_messages(
                                sender,
                                receiver,
                                verified_public_key,
                                rx,
                                incoming_peers,
                                message_handler,
                            )
                            .await;
                            return;
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    debug!(
                        "WebSocket connection closed during verification: {}",
                        connection_id
                    );
                    verifier.remove_connection(&connection_id).await;
                    break;
                }
                Ok(_) => {
                    // Ignore other message types during verification
                }
                Err(e) => {
                    warn!(
                        "WebSocket error during verification for {}: {}",
                        connection_id, e
                    );
                    verifier.remove_connection(&connection_id).await;
                    break;
                }
            }
        }
    }

    /// Handle normal messages for a verified WebSocket connection
    async fn handle_verified_websocket_messages(
        mut sender: futures::stream::SplitSink<WebSocket, Message>,
        mut receiver: futures::stream::SplitStream<WebSocket>,
        verified_node_id: NodeId,
        mut outgoing_rx: mpsc::UnboundedReceiver<Message>,
        peers: Arc<RwLock<HashMap<NodeId, WebSocketPeerConnection>>>,
        message_handler: MessageHandler,
    ) {
        // Spawn task to handle outgoing messages
        let outgoing_task = tokio::spawn(async move {
            while let Some(message) = outgoing_rx.recv().await {
                if sender.send(message).await.is_err() {
                    break;
                }
            }
        });

        // Handle incoming messages
        let node_id_clone = verified_node_id.clone();
        let peers_clone = peers.clone();

        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    // Update last activity
                    if let Some(peer) = peers_clone.write().get_mut(&node_id_clone) {
                        peer.last_activity = SystemTime::now();
                    }

                    // Forward raw bytes to message handler with verified node ID
                    message_handler(node_id_clone.clone(), data);
                }
                Ok(Message::Close(_)) => {
                    break;
                }
                Ok(_) => {
                    // Ignore other message types (text, ping, pong)
                }
                Err(e) => {
                    warn!("WebSocket error for verified node {}: {}", node_id_clone, e);
                    break;
                }
            }
        }

        // Cleanup when connection closes
        outgoing_task.abort();
        peers.write().remove(&verified_node_id);
        if let Some(peer) = peers.write().get_mut(&verified_node_id) {
            peer.connected = false;
        }
    }

    /// Handle outgoing WebSocket connection verification handshake
    async fn handle_outgoing_websocket_verification(
        &self,
        ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        connection_id: String,
        expected_node_id: NodeId,
    ) -> NetworkResult<()> {
        use futures::SinkExt;
        use futures::StreamExt;

        // Initialize the connection for verification
        self.verifier
            .initialize_connection(connection_id.clone())
            .await;

        // Split the WebSocket stream
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        // Send initial verification challenge
        let challenge_request = self
            .verifier
            .create_verification_request_for_connection(connection_id.clone())
            .await
            .map_err(|e| {
                NetworkError::SendFailed(format!("Failed to create verification request: {}", e))
            })?;

        // Send challenge as binary WebSocket message
        if let Err(e) = ws_sender
            .send(TungsteniteMessage::Binary(challenge_request.to_vec()))
            .await
        {
            self.verifier.remove_connection(&connection_id).await;
            return Err(NetworkError::SendFailed(format!(
                "Failed to send verification challenge: {}",
                e
            )));
        }

        // Handle verification messages until verification completes
        loop {
            let message_result = ws_receiver.next().await;

            match message_result {
                Some(Ok(TungsteniteMessage::Binary(data))) => {
                    let bytes = Bytes::from(data);

                    // Process verification message
                    match self
                        .verifier
                        .process_verification_message(connection_id.clone(), bytes)
                        .await
                    {
                        Ok(Some(response_data)) => {
                            // Send verification response
                            if let Err(e) = ws_sender
                                .send(TungsteniteMessage::Binary(response_data.to_vec()))
                                .await
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
                                "Verification failed for outgoing WebSocket connection {}: {}",
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

                            info!(
                                "Outgoing WebSocket connection {} verified with public key {}",
                                connection_id, &verified_public_key
                            );

                            // Recreate the WebSocket stream from the split parts
                            let ws_stream = ws_sender.reunite(ws_receiver).map_err(|e| {
                                NetworkError::ConnectionFailed(format!(
                                    "Failed to reunite WebSocket stream: {}",
                                    e
                                ))
                            })?;

                            // Store the connection with verified public key
                            self.store_verified_websocket_connection(
                                ws_stream,
                                verified_public_key.clone(),
                            )
                            .await?;

                            // Clean up verifier state
                            self.verifier.remove_connection(&connection_id).await;

                            return Ok(());
                        }
                    }
                }
                Some(Ok(TungsteniteMessage::Close(_))) => {
                    self.verifier.remove_connection(&connection_id).await;
                    return Err(NetworkError::ConnectionFailed(
                        "Connection closed during verification".to_string(),
                    ));
                }
                Some(Ok(_)) => {
                    // Ignore other message types during verification
                }
                Some(Err(e)) => {
                    warn!("WebSocket error during verification: {}", e);
                    self.verifier.remove_connection(&connection_id).await;
                    return Err(NetworkError::ConnectionFailed(format!(
                        "WebSocket error during verification: {}",
                        e
                    )));
                }
                None => {
                    self.verifier.remove_connection(&connection_id).await;
                    return Err(NetworkError::ConnectionFailed(
                        "Connection stream ended during verification".to_string(),
                    ));
                }
            }
        }
    }

    /// Store a verified WebSocket connection and start message handling
    async fn store_verified_websocket_connection(
        &self,
        ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        verified_public_key: NodeId,
    ) -> NetworkResult<()> {
        // Split the WebSocket stream
        let (ws_sender, ws_receiver) = ws_stream.split();

        // Create channel for sending messages to this peer
        let (tx, rx) = mpsc::unbounded_channel();

        // Store the sender in connections
        self.outgoing_peers.write().insert(
            verified_public_key.clone(),
            WebSocketPeerConnection {
                node_id: verified_public_key.clone(),
                connected: true,
                last_activity: SystemTime::now(),
                connection: tx.clone(),
            },
        );

        // Get message handler and set up bidirectional message handling
        let message_handler = self.message_handler.lock().unwrap().clone();

        // If we have a message handler, start message handling
        if let Some(message_handler) = message_handler {
            // Start message handling tasks
            let peers = self.outgoing_peers.clone();

            let verified_public_key_clone = verified_public_key.clone();
            let handle = tokio::spawn(async move {
                Self::handle_outgoing_websocket_messages(
                    ws_sender,
                    ws_receiver,
                    verified_public_key_clone,
                    rx,
                    peers,
                    message_handler,
                )
                .await;
            });

            // Store task handle for cleanup
            self.task_handles.lock().unwrap().push(handle);
        } else {
            warn!(
                "No message handler available for outgoing WebSocket connection to {}. Messages can only be sent, not received.",
                &verified_public_key
            );
        }

        Ok(())
    }

    /// Handle messages for outgoing WebSocket connections (using tokio-tungstenite types)
    async fn handle_outgoing_websocket_messages(
        mut sender: futures::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<TcpStream>>,
            TungsteniteMessage,
        >,
        mut receiver: futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        verified_node_id: NodeId,
        mut outgoing_rx: mpsc::UnboundedReceiver<Message>,
        peers: Arc<RwLock<HashMap<NodeId, WebSocketPeerConnection>>>,
        message_handler: MessageHandler,
    ) {
        use futures::FutureExt;

        loop {
            tokio::select! {
                // Handle outgoing messages from our internal channel
                outgoing_msg = outgoing_rx.recv() => {
                    match outgoing_msg {
                        Some(msg) => {
                            // Convert axum Message to tungstenite Message
                            let tungstenite_msg = match msg {
                                Message::Binary(data) => TungsteniteMessage::Binary(data.to_vec()),
                                Message::Text(text) => TungsteniteMessage::Text(text.to_string()),
                                Message::Close(frame) => {
                                    if let Some(frame) = frame {
                                        TungsteniteMessage::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(frame.code),
                                            reason: frame.reason.to_string().into(),
                                        }))
                                    } else {
                                        TungsteniteMessage::Close(None)
                                    }
                                }
                                _ => continue, // Skip other message types
                            };

                            if (sender.send(tungstenite_msg).await).is_err() {
                                break;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }

                // Handle incoming messages from the WebSocket
                incoming_msg = receiver.next().fuse() => {
                    match incoming_msg {
                        Some(Ok(TungsteniteMessage::Binary(data))) => {
                            // Update last activity
                            if let Some(peer) = peers.write().get_mut(&verified_node_id) {
                                peer.last_activity = SystemTime::now();
                            }

                            // Forward raw bytes to message handler
                            message_handler(verified_node_id.clone(), Bytes::from(data));
                        }
                        Some(Ok(TungsteniteMessage::Close(_))) => {
                            break;
                        }
                        Some(Ok(_)) => {
                            // Ignore other message types (text, ping, pong)
                        }
                        Some(Err(e)) => {
                            warn!("WebSocket error from peer {}: {}", verified_node_id, e);
                            break;
                        }
                        None => {
                            break;
                        }
                    }
                }
            }
        }

        // Cleanup when connection ends
        peers.write().remove(&verified_node_id);
        if let Some(peer) = peers.write().get_mut(&verified_node_id) {
            peer.connected = false;
        }
    }

    async fn start_internal(&self, message_handler: MessageHandler) -> NetworkResult<()> {
        info!("WebSocket transport ready for HTTP integration");

        // Store the message handler
        *self.message_handler.lock().unwrap() = Some(message_handler.clone());

        let (shutdown_tx, _) = broadcast::channel(1);
        *self.shutdown_tx.lock().unwrap() = Some(shutdown_tx);

        Ok(())
    }

    /// Establish an on-demand WebSocket connection to a peer
    /// This creates a WebSocket connection via HTTP upgrade to the peer's origin
    pub async fn establish_connection_on_demand(&self, node: &GovernanceNode) -> NetworkResult<()> {
        let node_id = NodeId::new(node.public_key);

        // Check if connection already exists
        {
            let outgoing_peers = self.outgoing_peers.read();
            if outgoing_peers.contains_key(&node_id) {
                return Ok(());
            }
        }

        // Parse the origin URL and convert http/https to ws/wss
        let mut url = Url::parse(&node.origin).map_err(|e| {
            NetworkError::ConnectionFailed(format!("Invalid origin URL '{}': {}", node.origin, e))
        })?;

        // Convert HTTP schemes to WebSocket schemes
        let current_scheme = url.scheme().to_string();
        let ws_scheme = match current_scheme.as_str() {
            "http" => "ws",
            "https" => "wss",
            "ws" | "wss" => &current_scheme, // Already a WebSocket URL
            scheme => {
                return Err(NetworkError::ConnectionFailed(format!(
                    "Unsupported scheme '{}' in origin '{}'. Expected http, https, ws, or wss",
                    scheme, node.origin
                )));
            }
        };

        url.set_scheme(ws_scheme).map_err(|_| {
            NetworkError::ConnectionFailed(format!(
                "Failed to set WebSocket scheme for origin '{}'",
                node.origin
            ))
        })?;

        // Add the WebSocket endpoint path
        // Ensure no double slashes by trimming trailing slash from URL
        let base_url = url.as_str().trim_end_matches('/');
        let ws_url = format!("{}/consensus/ws", base_url);

        // Connect with timeout
        let connect_result = tokio::time::timeout(CONNECTION_TIMEOUT, connect_async(&ws_url)).await;

        let (ws_stream, _response) = match connect_result {
            Ok(Ok((stream, response))) => (stream, response),
            Ok(Err(e)) => {
                return Err(NetworkError::ConnectionFailed(format!(
                    "WebSocket connection failed to {}: {}",
                    ws_url, e
                )));
            }
            Err(_) => {
                return Err(NetworkError::Timeout(format!(
                    "WebSocket connection timeout to {}",
                    ws_url
                )));
            }
        };

        // Handle outgoing verification handshake
        let connection_id = format!("ws-outgoing-{}", node_id);
        self.handle_outgoing_websocket_verification(ws_stream, connection_id, node_id)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl<G> NetworkTransport for WebSocketTransport<G>
where
    G: proven_governance::Governance + Send + Sync + 'static,
{
    async fn send_bytes(&self, target_node_id: &NodeId, data: Bytes) -> NetworkResult<()> {
        // For unidirectional connections, we only send via outgoing connections
        // If no outgoing connection exists, establish one on-demand

        // First try to find existing outgoing connection
        {
            let outgoing_peers = self.outgoing_peers.read();
            if let Some(peer) = outgoing_peers.get(target_node_id) {
                let message = Message::Binary(data);
                return peer
                    .connection
                    .send(message)
                    .map_err(|_| NetworkError::SendFailed("WebSocket send failed".to_string()));
            }
        }

        // No existing connection - look up peer in topology and establish connection on-demand
        if let Some(governance_node) = self
            .topology_manager
            .get_peer_by_node_id(target_node_id)
            .await
        {
            // Try to establish connection
            self.establish_connection_on_demand(&governance_node)
                .await?;

            // Now try to send again
            let outgoing_peers = self.outgoing_peers.read();
            if let Some(peer) = outgoing_peers.get(target_node_id) {
                let message = Message::Binary(data);
                return peer
                    .connection
                    .send(message)
                    .map_err(|_| NetworkError::SendFailed("WebSocket send failed".to_string()));
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
        info!("Shutting down WebSocket transport");

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

impl<G> HttpIntegratedTransport for WebSocketTransport<G>
where
    G: proven_governance::Governance + Send + Sync + 'static,
{
    fn websocket_endpoint(&self) -> &'static str {
        "/consensus/ws"
    }

    fn create_router_integration(&self) -> NetworkResult<Router> {
        let message_handler = self
            .message_handler
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| {
                NetworkError::TransportNotSupported(
                    "Message handler not set - call start_listener first".to_string(),
                )
            })?;

        let incoming_peers = self.incoming_peers.clone();
        let verifier = self.verifier.clone();

        let router = Router::new().route(
            "/consensus/ws",
            get(move |ws| {
                Self::handle_websocket_upgrade(ws, incoming_peers, verifier, message_handler)
            }),
        );

        Ok(router)
    }
}
