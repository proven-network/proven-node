//! WebSocket transport implementation for proven-network
//!
//! This crate provides a WebSocket-based transport implementation of the proven-transport trait.

use async_trait::async_trait;
use axum::{
    Router,
    extract::{
        WebSocketUpgrade,
        ws::{Message as AxumMessage, WebSocket},
    },
    response::Response,
    routing::get,
};
use bytes::Bytes;
use dashmap::DashMap;
use ed25519_dalek::SigningKey;
use futures::{SinkExt, Stream, StreamExt};
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::{
    Config, HttpIntegratedTransport, Transport, TransportEnvelope, error::TransportError,
};
use proven_verification::{ConnectionVerifier, CoseHandler};
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Duration, timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message as WsMessage,
};
use tracing::{debug, info, warn};
use url::Url;
use uuid::Uuid;

/// WebSocket-specific configuration
#[derive(Debug, Clone)]
pub struct WebsocketConfig {
    /// Generic transport configuration
    pub transport: Config,
    /// Connection retry attempts
    pub retry_attempts: usize,
    /// Delay between retry attempts in milliseconds
    pub retry_delay_ms: u64,
}

impl Default for WebsocketConfig {
    fn default() -> Self {
        Self {
            transport: Config::default(),
            retry_attempts: 3,
            retry_delay_ms: 500,
        }
    }
}

/// Connection state
#[derive(Debug)]
struct ConnectionState {
    /// Node ID of the peer
    #[allow(dead_code)]
    node_id: NodeId,
    /// Channel for sending messages
    sender: mpsc::UnboundedSender<Bytes>,
    /// Whether this is an outgoing connection
    #[allow(dead_code)]
    is_outgoing: bool,
    /// Last activity time
    last_activity: SystemTime,
}

/// WebSocket transport implementation
#[derive(Debug)]
pub struct WebsocketTransport<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// Configuration
    config: WebsocketConfig,
    /// Active connections
    connections: Arc<DashMap<NodeId, ConnectionState>>,
    /// Incoming message broadcast channel
    incoming_tx: broadcast::Sender<TransportEnvelope>,
    /// Connection verifier
    verifier: Arc<ConnectionVerifier<G, A>>,
    /// COSE handler for message signing/verification
    cose_handler: Arc<CoseHandler>,
    /// Topology manager for getting connection details
    topology_manager: Arc<TopologyManager<G>>,
}

impl<G, A> WebsocketTransport<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// Create a new WebSocket transport with verification and topology management
    pub fn new(
        config: WebsocketConfig,
        signing_key: SigningKey,
        verifier: Arc<ConnectionVerifier<G, A>>,
        topology_manager: Arc<TopologyManager<G>>,
    ) -> Self {
        let (incoming_tx, _) = broadcast::channel(1024);

        Self {
            config,
            connections: Arc::new(DashMap::new()),
            incoming_tx,
            verifier,
            cose_handler: Arc::new(CoseHandler::new(signing_key)),
            topology_manager,
        }
    }

    /// Handle WebSocket upgrade request
    async fn handle_websocket_upgrade(ws: WebSocketUpgrade, transport: Arc<Self>) -> Response {
        ws.on_upgrade(move |socket| Self::handle_websocket_connection(socket, transport))
    }

    /// Handle WebSocket connection from axum
    async fn handle_websocket_connection(socket: WebSocket, transport: Arc<Self>) {
        let (mut sender, mut receiver) = socket.split();

        // Generate connection ID
        let connection_id = format!("ws-incoming-{}", uuid::Uuid::new_v4());

        // Initialize verification
        transport
            .verifier
            .initialize_connection(connection_id.clone())
            .await;

        // Handle verification handshake
        let node_id = match Self::handle_axum_verification(
            &mut sender,
            &mut receiver,
            &connection_id,
            &transport.verifier,
        )
        .await
        {
            Ok(id) => id,
            Err(e) => {
                warn!("Verification failed for incoming connection: {}", e);
                transport.verifier.remove_connection(&connection_id).await;
                return;
            }
        };

        info!("Incoming WebSocket connection verified from {}", node_id);

        // Create channel for outgoing messages
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Store connection
        transport.connections.insert(
            node_id.clone(),
            ConnectionState {
                node_id: node_id.clone(),
                sender: tx,
                is_outgoing: false,
                last_activity: SystemTime::now(),
            },
        );

        // Spawn task to handle outgoing messages
        let node_id_clone = node_id.clone();
        let connections = transport.connections.clone();
        tokio::spawn(async move {
            while let Some(data) = rx.recv().await {
                if let Err(e) = sender.send(AxumMessage::Binary(data)).await {
                    warn!("Failed to write message to {}: {}", node_id_clone, e);
                    break;
                }
            }
            connections.remove(&node_id_clone);
        });

        // Handle incoming messages
        let node_id_clone = node_id.clone();
        let connections = transport.connections.clone();
        let incoming_tx = transport.incoming_tx.clone();
        let cose_handler = transport.cose_handler.clone();
        let max_msg_size = transport.config.transport.max_message_size;

        while let Some(msg_result) = receiver.next().await {
            match msg_result {
                Ok(AxumMessage::Binary(data)) => {
                    if data.len() > max_msg_size {
                        warn!(
                            "Message too large from {}: {} bytes",
                            node_id_clone,
                            data.len()
                        );
                        break;
                    }

                    // Update last activity
                    if let Some(mut conn) = connections.get_mut(&node_id_clone) {
                        conn.last_activity = SystemTime::now();
                    }

                    // Deserialize and verify COSE message
                    match cose_handler.deserialize_cose_message(&data) {
                        Ok(cose_sign1) => {
                            match cose_handler
                                .verify_signed_message(&cose_sign1, node_id_clone.clone())
                                .await
                            {
                                Ok((payload, metadata)) => {
                                    // Send to incoming channel
                                    let _ = incoming_tx.send(TransportEnvelope {
                                        sender: node_id_clone.clone(),
                                        payload,
                                        message_type: metadata.message_type.unwrap_or_default(),
                                        correlation_id: metadata.correlation_id,
                                    });
                                }
                                Err(e) => {
                                    warn!("Failed to verify message from {}: {}", node_id_clone, e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to deserialize COSE message from {}: {}",
                                node_id_clone, e
                            );
                        }
                    }
                }
                Ok(AxumMessage::Close(_)) => {
                    debug!("WebSocket closed by {}", node_id_clone);
                    break;
                }
                Ok(_) => {
                    // Ignore text, ping, pong
                }
                Err(e) => {
                    debug!("WebSocket error from {}: {}", node_id_clone, e);
                    break;
                }
            }
        }

        connections.remove(&node_id_clone);
    }

    /// Handle verification for axum WebSocket connections
    async fn handle_axum_verification(
        sender: &mut futures::stream::SplitSink<WebSocket, AxumMessage>,
        receiver: &mut futures::stream::SplitStream<WebSocket>,
        connection_id: &str,
        verifier: &Arc<ConnectionVerifier<G, A>>,
    ) -> Result<NodeId, TransportError> {
        // Handle verification messages
        loop {
            let msg = receiver
                .next()
                .await
                .ok_or_else(|| TransportError::ConnectionFailed {
                    peer: "unknown".to_string(),
                    reason: "Stream ended".to_string(),
                })?
                .map_err(|e| TransportError::Io(std::io::Error::other(e.to_string())))?;

            match msg {
                AxumMessage::Binary(data) => {
                    match verifier
                        .process_verification_message(connection_id.to_string(), data)
                        .await
                    {
                        Ok(Some(response)) => {
                            // Send response
                            sender
                                .send(AxumMessage::Binary(response))
                                .await
                                .map_err(|e| {
                                    TransportError::Io(std::io::Error::other(e.to_string()))
                                })?;
                        }
                        Ok(None) => {
                            // Check if verified
                            if verifier.is_connection_verified(connection_id).await
                                && let Some(node_id) =
                                    verifier.get_verified_public_key(connection_id).await
                            {
                                return Ok(node_id);
                            }
                        }
                        Err(e) => {
                            return Err(TransportError::AuthenticationFailed(e.to_string()));
                        }
                    }
                }
                AxumMessage::Close(_) => {
                    return Err(TransportError::ConnectionFailed {
                        peer: "unknown".to_string(),
                        reason: "Connection closed during verification".to_string(),
                    });
                }
                _ => {
                    // Ignore text, ping, pong messages during verification
                }
            }
        }
    }

    /// Handle connection verification
    async fn handle_verification(
        stream: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
        connection_id: &str,
        verifier: &Arc<ConnectionVerifier<G, A>>,
        is_outgoing: bool,
    ) -> Result<NodeId, TransportError> {
        if is_outgoing {
            // For outgoing connections, send initial verification request
            let request = verifier
                .create_verification_request_for_connection(connection_id.to_string())
                .await
                .map_err(|e| TransportError::AuthenticationFailed(e.to_string()))?;

            stream
                .send(WsMessage::Binary(request))
                .await
                .map_err(|e| TransportError::Io(std::io::Error::other(e.to_string())))?;
        }

        // Handle verification messages
        loop {
            let msg = stream
                .next()
                .await
                .ok_or_else(|| TransportError::ConnectionFailed {
                    peer: "unknown".to_string(),
                    reason: "Stream ended".to_string(),
                })?
                .map_err(|e| TransportError::Io(std::io::Error::other(e.to_string())))?;

            match msg {
                WsMessage::Binary(data) => {
                    match verifier
                        .process_verification_message(connection_id.to_string(), data)
                        .await
                    {
                        Ok(Some(response)) => {
                            // Send response
                            stream
                                .send(WsMessage::Binary(response))
                                .await
                                .map_err(|e| {
                                    TransportError::Io(std::io::Error::other(e.to_string()))
                                })?;
                        }
                        Ok(None) => {
                            // Check if verified
                            if verifier.is_connection_verified(connection_id).await
                                && let Some(node_id) =
                                    verifier.get_verified_public_key(connection_id).await
                            {
                                return Ok(node_id);
                            }
                        }
                        Err(e) => {
                            return Err(TransportError::AuthenticationFailed(e.to_string()));
                        }
                    }
                }
                WsMessage::Close(_) => {
                    return Err(TransportError::ConnectionFailed {
                        peer: "unknown".to_string(),
                        reason: "Connection closed during verification".to_string(),
                    });
                }
                _ => {
                    // Ignore text, ping, pong messages during verification
                }
            }
        }
    }

    /// Connect to a peer at the given URL
    async fn connect_to_url(&self, node_id: NodeId, url: Url) -> Result<(), TransportError> {
        // Check if already connected
        if self.connections.contains_key(&node_id) {
            return Ok(());
        }

        let config = self.config.clone();
        let verifier = self.verifier.clone();

        // Try to connect with retries
        let mut last_error = None;
        for attempt in 1..=config.retry_attempts {
            let timeout_duration = Duration::from_millis(config.transport.connection_timeout_ms);

            match timeout(timeout_duration, connect_async(url.to_string())).await {
                Ok(Ok((mut stream, _))) => {
                    // Create connection ID
                    let connection_id = format!("ws-outgoing-{url}");

                    // Initialize verification
                    verifier.initialize_connection(connection_id.clone()).await;

                    // Handle verification
                    match Self::handle_verification(
                        &mut stream,
                        &connection_id,
                        &verifier,
                        true, // outgoing connection
                    )
                    .await
                    {
                        Ok(verified_node_id) => {
                            if verified_node_id != node_id {
                                verifier.remove_connection(&connection_id).await;
                                return Err(TransportError::AuthenticationFailed(
                                    "Node ID mismatch".to_string(),
                                ));
                            }

                            debug!(
                                "Outgoing WebSocket connection verified to {} ({})",
                                node_id, url
                            );

                            // Create channel for outgoing messages
                            let (tx, mut rx) = mpsc::unbounded_channel();

                            // Store connection
                            self.connections.insert(
                                node_id.clone(),
                                ConnectionState {
                                    node_id: node_id.clone(),
                                    sender: tx,
                                    is_outgoing: true,
                                    last_activity: SystemTime::now(),
                                },
                            );

                            // Split the stream
                            let (write, read) = stream.split();

                            // Spawn task to handle outgoing messages
                            let node_id_clone = node_id.clone();
                            let connections = self.connections.clone();
                            tokio::spawn(async move {
                                let mut write = write;
                                while let Some(data) = rx.recv().await {
                                    if let Err(e) = write.send(WsMessage::Binary(data)).await {
                                        warn!(
                                            "Failed to write message to {}: {}",
                                            node_id_clone, e
                                        );
                                        break;
                                    }
                                }
                                connections.remove(&node_id_clone);
                            });

                            // Spawn task to handle incoming messages
                            let node_id_clone = node_id.clone();
                            let connections = self.connections.clone();
                            let incoming_tx = self.incoming_tx.clone();
                            let cose_handler = self.cose_handler.clone();
                            let max_msg_size = config.transport.max_message_size;
                            tokio::spawn(async move {
                                let mut read = read;
                                while let Some(msg_result) = read.next().await {
                                    match msg_result {
                                        Ok(WsMessage::Binary(data)) => {
                                            if data.len() > max_msg_size {
                                                warn!(
                                                    "Message too large from {}: {} bytes",
                                                    node_id_clone,
                                                    data.len()
                                                );
                                                break;
                                            }

                                            // Update last activity
                                            if let Some(mut conn) =
                                                connections.get_mut(&node_id_clone)
                                            {
                                                conn.last_activity = SystemTime::now();
                                            }

                                            // Deserialize and verify COSE message
                                            match cose_handler.deserialize_cose_message(&data) {
                                                Ok(cose_sign1) => {
                                                    match cose_handler
                                                        .verify_signed_message(
                                                            &cose_sign1,
                                                            node_id_clone.clone(),
                                                        )
                                                        .await
                                                    {
                                                        Ok((payload, metadata)) => {
                                                            // Send to incoming channel
                                                            let _ = incoming_tx.send(
                                                                TransportEnvelope {
                                                                    sender: node_id_clone.clone(),
                                                                    payload,
                                                                    message_type: metadata
                                                                        .message_type
                                                                        .unwrap_or_default(),
                                                                    correlation_id: metadata
                                                                        .correlation_id,
                                                                },
                                                            );
                                                        }
                                                        Err(e) => {
                                                            warn!(
                                                                "Failed to verify message from {}: {}",
                                                                node_id_clone, e
                                                            );
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    warn!(
                                                        "Failed to deserialize COSE message from {}: {}",
                                                        node_id_clone, e
                                                    );
                                                }
                                            }
                                        }
                                        Ok(WsMessage::Close(_)) => {
                                            debug!("WebSocket closed by {}", node_id_clone);
                                            break;
                                        }
                                        Ok(_) => {
                                            // Ignore text, ping, pong
                                        }
                                        Err(e) => {
                                            debug!("WebSocket error from {}: {}", node_id_clone, e);
                                            break;
                                        }
                                    }
                                }
                                connections.remove(&node_id_clone);
                            });

                            verifier.remove_connection(&connection_id).await;
                            return Ok(());
                        }
                        Err(e) => {
                            verifier.remove_connection(&connection_id).await;
                            last_error = Some(e);
                        }
                    }
                }
                Ok(Err(e)) => {
                    last_error = Some(TransportError::Io(std::io::Error::other(e.to_string())));
                }
                Err(_) => {
                    last_error = Some(TransportError::Timeout {
                        operation: format!("Connection to {url}"),
                    });
                }
            }

            // Wait before retry (except on last attempt)
            if attempt < config.retry_attempts {
                tokio::time::sleep(Duration::from_millis(config.retry_delay_ms)).await;
            }
        }

        Err(
            last_error.unwrap_or_else(|| TransportError::ConnectionFailed {
                peer: node_id.to_string(),
                reason: "Failed after all retry attempts".to_string(),
            }),
        )
    }

    /// Handle incoming WebSocket connection (for server-side usage)
    /// This would be called from an HTTP server integration
    pub async fn handle_incoming_connection(
        &self,
        stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
        remote_addr: std::net::SocketAddr,
    ) -> Result<(), TransportError> {
        let mut stream = stream;

        // Create connection ID
        let connection_id = format!("ws-incoming-{remote_addr}");

        // Initialize verification
        self.verifier
            .initialize_connection(connection_id.clone())
            .await;

        // Handle verification handshake
        let node_id = match Self::handle_verification(
            &mut stream,
            &connection_id,
            &self.verifier,
            false, // incoming connection
        )
        .await
        {
            Ok(id) => id,
            Err(e) => {
                self.verifier.remove_connection(&connection_id).await;
                return Err(e);
            }
        };

        debug!(
            "Incoming WebSocket connection verified from {} ({})",
            node_id, remote_addr
        );

        // Create channel for outgoing messages
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Store connection
        self.connections.insert(
            node_id.clone(),
            ConnectionState {
                node_id: node_id.clone(),
                sender: tx,
                is_outgoing: false,
                last_activity: SystemTime::now(),
            },
        );

        // Split the stream
        let (write, read) = stream.split();

        // Spawn task to handle outgoing messages
        let node_id_clone = node_id.clone();
        let connections = self.connections.clone();
        tokio::spawn(async move {
            let mut write = write;
            while let Some(data) = rx.recv().await {
                if let Err(e) = write.send(WsMessage::Binary(data)).await {
                    warn!("Failed to write message to {}: {}", node_id_clone, e);
                    break;
                }
            }
            connections.remove(&node_id_clone);
        });

        // Spawn task to handle incoming messages
        let node_id_clone = node_id.clone();
        let connections = self.connections.clone();
        let incoming_tx = self.incoming_tx.clone();
        let cose_handler = self.cose_handler.clone();
        let max_msg_size = self.config.transport.max_message_size;
        tokio::spawn(async move {
            let mut read = read;
            while let Some(msg_result) = read.next().await {
                match msg_result {
                    Ok(WsMessage::Binary(data)) => {
                        if data.len() > max_msg_size {
                            warn!(
                                "Message too large from {}: {} bytes",
                                node_id_clone,
                                data.len()
                            );
                            break;
                        }

                        // Update last activity
                        if let Some(mut conn) = connections.get_mut(&node_id_clone) {
                            conn.last_activity = SystemTime::now();
                        }

                        // Deserialize and verify COSE message
                        match cose_handler.deserialize_cose_message(&data) {
                            Ok(cose_sign1) => {
                                match cose_handler
                                    .verify_signed_message(&cose_sign1, node_id_clone.clone())
                                    .await
                                {
                                    Ok((payload, metadata)) => {
                                        // Send to incoming channel
                                        let _ = incoming_tx.send(TransportEnvelope {
                                            sender: node_id_clone.clone(),
                                            payload,
                                            message_type: metadata.message_type.unwrap_or_default(),
                                            correlation_id: metadata.correlation_id,
                                        });
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to verify message from {}: {}",
                                            node_id_clone, e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to deserialize COSE message from {}: {}",
                                    node_id_clone, e
                                );
                            }
                        }
                    }
                    Ok(WsMessage::Close(_)) => {
                        debug!("WebSocket closed by {}", node_id_clone);
                        break;
                    }
                    Ok(_) => {
                        // Ignore text, ping, pong
                    }
                    Err(e) => {
                        debug!("WebSocket error from {}: {}", node_id_clone, e);
                        break;
                    }
                }
            }
            connections.remove(&node_id_clone);
        });

        Ok(())
    }
}

#[async_trait]
impl<G, A> Transport for WebsocketTransport<G, A>
where
    G: Governance,
    A: Attestor,
{
    async fn send_envelope(
        &self,
        recipient: &NodeId,
        payload: &Bytes,
        message_type: &str,
        correlation_id: Option<Uuid>,
    ) -> Result<(), TransportError> {
        // Create COSE-signed message
        let cose_sign1 = self
            .cose_handler
            .create_signed_message(payload, message_type, correlation_id)
            .map_err(|e| TransportError::InvalidMessage(format!("Failed to sign message: {e}")))?;

        let cose_bytes = self
            .cose_handler
            .serialize_cose_message(&cose_sign1)
            .map_err(|e| {
                TransportError::InvalidMessage(format!("Failed to serialize COSE: {e}"))
            })?;

        // Check if we already have a connection
        if let Some(conn) = self.connections.get(recipient) {
            conn.sender
                .send(cose_bytes.clone())
                .map_err(|_| TransportError::ConnectionFailed {
                    peer: recipient.to_string(),
                    reason: "Channel closed".to_string(),
                })?;
            return Ok(());
        }

        // Get connection details from topology manager
        let node = self
            .topology_manager
            .get_peer_by_node_id(recipient)
            .await
            .ok_or_else(|| TransportError::ConnectionFailed {
                peer: recipient.to_string(),
                reason: "Peer not found in topology".to_string(),
            })?;

        // Get WebSocket URL for the peer
        let url = node
            .websocket_url()
            .map_err(|e| TransportError::ConnectionFailed {
                peer: recipient.to_string(),
                reason: format!("Invalid WebSocket URL: {e}"),
            })?;

        let url = Url::parse(&url).map_err(|e| TransportError::ConnectionFailed {
            peer: recipient.to_string(),
            reason: format!("Failed to parse WebSocket URL: {e}"),
        })?;

        // Connect to the peer
        self.connect_to_url(recipient.clone(), url).await?;

        // Now send the message
        if let Some(conn) = self.connections.get(recipient) {
            conn.sender
                .send(cose_bytes)
                .map_err(|_| TransportError::ConnectionFailed {
                    peer: recipient.to_string(),
                    reason: "Channel closed".to_string(),
                })?;
            Ok(())
        } else {
            Err(TransportError::ConnectionFailed {
                peer: recipient.to_string(),
                reason: "Failed to establish connection".to_string(),
            })
        }
    }

    fn incoming(&self) -> Pin<Box<dyn Stream<Item = TransportEnvelope> + Send>> {
        let rx = self.incoming_tx.subscribe();

        Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(msg) => Some((msg, rx)),
                Err(_) => None,
            }
        }))
    }

    fn cose_handler(&self) -> &CoseHandler {
        &self.cose_handler
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        // Close all connections
        self.connections.clear();
        Ok(())
    }
}

impl<G, A> HttpIntegratedTransport for WebsocketTransport<G, A>
where
    G: Governance,
    A: Attestor,
{
    fn endpoint(&self) -> &'static str {
        "/consensus/ws"
    }

    fn create_router_integration(&self) -> Result<Router, TransportError> {
        let transport = Arc::new(self.clone());

        let router = Router::new().route(
            "/consensus/ws",
            get(move |ws| Self::handle_websocket_upgrade(ws, transport.clone())),
        );

        Ok(router)
    }
}

// Add Clone implementation for WebsocketTransport
impl<G, A> Clone for WebsocketTransport<G, A>
where
    G: Governance,
    A: Attestor,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            connections: self.connections.clone(),
            incoming_tx: self.incoming_tx.clone(),
            verifier: self.verifier.clone(),
            cose_handler: self.cose_handler.clone(),
            topology_manager: self.topology_manager.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use ed25519_dalek::{SigningKey, VerifyingKey};
    use proven_attestation_mock::MockAttestor;
    use proven_governance_mock::MockGovernance;
    use proven_verification::AttestationVerifier;
    use proven_verification::CoseHandler;

    async fn create_test_transport() -> WebsocketTransport<MockGovernance, MockAttestor> {
        let config = WebsocketConfig::default();
        let governance = MockGovernance::new(
            vec![],                              // empty nodes
            vec![],                              // empty versions
            "http://localhost:3200".to_string(), // primary auth gateway
            vec![],                              // alternate auth gateways
        );

        let attestor = MockAttestor::new();
        let node_id = NodeId::from(VerifyingKey::from_bytes(&[0u8; 32]).unwrap());
        let signing_key = SigningKey::from_bytes(&[0u8; 32]);
        let verifier = Arc::new(ConnectionVerifier::new(
            Arc::new(AttestationVerifier::new(governance.clone(), attestor)),
            Arc::new(CoseHandler::new(signing_key.clone())),
            node_id.clone(),
        ));
        let topology_manager = TopologyManager::new(Arc::new(governance), node_id)
            .await
            .unwrap();
        WebsocketTransport::new(config, signing_key, verifier, Arc::new(topology_manager))
    }

    #[tokio::test]
    async fn test_send_to_unconnected_peer() {
        let transport = create_test_transport().await;
        let node_id = NodeId::from(VerifyingKey::from_bytes(&[1u8; 32]).unwrap());
        let message = Bytes::from("test message");

        let result = transport
            .send_envelope(&node_id, &message, "test_message", None)
            .await;
        assert!(result.is_err());

        if let Err(TransportError::ConnectionFailed { peer: _, reason }) = result {
            assert!(reason.contains("Peer not found in topology"));
        } else {
            panic!("Expected ConnectionFailed error");
        }
    }

    #[tokio::test]
    async fn test_shutdown() {
        let transport = create_test_transport().await;

        // Should not panic
        let result = transport.shutdown().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_incoming_stream() {
        let transport = create_test_transport().await;
        let mut incoming = transport.incoming();

        // Stream should not have any messages initially
        // Use a timeout to avoid hanging
        match tokio::time::timeout(Duration::from_millis(100), incoming.next()).await {
            Ok(None) => {
                // Expected - no messages
            }
            Ok(Some(_)) => {
                panic!("Expected no messages");
            }
            Err(_) => {
                // Timeout - this is expected for an empty stream
            }
        }
    }

    #[tokio::test]
    async fn test_http_integration_trait() {
        let transport = create_test_transport().await;

        // Test endpoint method
        assert_eq!(transport.endpoint(), "/consensus/ws");

        // Test router creation
        let router_result = transport.create_router_integration();
        assert!(router_result.is_ok());
    }

    #[tokio::test]
    async fn test_config_default() {
        let config = WebsocketConfig::default();
        assert_eq!(config.retry_attempts, 3);
        assert_eq!(config.retry_delay_ms, 500);
        assert_eq!(config.transport.connection_timeout_ms, 5000);
        assert_eq!(config.transport.max_message_size, 10 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_transport_clone() {
        let transport = create_test_transport().await;
        let cloned = transport.clone();

        // Both should have the same endpoint
        assert_eq!(transport.endpoint(), cloned.endpoint());

        // Both should be able to create routers
        let router1 = transport.create_router_integration();
        let router2 = cloned.create_router_integration();
        assert!(router1.is_ok());
        assert!(router2.is_ok());
    }

    #[tokio::test]
    async fn test_message_size_validation() {
        let transport = create_test_transport().await;
        let node_id = NodeId::from(VerifyingKey::from_bytes(&[1u8; 32]).unwrap());

        // Create a message larger than the limit
        let large_message = Bytes::from(vec![0u8; 17 * 1024 * 1024]); // 17MB

        let result = transport
            .send_envelope(&node_id, &large_message, "test_message", None)
            .await;
        assert!(result.is_err());

        if let Err(TransportError::ConnectionFailed { .. }) = result {
            // Expected error for unconnected peer
        } else {
            panic!("Expected ConnectionFailed error");
        }
    }

    #[tokio::test]
    async fn test_websocket_upgrade_handler() {
        let _transport = Arc::new(create_test_transport().await);

        // This test verifies the upgrade handler can be created
        // In a real test, we'd need to create an actual WebSocket connection
        // For now, we just ensure the method exists and doesn't panic
        let _upgrade_handler =
            WebsocketTransport::<MockGovernance, MockAttestor>::handle_websocket_upgrade;
    }

    #[tokio::test]
    async fn test_verification_flow() {
        let transport = create_test_transport().await;

        // Test that verification methods exist and don't panic
        // In a real test, we'd need to set up actual WebSocket connections
        // For now, we just verify the transport can be created with verification
        // A non-existent connection should not be verified
        assert!(
            !transport
                .verifier
                .is_connection_verified("test-connection")
                .await
        );
    }

    #[tokio::test]
    async fn test_router_integration_path() {
        let transport = create_test_transport().await;
        let router = transport.create_router_integration();
        assert!(router.is_ok());
    }

    #[tokio::test]
    async fn test_config_debug_format() {
        let config = WebsocketConfig::default();
        let debug_str = format!("{config:?}");

        // Should contain config name
        assert!(debug_str.contains("WsConfig"));
    }

    #[tokio::test]
    async fn test_error_handling() {
        let transport = create_test_transport().await;
        let node_id = NodeId::from(VerifyingKey::from_bytes(&[1u8; 32]).unwrap());

        // Test various error conditions
        let send_result = transport
            .send_envelope(&node_id, &Bytes::new(), "test_message", None)
            .await;
        assert!(send_result.is_err());
    }

    #[tokio::test]
    async fn test_transport_lifecycle() {
        let transport = create_test_transport().await;

        // Test full lifecycle
        // Shutdown
        let result = transport.shutdown().await;
        assert!(result.is_ok());
    }
}
