//! WebSocket transport implementation for proven-network
//!
//! This crate provides a WebSocket-based transport implementation of the proven-transport trait.

use async_trait::async_trait;
use axum::{
    Router,
    extract::{WebSocketUpgrade, ws::Message as AxumMessage},
    response::Response,
    routing::get,
};
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use futures::{SinkExt, Stream, StreamExt};
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::connection::ConnectionManager;
use proven_transport::error::TransportError;
use proven_transport::{Config, HttpIntegratedTransport, Transport, TransportEnvelope};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::time::{Duration, timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, warn};
use url::Url;

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

/// Outgoing message sender for a specific connection
type OutgoingSender = mpsc::Sender<TransportEnvelope>;

/// WebSocket transport implementation
#[derive(Debug)]
pub struct WebsocketTransport<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// Configuration
    config: WebsocketConfig,
    /// Connection manager for handling all connection logic
    connection_manager: Arc<ConnectionManager<G, A>>,
    /// Outgoing message senders by connection ID
    outgoing_senders: Arc<RwLock<HashMap<String, OutgoingSender>>>,
    /// Incoming message broadcast channel
    incoming_tx: broadcast::Sender<TransportEnvelope>,
    /// Topology manager for getting connection details
    topology_manager: Arc<TopologyManager<G>>,
}

impl<G, A> WebsocketTransport<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new WebSocket transport with verification and topology management
    pub fn new(
        config: WebsocketConfig,
        attestor: Arc<A>,
        governance: Arc<G>,
        signing_key: SigningKey,
        topology_manager: Arc<TopologyManager<G>>,
    ) -> Self {
        let (incoming_tx, _) = broadcast::channel(1024);

        // Create connection manager with internal verification components
        let connection_manager =
            Arc::new(ConnectionManager::new(attestor, governance, signing_key));

        Self {
            config,
            connection_manager,
            outgoing_senders: Arc::new(RwLock::new(HashMap::new())),
            incoming_tx,
            topology_manager,
        }
    }

    /// Handle WebSocket upgrade request from axum
    pub async fn handle_websocket_upgrade(ws: WebSocketUpgrade, transport: Arc<Self>) -> Response {
        ws.on_upgrade(move |socket| Self::handle_axum_websocket_connection(socket, transport))
    }

    /// Handle WebSocket connection from axum
    async fn handle_axum_websocket_connection(
        socket: axum::extract::ws::WebSocket,
        transport: Arc<Self>,
    ) {
        let connection_id = format!("ws-incoming-{}", uuid::Uuid::new_v4());
        debug!(
            "Handling incoming WebSocket connection with ID: {}",
            connection_id
        );

        let (sender, receiver) = socket.split();

        // Store the socket parts in shared locations for the I/O functions
        let sender_arc = Arc::new(tokio::sync::Mutex::new(sender));
        let receiver_arc = Arc::new(tokio::sync::Mutex::new(receiver));

        // Create I/O functions for the connection manager
        let send_sender = sender_arc.clone();
        let send_fn = move |data: Bytes| {
            let sender = send_sender.clone();
            Box::pin(async move {
                let mut sender_guard = sender.lock().await;
                sender_guard
                    .send(AxumMessage::Binary(data))
                    .await
                    .map_err(TransportError::transport)
            })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send>,
                >
        };

        let recv_receiver = receiver_arc.clone();
        let recv_fn = move || {
            let receiver = recv_receiver.clone();
            Box::pin(async move {
                let mut receiver_guard = receiver.lock().await;
                loop {
                    match receiver_guard.next().await {
                        Some(Ok(AxumMessage::Binary(data))) => {
                            return Ok(data);
                        }
                        Some(Ok(AxumMessage::Close(_))) => {
                            return Err(TransportError::transport("WebSocket closed"));
                        }
                        Some(Ok(_)) => {
                            // Ignore text, ping, pong messages
                            continue;
                        }
                        Some(Err(e)) => {
                            return Err(TransportError::transport(e));
                        }
                        None => {
                            return Err(TransportError::transport("WebSocket stream ended"));
                        }
                    }
                }
            })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<Bytes, TransportError>> + Send>,
                >
        };

        // Let the connection manager handle everything
        if let Err(e) = transport
            .connection_manager
            .handle_incoming_connection(
                connection_id.clone(),
                send_fn,
                recv_fn,
                transport.incoming_tx.clone(),
            )
            .await
        {
            warn!("Failed to handle incoming WebSocket connection: {}", e);
        }

        debug!("Incoming WebSocket connection {} completed", connection_id);
    }

    /// Handle outgoing WebSocket connection - just provide I/O functions to the manager
    async fn handle_outgoing_connection(
        stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
        connection_id: String,
        expected_node_id: NodeId,
        connection_manager: Arc<ConnectionManager<G, A>>,
        outgoing_rx: mpsc::Receiver<TransportEnvelope>,
    ) -> Result<(), TransportError> {
        debug!(
            "Starting outgoing WebSocket connection handling for: {}",
            connection_id
        );

        let (sender, receiver) = stream.split();

        // Store the stream parts in shared locations for the I/O functions
        let sender_arc = Arc::new(tokio::sync::Mutex::new(sender));
        let receiver_arc = Arc::new(tokio::sync::Mutex::new(receiver));

        // Create I/O functions for the connection manager
        let send_sender = sender_arc.clone();
        let send_fn = move |data: Bytes| {
            let sender = send_sender.clone();
            Box::pin(async move {
                let mut sender_guard = sender.lock().await;
                sender_guard
                    .send(tokio_tungstenite::tungstenite::Message::Binary(data))
                    .await
                    .map_err(TransportError::transport)
            })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send>,
                >
        };

        let recv_receiver = receiver_arc.clone();
        let recv_fn = move || {
            let receiver = recv_receiver.clone();
            Box::pin(async move {
                let mut receiver_guard = receiver.lock().await;
                loop {
                    match receiver_guard.next().await {
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(data))) => {
                            return Ok(data);
                        }
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) => {
                            return Err(TransportError::transport("WebSocket closed"));
                        }
                        Some(Ok(_)) => {
                            // Ignore text, ping, pong messages
                            continue;
                        }
                        Some(Err(e)) => {
                            return Err(TransportError::transport(e));
                        }
                        None => {
                            return Err(TransportError::transport("WebSocket stream ended"));
                        }
                    }
                }
            })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<Bytes, TransportError>> + Send>,
                >
        };

        // Let the connection manager handle everything including verification and ongoing messages
        connection_manager
            .handle_outgoing_connection(
                connection_id.clone(),
                expected_node_id,
                send_fn,
                recv_fn,
                outgoing_rx,
            )
            .await?;

        debug!("Outgoing WebSocket connection {} completed", connection_id);
        Ok(())
    }

    /// Connect to a peer at the given URL
    async fn connect_to_url(&self, node_id: NodeId, url: Url) -> Result<(), TransportError> {
        let connection_id = format!("ws-outgoing-{url}");

        // Check if we already have this connection
        if self
            .outgoing_senders
            .read()
            .await
            .contains_key(&connection_id)
        {
            return Ok(());
        }

        let config = self.config.clone();
        let mut last_error = None;

        // Try to connect with retries
        for attempt in 1..=config.retry_attempts {
            let timeout_duration = config.transport.connection.connection_timeout;

            debug!(
                "Attempting WebSocket connection to {} (attempt {} of {})",
                url, attempt, config.retry_attempts
            );

            match timeout(timeout_duration, connect_async(url.to_string())).await {
                Ok(Ok((stream, _))) => {
                    debug!("WebSocket connection established to {}", url);

                    // Create channels for outgoing messages
                    let (outgoing_tx, outgoing_rx) = mpsc::channel(1000);

                    // Store the outgoing sender
                    self.outgoing_senders
                        .write()
                        .await
                        .insert(connection_id.clone(), outgoing_tx);

                    // Handle the entire outgoing connection lifecycle
                    let connection_manager = self.connection_manager.clone();
                    let outgoing_senders = self.outgoing_senders.clone();
                    let connection_id_clone = connection_id.clone();

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_outgoing_connection(
                            stream,
                            connection_id_clone.clone(),
                            node_id,
                            connection_manager,
                            outgoing_rx,
                        )
                        .await
                        {
                            warn!("Outgoing WebSocket connection failed: {}", e);
                        }

                        // Clean up the sender when connection ends
                        outgoing_senders.write().await.remove(&connection_id_clone);
                    });

                    debug!(
                        "WebSocket connection established and handlers started for {}",
                        url
                    );
                    return Ok(());
                }
                Ok(Err(e)) => {
                    last_error = Some(TransportError::transport(e));
                }
                Err(_) => {
                    last_error = Some(TransportError::timeout(format!("Connection to {url}")));
                }
            }

            // Wait before retry (except on last attempt)
            if attempt < config.retry_attempts {
                tokio::time::sleep(Duration::from_millis(config.retry_delay_ms)).await;
            }
        }

        Err(last_error.unwrap_or_else(|| {
            TransportError::connection_failed(node_id, "Failed after all retry attempts")
        }))
    }
}

#[async_trait]
impl<G, A> Transport for WebsocketTransport<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    async fn send_envelope(
        &self,
        recipient: &NodeId,
        payload: &Bytes,
        message_type: &str,
        correlation_id: Option<uuid::Uuid>,
    ) -> Result<(), TransportError> {
        debug!(
            "Sending WebSocket message to {} with type: {}",
            recipient, message_type
        );

        // Get connection details from topology manager
        let node = self
            .topology_manager
            .get_peer_by_node_id(recipient)
            .await
            .ok_or_else(|| {
                TransportError::connection_failed(recipient, "Peer not found in topology")
            })?;

        // Get WebSocket URL for the peer
        let url_string = node.websocket_url().map_err(|e| {
            TransportError::connection_failed(recipient, format!("Invalid WebSocket URL: {e}"))
        })?;
        let url = Url::parse(&url_string).map_err(|e| {
            TransportError::connection_failed(
                recipient,
                format!("Failed to parse WebSocket URL: {e}"),
            )
        })?;

        let connection_id = format!("ws-outgoing-{url}");

        // Ensure we have a connection
        if !self
            .outgoing_senders
            .read()
            .await
            .contains_key(&connection_id)
        {
            debug!(
                "No existing connection, establishing one to {} at {}",
                recipient, url
            );
            self.connect_to_url(recipient.clone(), url).await?;
        }

        // Create the envelope
        let envelope = TransportEnvelope {
            correlation_id,
            message_type: message_type.to_string(),
            payload: payload.clone(),
            sender: self.connection_manager.our_node_id().clone(),
        };

        // Send through the connection's outgoing channel
        if let Some(sender) = self.outgoing_senders.read().await.get(&connection_id) {
            sender.send(envelope).await.map_err(|_| {
                TransportError::connection_failed(recipient, "Connection channel closed")
            })?;
            debug!("WebSocket message sent successfully to {}", recipient);
            Ok(())
        } else {
            Err(TransportError::connection_failed(
                recipient,
                "Connection not found after establishment",
            ))
        }
    }

    fn incoming(&self) -> Pin<Box<dyn Stream<Item = TransportEnvelope> + Send>> {
        debug!("WebSocket transport incoming() called, creating subscriber");
        let rx = self.incoming_tx.subscribe();
        Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(msg) => {
                    debug!("WebSocket transport received message from {}", msg.sender);
                    Some((msg, rx))
                }
                Err(_) => None,
            }
        }))
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        // Clear all outgoing senders
        self.outgoing_senders.write().await.clear();
        Ok(())
    }
}

impl<G, A> HttpIntegratedTransport for WebsocketTransport<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    fn create_router_integration(&self) -> Result<Router, TransportError> {
        // We need to clone the transport to move it into the closure
        let config = self.config.clone();
        let connection_manager = self.connection_manager.clone();
        let outgoing_senders = self.outgoing_senders.clone();
        let incoming_tx = self.incoming_tx.clone();
        let topology_manager = self.topology_manager.clone();

        let router = Router::new().route(
            "/ws",
            get(move |ws: WebSocketUpgrade| {
                let transport = Arc::new(WebsocketTransport {
                    config: config.clone(),
                    connection_manager: connection_manager.clone(),
                    outgoing_senders: outgoing_senders.clone(),
                    incoming_tx: incoming_tx.clone(),
                    topology_manager: topology_manager.clone(),
                });
                Self::handle_websocket_upgrade(ws, transport)
            }),
        );
        Ok(router)
    }

    fn endpoint(&self) -> &'static str {
        "/ws"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{SigningKey, VerifyingKey};
    use futures::StreamExt;
    use proven_attestation_mock::MockAttestor;
    use proven_governance_mock::MockGovernance;

    async fn create_test_transport() -> WebsocketTransport<MockGovernance, MockAttestor> {
        let config = WebsocketConfig::default();
        let governance = Arc::new(MockGovernance::new(
            vec![],
            vec![],
            "http://localhost:3200".to_string(),
            vec![],
        ));
        let attestor = Arc::new(MockAttestor::new());
        let node_id = NodeId::from(VerifyingKey::from_bytes(&[0u8; 32]).unwrap());
        let topology_manager = TopologyManager::new(governance.clone(), node_id);
        let signing_key = SigningKey::from_bytes(&[0u8; 32]);

        WebsocketTransport::new(
            config,
            attestor,
            governance,
            signing_key,
            Arc::new(topology_manager),
        )
    }

    #[tokio::test]
    async fn test_shutdown() {
        let transport = create_test_transport().await;
        let result = transport.shutdown().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_incoming_stream() {
        let transport = create_test_transport().await;
        let mut incoming = transport.incoming();
        // Use a timeout to avoid hanging
        match tokio::time::timeout(Duration::from_millis(100), incoming.next()).await {
            Ok(None) | Err(_) => {
                // Expected - no messages or timeout
            }
            Ok(Some(_)) => {
                panic!("Expected no messages");
            }
        }
    }

    #[tokio::test]
    async fn test_config_default() {
        let config = WebsocketConfig::default();
        assert_eq!(config.retry_attempts, 3);
        assert_eq!(config.retry_delay_ms, 500);
        assert_eq!(
            config.transport.connection.connection_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(config.transport.max_message_size, 10 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_simplified_constructor() {
        let config = WebsocketConfig::default();
        let governance = Arc::new(MockGovernance::new(
            vec![],
            vec![],
            "http://localhost:3200".to_string(),
            vec![],
        ));
        let attestor = Arc::new(MockAttestor::new());
        let node_id = NodeId::from(VerifyingKey::from_bytes(&[1u8; 32]).unwrap());
        let topology_manager = Arc::new(TopologyManager::new(governance.clone(), node_id));
        let signing_key = SigningKey::from_bytes(&[1u8; 32]);

        // This should work without manually creating verification components
        let transport =
            WebsocketTransport::new(config, attestor, governance, signing_key, topology_manager);

        // Verify the transport was created successfully
        let debug_str = format!("{transport:?}");
        assert!(debug_str.contains("WebsocketTransport"));
    }

    #[tokio::test]
    async fn test_http_integration() {
        let transport = create_test_transport().await;

        // Test router creation
        let router = transport.create_router_integration();
        assert!(router.is_ok());

        // Test endpoint
        assert_eq!(transport.endpoint(), "/ws");
    }
}
