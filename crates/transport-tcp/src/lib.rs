//! TCP transport implementation for proven-network
//!
//! This crate provides a TCP-based transport implementation of the proven-transport trait.

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use ed25519_dalek::SigningKey;
use futures::Stream;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::{Config, Transport, TransportEnvelope, error::TransportError};
use proven_verification::{ConnectionVerifier, CoseHandler};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::time::{Duration, timeout};
use tracing::{debug, error, info, warn};

/// TCP-specific configuration
#[derive(Debug, Clone)]
pub struct TcpConfig {
    /// Generic transport configuration
    pub transport: Config,
    /// Local address to bind to
    pub local_addr: SocketAddr,
    /// Connection retry attempts
    pub retry_attempts: usize,
    /// Delay between retry attempts in milliseconds
    pub retry_delay_ms: u64,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            transport: Config::default(),
            local_addr: "0.0.0.0:0".parse().unwrap(),
            retry_attempts: 3,
            retry_delay_ms: 500,
        }
    }
}

/// Connection state
#[derive(Debug)]
// TODO: transport trait should probably have query methods for this
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

/// TCP transport implementation
#[derive(Debug)]
pub struct TcpTransport<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// Configuration
    config: TcpConfig,
    /// Active connections
    connections: Arc<DashMap<NodeId, ConnectionState>>,
    /// Incoming message broadcast channel
    incoming_tx: broadcast::Sender<TransportEnvelope>,
    /// Shutdown signal
    shutdown_tx: broadcast::Sender<()>,
    /// Connection verifier
    verifier: Arc<ConnectionVerifier<G, A>>,
    /// COSE handler for message signing/verification
    cose_handler: Arc<CoseHandler>,
    /// Listener handle
    listener_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// Topology manager for getting connection details
    topology_manager: Arc<TopologyManager<G>>,
}

impl<G, A> TcpTransport<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// Create a new TCP transport with verification and topology management
    pub fn new(
        config: TcpConfig,
        signing_key: SigningKey,
        verifier: Arc<ConnectionVerifier<G, A>>,
        topology_manager: Arc<TopologyManager<G>>,
    ) -> Self {
        let (incoming_tx, _) = broadcast::channel(1024);
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            config,
            connections: Arc::new(DashMap::new()),
            incoming_tx,
            shutdown_tx,
            verifier,
            cose_handler: Arc::new(CoseHandler::new(signing_key)),
            listener_handle: Arc::new(RwLock::new(None)),
            topology_manager,
        }
    }

    /// Start the TCP listener
    pub async fn start(&self) -> Result<SocketAddr, TransportError> {
        let listener = TcpListener::bind(self.config.local_addr)
            .await
            .map_err(TransportError::Io)?;

        let local_addr = listener.local_addr().map_err(TransportError::Io)?;
        info!("TCP transport listening on {}", local_addr);

        let connections = self.connections.clone();
        let incoming_tx = self.incoming_tx.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let config = self.config.clone();
        let verifier = self.verifier.clone();
        let cose_handler = self.cose_handler.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, addr)) => {
                                debug!("Accepted connection from {}", addr);
                                let connections = connections.clone();
                                let incoming_tx = incoming_tx.clone();
                                let config = config.clone();
                                let verifier = verifier.clone();
                                let cose_handler = cose_handler.clone();

                                tokio::spawn(async move {
                                    if let Err(e) = Self::handle_incoming_connection(
                                        stream,
                                        addr,
                                        connections,
                                        incoming_tx,
                                        config,
                                        verifier,
                                        cose_handler,
                                    ).await {
                                        warn!("Failed to handle incoming connection from {}: {}", addr, e);
                                    }
                                });
                            }
                            Err(e) => {
                                error!("Failed to accept connection: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("TCP listener shutting down");
                        break;
                    }
                }
            }
        });

        *self.listener_handle.write().await = Some(handle);
        Ok(local_addr)
    }

    /// Handle an incoming connection
    async fn handle_incoming_connection(
        stream: TcpStream,
        addr: SocketAddr,
        connections: Arc<DashMap<NodeId, ConnectionState>>,
        incoming_tx: broadcast::Sender<TransportEnvelope>,
        config: TcpConfig,
        verifier: Arc<ConnectionVerifier<G, A>>,
        cose_handler: Arc<CoseHandler>,
    ) -> Result<(), TransportError> {
        // Create connection ID
        let connection_id = format!("tcp-incoming-{addr}");

        // Initialize verification
        verifier.initialize_connection(connection_id.clone()).await;

        // Handle verification handshake
        let node_id = match Self::handle_verification(
            &stream,
            &connection_id,
            &verifier,
            false, // incoming connection
        )
        .await
        {
            Ok(id) => id,
            Err(e) => {
                verifier.remove_connection(&connection_id).await;
                return Err(e);
            }
        };

        debug!("Incoming connection verified from {} ({})", node_id, addr);

        // Create channel for outgoing messages
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Store connection
        connections.insert(
            node_id.clone(),
            ConnectionState {
                node_id: node_id.clone(),
                sender: tx,
                is_outgoing: false,
                last_activity: SystemTime::now(),
            },
        );

        // Split the stream
        let (reader, writer) = stream.into_split();
        let reader = Arc::new(tokio::sync::Mutex::new(reader));
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        // Spawn task to handle outgoing messages
        let writer_clone = writer.clone();
        let node_id_clone = node_id.clone();
        let connections_clone = connections.clone();
        tokio::spawn(async move {
            while let Some(data) = rx.recv().await {
                if let Err(e) = Self::write_message(&writer_clone, &data).await {
                    warn!("Failed to write message to {}: {}", node_id_clone, e);
                    break;
                }
            }
            connections_clone.remove(&node_id_clone);
        });

        // Handle incoming messages
        let node_id_clone = node_id.clone();
        let connections_clone = connections.clone();
        tokio::spawn(async move {
            loop {
                match Self::read_message(&reader, &config.transport).await {
                    Ok(payload) => {
                        // Update last activity
                        if let Some(mut conn) = connections_clone.get_mut(&node_id_clone) {
                            conn.last_activity = SystemTime::now();
                        }

                        // Deserialize and verify COSE message
                        match cose_handler.deserialize_cose_message(&payload) {
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
                    Err(e) => {
                        debug!("Connection closed with {}: {}", node_id_clone, e);
                        break;
                    }
                }
            }
            connections_clone.remove(&node_id_clone);
        });

        Ok(())
    }

    /// Handle connection verification
    async fn handle_verification(
        stream: &TcpStream,
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

            Self::write_raw(stream, &request).await?;
        }

        // Handle verification messages
        loop {
            let data = Self::read_raw(stream).await?;

            match verifier
                .process_verification_message(connection_id.to_string(), data)
                .await
            {
                Ok(Some(response)) => {
                    // Send response
                    Self::write_raw(stream, &response).await?;
                }
                Ok(None) => {
                    // Check if verified
                    if verifier.is_connection_verified(connection_id).await
                        && let Some(node_id) = verifier.get_verified_public_key(connection_id).await
                    {
                        return Ok(node_id);
                    }
                }
                Err(e) => {
                    return Err(TransportError::AuthenticationFailed(e.to_string()));
                }
            }
        }
    }

    /// Read a message from the stream
    async fn read_message(
        reader: &Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedReadHalf>>,
        config: &Config,
    ) -> Result<Bytes, TransportError> {
        let mut reader = reader.lock().await;

        // Read length prefix (4 bytes)
        let mut len_buf = [0u8; 4];
        reader
            .read_exact(&mut len_buf)
            .await
            .map_err(TransportError::Io)?;

        let len = u32::from_be_bytes(len_buf) as usize;
        if len > config.max_message_size {
            return Err(TransportError::InvalidMessage(
                "Message too large".to_string(),
            ));
        }

        // Read message data
        let mut data = vec![0u8; len];
        reader
            .read_exact(&mut data)
            .await
            .map_err(TransportError::Io)?;

        Ok(Bytes::from(data))
    }

    /// Write a message to the stream
    async fn write_message(
        writer: &Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
        data: &Bytes,
    ) -> Result<(), TransportError> {
        let mut writer = writer.lock().await;

        // Write length prefix
        let len = data.len() as u32;
        writer
            .write_all(&len.to_be_bytes())
            .await
            .map_err(TransportError::Io)?;

        // Write message data
        writer.write_all(data).await.map_err(TransportError::Io)?;

        writer.flush().await.map_err(TransportError::Io)?;

        Ok(())
    }

    /// Read raw data from stream (for verification)
    async fn read_raw(stream: &TcpStream) -> Result<Bytes, TransportError> {
        let mut len_buf = [0u8; 4];
        stream.readable().await.map_err(TransportError::Io)?;

        let mut buf = vec![0u8; 4];
        let n = stream.try_read(&mut buf).map_err(TransportError::Io)?;
        if n < 4 {
            return Err(TransportError::ConnectionFailed {
                peer: "unknown".to_string(),
                reason: "Failed to read length prefix".to_string(),
            });
        }
        len_buf.copy_from_slice(&buf[..4]);

        let len = u32::from_be_bytes(len_buf) as usize;

        let mut data = vec![0u8; len];
        let mut total_read = 0;
        while total_read < len {
            stream.readable().await.map_err(TransportError::Io)?;
            let n = stream
                .try_read(&mut data[total_read..])
                .map_err(TransportError::Io)?;
            total_read += n;
        }

        Ok(Bytes::from(data))
    }

    /// Write raw data to stream (for verification)
    async fn write_raw(stream: &TcpStream, data: &Bytes) -> Result<(), TransportError> {
        let len = data.len() as u32;

        stream.writable().await.map_err(TransportError::Io)?;
        stream
            .try_write(&len.to_be_bytes())
            .map_err(TransportError::Io)?;

        stream.writable().await.map_err(TransportError::Io)?;
        stream.try_write(data).map_err(TransportError::Io)?;

        Ok(())
    }

    /// Connect to a peer at the given address
    async fn connect_to_address(
        &self,
        node_id: NodeId,
        addr: SocketAddr,
    ) -> Result<(), TransportError> {
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

            match timeout(timeout_duration, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    // Create connection ID
                    let connection_id = format!("tcp-outgoing-{addr}");

                    // Initialize verification
                    verifier.initialize_connection(connection_id.clone()).await;

                    // Handle verification
                    match Self::handle_verification(
                        &stream,
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

                            debug!("Outgoing connection verified to {} ({})", node_id, addr);

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
                            let (reader, writer) = stream.into_split();
                            let reader = Arc::new(tokio::sync::Mutex::new(reader));
                            let writer = Arc::new(tokio::sync::Mutex::new(writer));

                            // Spawn task to handle outgoing messages
                            let writer_clone = writer.clone();
                            let node_id_clone = node_id.clone();
                            let connections = self.connections.clone();
                            tokio::spawn(async move {
                                while let Some(data) = rx.recv().await {
                                    if let Err(e) = Self::write_message(&writer_clone, &data).await
                                    {
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
                            let config = config.clone();
                            let cose_handler = self.cose_handler.clone();
                            tokio::spawn(async move {
                                loop {
                                    match Self::read_message(&reader, &config.transport).await {
                                        Ok(payload) => {
                                            // Update last activity
                                            if let Some(mut conn) =
                                                connections.get_mut(&node_id_clone)
                                            {
                                                conn.last_activity = SystemTime::now();
                                            }

                                            // Deserialize and verify COSE message
                                            match cose_handler.deserialize_cose_message(&payload) {
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
                                        Err(e) => {
                                            debug!(
                                                "Connection closed with {}: {}",
                                                node_id_clone, e
                                            );
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
                    last_error = Some(TransportError::Io(e));
                }
                Err(_) => {
                    last_error = Some(TransportError::Timeout {
                        operation: format!("Connection to {addr}"),
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
}

#[async_trait]
impl<G, A> Transport for TcpTransport<G, A>
where
    G: Governance,
    A: Attestor,
{
    async fn send_envelope(
        &self,
        recipient: &NodeId,
        payload: &Bytes,
        message_type: &str,
        correlation_id: Option<uuid::Uuid>,
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

        // Get TCP socket address for the peer
        let addr = node
            .tcp_socket_addr()
            .await
            .map_err(|e| TransportError::ConnectionFailed {
                peer: recipient.to_string(),
                reason: format!("Invalid TCP address: {e}"),
            })?;

        // Connect to the peer
        self.connect_to_address(recipient.clone(), addr).await?;

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

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    use ed25519_dalek::{SigningKey, VerifyingKey};
    use futures::StreamExt;
    use proven_attestation_mock::MockAttestor;
    use proven_governance_mock::MockGovernance;
    use proven_verification::{AttestationVerifier, ConnectionVerifier, CoseHandler};

    async fn create_test_transport() -> TcpTransport<MockGovernance, MockAttestor> {
        let config = TcpConfig::default();
        let governance = MockGovernance::new(
            vec![],                              // empty nodes
            vec![],                              // empty versions
            "http://localhost:3200".to_string(), // primary auth gateway
            vec![],                              // alternate auth gateways
        );
        let attestor = MockAttestor::new();
        let node_id = NodeId::from(VerifyingKey::from_bytes(&[0u8; 32]).unwrap());
        let verifier = Arc::new(ConnectionVerifier::new(
            Arc::new(AttestationVerifier::new(governance.clone(), attestor)),
            Arc::new(CoseHandler::new(SigningKey::from_bytes(&[0u8; 32]))),
            node_id.clone(),
        ));
        let topology_manager = TopologyManager::new(Arc::new(governance), node_id)
            .await
            .unwrap();
        let signing_key = SigningKey::from_bytes(&[0u8; 32]);
        TcpTransport::new(config, signing_key, verifier, Arc::new(topology_manager))
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
    async fn test_config_default() {
        let config = TcpConfig::default();
        assert_eq!(config.retry_attempts, 3);
        assert_eq!(config.retry_delay_ms, 500);
        assert_eq!(config.transport.connection_timeout_ms, 5000);
        assert_eq!(config.transport.max_message_size, 10 * 1024 * 1024);
        assert_eq!(
            config.local_addr,
            "0.0.0.0:0".parse::<SocketAddr>().unwrap()
        );
    }

    #[tokio::test]
    async fn test_transport_debug_format() {
        let transport = create_test_transport().await;
        let debug_str = format!("{transport:?}");
        // Should contain transport name
        assert!(debug_str.contains("TcpTransport"));
    }

    #[tokio::test]
    async fn test_config_debug_format() {
        let config = TcpConfig::default();
        let debug_str = format!("{config:?}");
        // Should contain config name
        assert!(debug_str.contains("TcpConfig"));
    }
}
