//! TCP transport implementation for proven-network
//!
//! This crate provides a TCP-based transport implementation of the proven-transport trait.

use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use futures::Stream;
use proven_attestation::Attestor;
use proven_topology::TopologyAdaptor;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::connection::ConnectionManager;
use proven_transport::error::TransportError;
use proven_transport::{Config, Transport, TransportEnvelope};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
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

/// Outgoing message sender for a specific connection
type OutgoingSender = mpsc::Sender<TransportEnvelope>;

/// TCP transport implementation
#[derive(Debug)]
pub struct TcpTransport<G, A>
where
    G: TopologyAdaptor,
    A: Attestor,
{
    /// Configuration
    config: TcpConfig,
    /// Connection manager for handling all connection logic
    connection_manager: Arc<ConnectionManager<G, A>>,
    /// Outgoing message senders by connection ID
    outgoing_senders: Arc<RwLock<HashMap<String, OutgoingSender>>>,
    /// Incoming message broadcast channel
    incoming_tx: broadcast::Sender<TransportEnvelope>,
    /// Shutdown signal
    shutdown_tx: broadcast::Sender<()>,
    /// Listener handle
    listener_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// Topology manager for getting connection details
    topology_manager: Arc<TopologyManager<G>>,
}

impl<G, A> TcpTransport<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new TCP transport with verification and topology management
    pub fn new(
        config: TcpConfig,
        attestor: Arc<A>,
        governance: Arc<G>,
        signing_key: SigningKey,
        topology_manager: Arc<TopologyManager<G>>,
    ) -> Self {
        let (incoming_tx, _) = broadcast::channel(1024);
        let (shutdown_tx, _) = broadcast::channel(1);

        // Create connection manager with internal verification components
        let connection_manager =
            Arc::new(ConnectionManager::new(attestor, governance, signing_key));

        Self {
            config,
            connection_manager,
            outgoing_senders: Arc::new(RwLock::new(HashMap::new())),
            incoming_tx,
            shutdown_tx,
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

        let connection_manager = self.connection_manager.clone();
        let incoming_tx = self.incoming_tx.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, addr)) => {
                                debug!("Accepted connection from {}", addr);
                                let connection_manager = connection_manager.clone();
                                let incoming_tx = incoming_tx.clone();

                                tokio::spawn(async move {
                                    if let Err(e) = Self::handle_incoming_connection(
                                        stream,
                                        addr,
                                        connection_manager,
                                        incoming_tx,
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

    /// Handle an incoming connection - just provide I/O functions to the manager
    async fn handle_incoming_connection(
        stream: TcpStream,
        addr: SocketAddr,
        connection_manager: Arc<ConnectionManager<G, A>>,
        incoming_tx: broadcast::Sender<TransportEnvelope>,
    ) -> Result<(), TransportError> {
        let connection_id = format!("tcp-incoming-{addr}");
        debug!("Handling incoming connection with ID: {}", connection_id);

        // Store the stream in a shared location for the I/O functions
        let stream_arc = Arc::new(tokio::sync::Mutex::new(stream));

        // Create I/O functions for the connection manager
        let send_stream = stream_arc.clone();
        let send_fn = move |data: Bytes| {
            let stream = send_stream.clone();
            Box::pin(async move {
                let mut stream_guard = stream.lock().await;
                Self::write_raw(&mut stream_guard, &data).await
            })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send>,
                >
        };

        let recv_stream = stream_arc.clone();
        let recv_fn = move || {
            let stream = recv_stream.clone();
            Box::pin(async move {
                let mut stream_guard = stream.lock().await;
                Self::read_raw(&mut stream_guard).await
            })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<Bytes, TransportError>> + Send>,
                >
        };

        // Let the connection manager handle everything
        connection_manager
            .handle_incoming_connection(connection_id.clone(), send_fn, recv_fn, incoming_tx)
            .await?;

        debug!("Incoming connection {} completed", connection_id);
        Ok(())
    }

    /// Handle outgoing connection - just provide I/O functions to the manager
    async fn handle_outgoing_connection(
        stream: TcpStream,
        connection_id: String,
        expected_node_id: NodeId,
        connection_manager: Arc<ConnectionManager<G, A>>,
        outgoing_rx: mpsc::Receiver<TransportEnvelope>,
    ) -> Result<(), TransportError> {
        debug!(
            "Starting outgoing connection handling for: {}",
            connection_id
        );

        // Store the stream in a shared location for the I/O functions
        let stream_arc = Arc::new(tokio::sync::Mutex::new(stream));

        // Create I/O functions for the connection manager
        let send_stream = stream_arc.clone();
        let send_fn = move |data: Bytes| {
            let stream = send_stream.clone();
            Box::pin(async move {
                let mut stream_guard = stream.lock().await;
                Self::write_raw(&mut stream_guard, &data).await
            })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send>,
                >
        };

        let recv_stream = stream_arc.clone();
        let recv_fn = move || {
            let stream = recv_stream.clone();
            Box::pin(async move {
                let mut stream_guard = stream.lock().await;
                Self::read_raw(&mut stream_guard).await
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

        debug!("Outgoing connection {} completed", connection_id);
        Ok(())
    }

    /// Read raw data from stream
    async fn read_raw(stream: &mut TcpStream) -> Result<Bytes, TransportError> {
        // Read length prefix (4 bytes)
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(TransportError::Io)?;

        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 10 * 1024 * 1024 {
            // 10MB max
            return Err(TransportError::invalid_message("Message too large"));
        }

        // Read message data
        let mut data = vec![0u8; len];
        stream
            .read_exact(&mut data)
            .await
            .map_err(TransportError::Io)?;

        Ok(Bytes::from(data))
    }

    /// Write raw data to stream
    async fn write_raw(stream: &mut TcpStream, data: &Bytes) -> Result<(), TransportError> {
        // Write length prefix
        let len = data.len() as u32;
        stream
            .write_all(&len.to_be_bytes())
            .await
            .map_err(TransportError::Io)?;

        // Write message data
        stream.write_all(data).await.map_err(TransportError::Io)?;
        stream.flush().await.map_err(TransportError::Io)?;

        Ok(())
    }

    /// Connect to a peer at the given address
    async fn connect_to_address(
        &self,
        node_id: NodeId,
        addr: SocketAddr,
    ) -> Result<(), TransportError> {
        let connection_id = format!("tcp-outgoing-{addr}");

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
                "Attempting connection to {} (attempt {} of {})",
                addr, attempt, config.retry_attempts
            );

            match timeout(timeout_duration, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    debug!("TCP connection established to {}", addr);

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
                            warn!("Outgoing connection failed: {}", e);
                        }

                        // Clean up the sender when connection ends
                        outgoing_senders.write().await.remove(&connection_id_clone);
                    });

                    debug!("Connection established and handlers started for {}", addr);
                    return Ok(());
                }
                Ok(Err(e)) => {
                    last_error = Some(TransportError::Io(e));
                }
                Err(_) => {
                    last_error = Some(TransportError::timeout(format!("Connection to {addr}")));
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
impl<G, A> Transport for TcpTransport<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug + Clone,
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
            "Sending message to {} with type: {}",
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

        // Get TCP socket address for the peer
        let addr = node.tcp_socket_addr().await.map_err(|e| {
            TransportError::connection_failed(recipient, format!("Invalid TCP address: {e}"))
        })?;

        let connection_id = format!("tcp-outgoing-{addr}");

        // Ensure we have a connection
        if !self
            .outgoing_senders
            .read()
            .await
            .contains_key(&connection_id)
        {
            debug!(
                "No existing connection, establishing one to {} at {}",
                recipient, addr
            );
            self.connect_to_address(recipient.clone(), addr).await?;
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
            debug!("Message sent successfully to {}", recipient);
            Ok(())
        } else {
            Err(TransportError::connection_failed(
                recipient,
                "Connection not found after establishment",
            ))
        }
    }

    fn incoming(&self) -> Pin<Box<dyn Stream<Item = TransportEnvelope> + Send>> {
        debug!("TCP transport incoming() called, creating subscriber");
        let rx = self.incoming_tx.subscribe();
        Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(msg) => {
                    debug!("TCP transport received message from {}", msg.sender);
                    Some((msg, rx))
                }
                Err(_) => None,
            }
        }))
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        // Send shutdown signal
        let _ = self.shutdown_tx.send(());

        // Clear all outgoing senders
        self.outgoing_senders.write().await.clear();

        // Stop the listener
        if let Some(handle) = self.listener_handle.write().await.take() {
            handle.abort();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{SigningKey, VerifyingKey};
    use futures::StreamExt;
    use proven_attestation_mock::MockAttestor;
    use proven_topology_mock::MockTopologyAdaptor;
    use std::net::SocketAddr;

    async fn create_test_transport() -> TcpTransport<MockTopologyAdaptor, MockAttestor> {
        let config = TcpConfig::default();
        let governance = Arc::new(MockTopologyAdaptor::new(
            vec![],
            vec![],
            "http://localhost:3200".to_string(),
            vec![],
        ));
        let attestor = Arc::new(MockAttestor::new());
        let node_id = NodeId::from(VerifyingKey::from_bytes(&[0u8; 32]).unwrap());
        let topology_manager = TopologyManager::new(governance.clone(), node_id);
        let signing_key = SigningKey::from_bytes(&[0u8; 32]);

        TcpTransport::new(
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
        let config = TcpConfig::default();
        assert_eq!(config.retry_attempts, 3);
        assert_eq!(config.retry_delay_ms, 500);
        assert_eq!(
            config.transport.connection.connection_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(config.transport.max_message_size, 10 * 1024 * 1024);
        assert_eq!(
            config.local_addr,
            "0.0.0.0:0".parse::<SocketAddr>().unwrap()
        );
    }

    #[tokio::test]
    async fn test_simplified_constructor() {
        let config = TcpConfig::default();
        let governance = Arc::new(MockTopologyAdaptor::new(
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
            TcpTransport::new(config, attestor, governance, signing_key, topology_manager);

        // Verify the transport was created successfully
        let debug_str = format!("{transport:?}");
        assert!(debug_str.contains("TcpTransport"));
    }
}
