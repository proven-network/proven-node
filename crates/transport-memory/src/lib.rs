//! In-memory transport implementation for testing
//!
//! This transport routes messages between nodes within the same process,
//! perfect for testing and development scenarios.

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use proven_topology::{Node, NodeId};
use proven_transport::{Connection, Listener, Transport, TransportError};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};
use uuid::Uuid;

/// Global registry of memory transports for cross-connection routing
static GLOBAL_REGISTRY: once_cell::sync::Lazy<Arc<DashMap<NodeId, MemoryListener>>> =
    once_cell::sync::Lazy::new(|| Arc::new(DashMap::new()));

/// Configuration for memory transport
#[derive(Debug, Clone)]
pub struct MemoryOptions {
    /// Node ID to listen on (if acting as a listener)
    pub listen_node_id: Option<NodeId>,
}

/// Memory transport implementation
#[derive(Debug, Clone)]
pub struct MemoryTransport {
    options: MemoryOptions,
}

impl MemoryTransport {
    /// Create a new memory transport with options
    pub fn new(options: MemoryOptions) -> Self {
        Self { options }
    }

    /// Create a new memory transport with default options
    pub fn new_default() -> Self {
        Self::new(MemoryOptions {
            listen_node_id: None,
        })
    }

    /// Clear all global state (useful for tests)
    pub fn clear_global_state() {
        GLOBAL_REGISTRY.clear();
    }
}

impl Default for MemoryTransport {
    fn default() -> Self {
        Self::new_default()
    }
}

#[async_trait]
impl Transport for MemoryTransport {
    async fn connect(&self, node: &Node) -> Result<Box<dyn Connection>, TransportError> {
        // Use the node ID for routing
        let node_id = node.node_id();

        debug!("Connecting to memory node {}", node_id);

        // Find the listener at this node
        let listener = GLOBAL_REGISTRY.get(node_id).ok_or_else(|| {
            TransportError::ConnectionFailed(format!("No listener for node {node_id}"))
        })?;

        // Create a bidirectional connection pair
        let (client_to_server_tx, client_to_server_rx) = flume::bounded(100);
        let (server_to_client_tx, server_to_client_rx) = flume::bounded(100);

        // Create connection IDs
        let conn_id = Uuid::new_v4();

        // Create the client-side connection
        let client_conn = MemoryConnection {
            id: conn_id,
            sender: client_to_server_tx,
            receiver: Arc::new(RwLock::new(server_to_client_rx)),
            closed: Arc::new(RwLock::new(false)),
        };

        // Create the server-side connection
        let server_conn = MemoryConnection {
            id: conn_id,
            sender: server_to_client_tx,
            receiver: Arc::new(RwLock::new(client_to_server_rx)),
            closed: Arc::new(RwLock::new(false)),
        };

        // Send the server connection to the listener
        listener
            .incoming_tx
            .send_async(Box::new(server_conn))
            .await
            .map_err(|_| TransportError::ConnectionFailed("Listener closed".to_string()))?;

        info!("Memory connection established to {}", node_id);

        Ok(Box::new(client_conn))
    }

    async fn listen(&self) -> Result<Box<dyn Listener>, TransportError> {
        let node_id = self.options.listen_node_id.clone().ok_or_else(|| {
            TransportError::InvalidAddress("No listen node ID configured".to_string())
        })?;

        debug!("Creating memory listener for node {}", node_id);

        // Check if node is already listening
        if GLOBAL_REGISTRY.contains_key(&node_id) {
            return Err(TransportError::Other(format!(
                "Node {node_id} already has a listener"
            )));
        }

        // Create the listener
        let (incoming_tx, incoming_rx) = flume::unbounded();
        let listener = MemoryListener {
            node_id: node_id.clone(),
            incoming_rx: Arc::new(RwLock::new(incoming_rx)),
            incoming_tx,
            closed: Arc::new(RwLock::new(false)),
        };

        // Register in global registry
        GLOBAL_REGISTRY.insert(node_id.clone(), listener.clone());

        info!("Memory listener created for node {}", node_id);

        Ok(Box::new(listener))
    }
}

/// Memory connection implementation
#[derive(Clone)]
struct MemoryConnection {
    id: Uuid,
    sender: flume::Sender<Bytes>,
    receiver: Arc<RwLock<flume::Receiver<Bytes>>>,
    closed: Arc<RwLock<bool>>,
}

impl Debug for MemoryConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryConnection")
            .field("id", &self.id)
            .field("closed", &self.closed)
            .finish()
    }
}

#[async_trait]
impl Connection for MemoryConnection {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        if *self.closed.read().await {
            return Err(TransportError::ConnectionClosed);
        }

        debug!("Memory connection {} sending {} bytes", self.id, data.len());

        self.sender
            .send_async(data)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;

        Ok(())
    }

    async fn recv(&self) -> Result<Bytes, TransportError> {
        if *self.closed.read().await {
            return Err(TransportError::ConnectionClosed);
        }

        let receiver = self.receiver.read().await;
        match receiver.recv_async().await {
            Ok(data) => {
                debug!(
                    "Memory connection {} received {} bytes",
                    self.id,
                    data.len()
                );
                Ok(data)
            }
            Err(_) => {
                *self.closed.write().await = true;
                Err(TransportError::ConnectionClosed)
            }
        }
    }

    async fn close(self: Box<Self>) -> Result<(), TransportError> {
        debug!("Closing memory connection {}", self.id);
        *self.closed.write().await = true;
        Ok(())
    }
}

/// Memory listener implementation
#[derive(Clone)]
struct MemoryListener {
    node_id: NodeId,
    incoming_rx: Arc<RwLock<flume::Receiver<Box<dyn Connection>>>>,
    incoming_tx: flume::Sender<Box<dyn Connection>>,
    closed: Arc<RwLock<bool>>,
}

impl Debug for MemoryListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryListener")
            .field("node_id", &self.node_id)
            .field("closed", &self.closed)
            .finish()
    }
}

#[async_trait]
impl Listener for MemoryListener {
    async fn accept(&self) -> Result<Box<dyn Connection>, TransportError> {
        if *self.closed.read().await {
            return Err(TransportError::ConnectionClosed);
        }

        let receiver = self.incoming_rx.read().await;
        match receiver.recv_async().await {
            Ok(conn) => {
                info!(
                    "Memory listener accepted connection for node {}",
                    self.node_id
                );
                Ok(conn)
            }
            Err(_) => Err(TransportError::ConnectionClosed),
        }
    }

    async fn close(self: Box<Self>) -> Result<(), TransportError> {
        debug!("Closing memory listener for node {}", self.node_id);
        *self.closed.write().await = true;

        // Remove from global registry
        GLOBAL_REGISTRY.remove(&self.node_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;

    use proven_topology::{Node, NodeId};

    fn create_test_node(node_id: NodeId) -> Node {
        Node::new(
            "test-az".to_string(),
            "http://127.0.0.1:8080".to_string(),
            node_id,
            "test-region".to_string(),
            HashSet::new(),
        )
    }

    #[tokio::test]
    async fn test_memory_transport_creation() {
        let _ = tracing_subscriber::fmt::try_init();

        // Transport is stateless, creating it doesn't add to registry
        let initial_len = GLOBAL_REGISTRY.len();
        let _transport = MemoryTransport::new_default();
        assert_eq!(GLOBAL_REGISTRY.len(), initial_len);
    }

    #[tokio::test]
    async fn test_listen_and_connect() {
        let _ = tracing_subscriber::fmt::try_init();

        let node_id = NodeId::from_seed(1);
        let listener_transport = MemoryTransport::new(MemoryOptions {
            listen_node_id: Some(node_id.clone()),
        });

        // Create listener
        let listener = listener_transport.listen().await.unwrap();

        // Create a node for connection
        let node = create_test_node(node_id);

        // Connect to listener
        let connect_transport = MemoryTransport::new_default();
        let client_conn = connect_transport.connect(&node).await.unwrap();

        // Accept connection
        let server_conn = listener.accept().await.unwrap();

        // Test bidirectional communication
        let test_data = Bytes::from("Hello, Memory!");
        client_conn.send(test_data.clone()).await.unwrap();

        let received = server_conn.recv().await.unwrap();
        assert_eq!(test_data, received);

        // Send response
        let response = Bytes::from("Hello back!");
        server_conn.send(response.clone()).await.unwrap();

        let received_response = client_conn.recv().await.unwrap();
        assert_eq!(response, received_response);

        // Cleanup
        let _ = listener.close().await;
    }

    #[tokio::test]
    async fn test_node_already_listening() {
        let _ = tracing_subscriber::fmt::try_init();

        let node_id = NodeId::from_seed(2);
        let transport1 = MemoryTransport::new(MemoryOptions {
            listen_node_id: Some(node_id.clone()),
        });
        let transport2 = MemoryTransport::new(MemoryOptions {
            listen_node_id: Some(node_id.clone()),
        });

        // First listener should succeed
        let _listener1 = transport1.listen().await.unwrap();

        // Second listener on same node should fail
        let result = transport2.listen().await;
        assert!(result.is_err());
    }
}
