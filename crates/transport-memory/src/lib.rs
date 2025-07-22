//! In-memory transport implementation for testing
//!
//! This crate provides an in-memory transport that routes messages between
//! nodes within the same process. It's primarily intended for testing and
//! development scenarios where you want to run multiple nodes without
//! actual network communication.

#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::Stream;
use proven_logger::debug;
use proven_topology::NodeId;
use proven_transport::{Error, Transport, TransportEnvelope};
use tokio::sync::{Mutex, RwLock, mpsc};
use uuid::Uuid;

/// Type alias for the transport registry
type TransportRegistry = Arc<RwLock<HashMap<String, mpsc::UnboundedSender<TransportEnvelope>>>>;

/// Global registry for all memory transports
static GLOBAL_REGISTRY: OnceLock<TransportRegistry> = OnceLock::new();

/// Memory transport implementation
#[derive(Clone, Debug)]
pub struct MemoryTransport {
    /// Incoming messages stream
    incoming_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<TransportEnvelope>>>>,
    /// Sender for incoming messages
    incoming_tx: mpsc::UnboundedSender<TransportEnvelope>,
    /// Node ID for this transport
    node_id: NodeId,
}

impl MemoryTransport {
    /// Create a new memory transport
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        // Initialize global registry if needed
        GLOBAL_REGISTRY.get_or_init(|| Arc::new(RwLock::new(HashMap::new())));

        Self {
            incoming_rx: Arc::new(Mutex::new(Some(rx))),
            incoming_tx: tx,
            node_id,
        }
    }

    /// Register this transport to receive messages at the given address
    ///
    /// # Errors
    ///
    /// Returns an error if the address is already in use
    pub async fn register(&self, address: &str) -> Result<(), Error> {
        let registry = Self::registry();
        let mut registry_guard = registry.write().await;
        if registry_guard.contains_key(address) {
            return Err(Error::Other(format!("Address {address} already in use")));
        }
        registry_guard.insert(address.to_string(), self.incoming_tx.clone());
        drop(registry_guard);
        Ok(())
    }

    /// Get the global registry
    fn registry() -> &'static TransportRegistry {
        GLOBAL_REGISTRY.get().expect("Registry not initialized")
    }
}

#[async_trait]
impl Transport for MemoryTransport {
    async fn send_envelope(
        &self,
        recipient: &NodeId,
        payload: &Bytes,
        message_type: &str,
        correlation_id: Option<Uuid>,
    ) -> Result<(), Error> {
        debug!("Sending to {:?}: {} bytes", recipient, payload.len());

        // Look up the recipient's address (for simplicity, we use the node ID as address)
        let address = format!("memory://{recipient}");
        let registry = Self::registry();
        let registry_guard = registry.read().await;
        let sender = registry_guard
            .get(&address)
            .ok_or_else(|| Error::Other(format!("No transport registered at {address}")))?
            .clone();
        drop(registry_guard);

        // Create the envelope
        let envelope = TransportEnvelope {
            correlation_id,
            message_type: message_type.to_string(),
            payload: payload.clone(),
            sender: self.node_id.clone(),
        };

        // Send the envelope
        sender
            .send(envelope)
            .map_err(|_| Error::Other("Recipient channel closed".to_string()))?;

        Ok(())
    }

    fn incoming(&self) -> Pin<Box<dyn Stream<Item = TransportEnvelope> + Send>> {
        // Clone the receiver
        let rx = self.incoming_rx.clone();

        Box::pin(futures::stream::unfold(rx, |rx| async move {
            let mut rx_guard = rx.lock().await;
            if let Some(ref mut receiver) = *rx_guard {
                receiver.recv().await.map(|item| (item, rx.clone()))
            } else {
                None
            }
        }))
    }

    async fn shutdown(&self) -> Result<(), Error> {
        // Remove from registry
        let registry = Self::registry();
        registry
            .write()
            .await
            .retain(|_, sender| !sender.is_closed());

        // Close our receiver
        *self.incoming_rx.lock().await = None;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    // Clear the global registry before each test
    async fn clear_registry() {
        if let Some(registry) = GLOBAL_REGISTRY.get() {
            registry.write().await.clear();
        }
    }

    #[tokio::test]
    async fn test_send_and_receive() {
        clear_registry().await;

        let node1 = NodeId::from_seed(11);
        let node2 = NodeId::from_seed(12);

        let transport1 = MemoryTransport::new(node1.clone());
        let transport2 = MemoryTransport::new(node2.clone());

        // Register transports
        transport1
            .register(&format!("memory://{node1}"))
            .await
            .expect("Failed to register");
        transport2
            .register(&format!("memory://{node2}"))
            .await
            .expect("Failed to register");

        // Get incoming stream for transport2
        let mut incoming = transport2.incoming();

        // Send message from transport1 to transport2
        let correlation_id = Uuid::new_v4();
        transport1
            .send_envelope(
                &node2,
                &Bytes::from("Hello, transport2!"),
                "test_message",
                Some(correlation_id),
            )
            .await
            .expect("Failed to send");

        // Receive on transport2
        let envelope = incoming.next().await.expect("No message received");
        assert_eq!(envelope.payload, Bytes::from("Hello, transport2!"));
        assert_eq!(envelope.message_type, "test_message");
        assert_eq!(envelope.correlation_id, Some(correlation_id));
        assert_eq!(envelope.sender, node1);
    }

    #[tokio::test]
    async fn test_multiple_recipients() {
        clear_registry().await;

        let node1 = NodeId::from_seed(21);
        let node2 = NodeId::from_seed(22);
        let node3 = NodeId::from_seed(23);

        let transport1 = MemoryTransport::new(node1.clone());
        let transport2 = MemoryTransport::new(node2.clone());
        let transport3 = MemoryTransport::new(node3.clone());

        // Register all transports
        transport1
            .register(&format!("memory://{node1}"))
            .await
            .expect("Failed to register");
        transport2
            .register(&format!("memory://{node2}"))
            .await
            .expect("Failed to register");
        transport3
            .register(&format!("memory://{node3}"))
            .await
            .expect("Failed to register");

        // Get incoming streams
        let mut incoming2 = transport2.incoming();
        let mut incoming3 = transport3.incoming();

        // Send to both node2 and node3
        transport1
            .send_envelope(&node2, &Bytes::from("Hello, node2!"), "greeting", None)
            .await
            .expect("Failed to send to node2");

        transport1
            .send_envelope(&node3, &Bytes::from("Hello, node3!"), "greeting", None)
            .await
            .expect("Failed to send to node3");

        // Verify both received their messages
        let envelope2 = incoming2.next().await.expect("No message for node2");
        assert_eq!(envelope2.payload, Bytes::from("Hello, node2!"));
        assert_eq!(envelope2.sender, node1);

        let envelope3 = incoming3.next().await.expect("No message for node3");
        assert_eq!(envelope3.payload, Bytes::from("Hello, node3!"));
        assert_eq!(envelope3.sender, node1);
    }

    #[tokio::test]
    async fn test_shutdown() {
        clear_registry().await;

        let node1 = NodeId::from_seed(31);
        let transport = MemoryTransport::new(node1.clone());

        // Register transport
        transport
            .register(&format!("memory://{node1}"))
            .await
            .expect("Failed to register");

        // Verify it's registered
        let registry = MemoryTransport::registry();
        assert!(
            registry
                .read()
                .await
                .contains_key(&format!("memory://{node1}"))
        );

        // Shutdown
        transport.shutdown().await.expect("Failed to shutdown");

        // Registry should be cleaned up
        let registry_guard = registry.read().await;
        assert!(
            registry_guard.is_empty()
                || registry_guard
                    .values()
                    .all(tokio::sync::mpsc::UnboundedSender::is_closed)
        );
        drop(registry_guard);
    }

    #[tokio::test]
    async fn test_send_to_unregistered_node() {
        clear_registry().await;

        let node1 = NodeId::from_seed(41);
        let node2 = NodeId::from_seed(42);

        let transport1 = MemoryTransport::new(node1.clone());

        // Try to send without registering node2
        let result = transport1
            .send_envelope(&node2, &Bytes::from("Hello"), "test", None)
            .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No transport registered")
        );
    }
}
