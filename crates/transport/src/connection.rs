//! Transport connection manager for handling multiple connections
//!
//! This module provides the TransportConnectionManager which manages connections
//! using ConnectionVerifier directly for a simpler approach.

use bytes::Bytes;
use ed25519_dalek::SigningKey;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_topology::NodeId;
use serde_json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

use crate::TransportEnvelope;
use crate::error::TransportError;
use crate::verification::{AttestationVerifier, ConnectionVerifier, CoseHandler};

/// Configuration specific to connection management
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Connection establishment timeout
    pub connection_timeout: Duration,
    /// Verification timeout
    pub verification_timeout: Duration,
    /// Maximum verification attempts
    pub max_verification_attempts: usize,
    /// Connection idle timeout
    pub idle_timeout: Duration,
    /// Keep-alive interval
    pub keep_alive_interval: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connection_timeout: Duration::from_secs(5), // 5 seconds
            verification_timeout: Duration::from_secs(30),
            max_verification_attempts: 3,
            idle_timeout: Duration::from_secs(300), // 5 minutes
            keep_alive_interval: Duration::from_secs(60), // 1 minute
        }
    }
}

/// Manager for transport connections using ConnectionVerifier directly
pub struct ConnectionManager<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Connection verifier for handling verification protocol
    connection_verifier: Arc<ConnectionVerifier<G, A>>,
    /// COSE handler for message signing
    cose_handler: Arc<CoseHandler>,
    /// Our node ID
    our_node_id: NodeId,
}

impl<G, A> ConnectionManager<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new transport connection manager
    pub fn new(attestor: Arc<A>, governance: Arc<G>, signing_key: SigningKey) -> Self {
        // Create attestation verifier
        let attestation_verifier = Arc::new(AttestationVerifier::new(
            governance.clone(),
            attestor.clone(),
        ));

        // Create COSE handler
        let cose_handler = Arc::new(CoseHandler::new(signing_key.clone()));

        // Create connection verifier
        let our_node_id = NodeId::from(signing_key.verifying_key());
        let connection_verifier = Arc::new(ConnectionVerifier::new(
            attestation_verifier,
            cose_handler.clone(),
            our_node_id.clone(),
        ));

        Self {
            connection_verifier,
            cose_handler,
            our_node_id,
        }
    }

    /// Create a new transport connection manager with custom connection config
    pub fn with_connection_config(
        attestor: Arc<A>,
        governance: Arc<G>,
        signing_key: SigningKey,
    ) -> Self {
        // Create attestation verifier
        let attestation_verifier = Arc::new(AttestationVerifier::new(
            governance.clone(),
            attestor.clone(),
        ));

        // Create COSE handler
        let cose_handler = Arc::new(CoseHandler::new(signing_key.clone()));

        // Create connection verifier
        let our_node_id = NodeId::from(signing_key.verifying_key());
        let connection_verifier = Arc::new(ConnectionVerifier::new(
            attestation_verifier,
            cose_handler.clone(),
            our_node_id.clone(),
        ));

        Self {
            connection_verifier,
            cose_handler,
            our_node_id,
        }
    }

    /// Handle an incoming connection lifecycle using provided I/O functions
    /// Incoming connections start by waiting for the peer to send the first message
    pub async fn handle_incoming_connection<F, R>(
        &self,
        connection_id: String,
        send_fn: F,
        recv_fn: R,
        incoming_tx: broadcast::Sender<TransportEnvelope>,
    ) -> Result<(), TransportError>
    where
        F: Fn(
                Bytes,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Fn() -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<Bytes, TransportError>> + Send>,
            > + Send
            + Sync
            + 'static,
    {
        // Initialize connection in verifier
        self.connection_verifier
            .initialize_connection(connection_id.clone())
            .await;

        // Handle verification phase - incoming connections wait for peer to initiate
        let mut verified_peer: Option<NodeId> = None;

        while verified_peer.is_none() {
            // Read data using provided function
            let data = recv_fn().await?;

            // Process verification message
            match self
                .connection_verifier
                .process_verification_message(&connection_id, &data)
                .await
            {
                Ok(Some(response)) => {
                    // Send response using provided function
                    send_fn(response).await?;
                }
                Ok(None) => {
                    // No response needed, continue
                }
                Err(e) => {
                    // Check if verification completed successfully
                    if let Some(peer_id) = self
                        .connection_verifier
                        .get_verified_public_key(&connection_id)
                        .await
                    {
                        verified_peer = Some(peer_id);
                        break;
                    } else {
                        return Err(TransportError::auth_failed(e));
                    }
                }
            }

            // Check if verification completed
            if let Some(peer_id) = self
                .connection_verifier
                .get_verified_public_key(&connection_id)
                .await
            {
                verified_peer = Some(peer_id);
                // Clean up verification state immediately after verification completes
                self.connection_verifier
                    .remove_connection(&connection_id)
                    .await;
            }
        }

        // Handle post-verification messages - just receive and forward
        loop {
            let data = recv_fn().await?;

            // Deserialize and verify COSE message
            let cose_message = match self.cose_handler.deserialize_cose_message(&data) {
                Ok(cose_msg) => cose_msg,
                Err(_) => continue, // Skip invalid messages
            };

            // Verify signature and extract payload
            match self
                .cose_handler
                .verify_signed_message(&cose_message, verified_peer.as_ref().unwrap())
                .await
            {
                Ok((payload, metadata)) => {
                    // Deserialize as TransportEnvelope and forward
                    if let Ok(mut envelope) = serde_json::from_slice::<TransportEnvelope>(&payload)
                    {
                        // Use message type from COSE metadata if available
                        if let Some(msg_type) = metadata.message_type {
                            envelope.message_type = msg_type;
                        }
                        // Use correlation ID from COSE metadata if available
                        if metadata.correlation_id.is_some() {
                            envelope.correlation_id = metadata.correlation_id;
                        }

                        if incoming_tx.send(envelope).is_err() {
                            break; // Channel closed
                        }
                    }
                }
                Err(_) => continue, // Skip invalid signatures
            }
        }

        Ok(())
    }

    /// Handle an outgoing connection lifecycle using provided I/O functions
    /// Outgoing connections start by sending the initial verification request
    pub async fn handle_outgoing_connection<F, R>(
        &self,
        connection_id: String,
        expected_peer: NodeId,
        send_fn: F,
        recv_fn: R,
        mut outgoing_rx: tokio::sync::mpsc::Receiver<TransportEnvelope>,
    ) -> Result<(), TransportError>
    where
        F: Fn(
                Bytes,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Fn() -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<Bytes, TransportError>> + Send>,
            > + Send
            + Sync
            + 'static,
    {
        // Initialize connection in verifier
        self.connection_verifier
            .initialize_connection(connection_id.clone())
            .await;

        // Send initial verification request for outgoing connections
        let initial_request = self
            .connection_verifier
            .create_verification_request_for_connection(connection_id.clone())
            .await
            .map_err(TransportError::auth_failed)?;
        send_fn(initial_request).await?;

        // Handle verification phase
        let mut verified_peer: Option<NodeId> = None;

        while verified_peer.is_none() {
            // Read verification response
            let data = recv_fn().await?;

            // Process verification message
            match self
                .connection_verifier
                .process_verification_message(&connection_id, &data)
                .await
            {
                Ok(Some(response)) => {
                    // Send verification response
                    send_fn(response).await?;
                }
                Ok(None) => {
                    // No response needed, continue
                }
                Err(e) => {
                    // Check if verification completed successfully
                    if (self
                        .connection_verifier
                        .get_verified_public_key(&connection_id)
                        .await)
                        .is_some()
                    {
                        break;
                    } else {
                        return Err(TransportError::auth_failed(e));
                    }
                }
            }

            // Check if verification completed
            if let Some(peer_id) = self
                .connection_verifier
                .get_verified_public_key(&connection_id)
                .await
            {
                // Verify the node ID matches expected
                if peer_id != expected_peer {
                    return Err(TransportError::auth_failed(format!(
                        "Node ID mismatch: expected {expected_peer}, got {peer_id}"
                    )));
                }
                verified_peer = Some(peer_id);
                // Clean up verification state immediately after verification completes
                self.connection_verifier
                    .remove_connection(&connection_id)
                    .await;
            }
        }

        // Now handle outgoing messages - unidirectional sending only
        while let Some(envelope) = outgoing_rx.recv().await {
            // Serialize the envelope
            let payload = serde_json::to_vec(&envelope).map_err(TransportError::transport)?;

            // Create and serialize COSE message
            let cose_message = self
                .cose_handler
                .create_signed_message(&payload, &envelope.message_type, envelope.correlation_id)
                .map_err(TransportError::transport)?;
            let signed_message = self
                .cose_handler
                .serialize_cose_message(&cose_message)
                .map_err(TransportError::transport)?;

            // Send the signed message
            send_fn(signed_message).await?;
        }

        Ok(())
    }

    /// Prepare an outgoing message for sending (sign it with COSE)
    pub async fn prepare_outgoing_message(
        &self,
        envelope: &TransportEnvelope,
    ) -> Result<Bytes, TransportError> {
        // Serialize the envelope
        let payload = serde_json::to_vec(envelope).map_err(TransportError::transport)?;

        // Create and serialize COSE message
        let cose_message = self
            .cose_handler
            .create_signed_message(&payload, &envelope.message_type, envelope.correlation_id)
            .map_err(TransportError::transport)?;
        self.cose_handler
            .serialize_cose_message(&cose_message)
            .map_err(TransportError::transport)
    }

    /// Get our node ID
    pub fn our_node_id(&self) -> &NodeId {
        &self.our_node_id
    }
}

impl<G, A> std::fmt::Debug for ConnectionManager<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportConnectionManager")
            .field("our_node_id", &self.our_node_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use proven_attestation_mock::MockAttestor;
    use proven_governance_mock::MockGovernance;

    fn create_test_manager() -> ConnectionManager<MockGovernance, MockAttestor> {
        let governance = Arc::new(MockGovernance::new(
            vec![],        // governance nodes
            vec![],        // authorized versions
            String::new(), // primary auth gateway
            vec![],        // alternate auth gateways
        ));
        let attestor = Arc::new(MockAttestor::new());
        let signing_key = SigningKey::from_bytes(&[1u8; 32]);

        ConnectionManager::new(attestor, governance, signing_key)
    }

    #[tokio::test]
    async fn test_new_manager() {
        let manager = create_test_manager();

        // Should have valid node ID
        assert_ne!(manager.our_node_id, NodeId::from_seed(0));
    }

    #[test]
    fn test_manager_debug() {
        let manager = create_test_manager();
        let debug_str = format!("{manager:?}");

        assert!(debug_str.contains("TransportConnectionManager"));
        assert!(debug_str.contains("our_node_id"));
        assert!(debug_str.contains("config"));
    }

    #[tokio::test]
    async fn test_prepare_outgoing_message() {
        let manager = create_test_manager();

        let envelope = TransportEnvelope {
            correlation_id: Some(uuid::Uuid::new_v4()),
            message_type: "test_message".to_string(),
            payload: Bytes::from("test payload"),
            sender: manager.our_node_id.clone(),
        };

        let result = manager.prepare_outgoing_message(&envelope).await;

        // Should succeed and return signed message bytes
        assert!(result.is_ok());
        let message_bytes = result.unwrap();
        assert!(!message_bytes.is_empty());
    }
}
