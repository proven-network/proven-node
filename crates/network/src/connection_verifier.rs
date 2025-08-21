//! Connection verification system for secure transport authentication
//!
//! This module implements a mutual authentication handshake protocol that verifies
//! the identity of connecting peers using COSE-signed attestation documents.

use bytes::Bytes;
use dashmap::DashMap;
use ed25519_dalek::VerifyingKey;
use proven_topology::NodeId;
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;

use proven_attestation::{AttestationParams, Attestor};
use proven_topology::TopologyAdaptor;

use crate::attestation::AttestationVerifier;
use crate::cose::{CoseHandler, CoseSign1};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum VerificationError {
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Signature verification failed: {0}")]
    SignatureVerification(String),

    #[error("Attestation error: {0}")]
    Attestation(String),

    #[error("Topology error: {0}")]
    Topology(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Timeout")]
    Timeout,
}

pub type VerificationResult<T> = Result<T, VerificationError>;

/// Timeout for verification handshake
const VERIFICATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum number of verification attempts per connection
#[allow(dead_code)]
const MAX_VERIFICATION_ATTEMPTS: usize = 3;

/// Verification message types for the handshake protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VerificationMessage {
    /// Initial challenge request (no attestation, just a challenge)
    VerificationChallenge {
        /// Random challenge for peer to include in their attestation
        challenge: Bytes,
        /// Timestamp to prevent replay attacks
        timestamp: u64,
    },
    /// Response with attestation document containing challenge as nonce
    VerificationResponse {
        /// Attestation document with challenge as nonce and public key
        attestation_document: Bytes,
        /// Our challenge for mutual authentication
        challenge: Bytes,
        /// Timestamp
        timestamp: u64,
    },
    /// Final acknowledgment with attestation containing their challenge as nonce
    VerificationComplete {
        /// Our attestation document with their challenge as nonce
        attestation_document: Bytes,
        /// Timestamp
        timestamp: u64,
    },
    /// Verification failed
    VerificationFailed {
        /// Error message
        reason: String,
        /// Timestamp
        timestamp: u64,
    },
}

/// Connection verification state
#[derive(Debug, Clone)]
pub enum ConnectionState {
    /// Waiting for initial verification message
    WaitingForVerification,
    /// Verification in progress
    VerificationInProgress {
        /// Our challenge that peer must include in their attestation nonce
        our_challenge: Bytes,
        /// Peer's challenge we must include in our attestation nonce
        peer_challenge: Option<Bytes>,
        /// Number of attempts so far
        attempts: usize,
        /// When verification started
        started_at: SystemTime,
    },
    /// Connection successfully verified
    Verified {
        /// Verified public key of the peer
        public_key: NodeId,
        /// When verification completed
        verified_at: SystemTime,
    },
    /// Verification failed
    Failed {
        /// Reason for failure
        reason: String,
        /// When failure occurred
        failed_at: SystemTime,
    },
}

/// Connection verifier handles the secure handshake protocol
#[derive(Debug)]
pub struct ConnectionVerifier<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Attestation verifier for document validation
    attestation_verifier: Arc<AttestationVerifier<G, A>>,
    /// COSE handler for message signing/verification
    cose_handler: Arc<CoseHandler>,
    /// Our local node's public key
    local_public_key: NodeId,
    /// Active connection states by connection ID
    connection_states: Arc<DashMap<String, ConnectionState>>,
}

impl<G, A> ConnectionVerifier<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new connection verifier
    pub fn new(
        attestation_verifier: Arc<AttestationVerifier<G, A>>,
        cose_handler: Arc<CoseHandler>,
        local_node_id: NodeId,
    ) -> Self {
        Self {
            attestation_verifier,
            cose_handler,
            local_public_key: local_node_id,
            connection_states: Arc::new(DashMap::new()),
        }
    }

    /// Get the current state of a connection
    pub async fn get_connection_state(&self, connection_id: &str) -> Option<ConnectionState> {
        self.connection_states.get(connection_id).map(|s| s.clone())
    }

    /// Check if a connection is verified
    pub async fn is_connection_verified(&self, connection_id: &str) -> bool {
        matches!(
            self.get_connection_state(connection_id).await,
            Some(ConnectionState::Verified { .. })
        )
    }

    /// Get the verified public key for a connection
    pub async fn get_verified_public_key(&self, connection_id: &str) -> Option<NodeId> {
        match self.get_connection_state(connection_id).await {
            Some(ConnectionState::Verified { public_key, .. }) => Some(public_key),
            _ => None,
        }
    }

    /// Initialize a new connection for verification
    pub async fn initialize_connection(&self, connection_id: String) {
        self.connection_states.insert(
            connection_id.clone(),
            ConnectionState::WaitingForVerification,
        );
    }

    /// Remove a connection from tracking
    pub async fn remove_connection(&self, connection_id: &str) {
        self.connection_states.remove(connection_id);
    }

    /// Generate a verification request for outgoing connections
    pub async fn create_verification_request(&self) -> VerificationResult<Bytes> {
        let challenge = Self::generate_challenge();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let verification_message = VerificationMessage::VerificationChallenge {
            challenge,
            timestamp,
        };

        self.serialize_and_sign_message(verification_message).await
    }

    /// Generate a verification request for a specific outgoing connection and store the challenge
    pub async fn create_verification_request_for_connection(
        &self,
        connection_id: String,
    ) -> VerificationResult<Bytes> {
        let challenge = Self::generate_challenge();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Store the challenge in connection state for outgoing connections
        {
            self.connection_states.insert(
                connection_id.clone(),
                ConnectionState::VerificationInProgress {
                    our_challenge: challenge.clone(),
                    peer_challenge: None, // Will be set when peer responds
                    attempts: 1,
                    started_at: SystemTime::now(),
                },
            );
        }

        let verification_message = VerificationMessage::VerificationChallenge {
            challenge,
            timestamp,
        };

        self.serialize_and_sign_message(verification_message).await
    }

    /// Process an incoming verification message
    pub async fn process_verification_message(
        &self,
        connection_id: &String,
        message_data: &Bytes,
    ) -> VerificationResult<Option<Bytes>> {
        // First decode the COSE message to get the verification message
        let cose_sign1 = self.cose_handler.deserialize_cose_message(message_data)?;

        // Extract the verification message without verifying signature yet
        let verification_message = self.extract_verification_message(&cose_sign1)?;

        match verification_message {
            VerificationMessage::VerificationChallenge {
                ref challenge,
                timestamp,
            } => {
                self.handle_verification_challenge(connection_id, challenge, timestamp)
                    .await
            }
            VerificationMessage::VerificationResponse {
                ref attestation_document,
                ref challenge,
                timestamp,
            } => {
                self.handle_verification_response(
                    connection_id,
                    attestation_document,
                    challenge,
                    timestamp,
                )
                .await
            }
            VerificationMessage::VerificationComplete {
                ref attestation_document,
                timestamp,
            } => {
                self.handle_verification_complete(connection_id, attestation_document, timestamp)
                    .await
            }
            VerificationMessage::VerificationFailed { reason, .. } => {
                warn!(
                    "Received verification failure from connection {}: {}",
                    connection_id, reason
                );
                self.mark_connection_failed(connection_id, reason).await;
                Ok(None)
            }
        }
    }

    /// Generate a random challenge for authentication
    fn generate_challenge() -> Bytes {
        let mut challenge = vec![0u8; 32]; // 256-bit challenge
        OsRng.fill_bytes(&mut challenge);
        Bytes::from(challenge)
    }

    /// Generate attestation document with specific nonce and our public key
    async fn generate_attestation_with_nonce(&self, nonce: &Bytes) -> VerificationResult<Bytes> {
        // Get our public key bytes
        let public_key_bytes = self.local_public_key.to_bytes().to_vec();

        let params = AttestationParams {
            nonce: if nonce.is_empty() {
                None
            } else {
                Some(nonce.to_vec().into())
            },
            user_data: Some(b"proven-consensus-peer".to_vec().into()),
            public_key: Some(Bytes::from(public_key_bytes)),
        };

        let attestation = self
            .attestation_verifier
            .attestor()
            .attest(params)
            .await
            .map_err(|e| {
                VerificationError::Attestation(format!("Failed to generate attestation: {e:?}"))
            })?;

        Ok(attestation)
    }

    /// Serialize and COSE-sign a verification message
    async fn serialize_and_sign_message(
        &self,
        message: VerificationMessage,
    ) -> VerificationResult<Bytes> {
        // Serialize the verification message
        let mut message_data = Vec::new();
        ciborium::ser::into_writer(&message, &mut message_data).map_err(|e| {
            VerificationError::InvalidMessage(format!(
                "Failed to serialize verification message: {e}"
            ))
        })?;

        // Sign with COSE using the new API
        let empty_headers = std::collections::HashMap::new();
        let cose_sign1 = self
            .cose_handler
            .create_signed_message(&message_data, &empty_headers)?; // No headers for verification
        let cose_data = self.cose_handler.serialize_cose_message(&cose_sign1)?;

        Ok(cose_data)
    }

    /// Extract verification message from COSE message without signature verification
    fn extract_verification_message(
        &self,
        cose_sign1: &CoseSign1,
    ) -> VerificationResult<VerificationMessage> {
        // Extract payload without signature verification
        let payload_bytes = cose_sign1
            .payload
            .as_ref()
            .ok_or_else(|| VerificationError::InvalidMessage("Missing payload".to_string()))?;

        // Deserialize the verification message directly from payload bytes
        let verification_message: VerificationMessage =
            ciborium::de::from_reader(payload_bytes.as_slice()).map_err(|e| {
                VerificationError::InvalidMessage(format!(
                    "Failed to deserialize verification message: {e}"
                ))
            })?;

        Ok(verification_message)
    }

    /// Handle incoming verification challenge
    async fn handle_verification_challenge(
        &self,
        connection_id: &str,
        challenge: &Bytes,
        _timestamp: u64,
    ) -> VerificationResult<Option<Bytes>> {
        // Generate our challenge for mutual authentication
        let our_challenge = Self::generate_challenge();

        // Generate attestation document with their challenge as nonce
        let response_attestation = self.generate_attestation_with_nonce(challenge).await?;

        let response = VerificationMessage::VerificationResponse {
            attestation_document: response_attestation,
            challenge: our_challenge.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Update connection state
        {
            self.connection_states.insert(
                connection_id.to_string(),
                ConnectionState::VerificationInProgress {
                    our_challenge,
                    peer_challenge: Some(challenge.clone()),
                    attempts: 1,
                    started_at: SystemTime::now(),
                },
            );
        }

        let response_data = self.serialize_and_sign_message(response).await?;
        Ok(Some(response_data))
    }

    /// Handle verification response
    async fn handle_verification_response(
        &self,
        connection_id: &String,
        attestation_document: &Bytes,
        challenge: &Bytes,
        _timestamp: u64,
    ) -> VerificationResult<Option<Bytes>> {
        // Get our challenge from connection state
        let our_challenge = {
            match self.connection_states.get(connection_id).map(|s| s.clone()) {
                Some(ConnectionState::VerificationInProgress { our_challenge, .. }) => {
                    our_challenge.clone()
                }
                _ => {
                    return Err(VerificationError::InvalidMessage(
                        "No challenge found for connection".to_string(),
                    ));
                }
            }
        };

        // Verify attestation document with our challenge as expected nonce
        let public_key = self
            .verify_attestation_with_nonce(attestation_document, &our_challenge)
            .await?;

        // Send completion message with their challenge as nonce
        let completion_attestation = self.generate_attestation_with_nonce(challenge).await?;

        let completion = VerificationMessage::VerificationComplete {
            attestation_document: completion_attestation,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Mark connection as verified
        {
            self.connection_states.insert(
                connection_id.clone(),
                ConnectionState::Verified {
                    public_key,
                    verified_at: SystemTime::now(),
                },
            );
        }

        let completion_data = self.serialize_and_sign_message(completion).await?;
        Ok(Some(completion_data))
    }

    /// Handle verification completion
    async fn handle_verification_complete(
        &self,
        connection_id: &String,
        attestation_document: &Bytes,
        _timestamp: u64,
    ) -> VerificationResult<Option<Bytes>> {
        // Get our challenge from connection state
        let our_challenge = {
            match self.connection_states.get(connection_id).map(|s| s.clone()) {
                Some(ConnectionState::VerificationInProgress { our_challenge, .. }) => {
                    our_challenge.clone()
                }
                _ => {
                    return Err(VerificationError::InvalidMessage(
                        "No challenge found for completion verification".to_string(),
                    ));
                }
            }
        };

        // Verify attestation document with our challenge as expected nonce
        let public_key = self
            .verify_attestation_with_nonce(attestation_document, &our_challenge)
            .await?;

        {
            self.connection_states.insert(
                connection_id.clone(),
                ConnectionState::Verified {
                    public_key,
                    verified_at: SystemTime::now(),
                },
            );
        }

        Ok(None) // No response needed
    }

    /// Verify attestation document with expected nonce and extract public key
    async fn verify_attestation_with_nonce(
        &self,
        attestation_document: &Bytes,
        expected_nonce: &Bytes,
    ) -> VerificationResult<NodeId> {
        // First verify the attestation document
        let authorized = self
            .attestation_verifier
            .authorize_peer(attestation_document.clone())
            .await?;

        if !authorized {
            return Err(VerificationError::Attestation(
                "Peer not authorized by attestation".to_string(),
            ));
        }

        // Extract and verify the attestation document
        let verified_attestation = self
            .attestation_verifier
            .attestor()
            .verify(attestation_document.clone())
            .map_err(|e| {
                VerificationError::Attestation(format!("Failed to verify attestation: {e:?}"))
            })?;

        // Verify the nonce matches if we expect one
        if !expected_nonce.is_empty() {
            let attestation_nonce = verified_attestation
                .nonce
                .map(|n| Bytes::from(n.to_vec()))
                .unwrap_or_else(Bytes::new);

            if attestation_nonce != expected_nonce {
                return Err(VerificationError::Attestation(
                    "Attestation nonce does not match expected challenge".to_string(),
                ));
            }
        }

        let public_key = verified_attestation.public_key.ok_or_else(|| {
            VerificationError::Attestation("No public key in attestation document".to_string())
        })?;
        let public_key_bytes = public_key.to_vec();
        let public_key_bytes: [u8; 32] = public_key_bytes.try_into().unwrap();
        let verifying_key = VerifyingKey::from_bytes(&public_key_bytes).unwrap();

        let node_id = NodeId::new(verifying_key);

        Ok(node_id)
    }

    /// Mark a connection as failed
    async fn mark_connection_failed(&self, connection_id: &str, reason: String) {
        self.connection_states.insert(
            connection_id.to_string(),
            ConnectionState::Failed {
                reason: reason.clone(),
                failed_at: SystemTime::now(),
            },
        );
    }

    /// Clean up expired verification attempts
    pub async fn cleanup_expired_verifications(&self) {
        let now = SystemTime::now();

        self.connection_states.retain(|_connection_id, state| {
            match state {
                ConnectionState::VerificationInProgress { started_at, .. } => {
                    now.duration_since(*started_at).unwrap_or_default() <= VERIFICATION_TIMEOUT
                }
                _ => true, // Keep verified and failed connections for now
            }
        });
    }
}
