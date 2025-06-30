#![allow(dead_code)] // TODO: Remove this once we have a real implementation for app commands

use anyhow::Result;
use bytes::Bytes;
use coset::{CborSerializable, CoseSign1, CoseSign1Builder, HeaderBuilder, iana::Algorithm};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use proven_attestation::Attestor;
use proven_attestation_mock::MockAttestor;
use proven_core::{
    AnonymizeCommand, AnonymizeResponse, Command, CreateApplicationCommand,
    CreateApplicationResponse, IdentifyCommand, IdentifyResponse, Response, WhoAmICommand,
    WhoAmIResponse,
};
use rand::rngs::OsRng;
use reqwest::{blocking::Client, blocking::multipart};
use std::collections::HashMap;
use tracing::{debug, error, info};
use uuid::Uuid;

/// Types of sessions that can be created
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SessionType {
    /// Management session (one shared across all operations)
    Management,

    /// Application session (one per application ID)
    Application(Uuid),
}

/// A cached session with its details
#[derive(Debug, Clone)]
struct CachedSession {
    session_id: Uuid,
    signing_key: SigningKey,
    verifying_key: VerifyingKey,        // Our client's verifying key
    server_verifying_key: VerifyingKey, // Server's verifying key from attestation document
}

/// Client for sending RPC commands to Proven nodes with transparent session management
#[derive(Debug, Clone)]
pub struct RpcClient {
    /// HTTP client for making requests
    client: Client,
    /// Cached sessions by type
    sessions: HashMap<SessionType, CachedSession>,
}

/// Error types for RPC operations
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Invalid response format: {0}")]
    InvalidResponse(String),
    #[error("Server error: {0}")]
    ServerError(String),
    #[error("CBOR parsing error: {0}")]
    CborError(#[from] ciborium::de::Error<std::io::Error>),
    #[error("COSE parsing error: {0}")]
    CoseError(String),
    #[error("Attestation verification failed: {0}")]
    AttestationError(String),
    #[error("Session creation failed: {0}")]
    SessionCreation(String),
}

impl RpcClient {
    /// Create a new RPC client
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            sessions: HashMap::new(),
        }
    }

    /// Send a `WhoAmI` command to get session information (management session)
    pub fn who_am_i(&mut self, node_url: &str) -> Result<WhoAmIResponse, RpcError> {
        let session_id = {
            let session = self.ensure_management_session(node_url)?;
            session.session_id
        };

        let command = Command::WhoAmI(WhoAmICommand);
        match self.send_rpc_command(node_url, &session_id, &command)? {
            Response::WhoAmI(response) => Ok(response),
            _ => Err(RpcError::InvalidResponse(
                "Invalid response type".to_string(),
            )),
        }
    }

    /// Create a new application on the specified node (management session)
    pub fn create_application(&mut self, node_url: &str, _name: &str) -> Result<Uuid, RpcError> {
        let session_id = {
            let session = self.ensure_management_session(node_url)?;
            session.session_id
        };

        let command = Command::CreateApplication(CreateApplicationCommand);
        let Response::CreateApplication(response) =
            self.send_rpc_command(node_url, &session_id, &command)?
        else {
            return Err(RpcError::InvalidResponse(
                "Invalid response type".to_string(),
            ));
        };

        match response {
            CreateApplicationResponse::CreateApplicationSuccess(application) => {
                info!("Created application with ID: {}", application.id);
                Ok(application.id)
            }
            CreateApplicationResponse::CreateApplicationFailure(error) => Err(
                RpcError::ServerError(format!("Failed to create application: {error}")),
            ),
        }
    }

    /// Send an identify command to authenticate the session (management session)
    pub fn identify(&mut self, node_url: &str) -> Result<(), RpcError> {
        let session_id = {
            let session = self.ensure_management_session(node_url)?;
            session.session_id
        };

        // For now, create identify command with empty bytes - real implementation would include identity proof
        let command = Command::Identify(IdentifyCommand {
            passkey_prf_public_key_bytes: bytes::Bytes::new(),
            session_id_signature_bytes: bytes::Bytes::new(),
        });

        let Response::Identify(response) =
            self.send_rpc_command(node_url, &session_id, &command)?
        else {
            return Err(RpcError::InvalidResponse(
                "Invalid response type".to_string(),
            ));
        };

        match response {
            IdentifyResponse::IdentifySuccess(_identity) => {
                info!("Management session identified successfully");
                Ok(())
            }
            IdentifyResponse::IdentifyFailure(error) => Err(RpcError::ServerError(format!(
                "Failed to identify: {error}"
            ))),
        }
    }

    /// Send an anonymize command to remove session identity (management session)
    pub fn anonymize(&mut self, node_url: &str) -> Result<(), RpcError> {
        let session_id = {
            let session = self.ensure_management_session(node_url)?;
            session.session_id
        };

        let command = Command::Anonymize(AnonymizeCommand);
        let Response::Anonymize(response) =
            self.send_rpc_command(node_url, &session_id, &command)?
        else {
            return Err(RpcError::InvalidResponse(
                "Invalid response type".to_string(),
            ));
        };

        match response {
            AnonymizeResponse::AnonymizeSuccess => {
                info!("Management session anonymized successfully");
                Ok(())
            }
            AnonymizeResponse::AnonymizeFailure(error) => Err(RpcError::ServerError(format!(
                "Failed to anonymize: {error}"
            ))),
        }
    }

    /// Ensure we have a management session, creating one if needed
    fn ensure_management_session(&mut self, node_url: &str) -> Result<&CachedSession, RpcError> {
        if !self.sessions.contains_key(&SessionType::Management) {
            info!("Creating management session...");
            let session = self.create_management_session(node_url)?;
            self.sessions.insert(SessionType::Management, session);
        }

        Ok(self.sessions.get(&SessionType::Management).unwrap())
    }

    /// Create a management session with the specified node
    fn create_management_session(&self, node_url: &str) -> Result<CachedSession, RpcError> {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        // Generate a random nonce for this session creation
        let nonce = uuid::Uuid::new_v4().as_bytes().to_vec();
        let public_key = verifying_key.as_bytes().to_vec();

        // Create multipart form data
        let form = multipart::Form::new()
            .part("nonce", multipart::Part::bytes(nonce.clone()))
            .part("public_key", multipart::Part::bytes(public_key));

        let url = format!(
            "{}/auth/create_management_session",
            node_url.trim_end_matches('/')
        );

        let response = self
            .client
            .post(&url)
            .header("Origin", "http://example.com") // Dummy origin header
            .multipart(form)
            .send()?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .text()
                .unwrap_or_else(|_| "Could not read error response body".to_string());

            error!(
                "HTTP error during session creation: {} - Response body: {}",
                status, error_body
            );

            return Err(RpcError::SessionCreation(format!(
                "Session creation failed with status: {status} - {error_body}"
            )));
        }

        let response_bytes = response.bytes()?;

        // Parse the attestation document from response
        let (session_id, server_verifying_key) =
            Self::parse_attestation_document(&response_bytes, &nonce)?;

        info!("✅ Management session created: {}", session_id);

        Ok(CachedSession {
            session_id,
            signing_key,
            verifying_key,
            server_verifying_key,
        })
    }

    /// Parse attestation document and extract session ID using `MockAttestor`
    fn parse_attestation_document(
        attestation_doc: &[u8],
        expected_nonce: &[u8],
    ) -> Result<(Uuid, VerifyingKey), RpcError> {
        let attestor = MockAttestor::new();
        let attestation_bytes = Bytes::from(attestation_doc.to_vec());

        let verified = attestor.verify(attestation_bytes).map_err(|e| {
            RpcError::AttestationError(format!("Attestation verification failed: {e}"))
        })?;

        // Verify nonce matches what we sent
        let nonce = verified.nonce.ok_or_else(|| {
            RpcError::AttestationError("Missing nonce in attestation document".to_string())
        })?;

        if nonce.as_ref() != expected_nonce {
            return Err(RpcError::AttestationError(
                "Nonce mismatch in attestation document".to_string(),
            ));
        }

        // Extract the server's public key (this is what we'll use to verify responses)
        let server_public_key = verified.public_key.ok_or_else(|| {
            RpcError::AttestationError("Missing public key in attestation document".to_string())
        })?;

        let server_verifying_key =
            VerifyingKey::from_bytes(server_public_key.as_ref().try_into().map_err(|_| {
                RpcError::AttestationError("Invalid server public key format".to_string())
            })?)
            .map_err(|e| {
                RpcError::AttestationError(format!("Failed to parse server public key: {e}"))
            })?;

        // Extract session ID from user_data
        let user_data = verified.user_data.ok_or_else(|| {
            RpcError::AttestationError("Missing user_data in attestation document".to_string())
        })?;

        if user_data.len() != 16 {
            return Err(RpcError::AttestationError(
                "Invalid session ID length in user_data".to_string(),
            ));
        }

        let uuid_bytes: [u8; 16] = user_data.as_ref().try_into().map_err(|_| {
            RpcError::AttestationError("Failed to convert user_data to UUID".to_string())
        })?;

        let session_id = Uuid::from_bytes(uuid_bytes);
        Ok((session_id, server_verifying_key))
    }

    /// Send RPC command to management endpoint using COSE/CBOR
    fn send_rpc_command(
        &self,
        node_url: &str,
        session_id: &Uuid,
        command: &Command,
    ) -> Result<Response, RpcError> {
        debug!("Sending management RPC command to {}", node_url);

        let session = self
            .sessions
            .get(&SessionType::Management)
            .ok_or_else(|| RpcError::InvalidResponse("No management session".to_string()))?;

        // Serialize command directly to CBOR
        let mut cbor_payload = Vec::new();
        ciborium::ser::into_writer(command, &mut cbor_payload)
            .map_err(|e| RpcError::InvalidResponse(format!("CBOR encoding failed: {e}")))?;

        // Create COSE Sign1 message
        let cose_bytes = Self::create_cose_sign1(&cbor_payload, session_id, &session.signing_key)?;

        // Send to management RPC endpoint
        let url = format!(
            "{}/management/rpc/http?session={}",
            node_url.trim_end_matches('/'),
            session_id
        );

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/cbor")
            .header("Origin", "http://example.com") // Dummy origin header
            .body(cose_bytes)
            .send()?;

        Self::handle_cose_response(response, session_id, &session.server_verifying_key)
    }

    /// Create a COSE Sign1 message for the given payload
    fn create_cose_sign1(
        payload: &[u8],
        session_id: &Uuid,
        signing_key: &SigningKey,
    ) -> Result<Vec<u8>, RpcError> {
        let protected_header = HeaderBuilder::new().algorithm(Algorithm::EdDSA).build();

        let aad = session_id.as_bytes().to_vec();

        let sign1 = CoseSign1Builder::new()
            .protected(protected_header)
            .payload(payload.to_vec())
            .create_signature(&aad, |pt| signing_key.sign(pt).to_vec())
            .build();

        sign1
            .to_vec()
            .map_err(|e| RpcError::CoseError(format!("COSE Sign1 creation failed: {e}")))
    }

    /// Handle COSE response parsing and verification
    fn handle_cose_response(
        response: reqwest::blocking::Response,
        session_id: &Uuid,
        verifying_key: &VerifyingKey,
    ) -> Result<Response, RpcError> {
        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .text()
                .unwrap_or_else(|_| "Could not read error response body".to_string());

            error!(
                "HTTP error during RPC command: {} - Response body: {}",
                status, error_body
            );

            return Err(RpcError::ServerError(format!(
                "RPC command failed with status {status}: {error_body}"
            )));
        }

        let response_bytes = response.bytes()?;

        // Parse COSE Sign1 response
        let sign1 = CoseSign1::from_slice(&response_bytes)
            .map_err(|e| RpcError::CoseError(format!("COSE Sign1 parsing failed: {e}")))?;

        let payload = sign1
            .payload
            .as_ref()
            .ok_or_else(|| RpcError::CoseError("No payload in COSE Sign1".to_string()))?;

        // Verify signature
        let aad = session_id.as_bytes().to_vec();
        sign1
            .verify_signature(&aad, |signature_bytes, pt| {
                Signature::from_slice(signature_bytes)
                    .and_then(|signature| verifying_key.verify(pt, &signature))
                    .map_err(|_| ())
            })
            .map_err(|()| RpcError::CoseError("Signature verification failed".to_string()))?;

        // Parse CBOR payload directly as the response type
        let typed_result: Response =
            ciborium::de::from_reader(payload.as_slice()).map_err(|e| {
                RpcError::InvalidResponse(format!("Failed to deserialize CBOR response: {e}"))
            })?;

        debug!("RPC command completed successfully");
        Ok(typed_result)
    }

    /// Check if we have a management session
    pub fn has_management_session(&self) -> bool {
        self.sessions.contains_key(&SessionType::Management)
    }

    /// Get the management session ID (if any)
    pub fn management_session_id(&self) -> Option<Uuid> {
        self.sessions
            .get(&SessionType::Management)
            .map(|s| s.session_id)
    }

    /// Clear all cached sessions (useful for testing or reconnection)
    pub fn clear_sessions(&mut self) {
        self.sessions.clear();
        info!("All cached sessions cleared");
    }
}

impl Default for RpcClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_client_creation() {
        let client = RpcClient::new();
        assert!(!client.has_management_session());
    }

    #[test]
    fn test_session_type_equality() {
        let app_id = Uuid::new_v4();
        assert_eq!(SessionType::Management, SessionType::Management);
        assert_eq!(
            SessionType::Application(app_id),
            SessionType::Application(app_id)
        );
        assert_ne!(SessionType::Management, SessionType::Application(app_id));
    }
}
