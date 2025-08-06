//! RPC client with multi-user support for cluster testing

#![allow(clippy::unused_async)] // API consistency - these may need async in the future

use anyhow::Result;
use bytes::Bytes;
use coset::{CborSerializable, CoseSign1, CoseSign1Builder, HeaderBuilder, iana::Algorithm};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use proven_applications::Application;
use proven_attestation::Attestor;
use proven_attestation_mock::MockAttestor;
use proven_core::{
    AddAllowedOriginCommand, AddAllowedOriginResponse, AnonymizeCommand, AnonymizeResponse,
    Command, CreateApplicationCommand, CreateApplicationResponse, IdentifyCommand,
    IdentifyResponse, ListApplicationsByOwnerCommand, ListApplicationsByOwnerResponse, Response,
    WhoAmICommand, WhoAmIResponse, routes,
};
use proven_util::Origin;
use rand::rngs::OsRng;
use reqwest::{Client, multipart};
use tracing::{debug, error, info};
use uuid::Uuid;

/// User identity for RPC operations
#[derive(Debug, Clone)]
pub struct UserIdentity {
    /// Human-readable name for the user
    pub name: String,
    /// Ed25519 signing key for identity
    pub signing_key: SigningKey,
}

impl UserIdentity {
    /// Create a new random user identity
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signing_key: SigningKey::generate(&mut OsRng),
        }
    }

    /// Create from existing key (for persistent identities)
    #[must_use]
    pub fn from_key(name: &str, key: SigningKey) -> Self {
        Self {
            name: name.to_string(),
            signing_key: key,
        }
    }
}

/// Session information
#[derive(Debug, Clone)]
struct SessionInfo {
    session_id: Uuid,
    session_signing_key: SigningKey,
    _session_verifying_key: VerifyingKey,
    server_verifying_key: VerifyingKey,
    is_identified: bool,
}

/// Client for sending RPC commands to Proven nodes
#[derive(Debug, Clone)]
pub struct RpcClient {
    /// HTTP client for making requests
    client: Client,
    /// Current session (if any)
    session: Option<SessionInfo>,
    /// User identity (if any)
    identity: Option<UserIdentity>,
    /// Node URL
    node_url: Option<String>,
}

/// Error types for RPC operations
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    /// Response was not in the expected format
    #[error("Invalid response format: {0}")]
    InvalidResponse(String),
    /// Server returned an error
    #[error("Server error: {0}")]
    ServerError(String),
    /// Failed to parse CBOR data
    #[error("CBOR parsing error: {0}")]
    CborError(#[from] ciborium::de::Error<std::io::Error>),
    /// Failed to parse COSE data
    #[error("COSE parsing error: {0}")]
    CoseError(String),
    /// Attestation verification failed
    #[error("Attestation verification failed: {0}")]
    AttestationError(String),
    /// Failed to create a session
    #[error("Session creation failed: {0}")]
    SessionCreation(String),
    /// No session has been established
    #[error("No session established")]
    NoSession,
}

impl RpcClient {
    /// Create a new RPC client
    #[must_use]
    pub fn new() -> Self {
        // Create client with timeout
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            session: None,
            identity: None,
            node_url: None,
        }
    }

    /// Set the user identity for this client
    pub fn set_identity(&mut self, identity: UserIdentity) {
        self.identity = Some(identity);
    }

    /// Initialize session with a node
    ///
    /// # Errors
    ///
    /// Returns an error if session creation fails
    pub async fn initialize_session(&mut self, node_url: &str) -> Result<(), RpcError> {
        self.node_url = Some(node_url.to_string());
        self.ensure_session().await?;
        Ok(())
    }

    /// Send a `WhoAmI` command to get session information
    ///
    /// # Errors
    ///
    /// Returns an error if there's no session or the command fails
    pub async fn who_am_i(&mut self) -> Result<WhoAmIResponse, RpcError> {
        self.ensure_session().await?;
        let session = self.session.as_ref().ok_or(RpcError::NoSession)?;

        let command = Command::WhoAmI(WhoAmICommand);
        match self.send_rpc_command(&session.session_id, &command).await? {
            Response::WhoAmI(response) => Ok(response),
            _ => Err(RpcError::InvalidResponse(
                "Invalid response type".to_string(),
            )),
        }
    }

    /// Create a new application
    ///
    /// # Errors
    ///
    /// Returns an error if not identified or the creation fails
    pub async fn create_application(&mut self, _name: &str) -> Result<Uuid, RpcError> {
        self.ensure_identified_session().await?;
        let session = self.session.as_ref().ok_or(RpcError::NoSession)?;

        let command = Command::CreateApplication(CreateApplicationCommand);
        let Response::CreateApplication(response) =
            self.send_rpc_command(&session.session_id, &command).await?
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

    /// Send an identify command to authenticate the session
    ///
    /// # Errors
    ///
    /// Returns an error if there's no session, no identity, or the command fails
    pub async fn identify(&mut self) -> Result<(), RpcError> {
        self.ensure_session().await?;
        let session = self.session.as_ref().ok_or(RpcError::NoSession)?;

        let identity = self.identity.as_ref().ok_or_else(|| {
            RpcError::InvalidResponse("No identity set for this client".to_string())
        })?;

        // Sign the session ID with the identity key
        let session_id_bytes = session.session_id.as_bytes();
        let signature = identity.signing_key.sign(session_id_bytes);

        // Get the public key bytes
        let public_key_bytes = identity.signing_key.verifying_key().as_bytes().to_vec();

        let command = Command::Identify(IdentifyCommand {
            passkey_prf_public_key_bytes: bytes::Bytes::from(public_key_bytes),
            session_id_signature_bytes: bytes::Bytes::from(signature.to_bytes().to_vec()),
        });

        let Response::Identify(response) =
            self.send_rpc_command(&session.session_id, &command).await?
        else {
            return Err(RpcError::InvalidResponse(
                "Invalid response type".to_string(),
            ));
        };

        match response {
            IdentifyResponse::IdentifySuccess(_identity) => {
                info!(
                    "Session identified successfully for user: {}",
                    identity.name
                );

                // Update the session to mark it as identified
                if let Some(session) = &mut self.session {
                    session.is_identified = true;
                }

                Ok(())
            }
            IdentifyResponse::IdentifyFailure(error) => Err(RpcError::ServerError(format!(
                "Failed to identify: {error}"
            ))),
        }
    }

    /// Send an anonymize command to remove session identity
    ///
    /// # Errors
    ///
    /// Returns an error if there's no session or the command fails
    pub async fn anonymize(&mut self) -> Result<(), RpcError> {
        self.ensure_session().await?;
        let session = self.session.as_ref().ok_or(RpcError::NoSession)?;

        let command = Command::Anonymize(AnonymizeCommand);
        let Response::Anonymize(response) =
            self.send_rpc_command(&session.session_id, &command).await?
        else {
            return Err(RpcError::InvalidResponse(
                "Invalid response type".to_string(),
            ));
        };

        match response {
            AnonymizeResponse::AnonymizeSuccess => {
                info!("Session anonymized successfully");

                // Update the session to mark it as no longer identified
                if let Some(session) = &mut self.session {
                    session.is_identified = false;
                }

                Ok(())
            }
            AnonymizeResponse::AnonymizeFailure(error) => Err(RpcError::ServerError(format!(
                "Failed to anonymize: {error}"
            ))),
        }
    }

    /// List applications owned by the current user
    ///
    /// # Errors
    ///
    /// Returns an error if not identified or the command fails
    pub async fn list_applications_by_owner(&mut self) -> Result<Vec<Application>, RpcError> {
        self.ensure_identified_session().await?;
        let session = self.session.as_ref().ok_or(RpcError::NoSession)?;

        let command = Command::ListApplicationsByOwner(ListApplicationsByOwnerCommand);
        let Response::ListApplicationsByOwner(response) =
            self.send_rpc_command(&session.session_id, &command).await?
        else {
            return Err(RpcError::InvalidResponse(
                "Invalid response type".to_string(),
            ));
        };

        match response {
            ListApplicationsByOwnerResponse::ListApplicationsByOwnerSuccess(applications) => {
                info!("Retrieved {} applications", applications.len());
                Ok(applications)
            }
            ListApplicationsByOwnerResponse::ListApplicationsByOwnerFailure(error) => Err(
                RpcError::ServerError(format!("Failed to list applications: {error}")),
            ),
        }
    }

    /// Add an allowed origin to an application
    ///
    /// # Errors
    ///
    /// Returns an error if not identified or the command fails
    pub async fn add_allowed_origin(
        &mut self,
        application_id: Uuid,
        origin: Origin,
    ) -> Result<(), RpcError> {
        self.ensure_identified_session().await?;
        let session = self.session.as_ref().ok_or(RpcError::NoSession)?;

        let command = Command::AddAllowedOrigin(AddAllowedOriginCommand {
            application_id,
            origin,
        });
        let Response::AddAllowedOrigin(response) =
            self.send_rpc_command(&session.session_id, &command).await?
        else {
            return Err(RpcError::InvalidResponse(
                "Invalid response type".to_string(),
            ));
        };

        match response {
            AddAllowedOriginResponse::AddAllowedOriginSuccess => {
                info!(
                    "Successfully added allowed origin to application {}",
                    application_id
                );
                Ok(())
            }
            AddAllowedOriginResponse::AddAllowedOriginFailure(error) => Err(RpcError::ServerError(
                format!("Failed to add allowed origin: {error}"),
            )),
        }
    }

    /// Ensure we have a session, creating one if needed
    async fn ensure_session(&mut self) -> Result<(), RpcError> {
        if self.session.is_none() {
            let node_url = self
                .node_url
                .as_ref()
                .ok_or_else(|| RpcError::InvalidResponse("No node URL set".to_string()))?;
            info!("Creating new session...");
            let session = self.create_session(node_url).await?;
            self.session = Some(session);
        }
        Ok(())
    }

    /// Ensure we have an identified session
    async fn ensure_identified_session(&mut self) -> Result<(), RpcError> {
        self.ensure_session().await?;

        let needs_identification = self.session.as_ref().is_none_or(|s| !s.is_identified);

        if needs_identification {
            if self.identity.is_none() {
                return Err(RpcError::InvalidResponse(
                    "Cannot identify session without user identity".to_string(),
                ));
            }
            info!("Session not identified, sending identify command...");
            self.identify().await?;
        }

        Ok(())
    }

    /// Create a new session with the specified node
    async fn create_session(&self, node_url: &str) -> Result<SessionInfo, RpcError> {
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
            "{}{}",
            node_url.trim_end_matches('/'),
            routes::NEW_MANAGEMENT_SESSION
        );

        let response = self
            .client
            .post(&url)
            .header("Origin", "http://example.com")
            .multipart(form)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .text()
                .await
                .unwrap_or_else(|_| "Could not read error response body".to_string());

            error!(
                "HTTP error during session creation: {} - Response body: {} - URL: {}",
                status, error_body, url
            );

            return Err(RpcError::SessionCreation(format!(
                "Session creation failed with status: {status} - {error_body}"
            )));
        }

        let response_bytes = response.bytes().await?;

        // Parse the attestation document from response
        let (session_id, server_verifying_key) =
            Self::parse_attestation_document(&response_bytes, &nonce)?;

        info!(
            "✅ Session created: {} for user: {:?}",
            session_id,
            self.identity.as_ref().map(|i| &i.name)
        );

        Ok(SessionInfo {
            session_id,
            session_signing_key: signing_key,
            _session_verifying_key: verifying_key,
            server_verifying_key,
            is_identified: false,
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

        // Extract the server's public key
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

    /// Send RPC command using COSE/CBOR
    #[allow(clippy::cognitive_complexity)]
    async fn send_rpc_command(
        &self,
        session_id: &Uuid,
        command: &Command,
    ) -> Result<Response, RpcError> {
        let node_url = self
            .node_url
            .as_ref()
            .ok_or_else(|| RpcError::InvalidResponse("No node URL set".to_string()))?;

        let session = self.session.as_ref().ok_or(RpcError::NoSession)?;

        debug!("Sending RPC command to {}", node_url);

        // Serialize command directly to CBOR
        let mut cbor_payload = Vec::new();
        ciborium::ser::into_writer(command, &mut cbor_payload)
            .map_err(|e| RpcError::InvalidResponse(format!("CBOR encoding failed: {e}")))?;

        // Create COSE Sign1 message
        let cose_bytes =
            Self::create_cose_sign1(&cbor_payload, session_id, &session.session_signing_key)?;

        // Send to management RPC endpoint
        let url = format!(
            "{}{}?session={}",
            node_url.trim_end_matches('/'),
            routes::MANAGEMENT_HTTP_RPC,
            session_id
        );

        info!("Sending RPC POST to: {}", url);
        debug!("Command type: {:?}", command);

        let response = match self
            .client
            .post(&url)
            .header("Content-Type", "application/cbor")
            .header("Origin", "http://example.com")
            .body(cose_bytes)
            .send()
            .await
        {
            Ok(resp) => {
                info!("Received response with status: {}", resp.status());
                resp
            }
            Err(e) => {
                error!("Failed to send RPC request to {}: {}", url, e);
                if e.is_timeout() {
                    return Err(RpcError::ServerError(format!(
                        "Request timed out after 10 seconds: {e}"
                    )));
                }
                return Err(RpcError::ServerError(format!("HTTP request failed: {e}")));
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .text()
                .await
                .unwrap_or_else(|_| "Could not read error response body".to_string());

            error!(
                "HTTP error during RPC command: {} - Response body: {}",
                status, error_body
            );

            return Err(RpcError::ServerError(format!(
                "RPC command failed with status {status}: {error_body}"
            )));
        }

        Self::handle_cose_response(response, session_id, &session.server_verifying_key).await
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
    async fn handle_cose_response(
        response: reqwest::Response,
        session_id: &Uuid,
        verifying_key: &VerifyingKey,
    ) -> Result<Response, RpcError> {
        let response_bytes = response.bytes().await?;

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

    /// Check if we have a session
    #[must_use]
    pub const fn has_session(&self) -> bool {
        self.session.is_some()
    }

    /// Check if we have an identified session
    #[must_use]
    pub fn has_identified_session(&self) -> bool {
        self.session.as_ref().is_some_and(|s| s.is_identified)
    }

    /// Get the session ID (if any)
    #[must_use]
    pub fn session_id(&self) -> Option<Uuid> {
        self.session.as_ref().map(|s| s.session_id)
    }

    /// Clear the session (useful for testing or reconnection)
    pub fn clear_session(&mut self) {
        self.session = None;
        info!(
            "Session cleared for user: {:?}",
            self.identity.as_ref().map(|i| &i.name)
        );
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
        assert!(!client.has_session());
    }

    #[test]
    fn test_user_identity() {
        let alice = UserIdentity::new("alice");
        assert_eq!(alice.name, "alice");

        let key = SigningKey::generate(&mut OsRng);
        let bob = UserIdentity::from_key("bob", key.clone());
        assert_eq!(bob.name, "bob");
        assert_eq!(bob.signing_key.to_bytes(), key.to_bytes());
    }
}
