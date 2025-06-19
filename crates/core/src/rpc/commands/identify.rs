use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use proven_identity::Identity;
use serde::{Deserialize, Serialize};

use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IdentifyCommand {
    pub passkey_prf_public_key_bytes: Bytes,
    pub session_id_signature_bytes: Bytes,
}

#[derive(Debug, Serialize)]
pub enum IdentifyResponse {
    IdentifyFailure(String),
    // TODO: strip this down to something client-safe
    IdentifySuccess(Identity),
}

#[async_trait]
impl RpcCommand for IdentifyCommand {
    type Response = IdentifyResponse;

    async fn execute<AM, IM, RM>(&self, context: &mut RpcContext<AM, IM, RM>) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        IM: proven_identity::IdentityManagement,
        RM: proven_runtime::RuntimePoolManagement,
    {
        // Turn passkey_prf_public_key_bytes into [u8;32]
        let passkey_prf_public_key_bytes = self.passkey_prf_public_key_bytes.to_vec();
        let passkey_prf_public_key_bytes = match passkey_prf_public_key_bytes.try_into() {
            Ok(bytes) => bytes,
            Err(_) => return IdentifyResponse::IdentifyFailure("Invalid public key".to_string()),
        };

        // Turn passkey_prf_public_key_bytes into a VerifyingKey
        let passkey_prf_public_key = match VerifyingKey::from_bytes(&passkey_prf_public_key_bytes) {
            Ok(key) => key,
            Err(_) => return IdentifyResponse::IdentifyFailure("Invalid public key".to_string()),
        };

        // Get the session ID as bytes for signature verification
        let session_id = context.session.session_id();
        let session_id_bytes = session_id.as_bytes();

        // Turn session_id_signature_bytes into a Signature
        let signature_bytes = self.session_id_signature_bytes.to_vec();
        let signature = match Signature::from_slice(&signature_bytes) {
            Ok(sig) => sig,
            Err(_) => {
                return IdentifyResponse::IdentifyFailure(
                    "Invalid session ID signature".to_string(),
                );
            }
        };

        // Verify the session ID signature
        if passkey_prf_public_key
            .verify(session_id_bytes, &signature)
            .is_err()
        {
            return IdentifyResponse::IdentifyFailure(
                "Session ID signature verification failed".to_string(),
            );
        }

        // Only if the signature is valid, look up the identity by public key
        let identity = match context
            .identity_manager
            .get_or_create_identity_by_passkey_prf_public_key(&self.passkey_prf_public_key_bytes)
            .await
        {
            Ok(identity) => identity,
            Err(e) => return IdentifyResponse::IdentifyFailure(e.to_string()),
        };

        let session = match context
            .identity_manager
            .identify_session(&context.application_id, &session_id, &identity.identity_id)
            .await
        {
            Ok(session) => session,
            Err(e) => return IdentifyResponse::IdentifyFailure(e.to_string()),
        };

        // Update context with new session
        context.session = session;

        IdentifyResponse::IdentifySuccess(identity)
    }
}
