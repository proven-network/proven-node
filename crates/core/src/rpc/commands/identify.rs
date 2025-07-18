use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use proven_identity::Identity;
use proven_sessions::{ApplicationSession, ManagementSession, Session};
use serde::{Deserialize, Serialize};

/// Command to identify a session.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IdentifyCommand {
    /// The public key of the passkey PRF used to identify the session.
    pub passkey_prf_public_key_bytes: Bytes,

    /// The signature of the session ID signed by the passkey PRF.
    pub session_id_signature_bytes: Bytes,
}

/// Response to an identify command.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "result", content = "data")]
pub enum IdentifyResponse {
    /// A failure to identify the session.
    #[serde(rename = "failure")]
    IdentifyFailure(String),

    /// A success to identify the session.
    #[serde(rename = "success")]
    // TODO: strip this down to something client-safe
    IdentifySuccess(Identity),
}

#[async_trait]
impl RpcCommand for IdentifyCommand {
    type Response = IdentifyResponse;

    async fn execute<AM, IM, SM, RM>(
        &self,
        context: &mut RpcContext<AM, IM, SM, RM>,
    ) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        IM: proven_identity::IdentityManagement,
        SM: proven_sessions::SessionManagement,
        RM: proven_runtime::RuntimePoolManagement,
    {
        // Turn passkey_prf_public_key_bytes into [u8;32]
        let passkey_prf_public_key_bytes = self.passkey_prf_public_key_bytes.to_vec();
        let Ok(passkey_prf_public_key_bytes) = passkey_prf_public_key_bytes.try_into() else {
            return IdentifyResponse::IdentifyFailure("Invalid public key".to_string());
        };

        // Turn passkey_prf_public_key_bytes into a VerifyingKey
        let Ok(passkey_prf_public_key) = VerifyingKey::from_bytes(&passkey_prf_public_key_bytes)
        else {
            return IdentifyResponse::IdentifyFailure("Invalid public key".to_string());
        };

        // Get the session ID as bytes for signature verification
        let session_id = context.session.session_id();
        let session_id_bytes = session_id.as_bytes();

        // Turn session_id_signature_bytes into a Signature
        let signature_bytes = self.session_id_signature_bytes.to_vec();
        let Ok(signature) = Signature::from_slice(&signature_bytes) else {
            return IdentifyResponse::IdentifyFailure("Invalid session ID signature".to_string());
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
            .get_or_create_identity_by_prf_public_key(&self.passkey_prf_public_key_bytes)
            .await
        {
            Ok(identity) => identity,
            Err(e) => return IdentifyResponse::IdentifyFailure(e.to_string()),
        };

        let session = match &context.session {
            Session::Application(app_session) => match app_session {
                ApplicationSession::Anonymous { application_id, .. } => match context
                    .sessions_manager
                    .identify_session(application_id, session_id, &identity.id)
                    .await
                {
                    Ok(session) => session,
                    Err(e) => return IdentifyResponse::IdentifyFailure(e.to_string()),
                },
                ApplicationSession::Identified { .. } => {
                    return IdentifyResponse::IdentifyFailure(
                        "Session already identified".to_string(),
                    );
                }
            },
            Session::Management(management_session) => match management_session {
                ManagementSession::Anonymous { .. } => match context
                    .sessions_manager
                    .identify_management_session(session_id, &identity.id)
                    .await
                {
                    Ok(session) => session,
                    Err(e) => return IdentifyResponse::IdentifyFailure(e.to_string()),
                },
                ManagementSession::Identified { .. } => {
                    return IdentifyResponse::IdentifyFailure(
                        "Session already identified".to_string(),
                    );
                }
            },
        };

        // Update context with new session
        context.session = session;

        IdentifyResponse::IdentifySuccess(identity)
    }
}
