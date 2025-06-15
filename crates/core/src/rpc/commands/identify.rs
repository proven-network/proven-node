use async_trait::async_trait;
use proven_identity::Identity;
use serde::{Deserialize, Serialize};

use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

type PasskeyPrfPublicKey = String;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IdentifyCommand {
    pub passkey_prf_public_key: PasskeyPrfPublicKey,
    pub session_id_signature: String,
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
        match context
            .identity_manager
            .get_identity_by_passkey_prf_public_key(&self.passkey_prf_public_key)
            .await
        {
            Ok(Some(identity)) => IdentifyResponse::IdentifySuccess(identity),
            Ok(None) => IdentifyResponse::IdentifyFailure("Identity not found".to_string()),
            Err(e) => IdentifyResponse::IdentifyFailure(e.to_string()),
        }
    }
}
