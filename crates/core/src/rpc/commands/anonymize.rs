use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AnonymizeCommand;

#[derive(Debug, Serialize)]
pub enum AnonymizeResponse {
    AnonymizeFailure(String),
    AnonymizeSuccess,
}

#[async_trait]
impl RpcCommand for AnonymizeCommand {
    type Response = AnonymizeResponse;

    async fn execute<AM, IM, RM>(&self, context: &mut RpcContext<AM, IM, RM>) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        IM: proven_identity::IdentityManagement,
        RM: proven_runtime::RuntimePoolManagement,
    {
        let session = match context
            .identity_manager
            .anonymize_session(&context.application_id, &context.session.session_id())
            .await
        {
            Ok(session) => session,
            Err(e) => return AnonymizeResponse::AnonymizeFailure(e.to_string()),
        };

        // Update context with new session
        context.session = session;

        AnonymizeResponse::AnonymizeSuccess
    }
}
