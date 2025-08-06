use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

use async_trait::async_trait;
use proven_sessions::Session;
use serde::{Deserialize, Serialize};

/// Command to anonymize a session.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AnonymizeCommand;

/// Response to an anonymize command.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "result", content = "data")]
pub enum AnonymizeResponse {
    /// A failure to anonymize the session.
    #[serde(rename = "failure")]
    AnonymizeFailure(String),

    /// A success to anonymize the session.
    #[serde(rename = "success")]
    AnonymizeSuccess,
}

#[async_trait]
impl RpcCommand for AnonymizeCommand {
    type Response = AnonymizeResponse;

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
        let session = match &context.session {
            Session::Application(app_session) => {
                match context
                    .sessions_manager
                    .anonymize_session(app_session.application_id(), context.session.session_id())
                    .await
                {
                    Ok(session) => session,
                    Err(e) => return AnonymizeResponse::AnonymizeFailure(e.to_string()),
                }
            }
            Session::Management(_) => {
                match context
                    .sessions_manager
                    .anonymize_management_session(context.session.session_id())
                    .await
                {
                    Ok(session) => session,
                    Err(e) => return AnonymizeResponse::AnonymizeFailure(e.to_string()),
                }
            }
        };

        // Update context with new session
        context.session = session;

        AnonymizeResponse::AnonymizeSuccess
    }
}
