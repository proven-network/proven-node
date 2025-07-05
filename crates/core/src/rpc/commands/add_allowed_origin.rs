use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

use async_trait::async_trait;
use proven_sessions::Session;
use proven_util::Origin;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Command to add an allowed origin to an application.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AddAllowedOriginCommand {
    /// The unique identifier of the application.
    pub application_id: Uuid,

    /// The origin to add to the allowed origins.
    pub origin: Origin,
}

/// Response to an add allowed origin command.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "result", content = "data")]
pub enum AddAllowedOriginResponse {
    /// A failure to add an allowed origin.
    #[serde(rename = "failure")]
    AddAllowedOriginFailure(String),

    /// A success to add an allowed origin.
    #[serde(rename = "success")]
    AddAllowedOriginSuccess,
}

#[async_trait]
impl RpcCommand for AddAllowedOriginCommand {
    type Response = AddAllowedOriginResponse;

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
        let identity_id = match &context.session {
            Session::Application(app_session) => match app_session.identity_id() {
                Some(id) => *id,
                None => {
                    return AddAllowedOriginResponse::AddAllowedOriginFailure(
                        "must be signed in to add allowed origin".to_string(),
                    );
                }
            },
            Session::Management(mgmt_session) => match mgmt_session.identity_id() {
                Some(id) => *id,
                None => {
                    return AddAllowedOriginResponse::AddAllowedOriginFailure(
                        "must be signed in to add allowed origin".to_string(),
                    );
                }
            },
        };

        // Get the application to check ownership
        let application = match context
            .application_manager
            .get_application(&self.application_id)
            .await
        {
            Ok(Some(app)) => app,
            Ok(None) => {
                return AddAllowedOriginResponse::AddAllowedOriginFailure(
                    "Application not found".to_string(),
                );
            }
            Err(e) => {
                return AddAllowedOriginResponse::AddAllowedOriginFailure(e.to_string());
            }
        };

        // Check if the user is the owner
        if application.owner_id != identity_id {
            return AddAllowedOriginResponse::AddAllowedOriginFailure(
                "Only the application owner can add allowed origins".to_string(),
            );
        }

        match context
            .application_manager
            .add_allowed_origin(&self.application_id, &self.origin)
            .await
        {
            Ok(()) => AddAllowedOriginResponse::AddAllowedOriginSuccess,
            Err(e) => AddAllowedOriginResponse::AddAllowedOriginFailure(e.to_string()),
        }
    }
}
