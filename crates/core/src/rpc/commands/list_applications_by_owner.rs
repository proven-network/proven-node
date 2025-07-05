use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

use async_trait::async_trait;
use proven_applications::Application;
use proven_sessions::Session;
use serde::{Deserialize, Serialize};

/// Command to list applications owned by the current user.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ListApplicationsByOwnerCommand;

/// Response to a list applications by owner command.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "result", content = "data")]
pub enum ListApplicationsByOwnerResponse {
    /// A failure to list applications.
    #[serde(rename = "failure")]
    ListApplicationsByOwnerFailure(String),

    /// A success to list applications.
    #[serde(rename = "success")]
    ListApplicationsByOwnerSuccess(Vec<Application>),
}

#[async_trait]
impl RpcCommand for ListApplicationsByOwnerCommand {
    type Response = ListApplicationsByOwnerResponse;

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
                    return ListApplicationsByOwnerResponse::ListApplicationsByOwnerFailure(
                        "must be signed in to list applications".to_string(),
                    );
                }
            },
            Session::Management(mgmt_session) => match mgmt_session.identity_id() {
                Some(id) => *id,
                None => {
                    return ListApplicationsByOwnerResponse::ListApplicationsByOwnerFailure(
                        "must be signed in to list applications".to_string(),
                    );
                }
            },
        };

        match context
            .application_manager
            .list_applications_by_owner(&identity_id)
            .await
        {
            Ok(applications) => ListApplicationsByOwnerResponse::ListApplicationsByOwnerSuccess(applications),
            Err(e) => ListApplicationsByOwnerResponse::ListApplicationsByOwnerFailure(e.to_string()),
        }
    }
}