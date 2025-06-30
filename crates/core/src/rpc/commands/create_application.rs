use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

use async_trait::async_trait;
use proven_applications::{Application, CreateApplicationOptions};
use proven_sessions::Session;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CreateApplicationCommand;

#[derive(Debug, Serialize)]
#[serde(tag = "result", content = "data")]
pub enum CreateApplicationResponse {
    #[serde(rename = "failure")]
    CreateApplicationFailure(String),

    #[serde(rename = "success")]
    CreateApplicationSuccess(Application),
}

#[async_trait]
impl RpcCommand for CreateApplicationCommand {
    type Response = CreateApplicationResponse;

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
                    return CreateApplicationResponse::CreateApplicationFailure(
                        "must be signed in to create an application".to_string(),
                    );
                }
            },
            Session::Management(mgmt_session) => match mgmt_session.identity_id() {
                Some(id) => *id,
                None => {
                    return CreateApplicationResponse::CreateApplicationFailure(
                        "must be signed in to create an application".to_string(),
                    );
                }
            },
        };

        let application = match context
            .application_manager
            .create_application(&CreateApplicationOptions {
                owner_identity_id: identity_id,
            })
            .await
        {
            Ok(application) => application,
            Err(e) => return CreateApplicationResponse::CreateApplicationFailure(e.to_string()),
        };

        CreateApplicationResponse::CreateApplicationSuccess(application)
    }
}
