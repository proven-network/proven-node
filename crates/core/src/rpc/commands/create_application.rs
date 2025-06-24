use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

use async_trait::async_trait;
use proven_applications::{Application, CreateApplicationOptions};
use proven_identity::Session;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CreateApplicationCommand;

#[derive(Debug, Serialize)]
pub enum CreateApplicationResponse {
    CreateApplicationFailure(String),
    CreateApplicationSuccess(Application),
}

#[async_trait]
impl RpcCommand for CreateApplicationCommand {
    type Response = CreateApplicationResponse;

    async fn execute<AM, IM, RM>(&self, context: &mut RpcContext<AM, IM, RM>) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        IM: proven_identity::IdentityManagement,
        RM: proven_runtime::RuntimePoolManagement,
    {
        let identity_id = match context.session {
            Session::Anonymous { .. } => {
                return CreateApplicationResponse::CreateApplicationFailure(
                    "must be signed in to create an application".to_string(),
                );
            }
            Session::Identified { identity_id, .. } => identity_id,
        };

        let application = match context
            .application_manager
            .create_application(CreateApplicationOptions {
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
