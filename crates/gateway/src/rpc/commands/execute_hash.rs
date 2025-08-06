use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

use async_trait::async_trait;
use proven_runtime::{ExecutionRequest, ExecutionResult, HandlerSpecifier};
use proven_sessions::{ApplicationSession, Session};
use serde::{Deserialize, Serialize};

type Args = Vec<serde_json::Value>;
type CodePackageHash = String;
type HandlerSpecifierString = String;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecuteHashCommand {
    pub args: Args,
    pub handler_specifier: HandlerSpecifierString,
    pub module_hash: CodePackageHash, // Now represents CodePackage hash from previous Execute call
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "result", content = "data")]
#[allow(clippy::large_enum_variant)]
pub enum ExecuteHashResponse {
    #[serde(rename = "failure")]
    Failure(String),

    #[serde(rename = "error")]
    HashUnknown,

    #[serde(rename = "success")]
    Success(ExecutionResult),
}

#[async_trait]
impl RpcCommand for ExecuteHashCommand {
    type Response = ExecuteHashResponse;

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
        let Ok(handler_specifier) = HandlerSpecifier::parse(&self.handler_specifier) else {
            return ExecuteHashResponse::Failure("Invalid handler specifier".to_string());
        };

        let execution_request = match &context.session {
            Session::Application(app_session) => match app_session {
                ApplicationSession::Anonymous { application_id, .. } => ExecutionRequest::Rpc {
                    application_id: *application_id,
                    args: self.args.clone(),
                    handler_specifier,
                },
                ApplicationSession::Identified {
                    application_id,
                    identity_id,
                    ..
                } => match context.identity_manager.get_identity(identity_id).await {
                    Ok(Some(identity)) => ExecutionRequest::RpcWithIdentity {
                        application_id: *application_id,
                        args: self.args.clone(),
                        handler_specifier,
                        identity,
                    },
                    Ok(None) => {
                        return ExecuteHashResponse::Failure("Identity not found".to_string());
                    }
                    Err(e) => return ExecuteHashResponse::Failure(format!("{e:?}")),
                },
            },
            Session::Management(_) => {
                return ExecuteHashResponse::Failure(
                    "Execute hash command not available in management context".to_string(),
                );
            }
        };

        match context
            .runtime_pool_manager
            .execute_prehashed(self.module_hash.clone(), execution_request)
            .await
        {
            Ok(result) => ExecuteHashResponse::Success(result),
            Err(proven_runtime::Error::HashUnknown) => ExecuteHashResponse::HashUnknown,
            Err(e) => ExecuteHashResponse::Failure(format!("{e:?}")),
        }
    }
}
