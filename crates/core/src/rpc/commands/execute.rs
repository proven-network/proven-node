use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

use async_trait::async_trait;
use proven_code_package::CodePackage;
use proven_runtime::{ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader};
use proven_sessions::{ApplicationSession, Session};
use serde::{Deserialize, Serialize};

type Args = Vec<serde_json::Value>;
type Module = String;
type HandlerSpecifierString = String;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecuteCommand {
    pub args: Args,
    pub handler_specifier: HandlerSpecifierString,
    pub module: Module,
}

#[derive(Debug, Serialize)]
#[serde(tag = "result", content = "data")]
#[allow(clippy::large_enum_variant)]
pub enum ExecuteResponse {
    #[serde(rename = "failure")]
    Failure(String),

    #[serde(rename = "success")]
    Success(ExecutionResult),
}

#[async_trait]
impl RpcCommand for ExecuteCommand {
    type Response = ExecuteResponse;

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
            return ExecuteResponse::Failure("Invalid handler specifier".to_string());
        };

        let execution_request = match &context.session {
            Session::Application(app_session) => {
                let Some(application_id) = context.application_id() else {
                    return ExecuteResponse::Failure("No application ID available".to_string());
                };

                match app_session {
                    ApplicationSession::Anonymous { .. } => ExecutionRequest::Rpc {
                        application_id,
                        args: self.args.clone(),
                        handler_specifier,
                    },
                    ApplicationSession::Identified { identity_id, .. } => {
                        match context.identity_manager.get_identity(identity_id).await {
                            Ok(Some(identity)) => ExecutionRequest::RpcWithIdentity {
                                application_id,
                                args: self.args.clone(),
                                handler_specifier,
                                identity,
                            },
                            Ok(None) => {
                                return ExecuteResponse::Failure("Identity not found".to_string());
                            }
                            Err(e) => return ExecuteResponse::Failure(format!("{e:?}")),
                        }
                    }
                }
            }
            Session::Management(_) => {
                return ExecuteResponse::Failure(
                    "Execute command not available in management context".to_string(),
                );
            }
        };

        match context
            .runtime_pool_manager
            .execute(
                ModuleLoader::new(CodePackage::from_str(&self.module).unwrap()),
                execution_request,
            )
            .await
        {
            Ok(result) => ExecuteResponse::Success(result),
            Err(e) => ExecuteResponse::Failure(format!("{e:?}")),
        }
    }
}
