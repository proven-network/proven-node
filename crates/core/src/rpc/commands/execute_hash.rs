use async_trait::async_trait;
use proven_sessions::Session;
use serde::{Deserialize, Serialize};

use proven_runtime::{ExecutionRequest, ExecutionResult, HandlerSpecifier};

use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

type Args = Vec<serde_json::Value>;
type ModuleHash = String;
type HandlerSpecifierString = String;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecuteHashCommand {
    pub args: Args,
    pub handler_specifier: HandlerSpecifierString,
    pub module_hash: ModuleHash,
}

#[derive(Debug, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum ExecuteHashResponse {
    BadHandlerSpecifier,
    Failure(String),
    HashUnknown,
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
            return ExecuteHashResponse::BadHandlerSpecifier;
        };

        let execution_request = match context.session {
            Session::Anonymous { .. } => ExecutionRequest::Rpc {
                application_id: context.application_id,
                args: self.args.clone(),
                handler_specifier,
            },
            Session::Identified { identity_id, .. } => {
                match context.identity_manager.get_identity(identity_id).await {
                    Ok(Some(identity)) => ExecutionRequest::RpcWithIdentity {
                        application_id: context.application_id,
                        args: self.args.clone(),
                        handler_specifier,
                        identity,
                    },
                    Ok(None) => {
                        return ExecuteHashResponse::Failure("Identity not found".to_string());
                    }
                    Err(e) => return ExecuteHashResponse::Failure(format!("{e:?}")),
                }
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
